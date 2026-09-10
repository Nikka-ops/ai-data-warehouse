# -*- coding: utf-8 -*-
"""
实时 AI 风控算子

把 LLM 判断嵌进 Flink 流处理管道，对交易明细做实时风险研判 ——
而不是把数据先落库、再由外部脚本事后轮询分析。二者的区别是本质的：
前者能在下单当下拦截，后者只能事后复盘。

设计上有三个必须讲清楚的点，也是这个算子和「直接每条调 LLM」的分界：

1. 规则先筛，命中才调 LLM
   绝大多数订单是正常的。如果每条都调 LLM，既慢又贵，还把作业稳定性
   绑死在一个外部服务上。所以先用毫秒级的规则过一遍（价格异常、
   金额畸高、疑似刷单节奏），只有可疑订单才进入 LLM 研判。
   正常流量下 LLM 调用量趋近于零。

2. async I/O，不阻塞算子
   LLM 单次响应几百毫秒。如果在算子里同步等待，Flink 这个并行子任务
   就被这一条订单卡住，吞吐直接塌掉。用 Flink 的异步算子把请求并发
   发出去、拿到结果再往下走，一个子任务能同时在途几十个 LLM 请求。

3. 外部依赖必须可降级
   LLM 服务会超时、会限流、会挂。任何一种都不能拖垮整条实时链路。
   这里给每个请求设超时，失败则降级成「仅规则结论」继续往下走，
   而不是让算子抛异常触发作业重启。

运行（在 Flink 集群里以 PyFlink 提交）：
    flink run -py flink/ai_risk/risk_operator.py

依赖：apache-flink>=1.18, aiohttp
"""

import os
import json
import asyncio
import logging
from datetime import datetime

from pyflink.common import Types, Configuration
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import AsyncFunction, RuntimeContext

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ai_risk')


# ── 规则预筛：毫秒级，不调 LLM ────────────────────────────────

# 单笔金额畸高阈值（元）。真实场景应按品类动态设定，这里给个保守值。
HIGH_AMOUNT = 20000.0
# 单笔件数畸高，疑似批量刷单
HIGH_QTY = 20


def pre_screen(order: dict) -> tuple[bool, list[str]]:
    """
    规则预筛。返回（是否可疑, 命中的规则列表）。

    这一步替 LLM 挡掉绝大部分正常订单。命中任意一条才需要 LLM 深研。
    """
    hits = []

    # 1. DWD 已经打好的价格异常标记（成交价严重偏离标价）
    if order.get('is_price_abnormal') == 1:
        hits.append('成交价严重偏离商品标价')

    # 2. 单笔金额畸高
    amount = float(order.get('split_amount') or 0)
    if amount >= HIGH_AMOUNT:
        hits.append(f'单笔金额畸高 {amount:.0f} 元')

    # 3. 单笔件数畸高
    qty = int(order.get('sku_num') or 0)
    if qty >= HIGH_QTY:
        hits.append(f'单笔购买 {qty} 件，疑似批量')

    return (len(hits) > 0, hits)


# ── LLM 研判（仅对预筛命中的订单）──────────────────────────────

RISK_PROMPT = """你是电商交易风控专家。下面是一笔被规则初筛为可疑的订单，
请判断其真实风险等级并给出理由。

订单信息：
- 商品：{sku_name}（{category1_name}）
- 单价：{order_price} 元，数量：{sku_num}，金额：{split_amount} 元
- 收货地区：{region_name}{province_name}
- 规则命中：{rule_hits}

请只输出一行 JSON，不要任何多余文字：
{{"level":"HIGH|MEDIUM|LOW","reason":"一句话理由，不超过30字"}}"""


class LLMRiskJudge(AsyncFunction):
    """
    异步 LLM 研判算子。

    继承 AsyncFunction，Flink 会以 orderedWait/unorderedWait 的方式
    并发调度多个在途请求，单个请求的耗时不阻塞其他订单。
    """

    def __init__(self, api_key: str, base_url: str, model: str,
                 timeout_s: float = 8.0):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout_s = timeout_s
        self._session = None

    def open(self, ctx: RuntimeContext):
        # aiohttp 会话在算子初始化时建一次，复用连接池
        import aiohttp
        self._session = aiohttp.ClientSession()

    def close(self):
        if self._session:
            asyncio.get_event_loop().run_until_complete(self._session.close())

    async def async_invoke(self, order: dict, result_future):
        """对单笔订单做 LLM 研判，结果通过 result_future 回传"""
        suspicious, hits = pre_screen(order)

        # 预筛没命中：直接放行，连 LLM 都不调 —— 这是省钱的关键
        if not suspicious:
            result_future.set_result([self._emit(order, 'PASS', 'LOW',
                                                 '规则未命中', llm_called=0)])
            return

        # 预筛命中：异步调 LLM 深研
        try:
            level, reason = await self._call_llm(order, hits)
            result_future.set_result([self._emit(
                order, 'REVIEW', level, reason, llm_called=1)])
        except Exception as e:
            # LLM 不可用 → 降级：保留规则结论，不让整条流挂掉
            log.warning('LLM 研判失败，降级为规则结论：%s', e)
            result_future.set_result([self._emit(
                order, 'REVIEW', 'MEDIUM',
                f'规则命中待人工复核（LLM 不可用）：{"; ".join(hits)}',
                llm_called=0)])

    async def _call_llm(self, order: dict, hits: list[str]) -> tuple[str, str]:
        prompt = RISK_PROMPT.format(
            sku_name=order.get('sku_name', ''),
            category1_name=order.get('category1_name', ''),
            order_price=order.get('order_price', 0),
            sku_num=order.get('sku_num', 0),
            split_amount=order.get('split_amount', 0),
            region_name=order.get('region_name', ''),
            province_name=order.get('province_name', ''),
            rule_hits='；'.join(hits),
        )
        payload = {
            'model': self.model,
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': 0.1,
            'max_tokens': 100,
        }
        headers = {'Authorization': f'Bearer {self.api_key}'}

        async with self._session.post(
            f'{self.base_url}/chat/completions',
            json=payload, headers=headers,
            timeout=self.timeout_s,
        ) as resp:
            data = await resp.json()
            content = data['choices'][0]['message']['content'].strip()
            # 容错解析：LLM 偶尔会包 ```json
            content = content.replace('```json', '').replace('```', '').strip()
            obj = json.loads(content)
            return obj.get('level', 'MEDIUM'), obj.get('reason', '')[:60]

    @staticmethod
    def _emit(order: dict, action: str, level: str, reason: str,
              llm_called: int) -> dict:
        return {
            'order_id': order.get('order_id'),
            'user_id': order.get('user_id'),
            'sku_name': order.get('sku_name'),
            'split_amount': float(order.get('split_amount') or 0),
            'risk_action': action,       # PASS / REVIEW
            'risk_level': level,         # HIGH / MEDIUM / LOW
            'risk_reason': reason,
            'llm_called': llm_called,
            'judge_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }


# ── 作业装配 ──────────────────────────────────────────────────

def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(30_000)          # 30s，与 SQL 作业一致
    env.set_parallelism(3)

    # 消费 DWD 层的交易明细
    row_type = Types.ROW_NAMED(
        ['order_id', 'user_id', 'sku_name', 'category1_name',
         'order_price', 'sku_num', 'split_amount', 'is_price_abnormal',
         'province_name', 'region_name'],
        [Types.LONG(), Types.LONG(), Types.STRING(), Types.STRING(),
         Types.DOUBLE(), Types.INT(), Types.DOUBLE(), Types.INT(),
         Types.STRING(), Types.STRING()],
    )
    deser = JsonRowDeserializationSchema.builder().type_info(row_type).build()

    source = (KafkaSource.builder()
              .set_bootstrap_servers(os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092'))
              .set_topics('dwd_trade_order')
              .set_group_id('flink_ai_risk')
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(deser)
              .build())

    stream = env.from_source(source, watermark_strategy=None,
                             source_name='dwd_trade_order')

    # Row → dict，方便算子内取字段
    def to_dict(r):
        return {
            'order_id': r['order_id'], 'user_id': r['user_id'],
            'sku_name': r['sku_name'], 'category1_name': r['category1_name'],
            'order_price': r['order_price'], 'sku_num': r['sku_num'],
            'split_amount': r['split_amount'],
            'is_price_abnormal': r['is_price_abnormal'],
            'province_name': r['province_name'], 'region_name': r['region_name'],
        }
    dict_stream = stream.map(to_dict, output_type=Types.PICKLED_BYTE_ARRAY())

    # 异步 LLM 研判算子。
    # unordered_wait：结果不必按输入顺序，谁先回谁先走，吞吐更高；
    # capacity=50：单个子任务最多同时在途 50 个 LLM 请求，超过则反压上游。
    from pyflink.datastream import AsyncDataStream
    judged = AsyncDataStream.unordered_wait(
        dict_stream,
        LLMRiskJudge(
            api_key=os.getenv('DEEPSEEK_API_KEY', ''),
            base_url=os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com'),
            model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
        ),
        timeout=Time.seconds(15),   # 算子级兜底超时，比单请求超时更宽
        capacity=50,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # 只把命中风控的（REVIEW）写下游，PASS 的丢弃，减少写入压力
    risky = judged.filter(lambda x: x['risk_action'] == 'REVIEW')

    # 落 Doris 风控结果表（此处用 print sink 占位，
    # 生产用 flink-doris-connector 的 Python 封装或转 SQL 侧 sink）
    risky.map(lambda x: json.dumps(x, ensure_ascii=False),
              output_type=Types.STRING()).print()

    env.execute('ai-risk-realtime-judge')


if __name__ == '__main__':
    build_job()
