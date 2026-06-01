#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动特征工程 Agent — Auto Feature Engineering
核心能力：
  1. LLM 分析业务数据和现有特征，推荐高价值新特征
  2. 自动生成特征计算 SQL（带 PIT 正确性）
  3. 验证 SQL 可执行性（ClickHouse dry run）
  4. 生成特征 YAML 文件供人工审核后注册

这是 AI 数仓区别于传统数仓的核心：系统主动参与特征发现
"""
import os
import sys
import json
import re
import yaml
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('auto_feature')
_FEATURES_DIR = os.path.join(os.path.dirname(__file__), '..', 'features')
_GENERATED_DIR = os.path.join(_FEATURES_DIR, 'generated')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


_SUGGEST_PROMPT = """你是资深机器学习特征工程师，正在为电商订单数据设计高价值特征。

【当前数据库 Schema】
{schema_info}

【已有特征（避免重复）】
{existing_features}

【业务目标】
{business_goal}

【特征工程原则】
1. Point-in-Time 正确性：特征计算 SQL 必须包含时间过滤条件（event_time <= 某个参考时间）
2. 特征类型：优先考虑聚合特征（count、sum、avg、ratio）、交叉特征、时序特征
3. 业务意义：每个特征需有明确的业务解释
4. 可计算性：SQL 必须能在 ClickHouse 上运行
5. 避免未来泄漏：不使用 event_time 之后的数据

请推荐 5-8 个高价值新特征，输出严格 JSON 数组：
[
  {{
    "name": "feature_snake_case_name",
    "group": "user_behavior | category_stats | seller_stats | temporal_context",
    "type": "INT64 | FLOAT64 | STRING",
    "description": "特征含义（中文，1-2句）",
    "business_value": "对模型/业务的价值（中文，1句）",
    "computation_sql": "SELECT entity_id, CAST(xxx AS Float64) AS feature_value, now() AS feature_time FROM ... WHERE event_time >= now() - INTERVAL N DAY GROUP BY entity_id",
    "online_ttl": 3600,
    "default_value": "0.0",
    "max_staleness_seconds": 1800,
    "tags": ["tag1", "tag2"]
  }}
]

只输出 JSON 数组，不要其他内容。"""


_VALIDATE_PROMPT = """检查以下 ClickHouse SQL 特征计算语句是否有语法问题。
如果有问题，输出修正后的 SQL。如果没问题，原样输出。

SQL:
{sql}

要求：
- 必须有 entity_id 列
- 必须有 feature_value 列（数值型）
- 必须有 feature_time 列（DateTime）
- 时间过滤条件合理

只输出 SQL 语句本身，不要其他内容。"""


class AutoFeatureEngineer:
    """
    AI 驱动的特征工程 Agent
    分析现有数据和特征，自动推荐并生成新特征定义
    """

    def __init__(self):
        self._ch = _get_ch()
        os.makedirs(_GENERATED_DIR, exist_ok=True)

    def _get_schema_info(self) -> str:
        """采集数据库 schema 信息供 LLM 参考"""
        info_parts = []
        tables = [
            ('ods.orders_stream',
             'order_id, customer_id, product_category, seller_id, price, freight_value, order_status, state, city, event_time'),
            ('ods.payments_stream',
             'payment_id, order_id, payment_type, payment_value, installments, event_time'),
            ('dwd.realtime_order_detail',
             'order_id, customer_id, product_category, state, price, total_amount, payment_type, order_status, event_time, is_paid'),
        ]
        for table, cols in tables:
            try:
                sample = self._ch.query(f"SELECT {cols} FROM {table} LIMIT 3").result_rows
                info_parts.append(f"表 {table}：\n  列：{cols}\n  样本：{sample[:2]}")
            except Exception:
                info_parts.append(f"表 {table}：列 {cols}")

        # 数据统计
        try:
            stats = self._ch.query("""
                SELECT count() AS total_orders,
                       count(DISTINCT customer_id) AS unique_customers,
                       count(DISTINCT product_category) AS categories,
                       round(avg(price), 2) AS avg_price,
                       min(event_time) AS earliest,
                       max(event_time) AS latest
                FROM ods.orders_stream
            """).first_row
            if stats:
                info_parts.append(
                    f"数据规模：{stats[0]:,} 条订单，{stats[1]:,} 个用户，"
                    f"{stats[2]} 个品类，均价 R${stats[3]}，"
                    f"时间范围 {str(stats[4])[:10]} ~ {str(stats[5])[:10]}"
                )
        except Exception:
            pass
        return '\n\n'.join(info_parts)

    def _get_existing_features(self) -> str:
        """获取已有特征列表，避免 LLM 生成重复特征"""
        try:
            rows = self._ch.query("""
                SELECT group_name, feature_name, description
                FROM feature_store.feature_definitions
                WHERE is_active = 1
                ORDER BY group_name, feature_name
            """).result_rows
            if rows:
                return '\n'.join(f"  - {r[0]}.{r[1]}: {r[2]}" for r in rows)
        except Exception:
            pass
        return "（暂无已注册特征）"

    @llm_retry
    def suggest_features(self, business_goal: str = '预测用户取消率和GMV价值') -> list[dict]:
        """LLM 推荐新特征"""
        from openai import OpenAI
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=60.0)

        schema_info  = self._get_schema_info()
        existing     = self._get_existing_features()

        resp = client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content': _SUGGEST_PROMPT.format(
                schema_info=schema_info,
                existing_features=existing,
                business_goal=business_goal,
            )}],
            temperature=0.3,
            max_tokens=3000,
        )
        raw = resp.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            raise ValueError(f'LLM 未返回有效 JSON：{raw[:200]}')
        return json.loads(match.group())

    def validate_feature_sql(self, sql: str) -> tuple[bool, str]:
        """通过 ClickHouse EXPLAIN 验证特征 SQL 语法"""
        try:
            self._ch.query(f"EXPLAIN {sql.strip().rstrip(';')} LIMIT 0")
            return True, sql
        except Exception:
            # 尝试让 LLM 修复
            try:
                from openai import OpenAI
                client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=30.0)
                resp = client.chat.completions.create(
                    model=cfg.llm_model,
                    messages=[{'role': 'user', 'content': _VALIDATE_PROMPT.format(sql=sql)}],
                    temperature=0.1, max_tokens=500,
                )
                fixed_sql = resp.choices[0].message.content.strip()
                self._ch.query(f"EXPLAIN {fixed_sql.rstrip(';')} LIMIT 0")
                return True, fixed_sql
            except Exception:
                return False, sql

    def generate_and_save(self, features: list[dict], group_name: str = None) -> str:
        """
        验证特征 SQL，按组生成 YAML 文件保存到 features/generated/
        返回生成的文件路径
        """
        # 按 group 分组
        from collections import defaultdict
        groups: dict = defaultdict(list)
        for feat in features:
            g = feat.get('group', group_name or 'auto_generated')
            groups[g].append(feat)

        saved_files = []
        for g, feats in groups.items():
            validated_feats = []
            for feat in feats:
                sql = feat.get('computation_sql', '')
                if sql:
                    ok, fixed_sql = self.validate_feature_sql(sql)
                    feat['computation_sql'] = fixed_sql
                    feat['_sql_valid'] = ok
                    if ok:
                        log.info('特征 %s.%s SQL 验证通过', g, feat['name'])
                    else:
                        log.warning('特征 %s.%s SQL 未通过验证，已保留供人工修正', g, feat['name'])
                validated_feats.append(feat)

            # 构建 YAML group 结构
            yaml_content = {
                'feature_group': g,
                'entity_key': 'customer_id' if 'user' in g else ('product_category' if 'category' in g else 'entity_id'),
                'description': f'AI 自动生成的特征组（目标：{g}）',
                'owner': 'auto_feature_agent',
                'refresh_schedule': '*/10 * * * *',
                'source_tables': ['ods.orders_stream'],
                'features': [
                    {k: v for k, v in f.items() if k not in ['group', 'business_value', '_sql_valid']}
                    for f in validated_feats
                ],
            }
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            out_path = os.path.join(_GENERATED_DIR, f'{g}_{ts}.yaml')
            with open(out_path, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_content, f, allow_unicode=True, default_flow_style=False, indent=2)
            saved_files.append(out_path)
            log.info('已生成特征文件：%s（%d 个特征）', out_path, len(validated_feats))

        return '\n'.join(saved_files)

    def run(self, business_goal: str = '预测用户取消率和GMV价值') -> dict:
        """完整运行：建议 → 验证 → 保存"""
        log.info('Auto Feature Engineering 启动，目标：%s', business_goal)
        try:
            features = self.suggest_features(business_goal)
            log.info('LLM 推荐 %d 个候选特征', len(features))
        except Exception as e:
            log.error('特征建议失败：%s', e)
            return {'status': 'failed', 'error': str(e)}

        saved = self.generate_and_save(features)
        valid_count = sum(1 for f in features if f.get('_sql_valid', True))
        return {
            'status': 'completed',
            'suggested': len(features),
            'valid_sql': valid_count,
            'saved_files': saved,
            'features': [
                {'name': f['name'], 'group': f.get('group'), 'description': f.get('description'),
                 'business_value': f.get('business_value', '')}
                for f in features
            ],
        }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='AI 自动特征工程')
    parser.add_argument('--goal', default='预测用户订单取消风险和GMV潜力', help='业务目标描述')
    args = parser.parse_args()
    eng = AutoFeatureEngineer()
    result = eng.run(args.goal)
    print(json.dumps(result, ensure_ascii=False, indent=2))
