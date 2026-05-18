# -*- coding: utf-8 -*-
"""
全自动 AI ETL Agent
流程：ODS 数据质量检测 → LLM 生成清洗规则 → 规则持久化 → 规则执行修复 → 审计日志

运行：
  python ai_etl/ai_etl_agent.py              # 单次运行
  python ai_etl/ai_etl_agent.py --loop 60   # 每60秒循环一次
"""

import os, sys, json, uuid, time, re, argparse
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry, ch_retry

log = get_logger('ai_etl')

# DWD 表所有字段（按顺序，用于 INSERT SELECT 构建）
_DWD_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'product_category', 'seller_id',
    'state', 'city', 'price', 'freight_value', 'total_amount',
    'payment_type', 'payment_value', 'order_status',
    'event_time', 'event_date', 'event_hour', 'is_paid', '_ingest_time',
]

# 不参与规则转换的只读字段
_READONLY_FIELDS = {'order_id', 'event_time', 'event_date', 'event_hour', '_ingest_time'}


# ══════════════════════════════════════════════════════════════
# ClickHouse 连接
# ══════════════════════════════════════════════════════════════

@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


# ══════════════════════════════════════════════════════════════
# DataProfiler：ODS 数据质量检测
# ══════════════════════════════════════════════════════════════

class DataProfiler:
    """扫描 ODS 和 DWD 层数据，生成数据质量报告"""

    VALID_ORDER_STATUS = {
        'created', 'approved', 'invoiced', 'processing',
        'shipped', 'delivered', 'canceled', 'unavailable',
    }
    VALID_PAYMENT_TYPES = {'credit_card', 'boleto', 'voucher', 'debit_card', ''}

    def __init__(self, ch):
        self.ch = ch

    def profile(self, lookback_minutes: int = 5) -> dict[str, Any]:
        """
        检测最近 lookback_minutes 分钟内 ODS 和 DWD 数据的质量问题。
        返回结构化质量报告。
        """
        log.info('开始数据质量检测，回溯 %d 分钟', lookback_minutes)
        issues = []
        stats = {}

        try:
            # ── 1. ODS 基础统计 ─────────────────────────────────
            ods_stats = self.ch.query(f"""
                SELECT
                    count()                                             AS total,
                    countIf(price < 0)                                  AS neg_price,
                    countIf(price > 10000)                              AS extreme_price,
                    countIf(freight_value < 0)                          AS neg_freight,
                    countIf(product_category = '' OR product_category IS NULL) AS null_category,
                    countIf(order_status NOT IN {tuple(self.VALID_ORDER_STATUS)}) AS invalid_status,
                    countIf(NOT match(customer_id, '^C\\\\d{{5}}$'))    AS bad_customer_id,
                    countIf(NOT match(seller_id,   '^S\\\\d{{4}}$'))    AS bad_seller_id,
                    countIf(order_status = 'canceled')                  AS canceled,
                    round(avg(price), 2)                                AS avg_price,
                    round(max(price), 2)                                AS max_price
                FROM ods.orders_stream
                WHERE event_time >= now() - INTERVAL {lookback_minutes} MINUTE
            """).result_rows[0]

            cols = ['total', 'neg_price', 'extreme_price', 'neg_freight',
                    'null_category', 'invalid_status', 'bad_customer_id', 'bad_seller_id',
                    'canceled', 'avg_price', 'max_price']
            ods = dict(zip(cols, ods_stats))
            stats['ods'] = ods
            total = ods['total'] or 1

            # ── 2. 判断是否为问题 ────────────────────────────────
            if ods['neg_price'] > 0:
                issues.append({
                    'field': 'price', 'type': 'value_range',
                    'count': ods['neg_price'], 'rate': round(ods['neg_price'] / total, 4),
                    'description': f"存在 {ods['neg_price']} 笔负价格订单（占 {ods['neg_price']/total:.1%}）",
                    'sample_condition': 'price < 0',
                })
            if ods['extreme_price'] > 0:
                issues.append({
                    'field': 'price', 'type': 'value_extreme',
                    'count': ods['extreme_price'], 'rate': round(ods['extreme_price'] / total, 4),
                    'description': f"存在 {ods['extreme_price']} 笔超高价订单（price > R$10000，占 {ods['extreme_price']/total:.1%}）",
                    'sample_condition': 'price > 10000',
                })
            if ods['neg_freight'] > 0:
                issues.append({
                    'field': 'freight_value', 'type': 'value_range',
                    'count': ods['neg_freight'], 'rate': round(ods['neg_freight'] / total, 4),
                    'description': f"存在 {ods['neg_freight']} 笔负运费（占 {ods['neg_freight']/total:.1%}）",
                    'sample_condition': 'freight_value < 0',
                })
            if ods['null_category'] > 0:
                issues.append({
                    'field': 'product_category', 'type': 'null_value',
                    'count': ods['null_category'], 'rate': round(ods['null_category'] / total, 4),
                    'description': f"存在 {ods['null_category']} 条品类为空的记录（占 {ods['null_category']/total:.1%}）",
                    'sample_condition': "product_category = '' OR product_category IS NULL",
                })
            if ods['invalid_status'] > 0:
                issues.append({
                    'field': 'order_status', 'type': 'invalid_enum',
                    'count': ods['invalid_status'], 'rate': round(ods['invalid_status'] / total, 4),
                    'description': f"存在 {ods['invalid_status']} 条非法 order_status（不在枚举值范围内）",
                    'sample_condition': f"order_status NOT IN {tuple(self.VALID_ORDER_STATUS)}",
                })
            if ods['bad_customer_id'] > 0:
                issues.append({
                    'field': 'customer_id', 'type': 'format_error',
                    'count': ods['bad_customer_id'], 'rate': round(ods['bad_customer_id'] / total, 4),
                    'description': f"存在 {ods['bad_customer_id']} 条 customer_id 格式错误（应为 C+5位数字）",
                    'sample_condition': "NOT match(customer_id, '^C\\\\d{5}$')",
                })

            # ── 3. DWD 重复订单检测 ──────────────────────────────
            dwd_dup = self.ch.query(f"""
                SELECT count() AS dup_cnt
                FROM (
                    SELECT order_id, count() AS cnt
                    FROM dwd.realtime_order_detail
                    WHERE _ingest_time >= now() - INTERVAL {lookback_minutes + 2} MINUTE
                    GROUP BY order_id
                    HAVING cnt > 1
                )
            """).result_rows[0][0]

            stats['dwd_duplicates'] = dwd_dup
            if dwd_dup > 0:
                issues.append({
                    'field': 'order_id', 'type': 'duplicate',
                    'count': dwd_dup, 'rate': 0,
                    'description': f"DWD 层存在 {dwd_dup} 个重复 order_id（JOIN 重复写入）",
                    'sample_condition': None,
                })

            # ── 4. 取消率 ─────────────────────────────────────
            cancel_rate = ods['canceled'] / total
            stats['cancel_rate'] = round(cancel_rate, 4)
            if cancel_rate > 0.15:
                issues.append({
                    'field': 'order_status', 'type': 'business_anomaly',
                    'count': ods['canceled'], 'rate': round(cancel_rate, 4),
                    'description': f"取消率过高：{cancel_rate:.1%}（{ods['canceled']}/{total} 单）",
                    'sample_condition': None,
                })

        except Exception as e:
            log.error('数据质量检测失败：%s', e)
            return {'issues': [], 'stats': {}, 'error': str(e), 'scanned_at': datetime.now().isoformat()}

        # ── 5. 计算质量评分（100分制）───────────────────────────
        issue_fields = len({i['field'] for i in issues if i.get('rate', 0) > 0})
        quality_score = max(0.0, 100.0 - issue_fields * 15 - sum(
            min(i.get('rate', 0) * 100, 20) for i in issues
        ))

        report = {
            'scanned_at': datetime.now().isoformat(),
            'lookback_minutes': lookback_minutes,
            'total_records': total,
            'issues': issues,
            'stats': stats,
            'quality_score': round(quality_score, 1),
            'has_fixable_issues': any(
                i['type'] in ('value_range', 'value_extreme', 'null_value', 'invalid_enum')
                for i in issues
            ),
        }
        log.info('质量检测完成：%d 条记录，%d 个问题，质量分 %.1f',
                 total, len(issues), quality_score)
        return report


# ══════════════════════════════════════════════════════════════
# RuleEngine：规则加载 & 执行
# ══════════════════════════════════════════════════════════════

class RuleEngine:
    """从 ClickHouse 加载清洗规则，并将其应用到 DWD 层数据"""

    def __init__(self, ch):
        self.ch = ch

    def load_rules(self) -> list[dict]:
        """加载所有启用的清洗规则，按优先级排序"""
        try:
            rows = self.ch.query("""
                SELECT rule_id, rule_name, rule_type, field_name,
                       condition_sql, transform_expr, priority, ai_reason
                FROM stream.etl_rules
                WHERE enabled = 1
                ORDER BY priority ASC, created_at ASC
            """).result_rows
            rules = [
                {
                    'rule_id': r[0], 'rule_name': r[1], 'rule_type': r[2],
                    'field_name': r[3], 'condition_sql': r[4],
                    'transform_expr': r[5], 'priority': r[6], 'ai_reason': r[7],
                }
                for r in rows
            ]
            log.info('加载到 %d 条启用规则', len(rules))
            return rules
        except Exception as e:
            log.error('加载规则失败：%s', e)
            return []

    def save_rules(self, new_rules: list[dict]) -> int:
        """将 LLM 生成的新规则批量写入 stream.etl_rules"""
        if not new_rules:
            return 0
        saved = 0
        now = datetime.now()
        for r in new_rules:
            try:
                rule_id = str(uuid.uuid4())
                self.ch.insert(
                    'stream.etl_rules',
                    [[
                        rule_id, r.get('rule_name', rule_id), r.get('rule_type', 'custom'),
                        r.get('target_table', 'dwd.realtime_order_detail'),
                        r.get('field_name', ''), r.get('condition_sql', ''),
                        r.get('transform_expr', ''), r.get('priority', 50),
                        1, 'ai', r.get('ai_reason', ''), 0, now, now,
                    ]],
                    column_names=['rule_id', 'rule_name', 'rule_type', 'target_table',
                                  'field_name', 'condition_sql', 'transform_expr',
                                  'priority', 'enabled', 'generated_by', 'ai_reason',
                                  'hit_count', 'created_at', 'updated_at'],
                )
                saved += 1
                log.info('新规则已保存：[%s] %s', r.get('rule_type'), r.get('rule_name'))
            except Exception as e:
                log.error('保存规则失败：%s，错误：%s', r.get('rule_name'), e)
        return saved

    def apply(self, rules: list[dict], lookback_minutes: int = 3) -> dict[str, int]:
        """
        将规则应用到最近写入 DWD 的记录。
        通过 INSERT SELECT + transform_expr 覆盖脏数据，
        ReplacingMergeTree 会按 order_id + 新的 _ingest_time 完成去重。
        """
        if not rules:
            return {'records_fixed': 0, 'rules_applied': 0}

        # 按字段分组规则，每个字段最多用一个 CASE WHEN 链
        field_rules: dict[str, list[dict]] = {}
        for r in rules:
            f = r['field_name']
            if f in _READONLY_FIELDS or not r.get('condition_sql') or not r.get('transform_expr'):
                continue
            field_rules.setdefault(f, []).append(r)

        if not field_rules:
            return {'records_fixed': 0, 'rules_applied': 0}

        # 构建 WHERE 子句（有任意规则命中的记录）
        all_conditions = ' OR '.join(
            f'({r["condition_sql"]})'
            for rules_for_field in field_rules.values()
            for r in rules_for_field
        )

        # 为每个字段构建转换表达式
        select_exprs = []
        rules_applied = 0
        for col in _DWD_COLUMNS:
            if col == '_ingest_time':
                select_exprs.append('now() AS _ingest_time')
            elif col in field_rules:
                # 构建 CASE WHEN 链
                expr = col
                for r in field_rules[col]:
                    expr = f"if({r['condition_sql']}, ({r['transform_expr']}), ({expr}))"
                    rules_applied += 1
                select_exprs.append(f"{expr} AS {col}")
            else:
                select_exprs.append(col)

        fix_sql = f"""
        INSERT INTO dwd.realtime_order_detail
        SELECT {', '.join(select_exprs)}
        FROM dwd.realtime_order_detail
        WHERE _ingest_time >= now() - INTERVAL {lookback_minutes} MINUTE
          AND ({all_conditions})
        """

        try:
            # 先统计受影响行数
            count_sql = f"""
            SELECT count() FROM dwd.realtime_order_detail
            WHERE _ingest_time >= now() - INTERVAL {lookback_minutes} MINUTE
              AND ({all_conditions})
            """
            records_fixed = self.ch.query(count_sql).result_rows[0][0]

            if records_fixed > 0:
                self.ch.command(fix_sql)
                log.info('规则执行完成：修复 %d 条记录，应用 %d 条规则',
                         records_fixed, rules_applied)

                # 更新命中次数
                self._update_hit_counts(field_rules)
            else:
                log.info('当前数据无需修复（所有规则均未命中）')

            return {'records_fixed': records_fixed, 'rules_applied': rules_applied}

        except Exception as e:
            log.error('规则执行失败：%s', e)
            return {'records_fixed': 0, 'rules_applied': 0, 'error': str(e)}

    def _update_hit_counts(self, field_rules: dict):
        """更新规则命中次数（best-effort，失败不影响主流程）"""
        try:
            for rules_list in field_rules.values():
                for r in rules_list:
                    self.ch.command(f"""
                        ALTER TABLE stream.etl_rules
                        UPDATE hit_count = hit_count + 1,
                               updated_at = now()
                        WHERE rule_id = '{r['rule_id']}'
                    """)
        except Exception as e:
            log.warning('更新命中次数失败（非关键）：%s', e)

    def get_existing_rule_names(self) -> set[str]:
        """获取已存在的规则名称，避免重复生成"""
        try:
            rows = self.ch.query("SELECT rule_name FROM stream.etl_rules").result_rows
            return {r[0] for r in rows}
        except Exception:
            return set()


# ══════════════════════════════════════════════════════════════
# AIRuleGenerator：LLM 规则生成
# ══════════════════════════════════════════════════════════════

_RULE_GEN_PROMPT = """你是数据工程专家，负责为 ClickHouse 数据仓库生成数据清洗规则。

【数据质量报告】
{quality_report}

【已存在的规则（不要重复生成）】
{existing_rules}

【任务】
根据质量报告中发现的问题，生成清洗规则。每条规则必须严格符合以下 JSON 格式：

```json
[
  {{
    "rule_name": "fix_negative_price",
    "rule_type": "clamp_value",
    "field_name": "price",
    "condition_sql": "price < 0",
    "transform_expr": "greatest(price, 0.0)",
    "priority": 10,
    "ai_reason": "存在负价格，将其修正为0以保证数据合法性"
  }}
]
```

【规则类型说明】
- fill_null: 填充空值，transform_expr 使用 coalesce(field, 'default')
- clamp_value: 修正数值范围，transform_expr 使用 greatest/least/if 函数
- replace_invalid: 替换非法枚举值，transform_expr 使用 if/multiIf 条件
- custom: 自定义转换，transform_expr 为任意合法的 ClickHouse 表达式

【ClickHouse 表达式规范】
- 字段引用直接用字段名（如 price、freight_value）
- 函数：greatest(), least(), if(cond, true_val, false_val), coalesce(), multiIf()
- 字符串用单引号

【约束】
- condition_sql 和 transform_expr 必须是合法的 ClickHouse 语法
- 不允许修改只读字段：order_id, event_time, event_date, event_hour, _ingest_time
- 只针对报告中实际存在的问题生成规则
- 每个问题最多生成1条规则
- 只返回 JSON 数组，不要任何额外文字

如果没有需要修复的问题，返回空数组：[]
"""


class AIRuleGenerator:
    """调用 LLM 分析质量报告并生成清洗规则"""

    def __init__(self):
        from openai import OpenAI
        self.client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=30.0)

    @llm_retry
    def generate(self, quality_report: dict, existing_rule_names: set[str]) -> list[dict]:
        """根据质量报告生成新的清洗规则"""
        if not quality_report.get('has_fixable_issues'):
            log.info('无需 LLM 介入：没有可修复的质量问题')
            return []

        fixable_issues = [
            i for i in quality_report.get('issues', [])
            if i['type'] in ('value_range', 'value_extreme', 'null_value', 'invalid_enum', 'format_error')
        ]
        if not fixable_issues:
            return []

        prompt = _RULE_GEN_PROMPT.format(
            quality_report=json.dumps({
                'issues': fixable_issues,
                'stats': quality_report.get('stats', {}),
                'quality_score': quality_report.get('quality_score'),
            }, ensure_ascii=False, indent=2),
            existing_rules=json.dumps(sorted(existing_rule_names), ensure_ascii=False),
        )

        log.info('调用 LLM 生成清洗规则（%d 个问题）...', len(fixable_issues))
        resp = self.client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.1,
            max_tokens=1000,
        )
        raw = resp.choices[0].message.content.strip()

        # 提取 JSON 数组
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            log.warning('LLM 未返回有效 JSON，原始输出：%s', raw[:200])
            return []

        try:
            rules = json.loads(match.group())
            if not isinstance(rules, list):
                return []

            # 过滤掉已存在、重名、字段非法的规则
            valid = []
            for r in rules:
                if r.get('rule_name') in existing_rule_names:
                    log.debug('跳过重复规则：%s', r.get('rule_name'))
                    continue
                if r.get('field_name') in _READONLY_FIELDS:
                    log.warning('跳过只读字段规则：%s', r.get('field_name'))
                    continue
                if not r.get('condition_sql') or not r.get('transform_expr'):
                    log.warning('跳过不完整规则：%s', r.get('rule_name'))
                    continue
                valid.append(r)

            log.info('LLM 生成 %d 条有效新规则', len(valid))
            return valid

        except json.JSONDecodeError as e:
            log.error('规则 JSON 解析失败：%s，原始：%s', e, raw[:300])
            return []


# ══════════════════════════════════════════════════════════════
# AuditLogger：审计日志
# ══════════════════════════════════════════════════════════════

class AuditLogger:
    def __init__(self, ch):
        self.ch = ch

    def write(self, window_start: datetime, window_end: datetime,
              profile: dict, fix_result: dict, new_rules: int, status: str):
        try:
            issues = profile.get('issues', [])
            summary_parts = []
            if issues:
                summary_parts.append('问题：' + '；'.join(i['description'][:40] for i in issues[:3]))
            if fix_result.get('records_fixed', 0) > 0:
                summary_parts.append(f"已修复 {fix_result['records_fixed']} 条记录")
            if new_rules > 0:
                summary_parts.append(f"新增 {new_rules} 条规则")

            self.ch.insert(
                'stream.etl_audit_log',
                [[
                    str(uuid.uuid4()), datetime.now(),
                    window_start, window_end,
                    profile.get('total_records', 0),
                    len(issues),
                    fix_result.get('rules_applied', 0),
                    fix_result.get('records_fixed', 0),
                    new_rules,
                    profile.get('quality_score', 100.0),
                    status,
                    '、'.join(summary_parts) or '数据质量正常，无需处理',
                    datetime.now(),
                ]],
                column_names=['log_id', 'run_time', 'window_start', 'window_end',
                              'records_scanned', 'issues_found', 'rules_applied',
                              'records_fixed', 'new_rules_count', 'quality_score',
                              'status', 'summary', '_created_at'],
            )
            log.info('[审计] %s | 质量分 %.1f | 修复 %d 条 | 新规则 %d 条',
                     status, profile.get('quality_score', 100), fix_result.get('records_fixed', 0), new_rules)
        except Exception as e:
            log.error('审计日志写入失败：%s', e)


# ══════════════════════════════════════════════════════════════
# AIETLAgent：主编排器
# ══════════════════════════════════════════════════════════════

class AIETLAgent:
    """全自动 AI ETL Agent 主控制器"""

    def __init__(self, lookback_minutes: int = 5):
        self.lookback_minutes = lookback_minutes

    def run_once(self) -> dict:
        """执行一轮 AI ETL：检测 → 生成规则 → 执行修复 → 记录审计"""
        run_start = datetime.now()
        window_end = run_start.replace(second=0, microsecond=0)
        window_start = window_end - timedelta(minutes=self.lookback_minutes)
        status = 'success'
        fix_result = {'records_fixed': 0, 'rules_applied': 0}
        new_rules_saved = 0

        log.info('═' * 60)
        log.info('AI ETL Agent 启动 | 窗口 %s ~ %s',
                 window_start.strftime('%H:%M'), window_end.strftime('%H:%M'))

        try:
            ch = _get_ch()
            profiler   = DataProfiler(ch)
            engine     = RuleEngine(ch)
            auditor    = AuditLogger(ch)
            ai_gen     = AIRuleGenerator()

            # Step 1: 数据质量检测
            profile = profiler.profile(self.lookback_minutes)
            if 'error' in profile:
                log.error('质量检测异常，跳过本轮：%s', profile['error'])
                return {'status': 'failed', 'error': profile['error']}

            # Step 2: 加载现有规则
            existing_rules    = engine.load_rules()
            existing_names    = engine.get_existing_rule_names()

            # Step 3: LLM 生成新规则（仅有新质量问题时调用）
            if profile.get('has_fixable_issues'):
                new_rules = ai_gen.generate(profile, existing_names)
                if new_rules:
                    new_rules_saved = engine.save_rules(new_rules)
                    # 重新加载（含新规则）
                    existing_rules = engine.load_rules()
            else:
                log.info('数据质量良好（质量分 %.1f），无需调用 LLM', profile.get('quality_score', 100))

            # Step 4: 执行清洗规则
            if existing_rules:
                fix_result = engine.apply(existing_rules, lookback_minutes=self.lookback_minutes + 1)
                if 'error' in fix_result:
                    status = 'partial'
            else:
                log.info('暂无可执行规则')

            # Step 5: 写审计日志
            auditor.write(window_start, window_end, profile, fix_result, new_rules_saved, status)

        except Exception as e:
            log.error('AI ETL Agent 运行异常：%s', e, exc_info=True)
            status = 'failed'

        elapsed = (datetime.now() - run_start).total_seconds()
        log.info('本轮完成 | 耗时 %.1fs | 状态 %s', elapsed, status)
        log.info('═' * 60)

        return {
            'status': status,
            'quality_score': profile.get('quality_score', 0),
            'issues_found': len(profile.get('issues', [])),
            'records_fixed': fix_result.get('records_fixed', 0),
            'new_rules': new_rules_saved,
            'elapsed_seconds': round(elapsed, 1),
        }

    def run_loop(self, interval_seconds: int = 60):
        """持续循环运行，每隔 interval_seconds 秒执行一轮"""
        log.info('AI ETL Agent 进入循环模式，间隔 %ds', interval_seconds)
        while True:
            result = self.run_once()
            log.info('下次运行将在 %ds 后...', interval_seconds)
            time.sleep(interval_seconds)


# ══════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='全自动 AI ETL Agent')
    parser.add_argument('--loop', type=int, default=0, metavar='SECONDS',
                        help='循环模式：每隔 N 秒运行一次（0=单次运行）')
    parser.add_argument('--lookback', type=int, default=5, metavar='MINUTES',
                        help='数据质量检测回溯分钟数（默认5）')
    args = parser.parse_args()

    agent = AIETLAgent(lookback_minutes=args.lookback)

    if args.loop > 0:
        agent.run_loop(interval_seconds=args.loop)
    else:
        result = agent.run_once()
        print('\n运行结果：', json.dumps(result, ensure_ascii=False, indent=2))
