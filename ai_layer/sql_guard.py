# -*- coding: utf-8 -*-
"""
SQL 安全与方言校验（AST 级）

替代原来基于关键字正则的 validate_sql()。正则做这件事有两类硬伤：

  漏判：SQL 注释、字符串字面量、CTE 里的写操作都能绕过关键字匹配，
        比如  SELECT 1 /* DROP */  会误伤，而
        WITH x AS (INSERT ...) SELECT  又可能漏掉。
  错判：字段名叫 update_time、表名含 create 的正常查询会被当成写操作拦下。

改成 sqlglot 解析成 AST 之后，判断的是语法结构而不是字符串，
顺带还能把「这条 SQL 到底读了哪些表」精确抽出来 —— 这是 NL2SQL
评估里判定「选表对不对」的基础，正则做不到。

sqlglot 原生支持 doris 方言，解析失败本身就是一个有效信号：
说明 LLM 生成了 Doris 跑不了的语法。
"""

import re

import sqlglot
from sqlglot import exp


DIALECT = 'doris'

# 写操作节点：出现任意一个即判定为非只读
_WRITE_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
    exp.Alter, exp.TruncateTable, exp.Command,
)

# ClickHouse 专有函数 —— LLM 被旧提示词带偏时最常吐出来的一批
_CH_ONLY_FUNCS = {
    'countif', 'countdistinctif', 'argmax', 'argmin',
    'toyyyymm', 'toyyyymmdd', 'tostartofminute', 'tostartofhour',
    'formatdatetime', 'parsedatetimebesteffort', 'parsedatetimebesteffortornull',
    'stddevpop', 'uniqexact', 'uniq', 'replaceall',
    'tointervalday', 'todatetime64',
}

# FINAL 修饰符：Doris 没有，出现说明还在按 ReplacingMergeTree 的思路写
_FINAL_RE = re.compile(r'\bFINAL\b', re.IGNORECASE)


class SqlGuardError(Exception):
    """校验不通过"""

    def __init__(self, code: str, message: str):
        self.code = code          # 归因用的错误码
        super().__init__(message)


def parse(sql: str):
    """解析成 AST，语法错误抛 SqlGuardError(code='syntax_error')"""
    sql = (sql or '').strip().rstrip(';')
    if not sql:
        raise SqlGuardError('empty', 'SQL 为空')
    try:
        ast = sqlglot.parse_one(sql, dialect=DIALECT)
    except Exception as e:
        raise SqlGuardError('syntax_error', f'解析失败：{str(e)[:200]}')
    if ast is None:
        raise SqlGuardError('syntax_error', 'SQL 解析结果为空')
    return ast


def assert_readonly(ast) -> None:
    """只允许 SELECT / WITH ... SELECT"""
    for node_type in _WRITE_NODES:
        node = ast.find(node_type)
        if node is not None:
            raise SqlGuardError(
                'write_operation',
                f'检测到写操作：{node.__class__.__name__.upper()}',
            )
    # 顶层必须是查询
    if not isinstance(ast, (exp.Select, exp.Union, exp.Subquery)):
        raise SqlGuardError(
            'not_a_query',
            f'顶层语句不是查询：{ast.__class__.__name__}',
        )


def extract_tables(ast) -> set[str]:
    """
    抽出所有被引用的表，返回 {'库.表'} 形式。

    CTE 的别名会被排除 —— 它们是查询内定义的临时名字，不是真实表。
    """
    cte_names = {
        cte.alias_or_name.lower()
        for cte in ast.find_all(exp.CTE)
        if cte.alias_or_name
    }

    tables = set()
    for t in ast.find_all(exp.Table):
        name = (t.name or '').lower()
        if not name or name in cte_names:
            continue
        db = (t.db or '').lower()
        tables.add(f'{db}.{name}' if db else name)
    return tables


def _strip_literals(sql: str) -> str:
    """
    把字符串字面量和注释的内容抹成空格，位置和长度保持不变。

    先做这一步再做文本匹配，是为了避开正则方案最典型的两个坑：
    WHERE city = 'countIf' 里的字符串、以及 -- countIf 这样的注释，
    都不该被当成真的函数调用。
    """
    out = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        # 行注释
        if sql.startswith('--', i):
            j = sql.find('\n', i)
            j = n if j == -1 else j
            out.append(' ' * (j - i)); i = j; continue
        # 块注释
        if sql.startswith('/*', i):
            j = sql.find('*/', i + 2)
            j = n if j == -1 else j + 2
            out.append(' ' * (j - i)); i = j; continue
        # 字符串字面量
        if c in ("'", '"', '`'):
            j = i + 1
            while j < n:
                if sql[j] == '\\':
                    j += 2; continue
                if sql[j] == c:
                    j += 1; break
                j += 1
            out.append(c + ' ' * (j - i - 2) + c if j - i >= 2 else ' ' * (j - i))
            i = j; continue
        out.append(c); i += 1
    return ''.join(out)


def check_dialect(ast, raw_sql: str) -> list[str]:
    """
    检出 ClickHouse 残留写法，返回问题列表（空列表 = 合规）。
    不抛异常，交给调用方决定是拦截还是记为失败项。

    两条腿走路：
      结构层 —— 有些函数 sqlglot 会解析成具名节点（countIf → CountIf），
                这类无论怎么拼写在 Doris 上都不存在，按节点类型判即可；
      拼写层 —— 有些函数与 Doris 函数共用节点（argMax 与 MAX_BY 都是 ArgMax），
                只能看原始拼写，所以先抹掉字符串和注释再做词匹配。
    """
    issues = []
    clean = _strip_literals(raw_sql)

    # ── 结构层：Doris 完全不存在的函数节点 ──
    for node_type, label in ((exp.CountIf, 'countIf / COUNT_IF'),):
        if ast.find(node_type):
            issues.append(f'ClickHouse 专有函数：{label}，Doris 需用 SUM(CASE WHEN ... THEN 1 ELSE 0 END)')

    # ── 拼写层：匿名函数 + 与 Doris 同节点但拼写是 ClickHouse 的 ──
    for fn in ast.find_all(exp.Anonymous):
        if (fn.name or '').lower() in _CH_ONLY_FUNCS:
            issues.append(f'ClickHouse 专有函数：{fn.name}()')

    for name in ('argMax', 'argMin'):
        if re.search(rf'\b{name}\s*\(', clean, re.IGNORECASE):
            issues.append(f'ClickHouse 专有函数：{name}()，Doris 对应 MAX_BY / MIN_BY')

    # ── 无参 count() —— Doris 要求 COUNT(*) ──
    for c in ast.find_all(exp.Count):
        if c.this is None:
            issues.append('count() 无参数，Doris 需写 COUNT(*)')

    # ── FINAL 修饰符 ──
    if _FINAL_RE.search(clean):
        issues.append('使用了 FINAL，Doris Unique 表写时合并，不需要也不支持')

    return issues


def check_project_traps(ast, raw_sql: str) -> list[str]:
    """
    项目特有的口径陷阱。这些不是语法错误，SQL 能跑通，
    但结果是错的 —— 恰恰是最难发现的一类问题。
    """
    issues = []
    tables = extract_tables(ast)
    low = raw_sql.lower()

    # 陷阱一：交易窗口表混用粒度
    # 该表用 'ALL' 表示汇总行，不加过滤会把汇总行和明细行一起算，指标翻倍
    if any(t.endswith('trade_window_agg') for t in tables):
        if 'window_type' not in low:
            issues.append(
                'trade_window_agg 未按 window_type 过滤，'
                'TUMBLE_1M 与 CUMULATE_1D 会被混在一起'
            )
        if 'category1_name' not in low or 'region_name' not in low:
            issues.append(
                'trade_window_agg 未对 category1_name / region_name 做 ALL 过滤，'
                '汇总行与明细行叠加会导致重复计数'
            )

    # 陷阱二：支付窗口表同样有汇总行
    if any(t.endswith('pay_window_agg') for t in tables):
        if 'window_type' not in low:
            issues.append(
                'pay_window_agg 未按 window_type 过滤，'
                'TUMBLE_1M 与 CUMULATE_1D 会被混在一起'
            )
        if 'payment_type' not in low:
            issues.append(
                'pay_window_agg 未对 payment_type 做 ALL 过滤，'
                '汇总行与各支付方式明细行会叠加'
            )

    # 陷阱三：BITMAP 列被当普通列用
    if 'uv_bitmap' in low:
        if 'bitmap_union_count' not in low and 'bitmap_count' not in low:
            issues.append(
                'uv_bitmap 是 BITMAP 类型，必须用 BITMAP_UNION_COUNT 取基数，'
                '不能直接 SELECT / COUNT / SUM'
            )

    # 陷阱四：跨窗口的去重用户数被直接相加
    # order_user_cnt 是 Flink 在单个窗口状态里算的精确去重值，
    # 只在那个窗口内成立。跨窗口相加会把同一用户重复计数 ——
    # 这类错误 SQL 跑得通、结果偏大、没人会报错，正是最危险的一种。
    if re.search(r'sum\s*\(\s*(order_user_cnt|pay_user_cnt)\s*\)', low):
        issues.append(
            'SUM(order_user_cnt / pay_user_cnt) 会把跨窗口重复的用户重复计数，'
            '跨窗口或跨天的独立用户数应查 dws.user_active_daily 的 '
            'BITMAP_UNION_COUNT(uv_bitmap)'
        )

    return issues


def validate(sql: str, strict_dialect: bool = True) -> dict:
    """
    完整校验，返回结构化结果（不抛异常，便于批量评估时归因）。

    strict_dialect=True 时，方言问题也算失败。
    """
    result = {
        'ok': False,
        'error_code': None,
        'error': None,
        'tables': set(),
        'dialect_issues': [],
        'trap_issues': [],
    }

    try:
        ast = parse(sql)
        assert_readonly(ast)
    except SqlGuardError as e:
        result['error_code'] = e.code
        result['error'] = str(e)
        return result

    result['tables'] = extract_tables(ast)
    result['dialect_issues'] = check_dialect(ast, sql)
    result['trap_issues'] = check_project_traps(ast, sql)

    if strict_dialect and result['dialect_issues']:
        result['error_code'] = 'dialect_error'
        result['error'] = '；'.join(result['dialect_issues'])
        return result

    result['ok'] = True
    return result


def assert_safe(sql: str) -> None:
    """
    给线上调用路径用的严格版：不合规直接抛异常。
    nl2sql / agent_tools 执行 SQL 前调它。
    """
    r = validate(sql, strict_dialect=True)
    if not r['ok']:
        raise SqlGuardError(r['error_code'], r['error'])
