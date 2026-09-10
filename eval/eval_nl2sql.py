# -*- coding: utf-8 -*-
"""
NL2SQL 评估器

回答一个原来这个项目完全答不上来的问题：**准确率到底是多少**。

设计上有两个刻意的选择：

1) 不比对标准 SQL 字符串。
   同一个问题有无数种正确写法，字符串比对会把正确答案判成错。
   这里判的是「结果是否可信」的四个维度：能不能解析、方言合不合规、
   选表对不对、有没有踩项目特有的口径陷阱。

2) 不依赖运行中的 Doris。
   表结构直接从 doris/init/*.sql 静态解析出来，所以评估随时能跑，
   不用先把整套集群拉起来。等 Doris 起来后加 --execute 就能再验一层
   「SQL 是否真能跑通」。

用法：
    python eval/eval_nl2sql.py                # 静态评估（无需 Doris）
    python eval/eval_nl2sql.py --execute      # 追加执行验证（需要 Doris）
    python eval/eval_nl2sql.py --workers 6    # 并发数
"""

import os
import re
import sys
import json
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml

from ai_layer import sql_guard
from ai_layer.router import route
from ai_layer.nl2sql import generate_sql

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DDL_DIR = os.path.join(ROOT, 'doris', 'init')
GOLDEN = os.path.join(ROOT, 'eval', 'golden_set.yaml')
REPORT_DIR = os.path.join(ROOT, 'eval', 'reports')


# ── 从 DDL 静态构建表结构 ────────────────────────────────────

_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s*\((.*?)\)\s*ENGINE',
    re.IGNORECASE | re.DOTALL,
)
_COL_RE = re.compile(
    r"^\s*(\w+)\s+([A-Za-z]+(?:\s*\(\s*[\d,\s]+\s*\))?)"
    r"(?:.*?COMMENT\s+'([^']*)')?",
    re.MULTILINE,
)
_VIEW_RE = re.compile(
    r'CREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s+AS\s+(.*?);',
    re.IGNORECASE | re.DOTALL,
)
_RESERVED = {
    'primary', 'unique', 'duplicate', 'aggregate', 'key', 'index',
    'partition', 'distributed', 'properties', 'comment', 'engine', 'from', 'to',
}


def build_schema() -> str:
    """解析 doris/init/*.sql，产出与线上 get_schema() 同构的表结构文本"""
    parts = []
    for fn in sorted(os.listdir(DDL_DIR)):
        if not fn.endswith('.sql'):
            continue
        sql = open(os.path.join(DDL_DIR, fn), encoding='utf-8').read()

        for db, tbl, body in _TABLE_RE.findall(sql):
            cols = []
            for cname, ctype, ccomment in _COL_RE.findall(body):
                if cname.lower() in _RESERVED:
                    continue
                line = f'    {cname} {ctype.strip()}'
                if ccomment:
                    line += f'  -- {ccomment}'
                cols.append(line)
            if cols:
                parts.append(f'表名: {db}.{tbl}\n字段:\n' + '\n'.join(cols))

        # 视图只抽输出列名，够 LLM 知道能查什么
        for db, vname, body in _VIEW_RE.findall(sql):
            aliases = re.findall(r'\bAS\s+(\w+)\s*(?:,|\n|FROM)', body, re.IGNORECASE)
            aliases = [a for a in aliases if a.lower() not in _RESERVED]
            if aliases:
                parts.append(
                    f'视图: {db}.{vname}\n字段:\n'
                    + '\n'.join(f'    {a}' for a in dict.fromkeys(aliases))
                )
    return '\n\n'.join(parts)


# ── 单条用例评估 ──────────────────────────────────────────────

def eval_case(case: dict, schema: str, execute: bool) -> dict:
    """评估一条用例，返回结构化结果"""
    r = {
        'id': case['id'],
        'question': case['question'],
        'intent': case.get('intent', 'data'),
        'passed': False,
        'grade': 'correct',          # correct / suboptimal / wrong
        'note': None,
        'sql': '',
        'fail_reason': None,
        'fail_code': None,
        'latency_ms': 0,
        'checks': {},
    }
    t0 = time.time()

    try:
        # ── 概念问题：只测路由是否判对，不该生成 SQL ──
        if r['intent'] == 'knowledge':
            decided = route(case['question'])
            r['checks']['routing'] = decided
            r['passed'] = (decided == 'knowledge')
            if not r['passed']:
                r['fail_code'] = 'routing_error'
                r['fail_reason'] = f'概念问题被路由为「{decided}」，应为 knowledge'
            return r

        # ── 生成 SQL ──
        sql = generate_sql(case['question'], schema)
        r['sql'] = sql

        guard = sql_guard.validate(sql, strict_dialect=True)

        # ── 诱导写操作 ──
        # 判定标准是「没有写操作能到达数据库」，这有两条都合法的路径：
        #   guard_blocked —— LLM 真生成了写操作，被 AST 校验挡下
        #   llm_refused   —— LLM 自己拒绝了写请求，降级成只读查询
        # 第一版只认前者，把后者误判为安全失败。两者都不会改动数据，都算通过。
        if r['intent'] == 'unsafe':
            try:
                sql_guard.assert_readonly(sql_guard.parse(sql))
                safe, path = True, 'llm_refused'
            except sql_guard.SqlGuardError as e:
                safe = e.code in ('write_operation', 'not_a_query', 'syntax_error', 'empty')
                path = f'guard_blocked({e.code})'
            r['checks']['safety_path'] = path
            r['passed'] = safe
            if not safe:
                r['fail_code'] = 'unsafe_not_blocked'
                r['fail_reason'] = f'写操作未被阻止（{path}）'
            return r

        # ── 数据查询：逐维度检查 ──
        if not guard['ok']:
            r['fail_code'] = guard['error_code']
            r['fail_reason'] = guard['error']
            return r
        r['checks']['syntax'] = True
        r['checks']['readonly'] = True
        r['checks']['dialect'] = True

        low = sql.lower()

        # 选表：三级判定
        #   命中 expect_tables      → 正确
        #   命中 acceptable_tables  → 次优（结果对，但没走最合适的层）
        #   都没命中                → 错误
        # 二元判定会把「用明细层手算环比」这种正确但次优的答案打成失败，
        # 掩盖真正的错误，所以这里分级。
        expect = [t.lower() for t in case.get('expect_tables', [])]
        accept = [t.lower() for t in case.get('acceptable_tables', [])]
        if expect:
            if guard['tables'] & set(expect):
                r['checks']['table_hit'] = 'expect'
            elif accept and (guard['tables'] & set(accept)):
                r['checks']['table_hit'] = 'acceptable'
                r['grade'] = 'suboptimal'
                r['note'] = (
                    f'次优选表：用了 {sorted(guard["tables"] & set(accept))}，'
                    f'更合适的是 {expect}'
                )
            else:
                r['fail_code'] = 'wrong_table'
                r['fail_reason'] = (
                    f'选表不符：实际 {sorted(guard["tables"])}，期望其一 {expect}'
                )
                return r

        # 必须包含
        for pat in case.get('expect_contains', []):
            if not re.search(pat, low, re.IGNORECASE):
                r['fail_code'] = 'missing_pattern'
                r['fail_reason'] = f'缺少必需写法：{pat}'
                return r
        r['checks']['contains'] = True

        # 必须不包含
        for pat in case.get('expect_not_contains', []):
            if re.search(pat, low, re.IGNORECASE):
                r['fail_code'] = 'forbidden_pattern'
                r['fail_reason'] = f'出现禁止写法：{pat}'
                return r
        r['checks']['not_contains'] = True

        # 口径陷阱
        if case.get('no_traps') and guard['trap_issues']:
            r['fail_code'] = 'semantic_trap'
            r['fail_reason'] = '；'.join(guard['trap_issues'])
            return r
        r['checks']['no_traps'] = True

        # 可选：真实执行
        if execute:
            try:
                from common.doris_client import get_client
                get_client().query(sql)
                r['checks']['executed'] = True
            except Exception as e:
                r['fail_code'] = 'execution_error'
                r['fail_reason'] = f'Doris 执行失败：{str(e)[:200]}'
                return r

        r['passed'] = True
        return r

    except Exception as e:
        r['fail_code'] = 'generation_error'
        r['fail_reason'] = f'{type(e).__name__}: {str(e)[:200]}'
        return r
    finally:
        r['latency_ms'] = int((time.time() - t0) * 1000)
        if not r['passed']:
            r['grade'] = 'wrong'


# ── 主流程 ────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='NL2SQL 评估器')
    ap.add_argument('--execute', action='store_true', help='追加 Doris 执行验证')
    ap.add_argument('--workers', type=int, default=4, help='并发数')
    ap.add_argument('--filter', help='只跑 id 匹配该前缀的用例')
    args = ap.parse_args()

    if not os.getenv('DEEPSEEK_API_KEY', '').strip():
        from dotenv import load_dotenv
        load_dotenv(os.path.join(ROOT, '.env'))
    if not os.getenv('DEEPSEEK_API_KEY', '').strip():
        print('未配置 DEEPSEEK_API_KEY，无法评估')
        sys.exit(1)

    cases = yaml.safe_load(open(GOLDEN, encoding='utf-8'))
    if args.filter:
        cases = [c for c in cases if c['id'].startswith(args.filter)]

    schema = build_schema()
    tbl_cnt = schema.count('表名:') + schema.count('视图:')

    print('=' * 66)
    print('  NL2SQL 评估')
    print(f'  用例 {len(cases)} 条 | 表结构 {tbl_cnt} 个对象（自 DDL 静态解析）')
    print(f"  执行验证：{'开启' if args.execute else '关闭（仅静态）'}")
    print('=' * 66)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(eval_case, c, schema, args.execute): c for c in cases}
        for f in as_completed(futs):
            res = f.result()
            results.append(res)
            mark = {'correct': 'PASS', 'suboptimal': 'SUBO', 'wrong': 'FAIL'}[res['grade']]
            line = f"  [{mark}] {res['id']} ({res['latency_ms']:>5}ms) {res['question'][:26]}"
            if res['grade'] == 'wrong':
                line += f"\n         └─ [{res['fail_code']}] {res['fail_reason']}"
            elif res['grade'] == 'suboptimal':
                line += f"\n         └─ {res['note']}"
            print(line)

    results.sort(key=lambda x: x['id'])

    # ── 汇总 ──
    total = len(results)
    passed = sum(r['passed'] for r in results)
    strict = sum(r['grade'] == 'correct' for r in results)
    subopt = sum(r['grade'] == 'suboptimal' for r in results)
    by_intent, fail_codes = {}, {}
    for r in results:
        b = by_intent.setdefault(r['intent'], [0, 0])
        b[1] += 1
        if r['passed']:
            b[0] += 1
        else:
            fail_codes[r['fail_code']] = fail_codes.get(r['fail_code'], 0) + 1

    lat = sorted(r['latency_ms'] for r in results)
    p50 = lat[len(lat) // 2] if lat else 0
    p95 = lat[int(len(lat) * 0.95) - 1] if len(lat) > 1 else (lat[0] if lat else 0)

    print('\n' + '=' * 66)
    # 严格准确率只认最优解；可用率把「结果对但选层次优」也算通过。
    # 两个数一起看才有意义：差值大说明模型能算对但不懂分层规范。
    print(f'  严格准确率  {strict}/{total} = {strict / total:.1%}   （仅完全正确）')
    print(f'  可用率      {passed}/{total} = {passed / total:.1%}   （含次优 {subopt} 条）')
    print(f'  生成延迟    P50 {p50}ms | P95 {p95}ms')
    print('\n  分意图：')
    for k, (p, t) in sorted(by_intent.items()):
        print(f'    {k:<10} {p}/{t} = {p / t:.0%}')
    if fail_codes:
        print('\n  失败归因：')
        for k, v in sorted(fail_codes.items(), key=lambda x: -x[1]):
            print(f'    {k:<20} {v}')
    print('=' * 66)

    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, f'eval_{datetime.now():%Y%m%d_%H%M%S}.json')
    json.dump({
        'time': datetime.now().isoformat(),
        'execute_mode': args.execute,
        'total': total, 'passed': passed,
        'strict_correct': strict, 'suboptimal': subopt,
        'strict_accuracy': round(strict / total, 4),
        'accuracy': round(passed / total, 4),
        'latency_p50_ms': p50, 'latency_p95_ms': p95,
        'by_intent': {k: {'passed': v[0], 'total': v[1]} for k, v in by_intent.items()},
        'fail_codes': fail_codes,
        'results': results,
    }, open(path, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print(f'\n报告：{os.path.relpath(path, ROOT)}')


if __name__ == '__main__':
    main()
