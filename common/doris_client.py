# -*- coding: utf-8 -*-
"""
Doris 统一访问层

替代原来散落在 13 个文件里的 clickhouse_connect.get_client(...)。

刻意做成与 clickhouse_connect 接口兼容（query / query_df / command / insert），
这样上层业务代码只需要换 import，查询逻辑一行都不用动 —— 迁移的风险
集中在这一个文件里，而不是摊到每个调用点。

两条通道，用途不同：
    查询   → MySQL 协议（9030），Doris 完全兼容 MySQL 客户端
    批量写 → Stream Load HTTP（8030），走 BE 直写

为什么批量写不用 INSERT INTO VALUES：
Doris 每次导入都会生成一个新版本（rowset），高频小批量 INSERT 会产生
海量小版本，压垮后台 compaction，最终报 -235 too many versions。
Stream Load 一次提交一个大批次，是 Doris 官方推荐的导入方式。
"""

import os
import json
import time
from typing import Any, Sequence

import pymysql
import requests
import pandas as pd


# ── 连接配置 ──────────────────────────────────────────────────
DORIS_HOST       = os.getenv('DORIS_HOST', 'localhost')
DORIS_QUERY_PORT = int(os.getenv('DORIS_QUERY_PORT', '9030'))   # MySQL 协议
DORIS_HTTP_PORT  = int(os.getenv('DORIS_HTTP_PORT', '8030'))    # Stream Load
DORIS_USER       = os.getenv('DORIS_USER', 'root')
DORIS_PASSWORD   = os.getenv('DORIS_PASSWORD', '')


class QueryResult:
    """对齐 clickhouse_connect 的查询结果对象"""

    def __init__(self, rows: list[tuple], column_names: list[str]):
        self.result_rows  = rows
        self.column_names = column_names

    @property
    def first_row(self) -> tuple:
        """第一行；空结果返回全 None 的一行，避免调用方到处写 if not rows"""
        if self.result_rows:
            return self.result_rows[0]
        return tuple([None] * len(self.column_names)) if self.column_names else ()

    @property
    def row_count(self) -> int:
        return len(self.result_rows)

    def __iter__(self):
        return iter(self.result_rows)

    def __len__(self) -> int:
        return len(self.result_rows)


class DorisClient:
    """Doris 客户端。查询走 MySQL 协议，批量导入走 Stream Load。"""

    def __init__(
        self,
        host: str = None,
        query_port: int = None,
        http_port: int = None,
        username: str = None,
        password: str = None,
        database: str = None,
    ):
        self.host       = host       or DORIS_HOST
        self.query_port = query_port or DORIS_QUERY_PORT
        self.http_port  = http_port  or DORIS_HTTP_PORT
        self.username   = username   or DORIS_USER
        self.password   = DORIS_PASSWORD if password is None else password
        self.database   = database

    # ── 连接 ──────────────────────────────────────────────────
    def _connect(self):
        return pymysql.connect(
            host=self.host,
            port=self.query_port,
            user=self.username,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            autocommit=True,
            connect_timeout=10,
            read_timeout=300,
        )

    # ── 查询 ──────────────────────────────────────────────────
    def query(self, sql: str, parameters: Sequence[Any] = None) -> QueryResult:
        """执行查询，返回 QueryResult（.result_rows / .first_row）"""
        sql = sql.strip().rstrip(';')
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, parameters)
                rows = cur.fetchall() or []
                cols = [d[0] for d in cur.description] if cur.description else []
                return QueryResult(list(rows), cols)
        finally:
            conn.close()

    def query_df(self, sql: str, parameters: Sequence[Any] = None) -> pd.DataFrame:
        """查询并返回 DataFrame"""
        res = self.query(sql, parameters)
        return pd.DataFrame(res.result_rows, columns=res.column_names)

    def command(self, sql: str, parameters: Sequence[Any] = None) -> int:
        """执行 DDL / DML，返回影响行数"""
        sql = sql.strip().rstrip(';')
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                return cur.execute(sql, parameters)
        finally:
            conn.close()

    # ── 元数据 ────────────────────────────────────────────────
    def get_columns(self, database: str, table: str) -> list[tuple[str, str]]:
        """
        取表结构，返回 [(列名, 类型), ...]

        ClickHouse 查的是 system.columns，Doris 用标准的 information_schema。
        NL2SQL 动态注入表结构时靠它。
        """
        res = self.query(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (database, table),
        )
        return [(r[0], r[1]) for r in res.result_rows]

    def list_tables(self, database: str) -> list[str]:
        res = self.query(
            "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = %s",
            (database,),
        )
        return [r[0] for r in res.result_rows]

    def table_exists(self, database: str, table: str) -> bool:
        res = self.query(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (database, table),
        )
        return (res.first_row[0] or 0) > 0

    # ── 批量导入 ──────────────────────────────────────────────
    def insert(
        self,
        table: str,
        data: list[list],
        column_names: list[str],
        max_retries: int = 3,
    ) -> dict:
        """
        批量写入，签名对齐 clickhouse_connect.insert()

        table 形如 'dws.realtime_minute_stats'
        """
        if not data:
            return {'Status': 'Success', 'NumberLoadedRows': 0}

        if '.' not in table:
            raise ValueError(f"table 需要写成 '库名.表名' 形式，收到：{table}")
        database, table_name = table.split('.', 1)

        records = [dict(zip(column_names, row)) for row in data]
        return self.stream_load(database, table_name, records, max_retries=max_retries)

    def stream_load(
        self,
        database: str,
        table: str,
        records: list[dict],
        max_retries: int = 3,
    ) -> dict:
        """
        Stream Load 导入（JSON 格式）

        label 用「表名 + 毫秒时间戳」保证唯一：Doris 靠 label 做导入去重，
        同一个 label 重复提交会被判定为重复导入直接拒绝 —— 这正是
        导入幂等的实现方式，重试时沿用同一个 label 就不会写两遍。
        """
        url = (f"http://{self.host}:{self.http_port}"
               f"/api/{database}/{table}/_stream_load")

        payload = '\n'.join(
            json.dumps(r, ensure_ascii=False, default=str) for r in records
        )
        label = f"{table}_{int(time.time() * 1000)}"

        headers = {
            'Expect':             '100-continue',
            'label':              label,
            'format':             'json',
            'read_json_by_line':  'true',
            'strict_mode':        'false',
            # 单批允许 1% 的脏数据，超过则整批失败。
            # 设 0 的话一条脏数据就能卡住整条链路。
            'max_filter_ratio':   '0.01',
        }

        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.put(
                    url,
                    data=payload.encode('utf-8'),
                    headers=headers,
                    auth=(self.username, self.password),
                    # FE 会 307 重定向到 BE，requests 默认不对 PUT 保持 body，
                    # 所以显式允许重定向
                    allow_redirects=True,
                    timeout=120,
                )
                result = resp.json()

                if result.get('Status') in ('Success', 'Publish Timeout'):
                    return result

                # label 重复 = 这批数据已经成功导过了，视为成功（幂等）
                if 'Label Already Exists' in str(result.get('Message', '')):
                    return {'Status': 'Success', 'NumberLoadedRows': len(records),
                            'Message': 'label 已存在，跳过重复导入'}

                last_err = result.get('Message') or str(result)

            except Exception as e:
                last_err = str(e)

            if attempt < max_retries:
                time.sleep(2 ** attempt)

        raise RuntimeError(
            f"Stream Load 导入 {database}.{table} 失败（{max_retries} 次重试）：{last_err}"
        )

    def insert_df(self, table: str, df: pd.DataFrame, max_retries: int = 3) -> dict:
        """DataFrame 直接导入"""
        if df.empty:
            return {'Status': 'Success', 'NumberLoadedRows': 0}
        database, table_name = table.split('.', 1)
        records = df.where(pd.notnull(df), None).to_dict('records')
        return self.stream_load(database, table_name, records, max_retries=max_retries)

    # ── 健康检查 ──────────────────────────────────────────────
    def ping(self) -> bool:
        try:
            return self.query("SELECT 1").first_row[0] == 1
        except Exception:
            return False


# ── 模块级工厂：调用方式对齐 clickhouse_connect.get_client() ──
def get_client(
    host: str = None,
    port: int = None,
    username: str = None,
    password: str = None,
    database: str = None,
    **kwargs,
) -> DorisClient:
    """
    获取 Doris 客户端。

    保留 port 参数是为了兼容原来 get_client(host=..., port=8123, ...) 的写法：
    传进来的 ClickHouse HTTP 端口对 Doris 没有意义，直接忽略，
    走环境变量或默认值。
    """
    return DorisClient(
        host=host,
        query_port=None,
        http_port=None,
        username=username,
        password=password,
        database=database,
    )
