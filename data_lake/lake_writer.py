import logging
from datetime import datetime, timedelta

from data_lake.iceberg_adapter import IcebergAdapter

logger = logging.getLogger(__name__)

# 默认归档最近天数
_DEFAULT_DAYS = 7

# 每日归档的 ClickHouse 表配置：(ch_table, namespace, iceberg_table, partition_col)
_DAILY_ARCHIVE_TABLES = [
    ("dws.kappa_hourly_agg", "dws", "kappa_hourly_agg", "event_date"),
    ("dws.realtime_minute_stats", "dws", "realtime_minute_stats", "event_date"),
]


class LakeWriter:
    """将 ClickHouse 数据定期归档到 Iceberg 数据湖。"""

    def __init__(self, ch, adapter: IcebergAdapter):
        # ch: clickhouse_connect.Client 实例
        self._ch = ch
        self._adapter = adapter

    def archive_table(
        self,
        ch_table: str,
        namespace: str,
        iceberg_table: str,
        partition_col: str = "event_date",
        days: int = _DEFAULT_DAYS,
    ) -> int:
        """从 ClickHouse 查询最近 N 天数据并追加写入 Iceberg，返回归档行数。"""
        start_dt = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        sql = f"SELECT * FROM {ch_table} WHERE {partition_col} >= '{start_dt}'"
        start_time = datetime.utcnow()
        try:
            result = self._ch.query_df(sql)
        except Exception as e:
            logger.warning("LakeWriter: ClickHouse 查询失败 [%s]: %s", ch_table, e)
            return 0

        if result.empty:
            logger.info("LakeWriter: %s 近 %d 天无数据，跳过归档", ch_table, days)
            return 0

        ok = self._adapter.append(namespace, iceberg_table, result)
        elapsed = (datetime.utcnow() - start_time).total_seconds()

        if ok:
            logger.info(
                "LakeWriter: 归档完成 %s → %s.%s，共 %d 行，耗时 %.2fs",
                ch_table, namespace, iceberg_table, len(result), elapsed,
            )
            return len(result)
        else:
            logger.warning("LakeWriter: 归档写入失败 %s → %s.%s", ch_table, namespace, iceberg_table)
            return 0

    def run_daily_archive(self) -> dict[str, int]:
        """归档所有预定义的 DWS 层表，返回各表归档行数汇总。"""
        summary: dict[str, int] = {}
        logger.info("LakeWriter: 开始每日归档，共 %d 张表", len(_DAILY_ARCHIVE_TABLES))
        for ch_table, namespace, iceberg_table, partition_col in _DAILY_ARCHIVE_TABLES:
            rows = self.archive_table(ch_table, namespace, iceberg_table, partition_col)
            summary[ch_table] = rows
        logger.info("LakeWriter: 每日归档完成，汇总: %s", summary)
        return summary
