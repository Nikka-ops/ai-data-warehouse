# -*- coding: utf-8 -*-
"""
Iceberg 小文件合并任务（Compaction）
通过重写快照将碎片化小文件合并为目标大小，提升查询扫描性能
"""

from __future__ import annotations

from src.common.utils import get_logger

log = get_logger("storage.iceberg.compaction")

# 需要定期 compaction 的归档表列表（namespace, table_name）
_ARCHIVE_TABLES: list[tuple[str, str]] = [
    ("archive", "kappa_hourly_agg"),
    ("archive", "realtime_minute_stats"),
]


class CompactionJob:
    """定期合并 Iceberg 小文件，提升查询性能"""

    def __init__(self, adapter) -> None:
        """
        adapter: IcebergAdapter 实例，需具备 load_table / rewrite_data_files 方法
        """
        self.adapter = adapter

    def run(
        self,
        namespace: str,
        table: str,
        target_file_size_mb: int = 128,
    ) -> None:
        """
        对指定表执行 compaction（通过重写快照实现）
        target_file_size_mb: 合并后目标文件大小（MB），默认 128MB
        """
        log.info("开始 compaction：%s.%s 目标文件大小 %dMB", namespace, table, target_file_size_mb)
        try:
            tbl = self.adapter.load_table(namespace, table)  # 加载 Iceberg 表元数据
            target_bytes = target_file_size_mb * 1024 * 1024
            # 通过重写数据文件实现小文件合并
            result = tbl.rewrite_data_files(
                options={"target-file-size-bytes": str(target_bytes)}
            )
            log.info(
                "compaction 完成：%s.%s 合并文件数 %s → %s",
                namespace, table,
                getattr(result, "rewritten_data_files_count", "?"),
                getattr(result, "added_data_files_count", "?"),
            )
        except Exception as exc:
            # 单表失败不阻断其他表的 compaction
            log.error("compaction 失败：%s.%s 原因：%s", namespace, table, exc)

    def run_all(self) -> None:
        """对所有预定义归档表依次执行 compaction"""
        log.info("批量 compaction 开始，共 %d 张表", len(_ARCHIVE_TABLES))
        for namespace, table in _ARCHIVE_TABLES:
            self.run(namespace, table)
        log.info("批量 compaction 全部完成")
