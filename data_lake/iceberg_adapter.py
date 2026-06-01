import logging
import os
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.types import (
    BooleanType, DateType, DoubleType, FloatType,
    IntegerType, LongType, StringType, TimestampType,
)

logger = logging.getLogger(__name__)

# pandas dtype → Iceberg 类型映射
_DTYPE_MAP = {
    "int32": IntegerType(),
    "int64": LongType(),
    "float32": FloatType(),
    "float64": DoubleType(),
    "bool": BooleanType(),
    "object": StringType(),
    "string": StringType(),
    "datetime64[ns]": TimestampType(),
    "date": DateType(),
}


def _to_iceberg_type(dtype_str: str):
    return _DTYPE_MAP.get(dtype_str, StringType())


class IcebergAdapter:
    """封装 pyiceberg 的读写操作，对接 MinIO S3 + REST Catalog。"""

    def __init__(self):
        # 从环境变量读取 MinIO 和 Catalog 配置
        self._endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        self._access_key = os.environ.get("MINIO_ACCESS_KEY", "")
        self._secret_key = os.environ.get("MINIO_SECRET_KEY", "")
        self._warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse")
        self._catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
        self._catalog = self._init_catalog()

    def _init_catalog(self):
        try:
            catalog = load_catalog(
                "default",
                **{
                    "type": "rest",
                    "uri": self._catalog_uri,
                    "warehouse": self._warehouse,
                    "s3.endpoint": self._endpoint,
                    "s3.access-key-id": self._access_key,
                    "s3.secret-access-key": self._secret_key,
                },
            )
            return catalog
        except Exception as e:
            logger.warning("IcebergAdapter: 初始化 Catalog 失败: %s", e)
            return None

    def create_table(self, namespace: str, table_name: str, schema_dict: dict[str, str]) -> bool:
        """根据 schema_dict（列名→dtype字符串）创建 Iceberg 表，已存在则跳过。"""
        if self._catalog is None:
            logger.warning("IcebergAdapter: catalog 未就绪，跳过 create_table")
            return False
        try:
            # 确保 namespace 存在
            if (namespace,) not in self._catalog.list_namespaces():
                self._catalog.create_namespace(namespace)

            # 构造 Iceberg Schema
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField
            fields = [
                NestedField(field_id=i + 1, name=col, field_type=_to_iceberg_type(dtype), required=False)
                for i, (col, dtype) in enumerate(schema_dict.items())
            ]
            schema = Schema(*fields)
            identifier = f"{namespace}.{table_name}"
            self._catalog.create_table(identifier=identifier, schema=schema)
            logger.info("IcebergAdapter: 表 %s 创建成功", identifier)
            return True
        except Exception as e:
            logger.warning("IcebergAdapter: create_table 失败: %s", e)
            return False

    def append(self, namespace: str, table_name: str, df: pd.DataFrame) -> bool:
        """将 DataFrame 追加写入 Iceberg 表。"""
        if self._catalog is None:
            logger.warning("IcebergAdapter: catalog 未就绪，跳过 append")
            return False
        try:
            identifier = f"{namespace}.{table_name}"
            table = self._catalog.load_table(identifier)
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            table.append(arrow_table)
            logger.info("IcebergAdapter: append %d 行到 %s", len(df), identifier)
            return True
        except Exception as e:
            logger.warning("IcebergAdapter: append 失败: %s", e)
            return False

    def read(self, namespace: str, table_name: str, filters: Any = None) -> pd.DataFrame:
        """从 Iceberg 表读取数据，支持可选过滤条件，返回 DataFrame。"""
        if self._catalog is None:
            logger.warning("IcebergAdapter: catalog 未就绪，返回空 DataFrame")
            return pd.DataFrame()
        try:
            identifier = f"{namespace}.{table_name}"
            table = self._catalog.load_table(identifier)
            scan = table.scan(row_filter=filters) if filters else table.scan()
            arrow_table = scan.to_arrow()
            return arrow_table.to_pandas()
        except Exception as e:
            logger.warning("IcebergAdapter: read 失败: %s", e)
            return pd.DataFrame()

    def get_table_snapshots(self, namespace: str, table_name: str) -> list[dict]:
        """获取 Iceberg 表快照列表，用于时间点回溯查询。"""
        if self._catalog is None:
            logger.warning("IcebergAdapter: catalog 未就绪，返回空快照列表")
            return []
        try:
            identifier = f"{namespace}.{table_name}"
            table = self._catalog.load_table(identifier)
            snapshots = [
                {
                    "snapshot_id": s.snapshot_id,
                    "timestamp_ms": s.timestamp_ms,
                    "summary": s.summary,
                }
                for s in table.metadata.snapshots
            ]
            return snapshots
        except Exception as e:
            logger.warning("IcebergAdapter: get_table_snapshots 失败: %s", e)
            return []
