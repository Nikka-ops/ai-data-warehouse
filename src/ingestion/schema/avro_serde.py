# -*- coding: utf-8 -*-
"""
Avro 序列化/反序列化工具
支持 OrderEvent 及自定义 schema，依赖 avro-python3 或 fastavro（自动降级）
"""

from __future__ import annotations

import io

# 优先使用 fastavro（性能更好），否则降级到 avro-python3
try:
    import fastavro
    import fastavro.schema as fastavro_schema
    _BACKEND = "fastavro"
except ImportError:
    fastavro = None  # type: ignore[assignment]
    _BACKEND = None

if _BACKEND is None:
    try:
        import avro.schema
        import avro.io
        _BACKEND = "avro"
    except ImportError:
        avro = None  # type: ignore[assignment]
        _BACKEND = None  # 两个库都不可用时降级为 JSON

from src.common.utils import get_logger

log = get_logger("ingestion.schema.avro_serde")
log.info("Avro 后端：%s", _BACKEND or "none（降级为 JSON）")

# ── OrderEvent Avro Schema 定义 ────────────────────────────────────────
ORDER_SCHEMA_STR = '''
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.aiwarehouse",
  "fields": [
    {"name": "order_id",     "type": "string"},
    {"name": "customer_id",  "type": "string"},
    {"name": "price",        "type": "double"},
    {"name": "event_time",   "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
'''


def serialize(record: dict, schema_str: str = ORDER_SCHEMA_STR) -> bytes:
    """
    将 dict 序列化为 Avro 二进制格式
    两个 Avro 库都不可用时降级为 JSON bytes
    """
    if _BACKEND == "fastavro":
        parsed = fastavro_schema.parse_schema(
            __import__("json").loads(schema_str)
        )
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed, record)  # 不含 schema 头（Confluent 自行管理）
        return buf.getvalue()

    if _BACKEND == "avro":
        schema   = avro.schema.parse(schema_str)
        buf      = io.BytesIO()
        encoder  = avro.io.BinaryEncoder(buf)
        writer   = avro.io.DatumWriter(schema)
        writer.write(record, encoder)
        return buf.getvalue()

    # 降级：直接用 JSON 编码（兼容性兜底）
    import json
    log.warning("Avro 库不可用，降级为 JSON 序列化")
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


def deserialize(data: bytes, schema_str: str = ORDER_SCHEMA_STR) -> dict:
    """
    将 Avro 二进制数据反序列化为 dict
    两个 Avro 库都不可用时尝试 JSON 解码
    """
    if _BACKEND == "fastavro":
        parsed = fastavro_schema.parse_schema(
            __import__("json").loads(schema_str)
        )
        buf = io.BytesIO(data)
        return fastavro.schemaless_reader(buf, parsed)

    if _BACKEND == "avro":
        schema  = avro.schema.parse(schema_str)
        buf     = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(buf)
        reader  = avro.io.DatumReader(schema)
        return reader.read(decoder)

    # 降级：尝试 JSON 解码
    import json
    log.warning("Avro 库不可用，降级为 JSON 反序列化")
    return json.loads(data.decode("utf-8"))
