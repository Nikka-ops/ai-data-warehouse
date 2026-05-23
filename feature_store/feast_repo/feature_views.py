# Feast 特征视图定义 — 每个视图对应一张 Parquet 离线源
from datetime import timedelta

try:
    from feast import FeatureView, Field
    from feast.infra.offline_stores.file_source import FileSource
    from feast.types import Float32, Int64, String
    _FEAST_AVAILABLE = True
except ImportError:
    import warnings
    warnings.warn("feast 未安装，feature_views.py 仅作占位，请执行 pip install feast", stacklevel=1)
    _FEAST_AVAILABLE = False

from entities import user_entity, seller_entity, category_entity  # 本地实体定义

if _FEAST_AVAILABLE:
    # ── 用户统计特征源（RFM + 行为）──
    user_stats_source = FileSource(
        path="datasets/feast_offline/user_stats.parquet",
        timestamp_field="event_timestamp",
    )

    user_stats_fv = FeatureView(
        name="user_stats",
        entities=[user_entity],
        ttl=timedelta(days=7),
        schema=[
            Field(name="recency_days",        dtype=Float32),   # 最近购买距今天数
            Field(name="frequency_30d",       dtype=Int64),     # 近30天购买次数
            Field(name="monetary_30d",        dtype=Float32),   # 近30天消费金额
            Field(name="avg_order_value",     dtype=Float32),   # 平均订单金额
            Field(name="active_category_cnt", dtype=Int64),     # 活跃类目数
            Field(name="clv_score",           dtype=Float32),   # 客户生命周期价值评分
        ],
        source=user_stats_source,
        online=True,
    )

    # ── 卖家统计特征源 ──
    seller_stats_source = FileSource(
        path="datasets/feast_offline/seller_stats.parquet",
        timestamp_field="event_timestamp",
    )

    seller_stats_fv = FeatureView(
        name="seller_stats",
        entities=[seller_entity],
        ttl=timedelta(days=7),
        schema=[
            Field(name="gmv_7d",             dtype=Float32),   # 近7天成交额
            Field(name="order_cnt_7d",       dtype=Int64),     # 近7天订单数
            Field(name="refund_rate_30d",    dtype=Float32),   # 近30天退款率
            Field(name="avg_rating",         dtype=Float32),   # 平均评分
            Field(name="sku_cnt",            dtype=Int64),     # 在售 SKU 数
        ],
        source=seller_stats_source,
        online=True,
    )

    # ── 类目统计特征源 ──
    category_stats_source = FileSource(
        path="datasets/feast_offline/category_stats.parquet",
        timestamp_field="event_timestamp",
    )

    category_stats_fv = FeatureView(
        name="category_stats",
        entities=[category_entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="category_name",       dtype=String),    # 类目名称
            Field(name="pv_7d",               dtype=Int64),     # 近7天浏览量
            Field(name="conversion_rate_7d",  dtype=Float32),   # 近7天转化率
            Field(name="avg_price",           dtype=Float32),   # 类目均价
            Field(name="top_seller_cnt",      dtype=Int64),     # 头部卖家数量
        ],
        source=category_stats_source,
        online=True,
    )
else:
    user_stats_source = None      # type: ignore
    user_stats_fv = None          # type: ignore
    seller_stats_source = None    # type: ignore
    seller_stats_fv = None        # type: ignore
    category_stats_source = None  # type: ignore
    category_stats_fv = None      # type: ignore
