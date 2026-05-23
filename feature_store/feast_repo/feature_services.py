# Feast 特征服务定义 — 训练/推理一致性保障
try:
    from feast import FeatureService
    _FEAST_AVAILABLE = True
except ImportError:
    import warnings
    warnings.warn("feast 未安装，feature_services.py 仅作占位，请执行 pip install feast", stacklevel=1)
    _FEAST_AVAILABLE = False

from feature_views import user_stats_fv, seller_stats_fv, category_stats_fv  # 本地视图

if _FEAST_AVAILABLE:
    # 推荐场景服务 — 融合用户行为 + 类目统计
    recommendation_service = FeatureService(
        name="recommendation_service",
        features=[
            user_stats_fv,      # 用户 RFM 及行为特征
            category_stats_fv,  # 类目流量及转化特征
        ],
        description="用于推荐模型训练与在线推理",
    )

    # 监控场景服务 — 关注卖家健康度
    monitoring_service = FeatureService(
        name="monitoring_service",
        features=[
            seller_stats_fv,    # 卖家 GMV、退款率等指标
        ],
        description="用于卖家质量监控与预警",
    )
else:
    recommendation_service = None  # type: ignore
    monitoring_service = None      # type: ignore
