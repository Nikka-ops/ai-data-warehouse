# FeastStore — Feast 特征存储主入口，替代旧版 registry.py
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# 尝试导入 feast，未安装时降级为 stub
try:
    from feast import FeatureStore
    _FEAST_AVAILABLE = True
except ImportError:
    _FEAST_AVAILABLE = False
    logger.warning(
        "feast 未安装，FeastStore 将以 stub 模式运行。"
        "请执行 `pip install feast[redis]` 以启用完整功能。"
    )
    FeatureStore = None  # type: ignore


class FeastStore:
    """封装 feast.FeatureStore，提供在线/离线特征读写及物化接口。"""

    def __init__(self, repo_path: Optional[str] = None) -> None:
        # 默认指向同包下的 feast_repo 目录
        if repo_path is None:
            repo_path = str(Path(__file__).parent / "feast_repo")
        self.repo_path = repo_path

        if _FEAST_AVAILABLE:
            self._store = FeatureStore(repo_path=repo_path)
            logger.info("FeastStore 初始化完成，repo_path=%s", repo_path)
        else:
            self._store = None
            logger.warning("FeastStore 以 stub 模式运行，所有方法返回空数据。")

    # ── 在线特征读取 ────────────────────────────────────────────────────────────
    def get_online_features(
        self,
        entity_rows: List[Dict],
        feature_refs: List[str],
    ) -> Dict:
        """从 Redis online store 批量获取在线特征。

        Parameters
        ----------
        entity_rows:   实体列表，如 [{"user_id": "u001"}, ...]
        feature_refs:  特征引用，如 ["user_stats:recency_days", ...]

        Returns
        -------
        dict: feast 返回的特征字典（可直接转 DataFrame）
        """
        if not _FEAST_AVAILABLE or self._store is None:
            logger.warning("feast 不可用，返回空特征字典。")
            return {}
        response = self._store.get_online_features(
            features=feature_refs,
            entity_rows=entity_rows,
        )
        return response.to_dict()

    # ── 历史特征读取（Point-in-Time Join）────────────────────────────────────
    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        feature_refs: List[str],
    ) -> pd.DataFrame:
        """执行 PIT-correct 历史特征查询，用于训练集生成。

        Parameters
        ----------
        entity_df:    含实体键列 + event_timestamp 列的 DataFrame
        feature_refs: 特征引用列表

        Returns
        -------
        pd.DataFrame: 拼接了历史特征的宽表
        """
        if not _FEAST_AVAILABLE or self._store is None:
            logger.warning("feast 不可用，原样返回 entity_df。")
            return entity_df
        job = self._store.get_historical_features(
            entity_df=entity_df,
            features=feature_refs,
        )
        return job.to_df()

    # ── 物化到 online store ──────────────────────────────────────────────────
    def materialize(
        self,
        start_date: datetime,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        """将离线 Parquet 数据物化到 Redis online store。

        Parameters
        ----------
        start_date:    物化窗口起始时间（UTC）
        end_date:      物化窗口结束时间（UTC）
        feature_views: 指定视图名列表；None 表示全量物化
        """
        if not _FEAST_AVAILABLE or self._store is None:
            logger.warning("feast 不可用，跳过物化。")
            return
        logger.info("开始物化 %s → %s", start_date, end_date)
        self._store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views,
        )
        logger.info("物化完成。")

    # ── 从 ClickHouse 同步数据到 Parquet 并物化 ───────────────────────────────
    def sync_from_clickhouse(
        self,
        ch,  # clickhouse_driver.Client 或兼容对象
        end_date: Optional[datetime] = None,
        lookback_days: int = 7,
    ) -> None:
        """从 ClickHouse 导出特征数据到 feast_offline/ Parquet，然后物化。

        Parameters
        ----------
        ch:            ClickHouse 客户端（需支持 .execute(query, ...) 返回 DataFrame）
        end_date:      同步截止时间，默认为当前 UTC 时间
        lookback_days: 向前回溯天数，决定物化窗口大小
        """
        from datetime import timezone
        from datetime import timedelta

        if end_date is None:
            end_date = datetime.now(tz=timezone.utc)
        start_date = end_date - timedelta(days=lookback_days)

        offline_dir = Path(self.repo_path).parent.parent / "datasets" / "feast_offline"
        offline_dir.mkdir(parents=True, exist_ok=True)

        # 查询并导出三张特征表
        _EXPORT_QUERIES: Dict[str, str] = {
            "user_stats": """
                SELECT
                    user_id,
                    toDateTime(event_timestamp) AS event_timestamp,
                    recency_days,
                    frequency_30d,
                    monetary_30d,
                    avg_order_value,
                    active_category_cnt,
                    clv_score
                FROM dw.user_stats_feature
                WHERE event_timestamp BETWEEN %(start)s AND %(end)s
            """,
            "seller_stats": """
                SELECT
                    seller_id,
                    toDateTime(event_timestamp) AS event_timestamp,
                    gmv_7d,
                    order_cnt_7d,
                    refund_rate_30d,
                    avg_rating,
                    sku_cnt
                FROM dw.seller_stats_feature
                WHERE event_timestamp BETWEEN %(start)s AND %(end)s
            """,
            "category_stats": """
                SELECT
                    category,
                    toDateTime(event_timestamp) AS event_timestamp,
                    category_name,
                    pv_7d,
                    conversion_rate_7d,
                    avg_price,
                    top_seller_cnt
                FROM dw.category_stats_feature
                WHERE event_timestamp BETWEEN %(start)s AND %(end)s
            """,
        }

        params = {"start": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                  "end": end_date.strftime("%Y-%m-%d %H:%M:%S")}

        for table_name, query in _EXPORT_QUERIES.items():
            logger.info("正在从 ClickHouse 导出 %s ...", table_name)
            try:
                # 兼容 clickhouse_driver（返回列表）和 clickhouse-connect（返回 DataFrame）
                result = ch.execute(query, params, with_column_types=True)
                if isinstance(result, tuple):
                    rows, col_types = result
                    columns = [c[0] for c in col_types]
                    df = pd.DataFrame(rows, columns=columns)
                else:
                    df = result  # 已经是 DataFrame
                out_path = offline_dir / f"{table_name}.parquet"
                df.to_parquet(out_path, index=False)
                logger.info("已写出 %s (%d 行) → %s", table_name, len(df), out_path)
            except Exception as exc:
                logger.error("导出 %s 失败：%s", table_name, exc)
                raise

        # 全量物化到 Redis
        self.materialize(start_date=start_date, end_date=end_date)
