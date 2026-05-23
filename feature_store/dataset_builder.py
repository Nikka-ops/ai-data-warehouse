#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
训练数据集构建器 — Dataset Builder
核心能力：Point-in-Time 正确的特征拼接
保证训练样本中每个特征值来自 label 时间点之前，防止未来信息泄漏

用法：
  builder = DatasetBuilder()
  ds = builder.build(
      dataset_name="cancel_risk_v1",
      label_sql="SELECT customer_id AS entity_id, event_time, IF(order_status='canceled',1,0) AS label FROM ods.orders_stream WHERE event_time >= '2024-01-01' LIMIT 10000",
      feature_groups=["user_behavior", "category_stats"],
      output_path="/tmp/dataset_cancel_risk_v1.parquet"
  )
"""
import os
import sys
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('dataset_builder')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=120,
    )


class DatasetBuilder:
    """
    Point-in-Time 正确的训练数据集构建器

    工作流程：
    1. 执行 label_sql 生成标签表（entity_id, event_time, label）
    2. 对每个 feature_group，执行 ASOF JOIN 获取 PIT 特征
    3. 合并所有特征，导出 Parquet 文件
    4. 在 feature_store.training_datasets 注册数据集元数据
    """

    def __init__(self):
        self._ch = _get_ch()

    def build(
        self,
        dataset_name: str,
        label_sql: str,
        feature_groups: list[str],
        output_path: str,
        description: str = '',
        label_by: str = 'system',
    ) -> dict:
        """
        构建训练数据集

        Returns:
            dict: {dataset_id, row_count, file_path, feature_columns, status}
        """

        dataset_id = str(uuid.uuid4())
        log.info('开始构建数据集 %s（id=%s）', dataset_name, dataset_id[:8])

        # 1. 生成标签
        log.info('执行标签 SQL...')
        label_df = self._ch.query_df(label_sql.strip().rstrip(';'))
        if label_df.empty:
            log.warning('标签 SQL 返回空结果')
            return {'status': 'failed', 'error': '标签数据为空'}

        required_cols = {'entity_id', 'event_time', 'label'}
        if not required_cols.issubset(set(label_df.columns)):
            missing = required_cols - set(label_df.columns)
            return {'status': 'failed', 'error': f'标签 SQL 缺少列：{missing}'}

        log.info('标签行数：%d（正例：%d，负例：%d）',
                 len(label_df),
                 int((label_df['label'] == 1).sum()),
                 int((label_df['label'] == 0).sum()))

        # 2. PIT 特征拼接
        result_df = label_df.copy()
        feature_cols = []
        start_time = label_df['event_time'].min()
        end_time   = label_df['event_time'].max()

        for group_name in feature_groups:
            try:
                log.info('PIT 拼接特征组：%s', group_name)
                feature_df = self._pit_join_group(label_df, group_name)
                if not feature_df.empty:
                    # 移除重复的 join key 列
                    join_key = feature_df.columns.tolist()
                    new_cols = [c for c in join_key if c not in result_df.columns]
                    result_df = result_df.merge(
                        feature_df, on=['entity_id', 'event_time'], how='left'
                    )
                    feature_cols.extend([c for c in new_cols if c not in ['entity_id', 'event_time']])
                    log.info('  特征组 %s 拼接完成，新增 %d 列', group_name, len(new_cols))
            except Exception as e:
                log.error('特征组 %s PIT 拼接失败：%s', group_name, e)

        # 3. 用特征契约默认值填充缺失值
        result_df = self._apply_defaults(result_df, feature_groups)

        # 4. 导出 Parquet
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        result_df.to_parquet(output_path, index=False)
        log.info('数据集已导出：%s（%d 行，%d 列）', output_path, len(result_df), len(result_df.columns))

        # 5. 注册元数据
        self._register_dataset(
            dataset_id, dataset_name, description, feature_groups,
            label_sql, 'label', start_time, end_time,
            len(result_df), output_path, label_by,
        )

        return {
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            'row_count': len(result_df),
            'feature_count': len(feature_cols),
            'feature_columns': feature_cols,
            'file_path': output_path,
            'status': 'completed',
            'pos_rate': round(float((result_df['label'] == 1).mean()), 4),
        }

    def _pit_join_group(self, label_df, group_name: str):
        """
        对单个特征组执行 Point-in-Time JOIN
        使用 ClickHouse ASOF JOIN：对每个 (entity_id, event_time)
        找到 feature_time <= event_time 的最新特征值
        """
        import pandas as pd

        # 把 label 实体列表上传为临时查询
        entity_times = list(zip(
            label_df['entity_id'].astype(str).tolist(),
            label_df['event_time'].astype(str).tolist(),
        ))
        if not entity_times:
            return pd.DataFrame()

        # 获取该组所有特征
        feature_rows = self._ch.query(f"""
            SELECT feature_name FROM feature_store.feature_definitions
            WHERE group_name = '{group_name}' AND is_active = 1
        """).result_rows
        feature_names = [r[0] for r in feature_rows]
        if not feature_names:
            return pd.DataFrame()

        # 用 ASOF JOIN 对每个特征做 PIT 查询
        result = label_df[['entity_id', 'event_time']].copy()
        for feat_name in feature_names:
            try:
                # 构造 entity/time 过滤条件
                entity_set = "','".join(label_df['entity_id'].astype(str).unique()[:5000])
                min_ts = label_df['event_time'].min()
                max_ts = label_df['event_time'].max()

                pit_df = self._ch.query_df(f"""
                    SELECT
                        f.entity_id,
                        l.event_time,
                        f.feature_value AS {group_name}__{feat_name}
                    FROM (
                        SELECT DISTINCT entity_id,
                               event_time
                        FROM feature_store.feature_values
                        WHERE group_name = '{group_name}'
                          AND feature_name = '{feat_name}'
                          AND entity_id IN ('{entity_set}')
                          AND feature_time BETWEEN '{min_ts}' AND '{max_ts}'
                    ) l
                    ASOF JOIN (
                        SELECT entity_id, feature_value, feature_time
                        FROM feature_store.feature_values
                        WHERE group_name = '{group_name}'
                          AND feature_name = '{feat_name}'
                        ORDER BY entity_id, feature_time
                    ) f ON l.entity_id = f.entity_id AND l.event_time >= f.feature_time
                """)
                if not pit_df.empty:
                    result = result.merge(pit_df, on=['entity_id', 'event_time'], how='left')
            except Exception as e:
                log.debug('特征 %s.%s PIT 查询失败：%s', group_name, feat_name, e)
                result[f'{group_name}__{feat_name}'] = None

        return result

    def _apply_defaults(self, df, feature_groups: list[str]):
        """用特征契约中的默认值填充 NaN"""
        try:
            contracts = self._ch.query(f"""
                SELECT group_name, feature_name, default_value_float
                FROM feature_store.feature_contracts
                WHERE group_name IN ('{"','".join(feature_groups)}')
            """).result_rows
            for r in contracts:
                col = f'{r[0]}__{r[1]}'
                if col in df.columns:
                    df[col] = df[col].fillna(float(r[2]))
        except Exception as e:
            log.debug('应用默认值失败：%s', e)
        return df

    def _register_dataset(self, dataset_id, name, desc, feature_groups,
                           label_sql, label_col, start_time, end_time,
                           row_count, file_path, created_by):
        try:
            self._ch.insert('feature_store.training_datasets',
                [[dataset_id, name, desc, feature_groups, label_sql[:500], label_col,
                  start_time, end_time, row_count, file_path, 'completed', created_by,
                  datetime.now()]],
                column_names=['dataset_id', 'dataset_name', 'description', 'feature_groups',
                              'label_table', 'label_column', 'start_time', 'end_time',
                              'row_count', 'file_path', 'status', 'created_by', 'created_at'])
        except Exception as e:
            log.error('注册数据集元数据失败：%s', e)

    def list_datasets(self) -> list[dict]:
        try:
            rows = self._ch.query("""
                SELECT dataset_id, dataset_name, description, feature_groups,
                       row_count, file_path, status, created_at
                FROM feature_store.training_datasets
                ORDER BY created_at DESC LIMIT 20
            """).result_rows
            return [{'id': r[0][:8], 'name': r[1], 'desc': r[2],
                     'groups': list(r[3]), 'rows': r[4],
                     'path': r[5], 'status': r[6], 'created_at': str(r[7])} for r in rows]
        except Exception as e:
            log.error('列举数据集失败：%s', e)
            return []
