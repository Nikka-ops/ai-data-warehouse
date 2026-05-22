#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Point-in-Time 正确训练集生成 — PIT Join
解决训练数据集构建中的"数据穿越"（Data Leakage）问题。

核心设计：
  对于每个 (entity_id, event_time) 标签样本，
  仅使用 event_time 之前的最新特征值，严格避免未来信息泄露。

实现方式：
  利用 ClickHouse ASOF JOIN（按时间最近匹配）在服务端完成 PIT 关联，
  避免将全量特征数据拉到 Python 端做内存 merge，大幅节约网络带宽。

典型使用场景：
  - 用户流失预测：以用户退订事件为标签，关联事件发生前7天的行为特征
  - 欺诈检测：以支付事件为标签，关联当时的风险特征快照
  - 推荐系统：以点击/购买事件为标签，关联商品/用户实时特征
"""
import os
import sys
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('pit_join')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=600,
    )


class PITJoiner:
    """
    Point-in-Time 正确数据集生成器。
    将标签 DataFrame 与特征存储按时间点正确地关联，生成训练集。
    """

    def __init__(self):
        self._ch = None

    def _conn(self):
        if self._ch is None:
            self._ch = _get_ch()
        return self._ch

    # ──────────────────────────────────────────────────────────────────────────
    # PIT Join 主接口
    # ──────────────────────────────────────────────────────────────────────────

    def pit_join(
        self,
        label_df,
        group_name: str,
        feature_names: list[str],
        entity_col: str = 'entity_id',
        timestamp_col: str = 'event_time',
    ):
        """
        对标签 DataFrame 执行 Point-in-Time 正确的特征关联。

        对 label_df 中的每个 (entity_id, event_time) 行，
        查找 feature_store.feature_values 中该时刻之前最近的特征值。

        Args:
            label_df:      pandas DataFrame，至少包含 entity_col 和 timestamp_col 列
            group_name:    特征组名称
            feature_names: 需要关联的特征列表
            entity_col:    实体 ID 列名，默认 'entity_id'
            timestamp_col: 事件时间列名，默认 'event_time'

        Returns:
            pandas.DataFrame: label_df 的所有列 + 每个 feature_name 列
                              缺失特征值用 NaN 填充

        Raises:
            ValueError: 缺少必要列或特征列表为空
            RuntimeError: pandas 未安装
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('pandas 未安装，请执行 pip install pandas')

        if label_df is None or len(label_df) == 0:
            log.warning('label_df 为空，返回空 DataFrame')
            return pd.DataFrame(columns=[entity_col, timestamp_col] + list(feature_names))

        if entity_col not in label_df.columns:
            raise ValueError(f'label_df 缺少实体列：{entity_col}')
        if timestamp_col not in label_df.columns:
            raise ValueError(f'label_df 缺少时间列：{timestamp_col}')
        if not feature_names:
            raise ValueError('feature_names 不能为空')

        log.info(
            'PIT Join 开始：group=%s，特征数=%d，样本数=%d',
            group_name, len(feature_names), len(label_df),
        )

        # 将 label_df 上传到 ClickHouse 临时表，再做 ASOF JOIN
        # 使用内存表引擎（Memory）避免磁盘 IO，适合百万级以下样本
        results = []
        for fn in feature_names:
            joined = self._asof_join_single_feature(
                label_df, group_name, fn, entity_col, timestamp_col
            )
            results.append(joined)

        # 合并所有特征列（以 label_df 为基准 LEFT JOIN）
        merged = label_df.copy()
        for feat_col_df in results:
            merge_on = [entity_col, timestamp_col]
            merged = merged.merge(feat_col_df, on=merge_on, how='left')

        log.info(
            'PIT Join 完成：输出 %d 行 x %d 列',
            len(merged), len(merged.columns),
        )
        return merged

    def _asof_join_single_feature(
        self,
        label_df,
        group_name: str,
        feature_name: str,
        entity_col: str,
        timestamp_col: str,
    ):
        """
        对单个特征执行 ASOF JOIN，返回仅含 entity_col、timestamp_col、feature_name 三列的 DataFrame。

        通过 ClickHouse WITH + arrayJoin 将 Python 侧的标签数据传入，
        在服务端完成时间匹配，避免大量数据拉取到客户端。
        """
        import pandas as pd

        rows = label_df[[entity_col, timestamp_col]].drop_duplicates()

        # 将 (entity_id, as_of_time) 对编码为 ClickHouse array 字面量
        # 格式：[('e1', '2024-01-01 00:00:00'), ...]
        pair_literals = ', '.join(
            f"('{row[entity_col]}', toDateTime('{_fmt_dt(row[timestamp_col])}'))"
            for _, row in rows.iterrows()
        )

        if not pair_literals:
            return pd.DataFrame(columns=[entity_col, timestamp_col, feature_name])

        # ClickHouse ASOF JOIN：对每个 (entity_id, as_of_time) 找到最近的历史特征值
        # ASOF JOIN 要求 feature_values 按 (entity_id, feature_time) 排序
        asof_sql = f"""
        WITH labels AS (
            SELECT
                tup.1 AS entity_id,
                tup.2 AS as_of_time
            FROM (
                SELECT arrayJoin([{pair_literals}]) AS tup
            )
        )
        SELECT
            l.entity_id,
            l.as_of_time  AS event_time,
            f.feature_value
        FROM labels AS l
        ASOF JOIN (
            SELECT
                entity_id,
                feature_value,
                feature_time
            FROM feature_store.feature_values
            WHERE group_name   = '{group_name}'
              AND feature_name = '{feature_name}'
            ORDER BY entity_id, feature_time
        ) AS f
        ON  l.entity_id = f.entity_id
        AND l.as_of_time >= f.feature_time
        """

        try:
            result_rows = self._conn().query(asof_sql).result_rows
        except Exception as e:
            log.error('ASOF JOIN 失败（%s.%s）：%s', group_name, feature_name, e)
            # 返回全 NaN 列
            return rows.rename(columns={timestamp_col: timestamp_col}).assign(
                **{feature_name: float('nan')}
            )[[entity_col, timestamp_col, feature_name]]

        feat_df = pd.DataFrame(
            result_rows,
            columns=[entity_col, timestamp_col, feature_name],
        )

        # 确保时间列类型一致，便于后续 merge
        if pd.api.types.is_datetime64_any_dtype(label_df[timestamp_col]):
            feat_df[timestamp_col] = pd.to_datetime(feat_df[timestamp_col])

        log.debug(
            'ASOF JOIN 完成：%s.%s，命中 %d/%d 行',
            group_name, feature_name, len(feat_df), len(rows),
        )
        return feat_df

    # ──────────────────────────────────────────────────────────────────────────
    # 标签生成
    # ──────────────────────────────────────────────────────────────────────────

    def generate_training_labels(
        self,
        start_time: datetime,
        end_time: datetime,
        label_query: str,
        entity_col: str = 'entity_id',
        timestamp_col: str = 'event_time',
        label_col: str = 'label',
    ):
        """
        从 ClickHouse 执行自定义 SQL 生成标签 DataFrame。

        label_query 应返回至少以下三列：
            entity_id (String), event_time (DateTime), label (numeric/string)
        列名可通过 entity_col、timestamp_col、label_col 参数自定义。

        Args:
            start_time:    标签时间窗口起始
            end_time:      标签时间窗口结束
            label_query:   ClickHouse SQL，可使用 {start_time} {end_time} 占位符
            entity_col:    实体 ID 列名
            timestamp_col: 事件时间列名
            label_col:     标签值列名

        Returns:
            pandas.DataFrame: 标签数据，列为 [entity_col, timestamp_col, label_col]
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('pandas 未安装，请执行 pip install pandas')

        # 替换占位符
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        sql = label_query.replace('{start_time}', f"'{start_str}'")\
                         .replace('{end_time}', f"'{end_str}'")

        log.info('生成训练标签：%s ~ %s', start_str, end_str)

        try:
            result = self._conn().query(sql)
            col_names = [c.name for c in result.column_names] \
                if hasattr(result, 'column_names') else [entity_col, timestamp_col, label_col]
            df = pd.DataFrame(result.result_rows, columns=col_names)
            log.info('标签生成完成：%d 行，列：%s', len(df), list(df.columns))
            return df
        except Exception as e:
            log.error('标签生成失败：%s', e, exc_info=True)
            return pd.DataFrame(columns=[entity_col, timestamp_col, label_col])

    # ──────────────────────────────────────────────────────────────────────────
    # 训练数据集注册
    # ──────────────────────────────────────────────────────────────────────────

    def register_dataset(
        self,
        dataset_name: str,
        feature_groups: list[str],
        label_table: str,
        label_column: str,
        start_time: datetime,
        end_time: datetime,
        row_count: int,
        file_path: str = '',
        description: str = '',
        created_by: str = 'system',
    ) -> str:
        """
        将生成的训练数据集元数据注册到 feature_store.training_datasets。

        Args:
            dataset_name:   数据集名称（如 'churn_train_2024q1'）
            feature_groups: 使用的特征组列表
            label_table:    标签来源表（ClickHouse 全路径）
            label_column:   标签字段名
            start_time:     训练数据时间窗口起始
            end_time:       训练数据时间窗口结束
            row_count:      数据集行数
            file_path:      数据集文件路径（可选）
            description:    数据集描述
            created_by:     创建者

        Returns:
            str: 数据集 ID（UUID）
        """
        import uuid
        dataset_id = str(uuid.uuid4())
        now = datetime.now()

        try:
            self._conn().insert(
                'feature_store.training_datasets',
                [[
                    dataset_id, dataset_name, description,
                    feature_groups, label_table, label_column,
                    start_time, end_time, row_count, file_path,
                    'ready', created_by, now,
                ]],
                column_names=[
                    'dataset_id', 'dataset_name', 'description',
                    'feature_groups', 'label_table', 'label_column',
                    'start_time', 'end_time', 'row_count', 'file_path',
                    'status', 'created_by', 'created_at',
                ],
            )
            log.info('数据集 %s 已注册（id=%s，行数=%d）', dataset_name, dataset_id, row_count)
            return dataset_id
        except Exception as e:
            log.error('数据集注册失败（%s）：%s', dataset_name, e)
            return ''

    def build_training_dataset(
        self,
        dataset_name: str,
        group_name: str,
        feature_names: list[str],
        label_query: str,
        start_time: datetime,
        end_time: datetime,
        label_table: str = '',
        label_column: str = 'label',
        entity_col: str = 'entity_id',
        timestamp_col: str = 'event_time',
        output_path: str = '',
        created_by: str = 'system',
    ):
        """
        完整的训练数据集构建流程：
          1. 生成标签 DataFrame
          2. PIT Join 特征
          3. 可选保存为 Parquet
          4. 注册元数据到 ClickHouse

        Args:
            dataset_name:  数据集名称
            group_name:    特征组名称
            feature_names: 特征列表
            label_query:   标签生成 SQL（含 {start_time} {end_time} 占位符）
            start_time:    时间窗口起始
            end_time:      时间窗口结束
            label_table:   标签表名（元数据）
            label_column:  标签列名
            entity_col:    实体列名
            timestamp_col: 时间列名
            output_path:   Parquet 输出路径（可选）
            created_by:    创建者

        Returns:
            pandas.DataFrame: 完整的训练数据集
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('pandas 未安装，请执行 pip install pandas')

        log.info('开始构建训练数据集：%s', dataset_name)

        # Step 1: 生成标签
        label_df = self.generate_training_labels(
            start_time, end_time, label_query,
            entity_col=entity_col,
            timestamp_col=timestamp_col,
            label_col=label_column,
        )
        if label_df.empty:
            log.warning('标签数据为空，中止构建：%s', dataset_name)
            return label_df

        # Step 2: PIT Join 特征
        full_df = self.pit_join(
            label_df,
            group_name,
            feature_names,
            entity_col=entity_col,
            timestamp_col=timestamp_col,
        )

        # Step 3: 保存为 Parquet（可选）
        actual_path = output_path
        if output_path:
            try:
                full_df.to_parquet(output_path, index=False)
                log.info('训练数据集已保存：%s（%d 行）', output_path, len(full_df))
            except Exception as e:
                log.warning('Parquet 保存失败：%s', e)
                actual_path = ''

        # Step 4: 注册元数据
        self.register_dataset(
            dataset_name=dataset_name,
            feature_groups=[group_name],
            label_table=label_table or 'unknown',
            label_column=label_column,
            start_time=start_time,
            end_time=end_time,
            row_count=len(full_df),
            file_path=actual_path,
            created_by=created_by,
        )

        log.info('训练数据集构建完成：%s，共 %d 行', dataset_name, len(full_df))
        return full_df


def _fmt_dt(val) -> str:
    """将 Python datetime / pandas Timestamp / 字符串统一格式化为 ClickHouse DateTime 字符串"""
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    return str(val)


# ── 全局单例 ────────────────────────────────────────────────────────────────
_pit_joiner: Optional[PITJoiner] = None


def get_pit_joiner() -> PITJoiner:
    """获取全局 PITJoiner 单例"""
    global _pit_joiner
    if _pit_joiner is None:
        _pit_joiner = PITJoiner()
    return _pit_joiner


# ── 便捷函数（模块级 API）────────────────────────────────────────────────────

def pit_join(
    label_df,
    group_name: str,
    feature_names: list[str],
    entity_col: str = 'entity_id',
    timestamp_col: str = 'event_time',
):
    """
    模块级便捷函数，等价于 get_pit_joiner().pit_join(...)

    Args:
        label_df:      标签 DataFrame，必须含 entity_col 和 timestamp_col
        group_name:    特征组名称
        feature_names: 需要关联的特征列表
        entity_col:    实体 ID 列名
        timestamp_col: 事件时间列名

    Returns:
        pandas.DataFrame: label_df + 所有特征列（PIT 正确）
    """
    return get_pit_joiner().pit_join(
        label_df, group_name, feature_names, entity_col, timestamp_col
    )


def generate_training_labels(
    start_time: datetime,
    end_time: datetime,
    label_query: str,
):
    """
    模块级便捷函数，从 ClickHouse 生成标签 DataFrame。

    Args:
        start_time:  时间窗口起始
        end_time:    时间窗口结束
        label_query: 标签 SQL（含 {start_time} {end_time} 占位符）

    Returns:
        pandas.DataFrame
    """
    return get_pit_joiner().generate_training_labels(start_time, end_time, label_query)


if __name__ == '__main__':
    import pandas as pd
    from datetime import timedelta

    # 构造示例标签数据
    sample_labels = pd.DataFrame({
        'entity_id': ['user_001', 'user_002', 'user_003'],
        'event_time': [
            datetime(2024, 6, 1, 12, 0, 0),
            datetime(2024, 6, 2, 10, 0, 0),
            datetime(2024, 6, 3, 8, 0, 0),
        ],
        'label': [1, 0, 1],
    })

    joiner = PITJoiner()
    result = joiner.pit_join(
        sample_labels,
        group_name='user_behavior',
        feature_names=['order_count_7d', 'total_amount_30d'],
    )
    print(result)
