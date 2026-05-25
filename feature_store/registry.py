#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征注册中心 — Feature Registry
声明式特征定义（YAML）→ ClickHouse 元数据表

核心职责：
  1. 扫描 features/*.yaml，解析特征组定义
  2. 注册/更新到 feature_store.feature_definitions
  3. 写入特征契约（默认值、SLA、失效策略）
  4. 记录特征血缘（source_table → feature）
"""
import os
import sys
import uuid
import yaml
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('feature_registry')
_FEATURES_DIR = os.path.join(os.path.dirname(__file__), '..', 'features')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


class FeatureRegistry:
    """
    特征注册中心：管理特征定义的生命周期。
    - 从 YAML 文件加载特征组定义
    - 将定义持久化到 ClickHouse 元数据表
    - 提供特征查询接口
    """

    def __init__(self):
        self._ch = None
        self._cache: dict = {}  # group_name → {feature_name → definition}

    def _conn(self):
        """获取或重连 ClickHouse 客户端"""
        if self._ch is None:
            self._ch = _get_ch()
        return self._ch

    def load_all(self) -> int:
        """
        扫描 features/ 目录，加载所有 YAML 特征定义并注册到 ClickHouse。

        Returns:
            int: 成功注册的特征总数
        """
        features_path = Path(_FEATURES_DIR)
        if not features_path.exists():
            log.warning('features/ 目录不存在：%s', _FEATURES_DIR)
            return 0

        loaded = 0
        yaml_files = list(features_path.glob('*.yaml')) + list(features_path.glob('*.yml'))
        if not yaml_files:
            log.warning('features/ 目录下未发现 YAML 文件')
            return 0

        for yaml_file in sorted(yaml_files):
            try:
                with open(yaml_file, encoding='utf-8') as f:
                    group_def = yaml.safe_load(f)
                if not group_def or 'feature_group' not in group_def:
                    log.warning('跳过无效 YAML（缺少 feature_group 字段）：%s', yaml_file)
                    continue
                self._register_group(group_def, str(yaml_file))
                count = len(group_def.get('features', []))
                loaded += count
                log.info('已加载特征组 %s（%d 个特征）', group_def['feature_group'], count)
            except yaml.YAMLError as e:
                log.error('YAML 解析失败 %s：%s', yaml_file, e)
            except Exception as e:
                log.error('加载 %s 失败：%s', yaml_file, e, exc_info=True)

        log.info('特征加载完成，共注册 %d 个特征', loaded)
        return loaded

    def _register_group(self, group_def: dict, source_file: str):
        """
        注册单个特征组及其所有特征到 ClickHouse。
        包括：feature_groups、feature_definitions、feature_contracts、feature_lineage
        """
        ch = self._conn()
        group_name = group_def['feature_group']
        entity_key = group_def.get('entity_key', 'entity_id')
        desc = group_def.get('description', '')
        owner = group_def.get('owner', 'system')
        now = datetime.now()

        # 注册/更新特征组元数据
        try:
            ch.insert(
                'feature_store.feature_groups',
                [[group_name, entity_key, desc, owner, now, now]],
                column_names=['group_name', 'entity_key', 'description', 'owner',
                              'created_at', 'updated_at'],
            )
        except Exception as e:
            log.warning('写入 feature_groups 失败（group=%s）：%s', group_name, e)

        # 注册每个特征及其血缘
        source_tables = group_def.get('source_tables', [])
        for feat in group_def.get('features', []):
            self._register_feature(group_name, feat)
            # 记录血缘：source_table → feature_group
            for src_table in source_tables:
                try:
                    ch.insert(
                        'feature_store.feature_lineage',
                        [[
                            str(uuid.uuid4()),
                            'clickhouse_table',
                            src_table,
                            'feature_group',
                            f'{group_name}.{feat["name"]}',
                            feat.get('computation_sql', ''),
                            now,
                        ]],
                        column_names=[
                            'lineage_id', 'source_type', 'source_name',
                            'target_type', 'target_name',
                            'transformation_sql', 'recorded_at',
                        ],
                    )
                except Exception as e:
                    log.debug('写入 feature_lineage 失败（%s.%s）：%s', group_name, feat.get('name'), e)

        # 更新内存缓存
        self._cache[group_name] = {
            f['name']: f for f in group_def.get('features', [])
        }

    def _register_feature(self, group_name: str, feat: dict):
        """
        注册单个特征定义到 feature_definitions 和 feature_contracts 表。
        """
        ch = self._conn()
        now = datetime.now()
        tags = feat.get('tags', [])
        if not isinstance(tags, list):
            tags = [str(tags)]

        # 写入 feature_definitions
        try:
            ch.insert(
                'feature_store.feature_definitions',
                [[
                    str(uuid.uuid4()),
                    group_name,
                    feat['name'],
                    feat.get('type', 'FLOAT64'),
                    feat.get('description', ''),
                    feat.get('computation_sql', ''),
                    feat.get('refresh_schedule', '*/5 * * * *'),
                    int(feat.get('online_ttl', 3600)),
                    str(feat.get('default_value', '0')),
                    int(feat.get('max_staleness_seconds', 3600)),
                    1,   # version
                    1,   # is_active
                    tags,
                    now,
                    now,
                ]],
                column_names=[
                    'feature_id', 'group_name', 'feature_name', 'feature_type',
                    'description', 'computation_sql', 'refresh_schedule',
                    'online_ttl', 'default_value', 'max_staleness_seconds',
                    'version', 'is_active', 'tags', 'created_at', 'updated_at',
                ],
            )
        except Exception as e:
            log.error('写入 feature_definitions 失败（%s.%s）：%s', group_name, feat.get('name'), e)

        # 写入 feature_contracts（SLA 与默认值契约）
        try:
            default_raw = str(feat.get('default_value', '0'))
            # 尝试解析为浮点，失败则用 0.0
            try:
                default_float = float(default_raw)
            except (ValueError, TypeError):
                default_float = 0.0

            ch.insert(
                'feature_store.feature_contracts',
                [[
                    group_name,
                    feat['name'],
                    default_float,
                    default_raw,
                    int(feat.get('max_staleness_seconds', 3600)),
                    float(feat.get('min_coverage_pct', 0.9)),
                    int(feat.get('sla_freshness_seconds', 300)),
                    feat.get('on_breach_action', 'use_default'),
                    now,
                ]],
                column_names=[
                    'group_name', 'feature_name',
                    'default_value_float', 'default_value_str',
                    'max_staleness_seconds', 'min_coverage_pct',
                    'sla_freshness_seconds', 'on_breach_action', 'updated_at',
                ],
            )
        except Exception as e:
            log.debug('写入 feature_contracts 失败（%s.%s）：%s', group_name, feat.get('name'), e)

    def get_feature_def(self, group_name: str, feature_name: str) -> Optional[dict]:
        """
        获取特征定义，优先查缓存，缓存未命中则查询 ClickHouse。

        Args:
            group_name: 特征组名称
            feature_name: 特征名称

        Returns:
            特征定义字典，未找到时返回 None
        """
        # 先查内存缓存
        if group_name in self._cache and feature_name in self._cache[group_name]:
            return self._cache[group_name][feature_name]

        # 缓存未命中，查询 ClickHouse
        try:
            rows = self._conn().query(
                """
                SELECT feature_name, feature_type, description, computation_sql,
                       online_ttl, default_value, max_staleness_seconds, tags
                FROM feature_store.feature_definitions
                WHERE group_name = {group_name:String}
                  AND feature_name = {feature_name:String}
                  AND is_active = 1
                LIMIT 1
                """,
                parameters={'group_name': group_name, 'feature_name': feature_name},
            ).result_rows

            if rows:
                r = rows[0]
                feat_def = {
                    'name': r[0],
                    'type': r[1],
                    'description': r[2],
                    'computation_sql': r[3],
                    'online_ttl': r[4],
                    'default_value': r[5],
                    'max_staleness_seconds': r[6],
                    'tags': list(r[7]) if r[7] else [],
                }
                # 回填缓存
                self._cache.setdefault(group_name, {})[feature_name] = feat_def
                return feat_def

        except Exception as e:
            log.error('查询特征定义失败（%s.%s）：%s', group_name, feature_name, e)

        return None

    def list_feature_groups(self) -> list[dict]:
        """
        列举所有特征组及其特征数量。

        Returns:
            特征组信息列表，每项包含 group_name、entity_key、description、feature_count
        """
        try:
            rows = self._conn().query(
                """
                SELECT
                    g.group_name,
                    g.entity_key,
                    g.description,
                    countIf(d.is_active = 1) AS feature_count
                FROM feature_store.feature_groups g
                LEFT JOIN feature_store.feature_definitions d
                       ON g.group_name = d.group_name
                GROUP BY g.group_name, g.entity_key, g.description
                ORDER BY g.group_name
                """
            ).result_rows
            return [
                {
                    'group_name': r[0],
                    'entity_key': r[1],
                    'description': r[2],
                    'feature_count': r[3],
                }
                for r in rows
            ]
        except Exception as e:
            log.error('列举特征组失败：%s', e)
            return []

    def list_features(self, group_name: Optional[str] = None) -> list[dict]:
        """
        列举特征，可按特征组过滤。

        Args:
            group_name: 可选，指定特征组名称过滤

        Returns:
            特征信息列表
        """
        try:
            if group_name:
                rows = self._conn().query(
                    """
                    SELECT group_name, feature_name, feature_type, description,
                           online_ttl, max_staleness_seconds, tags
                    FROM feature_store.feature_definitions
                    WHERE group_name = {group_name:String} AND is_active = 1
                    ORDER BY feature_name
                    """,
                    parameters={'group_name': group_name},
                ).result_rows
            else:
                rows = self._conn().query(
                    """
                    SELECT group_name, feature_name, feature_type, description,
                           online_ttl, max_staleness_seconds, tags
                    FROM feature_store.feature_definitions
                    WHERE is_active = 1
                    ORDER BY group_name, feature_name
                    """
                ).result_rows

            return [
                {
                    'group': r[0],
                    'name': r[1],
                    'type': r[2],
                    'description': r[3],
                    'online_ttl': r[4],
                    'max_staleness': r[5],
                    'tags': list(r[6]) if r[6] else [],
                }
                for r in rows
            ]
        except Exception as e:
            log.error('列举特征失败：%s', e)
            return []

    def invalidate_cache(self, group_name: Optional[str] = None):
        """清除内存缓存，可按特征组清除或全量清除"""
        if group_name:
            self._cache.pop(group_name, None)
            log.debug('已清除特征组缓存：%s', group_name)
        else:
            self._cache.clear()
            log.debug('已清除所有特征缓存')


# ── 全局单例 ────────────────────────────────────────────────────────────────
_registry: Optional[FeatureRegistry] = None


def get_registry() -> FeatureRegistry:
    """获取全局 FeatureRegistry 单例"""
    global _registry
    if _registry is None:
        _registry = FeatureRegistry()
    return _registry


if __name__ == '__main__':
    reg = FeatureRegistry()
    n = reg.load_all()
    print(f'已注册 {n} 个特征')
    for g in reg.list_feature_groups():
        print(f"  {g['group_name']} ({g['feature_count']} 个特征): {g['description']}")
