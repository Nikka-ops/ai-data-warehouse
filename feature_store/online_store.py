#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在线特征存储 — Online Feature Store（Redis 后端）
提供毫秒级低延迟的在线特征读写服务。

核心职责：
  1. 以 Redis Hash 存储实体特征，TTL 由 feature_contracts 控制
  2. 批量读写（pipeline）降低网络往返延迟
  3. 缓存未命中时回退到 ClickHouse 离线存储（Fallback）
  4. 维护命中率统计，供 Prometheus 指标暴露

Redis Key 规范：
  feat:{group_name}:{feature_name}:{entity_id}  → String（特征值）
  feat_meta:{group_name}:{entity_id}            → Hash（元数据：last_update）
"""
import os
import sys
import json
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('online_store')

# Redis 连接配置（从环境变量读取）
_REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
_REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
_REDIS_DB = int(os.getenv('REDIS_DB', '0'))
_REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Key 前缀
_KEY_PREFIX = 'feat'
_META_PREFIX = 'feat_meta'


def _redis_key(group_name: str, feature_name: str, entity_id: str) -> str:
    """构造特征值 Redis Key"""
    return f'{_KEY_PREFIX}:{group_name}:{feature_name}:{entity_id}'


def _meta_key(group_name: str, entity_id: str) -> str:
    """构造特征元数据 Redis Key"""
    return f'{_META_PREFIX}:{group_name}:{entity_id}'


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


class OnlineFeatureStore:
    """
    在线特征存储：Redis 为主缓存，ClickHouse 为兜底。
    """

    def __init__(self):
        self._redis = None
        self._ch = None
        # 命中率统计
        self._hits = 0
        self._misses = 0
        self._fallback_hits = 0

    def _get_redis(self):
        """懒加载 Redis 客户端，连接失败时抛出明确错误"""
        if self._redis is None:
            try:
                import redis
                self._redis = redis.Redis(
                    host=_REDIS_HOST,
                    port=_REDIS_PORT,
                    db=_REDIS_DB,
                    password=_REDIS_PASSWORD,
                    decode_responses=True,
                    socket_timeout=2,
                    socket_connect_timeout=3,
                    retry_on_timeout=True,
                )
                self._redis.ping()
                log.info('Redis 连接成功：%s:%d/db%d', _REDIS_HOST, _REDIS_PORT, _REDIS_DB)
            except ImportError:
                raise RuntimeError('redis-py 未安装，请执行 pip install redis')
            except Exception as e:
                self._redis = None
                raise ConnectionError(f'Redis 连接失败 ({_REDIS_HOST}:{_REDIS_PORT})：{e}')
        return self._redis

    def _get_ch(self):
        """懒加载 ClickHouse 客户端（用于 Fallback 查询）"""
        if self._ch is None:
            self._ch = _get_ch()
        return self._ch

    def _load_contracts(self, group_name: str) -> dict:
        """
        从 ClickHouse 加载特征契约（默认值），用于缓存未命中时的降级处理。

        Returns:
            dict: {feature_name: {'default_float': float, 'default_str': str}}
        """
        try:
            rows = self._get_ch().query(
                """
                SELECT feature_name, default_value_float, default_value_str
                FROM feature_store.feature_contracts
                WHERE group_name = {g:String}
                """,
                parameters={'g': group_name},
            ).result_rows
            return {
                r[0]: {'default_float': r[1], 'default_str': r[2]}
                for r in rows
            }
        except Exception as e:
            log.warning('加载特征契约失败（%s）：%s', group_name, e)
            return {}

    # ──────────────────────────────────────────────────────────────────────────
    # 写操作
    # ──────────────────────────────────────────────────────────────────────────

    def set_features(
        self,
        entity_id: str,
        group_name: str,
        features_dict: dict,
        ttl: int = 3600,
    ) -> bool:
        """
        将实体的特征值写入 Redis。

        Args:
            entity_id:     实体 ID
            group_name:    特征组名称
            features_dict: {feature_name: value} 字典
            ttl:           过期时间（秒），默认 3600

        Returns:
            bool: 写入是否成功
        """
        if not features_dict:
            return True

        try:
            r = self._get_redis()
            pipe = r.pipeline(transaction=False)

            ts = datetime.now().isoformat()
            for feat_name, value in features_dict.items():
                key = _redis_key(group_name, feat_name, entity_id)
                # 统一序列化为字符串
                str_val = json.dumps(value) if not isinstance(value, str) else value
                pipe.set(key, str_val, ex=ttl)

            # 更新元数据（最后写入时间）
            meta_k = _meta_key(group_name, entity_id)
            pipe.hset(meta_k, mapping={'last_update': ts, 'ttl': str(ttl)})
            pipe.expire(meta_k, ttl)

            pipe.execute()
            log.debug('写入 Redis 成功：entity=%s group=%s feat_count=%d',
                      entity_id, group_name, len(features_dict))
            return True

        except Exception as e:
            log.error('Redis 写入失败（entity=%s group=%s）：%s', entity_id, group_name, e)
            return False

    def set_features_batch(
        self,
        records: list[tuple[str, str, dict]],
        ttl: int = 3600,
    ) -> int:
        """
        批量写入多个实体的特征（单次 pipeline）。

        Args:
            records: [(entity_id, group_name, features_dict), ...]
            ttl:     过期时间（秒）

        Returns:
            int: 成功写入的实体数
        """
        if not records:
            return 0

        try:
            r = self._get_redis()
            pipe = r.pipeline(transaction=False)
            ts = datetime.now().isoformat()

            for entity_id, group_name, features_dict in records:
                for feat_name, value in features_dict.items():
                    key = _redis_key(group_name, feat_name, entity_id)
                    str_val = json.dumps(value) if not isinstance(value, str) else value
                    pipe.set(key, str_val, ex=ttl)

                meta_k = _meta_key(group_name, entity_id)
                pipe.hset(meta_k, mapping={'last_update': ts, 'ttl': str(ttl)})
                pipe.expire(meta_k, ttl)

            pipe.execute()
            log.debug('批量写入 Redis：%d 条记录', len(records))
            return len(records)

        except Exception as e:
            log.error('Redis 批量写入失败：%s', e)
            return 0

    # ──────────────────────────────────────────────────────────────────────────
    # 读操作
    # ──────────────────────────────────────────────────────────────────────────

    def get_features(
        self,
        entity_id: str,
        group_name: str,
        feature_names: Optional[list[str]] = None,
    ) -> dict:
        """
        读取单个实体的特征值。

        缓存未命中时：
          1. 查询 ClickHouse feature_values 离线存储（Fallback）
          2. 若离线也无数据，则使用 feature_contracts 中的默认值

        Args:
            entity_id:     实体 ID
            group_name:    特征组名称
            feature_names: 需要的特征列表；None 表示从契约中获取所有已知特征

        Returns:
            dict: {feature_name: value}
        """
        # 若未指定特征名，从契约表获取
        if feature_names is None:
            contracts = self._load_contracts(group_name)
            feature_names = list(contracts.keys())
        else:
            contracts = None

        if not feature_names:
            return {}

        result = {}
        missing = []

        # 尝试从 Redis 读取
        try:
            r = self._get_redis()
            pipe = r.pipeline(transaction=False)
            for fn in feature_names:
                pipe.get(_redis_key(group_name, fn, entity_id))
            raw_values = pipe.execute()

            for fn, raw in zip(feature_names, raw_values):
                if raw is not None:
                    try:
                        result[fn] = json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        result[fn] = raw
                    self._hits += 1
                else:
                    missing.append(fn)
                    self._misses += 1

        except Exception as e:
            log.warning('Redis 读取失败，降级到 ClickHouse：%s', e)
            missing = list(feature_names)

        # Fallback：从 ClickHouse 查询缺失特征
        if missing:
            fallback = self._fallback_from_clickhouse(entity_id, group_name, missing)
            result.update(fallback)
            self._fallback_hits += len(fallback)

            # 仍然缺失的特征使用默认值
            still_missing = set(missing) - set(fallback.keys())
            if still_missing:
                if contracts is None:
                    contracts = self._load_contracts(group_name)
                for fn in still_missing:
                    if fn in contracts:
                        result[fn] = contracts[fn]['default_float']
                    else:
                        result[fn] = 0.0

        return result

    def get_multi_entity_features(
        self,
        entity_ids: list[str],
        group_name: str,
        feature_names: Optional[list[str]] = None,
    ) -> dict[str, dict]:
        """
        批量读取多个实体的特征（Redis pipeline 优化）。

        Args:
            entity_ids:    实体 ID 列表
            group_name:    特征组名称
            feature_names: 需要的特征列表

        Returns:
            dict: {entity_id: {feature_name: value}}
        """
        if not entity_ids:
            return {}

        if feature_names is None:
            contracts = self._load_contracts(group_name)
            feature_names = list(contracts.keys())
        else:
            contracts = None

        if not feature_names:
            return {eid: {} for eid in entity_ids}

        # 初始化结果结构
        results: dict[str, dict] = {eid: {} for eid in entity_ids}
        missing_pairs: list[tuple[str, str]] = []  # (entity_id, feature_name)

        # 批量 Redis 读取
        try:
            r = self._get_redis()
            pipe = r.pipeline(transaction=False)
            # 按 (entity, feature) 顺序发送 GET 命令
            ordered_pairs = [(eid, fn) for eid in entity_ids for fn in feature_names]
            for eid, fn in ordered_pairs:
                pipe.get(_redis_key(group_name, fn, eid))
            raw_values = pipe.execute()

            for (eid, fn), raw in zip(ordered_pairs, raw_values):
                if raw is not None:
                    try:
                        results[eid][fn] = json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        results[eid][fn] = raw
                    self._hits += 1
                else:
                    missing_pairs.append((eid, fn))
                    self._misses += 1

        except Exception as e:
            log.warning('批量 Redis 读取失败，降级到 ClickHouse：%s', e)
            missing_pairs = [(eid, fn) for eid in entity_ids for fn in feature_names]

        # Fallback：从 ClickHouse 补全缺失值
        if missing_pairs:
            missing_entities = list({eid for eid, _ in missing_pairs})
            missing_feats = list({fn for _, fn in missing_pairs})
            fallback = self._fallback_batch_from_clickhouse(
                missing_entities, group_name, missing_feats
            )
            # 合并结果
            for eid in missing_entities:
                if eid in fallback:
                    results[eid].update(fallback[eid])

            # 仍缺失的用默认值填充
            if contracts is None:
                contracts = self._load_contracts(group_name)
            for eid, fn in missing_pairs:
                if fn not in results[eid]:
                    results[eid][fn] = (
                        contracts[fn]['default_float'] if fn in contracts else 0.0
                    )

        return results

    def _fallback_from_clickhouse(
        self,
        entity_id: str,
        group_name: str,
        feature_names: list[str],
    ) -> dict:
        """从 ClickHouse 查询单实体的最新特征值（Fallback）"""
        feat_list = ', '.join(f"'{fn}'" for fn in feature_names)
        try:
            rows = self._get_ch().query(
                f"""
                SELECT feature_name,
                       argMax(feature_value, feature_time) AS val
                FROM feature_store.feature_values
                WHERE entity_id = '{entity_id}'
                  AND group_name = '{group_name}'
                  AND feature_name IN ({feat_list})
                GROUP BY feature_name
                """
            ).result_rows
            return {r[0]: r[1] for r in rows}
        except Exception as e:
            log.debug('ClickHouse Fallback 失败（entity=%s group=%s）：%s',
                      entity_id, group_name, e)
            return {}

    def _fallback_batch_from_clickhouse(
        self,
        entity_ids: list[str],
        group_name: str,
        feature_names: list[str],
    ) -> dict[str, dict]:
        """从 ClickHouse 批量查询多实体的最新特征值（Fallback）"""
        entity_list = ', '.join(f"'{eid}'" for eid in entity_ids)
        feat_list = ', '.join(f"'{fn}'" for fn in feature_names)
        try:
            rows = self._get_ch().query(
                f"""
                SELECT entity_id, feature_name,
                       argMax(feature_value, feature_time) AS val
                FROM feature_store.feature_values
                WHERE entity_id IN ({entity_list})
                  AND group_name = '{group_name}'
                  AND feature_name IN ({feat_list})
                GROUP BY entity_id, feature_name
                """
            ).result_rows
            result: dict[str, dict] = {}
            for eid, fn, val in rows:
                result.setdefault(eid, {})[fn] = val
            return result
        except Exception as e:
            log.debug('ClickHouse 批量 Fallback 失败（group=%s）：%s', group_name, e)
            return {}

    # ──────────────────────────────────────────────────────────────────────────
    # 离线同步到在线（Offline → Online Sync）
    # ──────────────────────────────────────────────────────────────────────────

    def sync_from_offline(
        self,
        group_name: str,
        feature_name: str,
        limit: int = 10_000,
    ) -> int:
        """
        将 ClickHouse 离线最新特征值同步到 Redis 在线存储。

        适合全量刷新场景（如每日凌晨全量推送），避免在线冷启动。

        Args:
            group_name:   特征组名称
            feature_name: 特征名称
            limit:        同步的最大实体数

        Returns:
            int: 成功同步的实体数
        """
        # 查询契约确定 TTL
        try:
            ttl_rows = self._get_ch().query(
                """
                SELECT online_ttl
                FROM feature_store.feature_definitions
                WHERE group_name = {g:String} AND feature_name = {f:String}
                  AND is_active = 1
                LIMIT 1
                """,
                parameters={'g': group_name, 'f': feature_name},
            ).result_rows
            ttl = int(ttl_rows[0][0]) if ttl_rows else 3600
        except Exception:
            ttl = 3600

        # 查询最新特征值（每实体取最新一条）
        try:
            rows = self._get_ch().query(
                f"""
                SELECT entity_id, argMax(feature_value, feature_time) AS val
                FROM feature_store.feature_values
                WHERE group_name = '{group_name}'
                  AND feature_name = '{feature_name}'
                  AND feature_time >= now() - INTERVAL {ttl} SECOND
                GROUP BY entity_id
                ORDER BY entity_id
                LIMIT {limit}
                """
            ).result_rows
        except Exception as e:
            log.error('查询离线特征失败（%s.%s）：%s', group_name, feature_name, e)
            return 0

        if not rows:
            log.info('离线特征 %s.%s 无数据可同步', group_name, feature_name)
            return 0

        # 批量写入 Redis
        try:
            r = self._get_redis()
            pipe = r.pipeline(transaction=False)
            for entity_id, val in rows:
                key = _redis_key(group_name, feature_name, entity_id)
                pipe.set(key, json.dumps(val), ex=ttl)
            pipe.execute()
            log.info(
                '离线→在线同步完成：%s.%s，写入 %d 条，TTL=%ds',
                group_name, feature_name, len(rows), ttl,
            )
            return len(rows)
        except Exception as e:
            log.error('Redis 批量写入失败（%s.%s）：%s', group_name, feature_name, e)
            return 0

    # ──────────────────────────────────────────────────────────────────────────
    # 新鲜度查询
    # ──────────────────────────────────────────────────────────────────────────

    def get_freshness(
        self,
        entity_id: str,
        group_name: str,
    ) -> dict:
        """
        获取实体特征的新鲜度信息。

        Returns:
            dict: {
                'entity_id': str,
                'group_name': str,
                'last_update': str (ISO datetime 或 None),
                'ttl_remaining': int (Redis TTL 秒数, -1=无限, -2=不存在),
                'feature_count': int (Redis 中该实体的特征数量),
            }
        """
        info = {
            'entity_id': entity_id,
            'group_name': group_name,
            'last_update': None,
            'ttl_remaining': -2,
            'feature_count': 0,
        }

        try:
            r = self._get_redis()

            # 元数据
            meta_k = _meta_key(group_name, entity_id)
            meta = r.hgetall(meta_k)
            if meta:
                info['last_update'] = meta.get('last_update')
                info['ttl_remaining'] = r.ttl(meta_k)

            # 统计该实体当前在 Redis 中的特征数量（SCAN 匹配）
            pattern = f'{_KEY_PREFIX}:{group_name}:*:{entity_id}'
            count = 0
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=pattern, count=100)
                count += len(keys)
                if cursor == 0:
                    break
            info['feature_count'] = count

        except Exception as e:
            log.warning('获取特征新鲜度失败（entity=%s group=%s）：%s',
                        entity_id, group_name, e)

        return info

    # ──────────────────────────────────────────────────────────────────────────
    # 统计
    # ──────────────────────────────────────────────────────────────────────────

    def get_hit_stats(self) -> dict:
        """
        返回当前累计的命中率统计。

        Returns:
            dict: {hits, misses, fallback_hits, hit_rate, total}
        """
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        return {
            'hits': self._hits,
            'misses': self._misses,
            'fallback_hits': self._fallback_hits,
            'hit_rate': round(hit_rate, 4),
            'total': total,
        }

    def reset_stats(self):
        """重置命中率统计计数器"""
        self._hits = 0
        self._misses = 0
        self._fallback_hits = 0


# ── 全局单例 ────────────────────────────────────────────────────────────────
_online_store: Optional[OnlineFeatureStore] = None


def get_online_store() -> OnlineFeatureStore:
    """获取全局 OnlineFeatureStore 单例"""
    global _online_store
    if _online_store is None:
        _online_store = OnlineFeatureStore()
    return _online_store


if __name__ == '__main__':
    store = OnlineFeatureStore()

    # 测试写入
    store.set_features('user_001', 'user_behavior', {
        'order_count_7d': 5,
        'total_amount_30d': 1234.56,
        'avg_order_value': 246.91,
    }, ttl=3600)

    # 测试读取
    feats = store.get_features('user_001', 'user_behavior',
                               ['order_count_7d', 'total_amount_30d'])
    print(f'读取特征：{feats}')

    # 命中率
    stats = store.get_hit_stats()
    print(f'命中率统计：{stats}')
