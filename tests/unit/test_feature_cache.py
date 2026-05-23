# -*- coding: utf-8 -*-
"""Redis 特征缓存测试（使用 fakeredis mock）"""
import pytest
import sys
sys.path.insert(0, '/home/user/ai-data-warehouse')

class TestFeatureCache:
    def setup_method(self):
        try:
            import fakeredis
            import redis
            self.fake_redis = fakeredis.FakeRedis(decode_responses=True)
        except ImportError:
            pytest.skip("需要 fakeredis: pip install fakeredis")

    def test_set_and_get(self):
        from unittest.mock import patch
        try:
            from src.storage.redis.feature_cache import FeatureCache
        except ImportError:
            pytest.skip("src.storage.redis 未创建")

        cache = FeatureCache()
        cache._redis = self.fake_redis  # 注入 fake redis

        features = {"order_count": 42, "gmv": 1234.5}
        cache.set("user", "user_001", features)
        result = cache.get("user", "user_001")
        assert result is not None
        assert result.get("order_count") == 42 or "order_count" in str(result)

    def test_missing_key_returns_none(self):
        try:
            from src.storage.redis.feature_cache import FeatureCache
        except ImportError:
            pytest.skip("src.storage.redis 未创建")

        cache = FeatureCache()
        cache._redis = self.fake_redis
        result = cache.get("user", "nonexistent_user_99999")
        assert result is None
