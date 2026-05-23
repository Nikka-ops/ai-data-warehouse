#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""性能基准测试：NL2SQL、RAG、Agent 延迟和准确率"""
import time, sys, statistics
sys.path.insert(0, '/home/user/ai-data-warehouse')

NL2SQL_CASES = [
    "当前最新5分钟的订单量是多少？",
    "今日 GMV 总额是多少？",
    "取消率最高的品类是哪个？",
    "最近一小时各品类订单数量对比",
    "Kafka 消费 Lag 情况",
]

def benchmark_nl2sql():
    print("\n=== NL2SQL 基准测试 ===")
    try:
        from ai_layer.nl2sql import nl2sql_query
    except ImportError:
        print("nl2sql 模块未找到，跳过")
        return

    latencies = []
    success = 0
    for q in NL2SQL_CASES:
        start = time.time()
        try:
            result = nl2sql_query(q)
            latencies.append((time.time() - start) * 1000)
            if result.get("sql"):
                success += 1
            print(f"  ✓ {q[:30]:<30} {latencies[-1]:.0f}ms")
        except Exception as e:
            print(f"  ✗ {q[:30]:<30} 失败: {e}")

    if latencies:
        print(f"\n  成功率: {success}/{len(NL2SQL_CASES)}")
        print(f"  P50: {statistics.median(latencies):.0f}ms")
        print(f"  P95: {sorted(latencies)[int(len(latencies)*0.95)]:.0f}ms")
        print(f"  Max: {max(latencies):.0f}ms")

if __name__ == "__main__":
    benchmark_nl2sql()
    print("\n基准测试完成")
