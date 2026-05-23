#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成测试数据（巴西电商模拟）"""
import sys, os
sys.path.insert(0, '/home/user/ai-data-warehouse')

def main():
    try:
        from src.ingestion.producers.mock_producer import BrazilianEcommerceSimulator, KafkaProducer
    except ImportError:
        from kafka.producer import main as old_main
        old_main()
        return

    sim = BrazilianEcommerceSimulator()
    rate = int(os.getenv("RATE_PER_SECOND", "20"))
    print(f"开始生成模拟数据，速率: {rate} 条/秒")
    sim.run(rate_per_second=rate)

if __name__ == "__main__":
    main()
