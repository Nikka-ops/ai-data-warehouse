# -*- coding: utf-8 -*-
"""
Pipeline 编排器 —— 轻量级数据管道阶段调度器

串联 Kafka 接入 → Flink 流处理 → 特征计算 → 告警检测 → AI 洞察 → Iceberg 归档。
各阶段独立注册、独立执行；单阶段失败不中断后续阶段。
"""

import time
import logging
import argparse
from enum import Enum
from typing import Callable, Optional

log = logging.getLogger('pipeline.coordinator')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(name)s  %(message)s',
)


# ── 管道阶段枚举 ─────────────────────────────────────────────────

class PipelineStage(Enum):
    INGEST  = "ingest"    # Kafka 数据接入
    STREAM  = "stream"    # Flink 流处理
    FEATURE = "feature"   # 特征计算
    ALERT   = "alert"     # 告警检测
    INSIGHT = "insight"   # AI 洞察生成
    ARCHIVE = "archive"   # 数据归档到 Iceberg


# ── 编排器 ───────────────────────────────────────────────────────

class PipelineCoordinator:
    """
    轻量级管道编排器。

    使用示例：
        coord = PipelineCoordinator()
        coord.register_stage(PipelineStage.INGEST, my_ingest_fn)
        coord.run()                          # 执行全部阶段
        coord.run(stages=[PipelineStage.ALERT, PipelineStage.INSIGHT])  # 指定阶段
        coord.run_stage(PipelineStage.ALERT) # 单阶段
    """

    def __init__(self):
        # 按注册顺序保存处理器，key=PipelineStage
        self._handlers: dict[PipelineStage, Callable] = {}

    def register_stage(self, stage: PipelineStage, handler_fn: Callable) -> None:
        """注册阶段处理器；同一阶段重复注册则覆盖"""
        self._handlers[stage] = handler_fn
        log.info('[Coordinator] 注册阶段 %s → %s', stage.value, handler_fn.__name__)

    def run_stage(self, stage: PipelineStage) -> bool:
        """
        执行单个阶段。
        返回 True 表示成功，False 表示失败（异常已记录）。
        未注册该阶段时记 warning 并返回 False。
        """
        handler = self._handlers.get(stage)
        if handler is None:
            log.warning('[Coordinator] 阶段 %s 未注册处理器，跳过', stage.value)
            return False

        log.info('[Coordinator] ▶ 开始阶段 %s', stage.value)
        t0 = time.perf_counter()
        try:
            handler()
            elapsed = time.perf_counter() - t0
            log.info('[Coordinator] ✓ 阶段 %s 完成，耗时 %.2fs', stage.value, elapsed)
            return True
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            log.error('[Coordinator] ✗ 阶段 %s 失败（%.2fs）：%s', stage.value, elapsed, exc)
            return False

    def run(self, stages: Optional[list] = None) -> dict:
        """
        按顺序执行阶段列表。
        stages=None 时按 PipelineStage 枚举定义顺序执行全部已注册阶段。
        返回各阶段执行结果摘要 {stage_value: True/False}。
        """
        if stages is None:
            # 按枚举声明顺序，只执行已注册的阶段
            stages = [s for s in PipelineStage if s in self._handlers]

        log.info('[Coordinator] 管道启动，共 %d 个阶段：%s',
                 len(stages), [s.value for s in stages])
        results = {}
        for stage in stages:
            results[stage.value] = self.run_stage(stage)

        success = sum(1 for v in results.values() if v)
        failed  = len(results) - success
        log.info('[Coordinator] 管道执行完毕：成功 %d / 失败 %d', success, failed)
        return results


# ── CLI 入口 ─────────────────────────────────────────────────────

def _placeholder(stage_name: str) -> Callable:
    """为未实现阶段返回占位处理器（仅打印日志）"""
    def _fn():
        log.info('[%s] 占位处理器执行（未连接实际逻辑）', stage_name)
    _fn.__name__ = f'placeholder_{stage_name}'
    return _fn


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AI 数据仓库 Pipeline 编排器')
    parser.add_argument(
        '--stage',
        choices=[s.value for s in PipelineStage],
        default=None,
        help='指定单阶段执行（不指定则运行全部阶段）',
    )
    args = parser.parse_args()

    # 初始化编排器，注册占位处理器（生产环境替换为真实实现）
    coordinator = PipelineCoordinator()
    for s in PipelineStage:
        coordinator.register_stage(s, _placeholder(s.value))

    if args.stage:
        # 单阶段模式
        target = PipelineStage(args.stage)
        ok = coordinator.run_stage(target)
        exit(0 if ok else 1)
    else:
        # 全量模式
        summary = coordinator.run()
        failed_stages = [k for k, v in summary.items() if not v]
        if failed_stages:
            log.warning('以下阶段执行失败：%s', failed_stages)
        exit(0 if not failed_stages else 1)
