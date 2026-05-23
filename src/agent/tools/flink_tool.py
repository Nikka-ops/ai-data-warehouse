# -*- coding: utf-8 -*-
"""Flink 作业管理工具"""
import urllib.request, json
from langchain_core.tools import tool

FLINK_API = "http://flink-jobmanager:8081"


@tool
def get_flink_jobs() -> str:
    """查询所有 Flink 作业状态"""
    try:
        with urllib.request.urlopen(f"{FLINK_API}/jobs", timeout=10) as resp:
            data = json.loads(resp.read())
        jobs = data.get("jobs", [])
        if not jobs:
            return "当前无 Flink 作业"
        lines = [f"## Flink 作业列表（共 {len(jobs)} 个）\n"]
        for j in jobs:
            lines.append(f"- {j.get('id', 'N/A')}  状态：{j.get('status', 'N/A')}")
        return "\n".join(lines)
    except Exception as e:
        return f"查询 Flink 作业失败: {e}"


@tool
def get_flink_job_metrics(job_id: str) -> str:
    """查询指定 Flink 作业的关键指标（延迟、吞吐量）"""
    try:
        metric_keys = "numRecordsInPerSecond,numRecordsOutPerSecond,lastCheckpointDuration"
        url = f"{FLINK_API}/jobs/{job_id}/metrics?get={metric_keys}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        if not data:
            return f"作业 {job_id} 暂无指标数据"
        lines = [f"## 作业 {job_id} 指标\n"]
        for m in data:
            lines.append(f"- {m.get('id')}: {m.get('value', 'N/A')}")
        return "\n".join(lines)
    except Exception as e:
        return f"查询 Flink 指标失败: {e}"


@tool
def trigger_savepoint(job_id: str, target_dir: str = "s3://warehouse/savepoints") -> str:
    """为运行中的 Flink 作业触发 savepoint"""
    try:
        payload = json.dumps({"target-directory": target_dir, "cancel-job": False}).encode()
        req = urllib.request.Request(
            f"{FLINK_API}/jobs/{job_id}/savepoints",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        request_id = data.get("request-id", "N/A")
        return f"Savepoint 触发成功：request-id={request_id}，目标目录={target_dir}"
    except Exception as e:
        return f"触发 savepoint 失败: {e}"
