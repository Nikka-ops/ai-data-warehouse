#!/usr/bin/env bash
# Flink Savepoint 管理脚本

FLINK_API=${FLINK_API:-"http://localhost:8081"}

list() {
    echo "当前运行的 Flink 作业："
    curl -s "${FLINK_API}/jobs" | python3 -m json.tool
}

savepoint() {
    JOB_ID=${1:?"用法: $0 savepoint <job_id> [target_dir]"}
    TARGET=${2:-"s3://warehouse/flink-savepoints"}
    echo "为作业 ${JOB_ID} 创建 savepoint..."
    curl -s -X POST "${FLINK_API}/jobs/${JOB_ID}/savepoints" \
        -H "Content-Type: application/json" \
        -d "{\"target-directory\": \"${TARGET}\", \"cancel-job\": false}"
}

restore() {
    JAR=${1:?"用法: $0 restore <jar_path> <savepoint_path>"}
    SAVEPOINT=${2:?"用法: $0 restore <jar_path> <savepoint_path>"}
    echo "从 savepoint ${SAVEPOINT} 恢复..."
    JAR_ID=$(curl -s -X POST "${FLINK_API}/jars/upload" -F "jarfile=@${JAR}" | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")
    curl -s -X POST "${FLINK_API}/jars/${JAR_ID}/run" \
        -H "Content-Type: application/json" \
        -d "{\"savepointPath\": \"${SAVEPOINT}\", \"allowNonRestoredState\": false}"
}

case "${1}" in
    list)      list ;;
    savepoint) savepoint "${2}" "${3}" ;;
    restore)   restore "${2}" "${3}" ;;
    *) echo "用法: $0 {list|savepoint <job_id>|restore <jar> <savepoint>}" ;;
esac
