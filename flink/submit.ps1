# ============================================================
# 提交实时数仓作业到 Flink 集群
#
# 主链路是两个独立作业，不是一个：
#
#   rtdw-dwd   CDC → 清洗打宽 → Kafka(dwd_trade_*) + Doris(订单快照)
#   rtdw-dws   Kafka(dwd_trade_*) → 窗口聚合 → Doris(dws.*) + 迟到兜底
#
# 拆成两个作业而不是塞进一个 STATEMENT SET，是为了让故障域分开：
# DWS 的窗口聚合是有状态大户，改窗口口径要重启；DWD 只是无状态清洗，
# 没必要跟着一起停 —— 停了就等于业务库到数仓的入口断了。
# 拆开之后 DWS 重启期间 DWD 照常写 Kafka，恢复后从 group-offsets 追上来。
#
# 用法：
#   pwsh flink/submit.ps1                # 提交两个作业
#   pwsh flink/submit.ps1 -Job dwd       # 只提交 DWD
#   pwsh flink/submit.ps1 -Job dws       # 只提交 DWS
#   pwsh flink/submit.ps1 -Status        # 看作业状态
#   pwsh flink/submit.ps1 -Stop          # 停止全部作业并各打一个 savepoint
# ============================================================

param(
    [ValidateSet('all', 'dwd', 'dws')]
    [string]$Job = 'all',
    [switch]$Status,
    [switch]$Stop
)

# 这里刻意不用 'Stop'。
#
# Windows PowerShell 5.1 会把原生命令写到 stderr 的每一行包成 ErrorRecord，
# 配合 $ErrorActionPreference='Stop' 就变成终止性错误 —— 而 flink list
# 启动时会往 stderr 打一行 "WARNING: Unknown module: jdk.compiler" 的 JVM 警告，
# 于是脚本在退出码明明是 0 的情况下直接崩掉。
#
# 驱动原生工具的脚本应该只看退出码，下面每一处调用都显式检查 $LASTEXITCODE。
$ErrorActionPreference = 'Continue'

$JM      = 'ai_dw_flink_jm'
$FlinkUI = 'http://localhost:8081'
$InitSql = '/opt/flink/sql/00_init.sql'

# 作业名要和各 job sql 里的 SET 'pipeline.name' 严格一致，
# 否则 -Stop 找不到作业。
$Jobs = [ordered]@{
    'dwd' = @{ Name = 'rtdw-dwd'; Sql = '/opt/flink/sql/10_dwd_job.sql' }
    'dws' = @{ Name = 'rtdw-dws'; Sql = '/opt/flink/sql/20_dws_job.sql' }
}

function Assert-JobManager {
    $running = docker ps --filter "name=$JM" --filter "status=running" --format '{{.Names}}' 2>&1 |
               ForEach-Object { "$_" } | Where-Object { $_ -match [regex]::Escape($JM) }
    if (-not $running) {
        Write-Host "JobManager 容器 $JM 没在运行。先执行 docker compose up -d" -ForegroundColor Red
        exit 1
    }
}

function Get-JobId([string]$jobName) {
    # 2>&1 把 stderr 并进来一起当文本处理；ForEach 里转成字符串是为了
    # 把 5.1 包出来的 ErrorRecord 摊平，否则 -match 匹配的是对象不是行内容
    $lines = docker exec $JM /opt/flink/bin/flink list -r 2>&1 | ForEach-Object { "$_" }
    foreach ($line in $lines) {
        if ($line -match [regex]::Escape($jobName)) {
            $m = [regex]::Match($line, '([0-9a-f]{32})')
            if ($m.Success) { return $m.Value }
        }
    }
    return $null
}

# ── 查状态 ───────────────────────────────────────────────────
if ($Status) {
    Assert-JobManager
    Write-Host "Flink 作业列表：" -ForegroundColor Cyan
    docker exec $JM /opt/flink/bin/flink list
    Write-Host ""
    foreach ($key in $Jobs.Keys) {
        $name = $Jobs[$key].Name
        $id   = Get-JobId $name
        if ($id) {
            Write-Host ("  {0,-10} RUNNING  {1}" -f $name, $id) -ForegroundColor Green
        } else {
            Write-Host ("  {0,-10} 未运行" -f $name) -ForegroundColor Yellow
        }
    }
    Write-Host ""
    Write-Host "Web UI: $FlinkUI" -ForegroundColor DarkGray
    exit 0
}

# ── 停作业（带 savepoint）────────────────────────────────────
# 直接 cancel 会丢掉算子状态，下次启动只能从 Kafka 位点重放，
# 窗口里攒了一半的数据全部作废。打 savepoint 停止才能原样恢复。
if ($Stop) {
    Assert-JobManager
    $stopped = 0
    foreach ($key in $Jobs.Keys) {
        $name  = $Jobs[$key].Name
        $jobId = Get-JobId $name
        if (-not $jobId) {
            Write-Host "$name 没在运行，跳过" -ForegroundColor DarkGray
            continue
        }
        Write-Host "停止 $name ($jobId) 并写 savepoint..." -ForegroundColor Yellow
        docker exec $JM /opt/flink/bin/flink stop --savepointPath /flink/savepoints $jobId
        $stopped++
    }
    if ($stopped -eq 0) { Write-Host "没有正在运行的作业" -ForegroundColor Yellow }
    exit 0
}

# ── 提交 ─────────────────────────────────────────────────────
Assert-JobManager

# connector jar 必须先就位，否则 SQL Client 解析 DDL 时就报 connector 找不到
$libDir = Join-Path $PSScriptRoot 'lib'
$jarCount = 0
if (Test-Path $libDir) {
    $jarCount = (Get-ChildItem $libDir -Filter '*.jar' -ErrorAction SilentlyContinue).Count
}
if ($jarCount -lt 5) {
    Write-Host "flink/lib 下只有 $jarCount 个 jar，缺 connector。" -ForegroundColor Red
    Write-Host "先执行：pwsh flink/download_jars.ps1" -ForegroundColor Yellow
    exit 1
}

# ── 为本次提交生成独立的 Doris label 前缀 ────────────────────
#
# Doris 的导入 label 是 {label-prefix}_{子任务号}_{checkpoint号}，
# 靠它做导入去重。作业 cancel 后重新提交，checkpoint 号从 1 重新数，
# label 与上一轮完全相同，Doris 判定为重复导入直接拒绝，
# 报 "Label Already Exists and load job finished"，TaskManager 随即退出。
#
# 所以每次全新提交都换一个前缀。同一次运行内从 checkpoint 自动恢复时
# 用的还是这份生成好的 SQL，前缀不变，幂等性照常成立。
$runId    = Get-Date -Format 'yyyyMMddHHmmss'
$runDir   = Join-Path $PSScriptRoot 'sql/.run'
if (-not (Test-Path $runDir)) { New-Item -ItemType Directory -Path $runDir | Out-Null }

$initSrc  = Join-Path $PSScriptRoot 'sql/00_init.sql'
$initGen  = Join-Path $runDir '00_init.generated.sql'
# 必须写成不带 BOM 的 UTF-8：Windows PowerShell 5.1 的 Set-Content -Encoding UTF8
# 会在文件开头塞一个 BOM，Flink SQL Client 把它当成 SQL 的一部分，
# 于是第一条语句解析失败，只报一句 "Failed to initialize from sql script"。
$initText = (Get-Content $initSrc -Raw -Encoding UTF8).Replace('__RUNID__', $runId)
[System.IO.File]::WriteAllText(
    $initGen, $initText, (New-Object System.Text.UTF8Encoding($false))
)

# flink/sql 整个目录挂进容器，生成文件跟着可见
$InitSql = '/opt/flink/sql/.run/00_init.generated.sql'
Write-Host "本次运行 ID: $runId" -ForegroundColor DarkGray

$targets = if ($Job -eq 'all') { @('dwd', 'dws') } else { @($Job) }

foreach ($key in $targets) {
    $name = $Jobs[$key].Name
    $sql  = $Jobs[$key].Sql

    if (Get-JobId $name) {
        Write-Host "$name 已在运行，跳过提交。要重提先执行 -Stop。" -ForegroundColor Yellow
        continue
    }

    Write-Host ""
    Write-Host "提交 $name" -ForegroundColor Cyan
    Write-Host "  init: $InitSql" -ForegroundColor DarkGray
    Write-Host "  job : $sql"     -ForegroundColor DarkGray

    docker exec $JM /opt/flink/bin/sql-client.sh -i $InitSql -f $sql

    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "$name 提交失败。排查顺序：" -ForegroundColor Red
        Write-Host "  1. Doris 建表是否完成 : docker logs ai_dw_doris_init --tail 30" -ForegroundColor Yellow
        Write-Host "  2. connector 是否加载  : docker exec $JM ls /opt/flink/lib" -ForegroundColor Yellow
        Write-Host "  3. MySQL binlog 是否开 : docker exec ai_dw_mysql mysql -uroot -proot123 -e 'SHOW VARIABLES LIKE \"log_bin\"'" -ForegroundColor Yellow
        Write-Host "  4. JobManager 日志     : docker logs $JM --tail 80" -ForegroundColor Yellow
        exit 1
    }

    # DWS 消费的是 DWD 产出的 topic。先提交 DWD 让 topic 建出来，
    # 否则 DWS 启动时 topic 不存在，要等一轮 metadata 刷新才追得上。
    if ($key -eq 'dwd' -and $targets.Count -gt 1) {
        Write-Host "  等待 DWD 产出 dwd_trade_* topic..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 20
    }
}

Write-Host ""
Write-Host "已提交。" -ForegroundColor Green
Write-Host "  Flink UI : $FlinkUI" -ForegroundColor White
Write-Host "  看板     : streamlit run app/realtime_dashboard.py" -ForegroundColor White
Write-Host ""
Write-Host "还没有数据的话，先起业务库模拟器（它只写 MySQL，CDC 会自己捕获）：" -ForegroundColor DarkGray
Write-Host "  python mock/business_simulator.py --seed-dim" -ForegroundColor White
Write-Host "  python mock/business_simulator.py --rate 20" -ForegroundColor White
