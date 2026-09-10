# ============================================================
# 下载 Flink connector jar 到 flink/lib/
#
# 这些 jar 没有进版本库（几十 MB，且可从 Maven 中央仓库稳定获取），
# docker-compose 把 flink/lib 挂进容器，启动时复制到 Flink 的 lib 目录。
#
# 用法：  pwsh flink/download_jars.ps1
#   （没装 PowerShell 7 的话，Windows 自带的 powershell 5.1 同样可以：
#     powershell -ExecutionPolicy Bypass -File flink/download_jars.ps1）
# ============================================================

$ErrorActionPreference = 'Stop'

# Windows PowerShell 5.1 的 Invoke-WebRequest 会为每个数据块重绘进度条，
# 下载几十 MB 的 jar 时这件事本身比下载还慢（能差一个数量级）。
# PowerShell 7 已经不受影响，但这行对两边都无害。
$ProgressPreference = 'SilentlyContinue'

$LibDir = Join-Path $PSScriptRoot 'lib'
if (-not (Test-Path $LibDir)) {
    New-Item -ItemType Directory -Path $LibDir | Out-Null
}

$Maven = 'https://repo1.maven.org/maven2'

# 版本对齐说明：
#   Flink 1.18  ←→  flink-sql-connector-kafka 3.1.0-1.18
#   Flink 1.18  ←→  flink-doris-connector-1.18 24.0.1
# connector 的版本号后缀必须和 Flink 主版本严格对上，否则 ClassNotFound。
$Jars = @(
    @{
        Name = 'flink-sql-connector-kafka-3.1.0-1.18.jar'
        Url  = "$Maven/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
        Desc = 'Kafka source/sink（已 shade，自带 kafka-clients）'
    },
    @{
        Name = 'flink-doris-connector-1.18-24.0.1.jar'
        Url  = "$Maven/org/apache/doris/flink-doris-connector-1.18/24.0.1/flink-doris-connector-1.18-24.0.1.jar"
        Desc = 'Doris sink（Stream Load 2PC）+ lookup source'
    },
    @{
        Name = 'flink-sql-connector-mysql-cdc-3.1.0.jar'
        Url  = "$Maven/org/apache/flink/flink-sql-connector-mysql-cdc/3.1.0/flink-sql-connector-mysql-cdc-3.1.0.jar"
        Desc = 'Flink CDC —— 伪装成 MySQL 从库拉取 binlog，实时链路的源头'
    },
    @{
        Name = 'flink-connector-jdbc-3.1.2-1.18.jar'
        Url  = "$Maven/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar"
        Desc = 'JDBC lookup source —— 订单主表按处理时间点查，避免把 changelog 拖进常规 join'
    },
    @{
        Name = 'mysql-connector-j-8.0.33.jar'
        Url  = "$Maven/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
        Desc = 'MySQL JDBC 驱动，lookup source 用它连业务库'
    }
)

Write-Host "下载 Flink connector 到 $LibDir" -ForegroundColor Cyan
Write-Host ""

foreach ($jar in $Jars) {
    $dest = Join-Path $LibDir $jar.Name

    if (Test-Path $dest) {
        $sizeMB = [math]::Round((Get-Item $dest).Length / 1MB, 1)
        Write-Host ("  [跳过] {0}  ({1} MB 已存在)" -f $jar.Name, $sizeMB) -ForegroundColor DarkGray
        continue
    }

    Write-Host ("  [下载] {0}" -f $jar.Name) -ForegroundColor Yellow
    Write-Host ("         {0}" -f $jar.Desc) -ForegroundColor DarkGray

    try {
        Invoke-WebRequest -Uri $jar.Url -OutFile $dest -UseBasicParsing
        $sizeMB = [math]::Round((Get-Item $dest).Length / 1MB, 1)
        Write-Host ("         完成 {0} MB" -f $sizeMB) -ForegroundColor Green
    }
    catch {
        Write-Host ("         失败：{0}" -f $_.Exception.Message) -ForegroundColor Red
        if (Test-Path $dest) { Remove-Item $dest -Force }
        throw
    }
}

Write-Host ""
Write-Host "全部就绪。接下来：" -ForegroundColor Cyan
Write-Host "  docker compose up -d" -ForegroundColor White
Write-Host "  pwsh flink/submit.ps1" -ForegroundColor White
