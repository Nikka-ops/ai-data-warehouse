# -*- coding: utf-8 -*-
"""
实时监控看板

四块内容，对应实时数仓的四个关注点：
    今日大盘   累计 KPI + 支付转化漏斗
    分钟趋势   订单量 / GMV / 件均价的瞬时波动
    维度下钻   品类与地域分布
    链路健康   端到端延迟、迟到数据、作业指标、AI 质检告警

查询走的层次是刻意区分的，也是 ADS 分层的意义所在：
    当前状态卡片  → ads.v_today_* 普通视图（查询即最新，一次点查）
    分钟折线      → ads.v_minute_trend 普通视图（当天 120 行，现算够快）
    维度排行      → ads.category_rank / region_rank 结果表（调度预刷）
看板不直接聚合 DWD 明细，一次都没有。

运行：streamlit run app/realtime_dashboard.py
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from common.doris_client import get_client


st.set_page_config(page_title='实时数仓监控', page_icon='📡', layout='wide')

REFRESH_SECONDS = 15


@st.cache_resource
def doris():
    return get_client()


def q(sql: str, params=None) -> pd.DataFrame:
    """查询并返回 DataFrame，失败时返回空表而不是让整个页面崩掉"""
    try:
        return doris().query_df(sql, params)
    except Exception as e:
        st.error(f'查询失败：{e}')
        return pd.DataFrame()


# ── 顶栏 ──────────────────────────────────────────────────────
st.title('📡 实时数仓监控')

top_l, top_r = st.columns([4, 1])
with top_r:
    auto = st.toggle('自动刷新', value=False, help=f'每 {REFRESH_SECONDS} 秒刷新一次')
    if st.button('立即刷新', use_container_width=True):
        st.rerun()
with top_l:
    st.caption(
        'MySQL binlog → Flink CDC → Kafka → Flink SQL'
        '（watermark 5min / checkpoint 30s / Exactly-Once）→ Doris'
        f'　·　更新于 {datetime.now():%H:%M:%S}'
    )

if not doris().ping():
    st.error('连不上 Doris。检查 `docker compose ps`，以及环境变量 DORIS_HOST。')
    st.stop()


EMPTY_HINT = (
    '今天还没有数据。按顺序起三样：\n\n'
    '```\n'
    'python mock/business_simulator.py --seed-dim\n'
    'python mock/business_simulator.py --rate 20\n'
    'pwsh flink/submit.ps1\n'
    '```\n'
    '第一批窗口结果约 1 分钟后出现 —— Flink 要等 watermark 越过窗口终点才触发输出。'
)


# ============================================================
# 一、今日大盘
# ============================================================
st.subheader('今日累计')

kpi  = q('SELECT * FROM ads.v_today_trade_kpi')
pay  = q('SELECT * FROM ads.v_today_pay_kpi')
conv = q('SELECT * FROM ads.v_today_conversion')

if kpi.empty:
    st.info(EMPTY_HINT)
else:
    r = kpi.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric('订单数',  f"{int(r.today_order_cnt):,}")
    c2.metric('GMV',     f"￥{float(r.today_gmv):,.0f}")
    c3.metric('下单用户', f"{int(r.today_order_user_cnt):,}")
    c4.metric('客单价',   f"￥{float(r.avg_order_value):,.2f}")
    c5.metric('商品件数', f"{int(r.today_sku_num):,}")

    if not pay.empty:
        c6.metric('实付金额', f"￥{float(pay.iloc[0].today_pay_amount):,.0f}")
    else:
        c6.metric('实付金额', '—')

    st.caption(
        f"统计截至 {r.as_of_time}　·　CUMULATE 日累计窗口，"
        '窗口起点钉在零点、终点每分钟前推一格，增量维护，不做全表扫描'
    )

    # ── 支付转化漏斗 ──
    # 这几个数只能从累积快照算：问的是「有多少订单当前处于某状态」，
    # 而窗口聚合表装的是「发生了多少次事件」，回答不了状态分布。
    if not conv.empty:
        v = conv.iloc[0]
        st.markdown('**支付转化**')
        f1, f2, f3, f4, f5 = st.columns(5)
        f1.metric('支付转化率', f"{float(v.pay_conversion_pct):.1f}%")
        f2.metric('已支付', f"{int(v.paid_cnt):,}")
        f3.metric('已送达', f"{int(v.delivered_cnt):,}")
        f4.metric(
            '取消率', f"{float(v.cancel_rate_pct):.1f}%",
            delta='偏高' if float(v.cancel_rate_pct) > 20 else None,
            delta_color='inverse',
        )
        f5.metric('平均支付耗时', f"{float(v.avg_pay_lag_seconds or 0):.0f} s")
        st.caption(
            '来自 dwd.order_snapshot 订单累积快照。同一订单随状态流转被 CDC '
            '写入多次，靠 Unique Key 按主键覆盖 + update_time 做 sequence 列，'
            '迟到的旧状态盖不掉新状态 —— 所以这里读到的一定是当前状态。'
        )


# ============================================================
# 二、分钟趋势
# ============================================================
st.subheader('分钟级趋势（近 2 小时）')

trend = q("""
    SELECT window_start, order_cnt, gmv, avg_price,
           order_user_cnt, sku_num, abnormal_price_cnt, max_lag_seconds
    FROM ads.v_minute_trend
    ORDER BY window_start
""")

if trend.empty:
    st.info('暂无窗口数据。Flink 需要 watermark 越过窗口终点才会触发输出，第一批结果约 1 分钟后出现。')
else:
    trend['window_start'] = pd.to_datetime(trend['window_start'])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend['window_start'], y=trend['order_cnt'],
        name='订单数', mode='lines', line=dict(width=2),
    ))
    fig.add_trace(go.Scatter(
        x=trend['window_start'], y=trend['gmv'],
        name='GMV (￥)', mode='lines', yaxis='y2',
        line=dict(width=2, dash='dot'),
    ))
    fig.update_layout(
        height=340,
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(title='订单数'),
        yaxis2=dict(title='GMV (￥)', overlaying='y', side='right'),
        legend=dict(orientation='h', y=1.12, x=0),
        hovermode='x unified',
    )
    st.plotly_chart(fig, use_container_width=True)

    m1, m2 = st.columns(2)
    with m1:
        st.plotly_chart(
            px.line(trend, x='window_start', y='order_user_cnt',
                    title='下单用户数（Flink 状态内精确去重，仅窗口内有效）', height=260)
            .update_layout(margin=dict(l=0, r=0, t=40, b=0)),
            use_container_width=True,
        )
    with m2:
        st.plotly_chart(
            px.line(trend, x='window_start', y='avg_price',
                    title='件均价 (￥)', height=260)
            .update_layout(margin=dict(l=0, r=0, t=40, b=0)),
            use_container_width=True,
        )


# ============================================================
# 三、维度下钻
#
# 读的是 ADS 排行结果表，由 pipelines/refresh_ads.py 定时刷新。
# 不在这里现场聚合 DWS 的原因是 UV 列：窗口表里的 order_user_cnt
# 只在单个窗口内有效，把多个窗口 SUM 起来会把同一用户重复计数。
# 全天去重必须走 dws.user_active_daily 的 BITMAP，那是刷新任务干的活。
# ============================================================
st.subheader('维度下钻（今日累计）')

d1, d2 = st.columns(2)

with d1:
    cat = q("""
        SELECT category1_name, order_cnt, sku_num, gmv, avg_price,
               rank_by_gmv, gmv_share_pct
        FROM ads.category_rank
        WHERE stat_date = CURDATE()
        ORDER BY rank_by_gmv
        LIMIT 10
    """)
    if cat.empty:
        st.caption('暂无品类排行。由 ADS 刷新任务写入：`python pipelines/refresh_ads.py`')
    else:
        st.plotly_chart(
            px.bar(cat, x='gmv', y='category1_name', orientation='h',
                   title='品类 GMV Top 10', height=340)
            .update_layout(margin=dict(l=0, r=0, t=40, b=0),
                           yaxis=dict(categoryorder='total ascending')),
            use_container_width=True,
        )

with d2:
    geo = q("""
        SELECT region_name, order_cnt, gmv, uv, pay_uv, rank_by_gmv
        FROM ads.region_rank
        WHERE stat_date = CURDATE()
        ORDER BY rank_by_gmv
        LIMIT 10
    """)
    if geo.empty:
        st.caption('暂无地域排行。由 ADS 刷新任务写入：`python pipelines/refresh_ads.py`')
    else:
        st.plotly_chart(
            px.bar(geo, x='region_name', y='gmv', title='各大区 GMV', height=340)
            .update_layout(margin=dict(l=0, r=0, t=40, b=0)),
            use_container_width=True,
        )
        st.caption('uv / pay_uv 列来自 dws.user_active_daily 的 BITMAP_UNION_COUNT，'
                   '是跨窗口精确去重值，不是各分钟窗口用户数之和。')

with st.expander('跨天独立用户数（BITMAP 精确合并）'):
    st.caption(
        '这是 BITMAP 存在的理由：日 UV 存成位图后，任意多天的独立用户数 '
        '= BITMAP_UNION_COUNT(uv_bitmap)，Doris 在压缩位图上做或运算，'
        '不回明细、结果精确。换成 COUNT(DISTINCT) 就得扫这几天的全部订单明细。'
    )
    uv = q("""
        SELECT
            '近 1 天'  AS span, BITMAP_UNION_COUNT(uv_bitmap) AS uv,
            BITMAP_UNION_COUNT(pay_uv_bitmap) AS pay_uv
        FROM dws.user_active_daily
        WHERE region_name = 'ALL' AND dt >= CURDATE()
        UNION ALL
        SELECT '近 7 天',  BITMAP_UNION_COUNT(uv_bitmap), BITMAP_UNION_COUNT(pay_uv_bitmap)
        FROM dws.user_active_daily
        WHERE region_name = 'ALL' AND dt >= CURDATE() - INTERVAL 6 DAY
        UNION ALL
        SELECT '近 30 天', BITMAP_UNION_COUNT(uv_bitmap), BITMAP_UNION_COUNT(pay_uv_bitmap)
        FROM dws.user_active_daily
        WHERE region_name = 'ALL' AND dt >= CURDATE() - INTERVAL 29 DAY
    """)
    if uv.empty:
        st.caption('暂无数据')
    else:
        st.dataframe(uv, use_container_width=True, hide_index=True)


# ============================================================
# 四、链路健康
# ============================================================
st.subheader('链路健康')

h1, h2, h3, h4 = st.columns(4)

lag = q("""
    SELECT
        MAX(max_lag_seconds) AS max_lag,
        AVG(avg_lag_seconds) AS avg_lag,
        SUM(order_cnt)       AS total_cnt
    FROM dws.trade_window_agg
    WHERE window_type    = 'TUMBLE_1M'
      AND category1_name = 'ALL'
      AND region_name    = 'ALL'
      AND stat_date      = CURDATE()
      AND window_start  >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
""")
late_cnt_df = q("""
    SELECT COUNT(*) AS late_cnt
    FROM stream.late_records
    WHERE late_date >= CURDATE()
      AND detect_time >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
""")

if not lag.empty and lag.iloc[0]['total_cnt']:
    lr = lag.iloc[0]
    total = int(lr.total_cnt or 0)
    late  = int(late_cnt_df.iloc[0]['late_cnt']) if not late_cnt_df.empty else 0
    late_pct = (late / total * 100) if total else 0

    h1.metric('最大端到端延迟', f"{int(lr.max_lag or 0)} s")
    h2.metric('平均端到端延迟', f"{float(lr.avg_lag or 0):.1f} s")
    h3.metric('迟到数据', f"{late:,} 条",
              delta=f"{late_pct:.2f}%", delta_color='inverse')
    h4.metric('近 30 分钟处理量', f"{total:,} 条")
else:
    h1.metric('最大端到端延迟', '—')
    h2.metric('平均端到端延迟', '—')
    h3.metric('迟到数据', '—')
    h4.metric('近 30 分钟处理量', '—')

with st.expander('迟到数据明细（超出 watermark 容忍度，已落兜底表）'):
    st.caption(
        'watermark 给了 5 分钟乱序容忍，这个范围内的乱序数据能正常进窗口参与聚合。'
        '真正超出 5 分钟的进不了原窗口 —— 窗口 TVF 会直接丢弃且不留痕迹。'
        'DWS 作业用 CURRENT_WATERMARK() 把这批记录分流到 `stream.late_records`，'
        '连同 Kafka 的 partition / offset 一起存，可以按位点精确回放补算。'
        '于是「丢数」变成「可量化、可回放、可对账」。'
    )
    late_df = q("""
        SELECT detect_time, record_key, source_table, event_time,
               watermark_time, lateness_seconds,
               kafka_partition, kafka_offset, order_id, split_amount, is_compensated
        FROM stream.late_records
        WHERE late_date >= CURDATE() - INTERVAL 1 DAY
        ORDER BY detect_time DESC
        LIMIT 200
    """)
    if late_df.empty:
        st.success('没有超出容忍度的迟到数据')
    else:
        st.dataframe(late_df, use_container_width=True, hide_index=True)

with st.expander('对账结果（业务库 vs 数仓）'):
    st.caption(
        'Exactly-Once 和主键幂等是设计上的保证，对账是对这个保证的持续检验。'
        '当天正在进行的时段有小额差异是正常的 —— 业务库已写入的订单，'
        'Flink 窗口可能还没关闭。'
    )
    rec = q("""
        SELECT check_item, check_time, source_value, target_value,
               diff_value, diff_pct, status, detail
        FROM ads.reconcile_result
        WHERE check_date >= CURDATE() - INTERVAL 1 DAY
        ORDER BY check_date DESC, check_item
    """)
    if rec.empty:
        st.caption('暂无对账记录。执行：`python pipelines/reconcile.py`')
    else:
        st.dataframe(rec, use_container_width=True, hide_index=True)

with st.expander('Flink 作业指标'):
    jm = q("""
        SELECT collect_time, job_name, job_state, parallelism,
               records_in_per_sec, kafka_consumer_lag,
               busy_time_ms_per_sec, backpressure_level,
               current_watermark_lag, last_ckpt_duration_ms,
               ckpt_failed_count, restart_count
        FROM stream.job_metrics
        WHERE stat_date >= CURDATE()
        ORDER BY collect_time DESC
        LIMIT 50
    """)
    if jm.empty:
        st.caption(
            '暂无采集数据。作业指标由 `python -m ops_agent.collect` 从 Flink REST API '
            '拉取后写入 `stream.job_metrics`；也可以直接看 Flink UI: http://localhost:8081'
        )
    else:
        st.dataframe(jm, use_container_width=True, hide_index=True)


# ============================================================
# 五、AI 质检告警
# ============================================================
st.subheader('AI 质检告警')

alerts = q("""
    SELECT alert_time, alert_type, severity, field_name, detail,
           ai_suggestion, window_start, metric_value, baseline_value,
           threshold_value, llm_called
    FROM stream.ai_quality_alerts
    WHERE alert_date >= CURDATE()
    ORDER BY alert_time DESC
    LIMIT 50
""")

if alerts.empty:
    st.success('今天没有告警')
    st.caption('质检器每分钟跑一轮，规则不命中时不调用 LLM。启动：`python quality/checker.py`')
else:
    sev_icon = {'HIGH': '🔴', 'MEDIUM': '🟠', 'LOW': '🟡'}
    llm_calls = int(alerts['llm_called'].fillna(0).sum())
    st.caption(
        f"共 {len(alerts)} 条告警，其中 {llm_calls} 条调用了 LLM —— "
        '规则先判、命中才调，正常窗口 0 次 LLM 调用。'
    )

    for _, a in alerts.head(15).iterrows():
        icon = sev_icon.get(a.severity, '⚪')
        with st.expander(
            f"{icon} [{a.alert_type}] {str(a.detail)[:70]}　·　{a.alert_time}"
        ):
            ca, cb, cc = st.columns(3)
            ca.metric('指标值',   f"{float(a.metric_value or 0):,.2f}")
            cb.metric('基线值',   f"{float(a.baseline_value or 0):,.2f}")
            cc.metric('触发阈值', f"{float(a.threshold_value or 0):,.2f}")
            st.write(f"**窗口**：{a.window_start}　**字段**：{a.field_name}")
            if a.ai_suggestion:
                st.info(f"**AI 建议**：{a.ai_suggestion}")


# ── 自动刷新 ──────────────────────────────────────────────────
# 放在最后：sleep + rerun 会阻塞脚本，提前触发的话下面的内容渲染不出来
if auto:
    import time as _time
    _time.sleep(REFRESH_SECONDS)
    st.rerun()
