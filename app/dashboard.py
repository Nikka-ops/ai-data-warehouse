# -*- coding: utf-8 -*-
"""实时数仓看板 - NL2SQL + 实时监控 + Agent"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import clickhouse_connect

from config import cfg

st.set_page_config(
    page_title="实时 AI 数仓", page_icon="⚡",
    layout="wide", initial_sidebar_state="expanded"
)

# ── ClickHouse 连接（缓存，30秒TTL）──────────────────────────
@st.cache_resource(ttl=30)
def get_ch():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=5, send_receive_timeout=30,
    )


def safe_query(sql: str) -> pd.DataFrame:
    try:
        return get_ch().query_df(sql)
    except Exception as e:
        st.warning(f'查询失败：{e}')
        return pd.DataFrame()


# ── 侧边栏 ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ 实时 AI 数仓")

    # 连接状态
    try:
        cnt = get_ch().query(
            "SELECT count() FROM ods.orders_stream WHERE _ingest_time >= now() - INTERVAL 1 MINUTE"
        ).first_row[0]
        st.success(f"ClickHouse 已连接")
        st.metric("近1分钟入库", f"{cnt} 条")
    except Exception as e:
        st.error(f"连接失败：{e}")

    st.markdown("---")

    # 自动刷新控制
    auto_refresh = st.toggle("自动刷新（30秒）", value=True)
    if auto_refresh:
        st.caption("页面每30秒自动刷新")

    st.markdown("---")
    st.markdown("**导航**")
    page = st.radio("", ["实时监控", "智能查询", "Agent 分析"], label_visibility="collapsed")

    st.markdown("---")
    st.caption("数据来源：Kafka → Flink → ClickHouse")


# ══════════════════════════════════════════════════════════════
# 页面 1：实时监控看板
# ══════════════════════════════════════════════════════════════
if page == "实时监控":
    st.title("⚡ 实时监控看板")

    # ── 顶部 KPI ─────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    df_cur = safe_query("""
        SELECT order_cnt, total_gmv, avg_price, unique_customers, top_category
        FROM dws.realtime_minute_stats
        ORDER BY window_start DESC LIMIT 1
    """)
    df_prev = safe_query("""
        SELECT order_cnt, total_gmv, avg_price
        FROM dws.realtime_minute_stats
        ORDER BY window_start DESC LIMIT 1 OFFSET 1
    """)

    if not df_cur.empty:
        cur = df_cur.iloc[0]
        prev = df_prev.iloc[0] if not df_prev.empty else None

        def delta(field):
            if prev is None: return None
            return float(cur[field]) - float(prev[field])

        col1.metric("订单量（本分钟）", int(cur['order_cnt']), delta('order_cnt'))
        col2.metric("GMV（本分钟）", f"R$ {cur['total_gmv']:,.0f}", delta('total_gmv'))
        col3.metric("均价", f"R$ {cur['avg_price']:.2f}", delta('avg_price'))
        col4.metric("独立用户", int(cur['unique_customers']))
        col5.metric("热门品类", str(cur['top_category']))
    else:
        for col in [col1, col2, col3, col4, col5]:
            col.metric("—", "暂无数据")

    st.markdown("---")

    # ── 趋势图 ────────────────────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("订单量 & GMV 趋势（近60分钟）")
        df_trend = safe_query("""
            SELECT window_start, order_cnt, round(total_gmv, 0) AS total_gmv
            FROM dws.realtime_minute_stats
            WHERE window_start >= now() - INTERVAL 60 MINUTE
            ORDER BY window_start
        """)
        if not df_trend.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_trend['window_start'], y=df_trend['order_cnt'],
                name='订单量', line=dict(color='#4C9BE8'), yaxis='y'
            ))
            fig.add_trace(go.Scatter(
                x=df_trend['window_start'], y=df_trend['total_gmv'],
                name='GMV', line=dict(color='#F97316'), yaxis='y2'
            ))
            fig.update_layout(
                yaxis=dict(title='订单量', showgrid=False),
                yaxis2=dict(title='GMV (R$)', overlaying='y', side='right'),
                legend=dict(orientation='h', y=1.1),
                margin=dict(l=0, r=0, t=20, b=0),
                hovermode='x unified',
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无趋势数据，Flink 作业可能尚未启动")

    with right:
        st.subheader("今日品类 Top 10")
        df_cat = safe_query("SELECT product_category, order_cnt, round(gmv,0) AS gmv FROM ads.realtime_category_today LIMIT 10")
        if not df_cat.empty:
            fig = px.bar(
                df_cat, x='gmv', y='product_category', orientation='h',
                color='gmv', color_continuous_scale='Blues',
                labels={'gmv': 'GMV (R$)', 'product_category': '品类'},
            )
            fig.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False, margin=dict(l=0, r=0, t=0, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("今日暂无订单数据")

    st.markdown("---")

    # ── 今日小时趋势 ──────────────────────────────────────────
    st.subheader("今日小时趋势")
    df_hourly = safe_query("""
        SELECT hour_start, order_cnt, round(gmv, 0) AS gmv, round(avg_price, 2) AS avg_price
        FROM ads.realtime_hourly ORDER BY hour_start
    """)
    if not df_hourly.empty:
        fig = px.bar(
            df_hourly, x='hour_start', y='order_cnt',
            labels={'hour_start': '小时', 'order_cnt': '订单量'},
            color='gmv', color_continuous_scale='Viridis',
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("今日暂无小时统计数据")

    # ── 实时订单流 ────────────────────────────────────────────
    st.subheader("实时订单流（最新20条）")
    df_orders = safe_query("""
        SELECT event_time, order_id, product_category, state,
               round(price, 2) AS price, order_status
        FROM ods.orders_stream
        ORDER BY _ingest_time DESC LIMIT 20
    """)
    if not df_orders.empty:
        st.dataframe(df_orders, use_container_width=True, hide_index=True)
    else:
        st.info("暂无实时订单，请检查 Kafka 生产者是否运行")

    # ── 告警面板 ──────────────────────────────────────────────
    df_alerts = safe_query("""
        SELECT alert_time, severity, alert_type, detail, ai_suggestion
        FROM stream.ai_quality_alerts
        WHERE alert_time >= now() - INTERVAL 1 HOUR
        ORDER BY alert_time DESC LIMIT 5
    """)
    if not df_alerts.empty:
        st.markdown("---")
        st.subheader(f"⚠️ 近1小时告警（{len(df_alerts)} 条）")
        for _, row in df_alerts.iterrows():
            color = 'red' if row['severity'] == 'HIGH' else 'orange'
            with st.expander(f"[{row['severity']}] {row['detail'][:60]}...", expanded=False):
                st.markdown(f"**时间**：{row['alert_time']}")
                st.markdown(f"**类型**：{row['alert_type']}")
                st.markdown(f"**详情**：{row['detail']}")
                st.markdown(f"**AI建议**：{row['ai_suggestion']}")

    # 自动刷新
    if auto_refresh:
        time.sleep(30)
        st.rerun()


# ══════════════════════════════════════════════════════════════
# 页面 2：智能查询（多轮对话 NL2SQL + RAG）
# ══════════════════════════════════════════════════════════════
elif page == "智能查询":
    st.title("💬 智能查询")
    st.caption("支持多轮对话 · 自动路由 NL2SQL / 知识库问答 · 数据来自 Kafka 实时流")

    # ── 初始化 session state ──────────────────────────────────
    if 'chat_messages' not in st.session_state:
        st.session_state['chat_messages'] = []   # 展示用消息列表
    if 'nl2sql_history' not in st.session_state:
        st.session_state['nl2sql_history'] = []  # NL2SQL 历史（含 sql/result_summary）
    if 'rag_history' not in st.session_state:
        st.session_state['rag_history'] = []     # RAG 历史（含 question/answer）

    # ── 工具函数：自动绘图 ────────────────────────────────────
    def _auto_chart(df: pd.DataFrame):
        if df.empty or len(df.columns) < 2:
            st.dataframe(df, use_container_width=True)
            return
        all_cols = df.columns.tolist()
        num_cols = df.select_dtypes(include='number').columns.tolist()
        if not num_cols:
            st.dataframe(df, use_container_width=True)
            return
        x_col = all_cols[0]
        x_lower = str(x_col).lower()
        if any(k in x_lower for k in ['time', 'start', 'hour', 'dt', 'date']):
            fig = px.line(df, x=x_col, y=num_cols, markers=True)
        elif df[x_col].dtype == object:
            fig = px.bar(df.head(20), x=num_cols[0], y=x_col, orientation='h',
                         color=num_cols[0], color_continuous_scale='Blues')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        else:
            fig = px.bar(df.head(20), x=x_col, y=num_cols[0])
        fig.update_layout(margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # ── 顶栏：示例问题 + 清空按钮 ─────────────────────────────
    top_l, top_r = st.columns([5, 1])
    with top_r:
        if st.button("🗑 清空对话", use_container_width=True):
            st.session_state['chat_messages']  = []
            st.session_state['nl2sql_history'] = []
            st.session_state['rag_history']    = []
            st.rerun()

    with top_l:
        examples = [
            "最近10分钟每分钟订单量和GMV趋势",
            "今日各品类销售额排行",
            "GMV 和 payment_value 有什么区别？",
            "order_status 有哪些状态？",
        ]
        st.markdown("**快速提问**")
        ecols = st.columns(4)
        for i, ex in enumerate(examples):
            if ecols[i].button(ex, key=f"ex_{i}", use_container_width=True):
                st.session_state['pending_question'] = ex

    st.markdown("---")

    # ── 历史消息渲染 ──────────────────────────────────────────
    for msg in st.session_state['chat_messages']:
        with st.chat_message(msg['role']):
            if msg['role'] == 'user':
                st.markdown(msg['content'])
            else:
                msg_type = msg.get('type', 'text')
                if msg_type == 'nl2sql':
                    if msg.get('error'):
                        st.error(f"查询失败：{msg['error']}")
                    else:
                        if msg.get('insight'):
                            st.info(f"💡 {msg['insight']}")
                        with st.expander("查看 SQL", expanded=False):
                            st.code(msg.get('sql', ''), language='sql')
                        df = msg.get('data')
                        if df is not None and not df.empty:
                            tab1, tab2 = st.tabs(["图表", "数据表"])
                            with tab1:
                                _auto_chart(df)
                            with tab2:
                                st.dataframe(df, use_container_width=True, hide_index=True)
                                st.download_button(
                                    "下载 CSV",
                                    df.to_csv(index=False, encoding='utf-8-sig'),
                                    f"result_{len(st.session_state['chat_messages'])}.csv",
                                    "text/csv",
                                    key=f"dl_{len(st.session_state['chat_messages'])}_{id(df)}"
                                )
                        else:
                            st.warning("查询结果为空")
                elif msg_type == 'rag':
                    st.markdown(msg['content'])
                    if msg.get('sources'):
                        st.caption(f"来源：{', '.join(msg['sources'])}")
                else:
                    st.markdown(msg['content'])

    # ── 输入框 ─────────────────────────────────────────────────
    pending = st.session_state.pop('pending_question', None)
    question = st.chat_input("输入问题，支持追问（例如：再按州分组 / 那取消率呢？）")
    question = question or pending

    if question:
        # 追加用户消息
        st.session_state['chat_messages'].append({'role': 'user', 'content': question})

        with st.chat_message('user'):
            st.markdown(question)

        with st.chat_message('assistant'):
            # ── 路由判断 ──────────────────────────────────────
            from ai_layer.rag_engine import route_question, rag_query
            from ai_layer.nl2sql import nl2sql

            # 有 NL2SQL 历史时，追问大概率还是数据查询，跳过路由 LLM 节省成本
            route = 'nl2sql'
            if not st.session_state['nl2sql_history'] and not st.session_state['rag_history']:
                with st.spinner("路由分析中..."):
                    route = route_question(question)
            elif st.session_state['rag_history'] and not st.session_state['nl2sql_history']:
                # 上一轮是 RAG，先用快速规则判断
                kw_data = any(k in question for k in ['查', '多少', '排行', '趋势', '分钟', '今日', '最近', 'SQL', 'sql'])
                route = 'nl2sql' if kw_data else 'rag'
            elif st.session_state['rag_history']:
                # 混合历史：关键词判断
                kw_knowledge = any(k in question for k in ['什么是', '定义', '含义', '区别', '规则', '说明', '怎么', '为什么'])
                route = 'rag' if kw_knowledge else 'nl2sql'

            # ── NL2SQL 分支 ───────────────────────────────────
            if route == 'nl2sql':
                with st.spinner("生成 SQL 并查询中..."):
                    res = nl2sql(
                        question,
                        with_insight=True,
                        history=st.session_state['nl2sql_history'],
                    )

                if res['error']:
                    st.error(f"查询失败：{res['error']}")
                    st.session_state['chat_messages'].append({
                        'role': 'assistant', 'type': 'nl2sql',
                        'error': res['error'],
                    })
                else:
                    if res.get('insight'):
                        st.info(f"💡 {res['insight']}")
                    with st.expander("查看 SQL", expanded=False):
                        st.code(res['sql'], language='sql')
                    df = res['data']
                    if not df.empty:
                        tab1, tab2 = st.tabs(["图表", "数据表"])
                        with tab1:
                            _auto_chart(df)
                        with tab2:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                            st.download_button(
                                "下载 CSV",
                                df.to_csv(index=False, encoding='utf-8-sig'),
                                "result.csv", "text/csv",
                                key=f"dl_new_{len(st.session_state['chat_messages'])}"
                            )
                    else:
                        st.warning("查询结果为空")

                    # 更新 NL2SQL 历史
                    st.session_state['nl2sql_history'].append({
                        'question': question,
                        'sql': res['sql'],
                        'result_summary': res.get('result_summary', ''),
                    })
                    if len(st.session_state['nl2sql_history']) > 5:
                        st.session_state['nl2sql_history'].pop(0)

                    st.session_state['chat_messages'].append({
                        'role': 'assistant', 'type': 'nl2sql',
                        'sql': res['sql'],
                        'insight': res.get('insight', ''),
                        'data': df,
                        'error': None,
                    })

            # ── RAG 分支 ──────────────────────────────────────
            else:
                with st.spinner("检索知识库中..."):
                    res = rag_query(question, history=st.session_state['rag_history'])

                st.markdown(res['answer'])
                if res.get('sources'):
                    st.caption(f"来源：{', '.join(res['sources'])}")

                # 更新 RAG 历史
                st.session_state['rag_history'].append({
                    'question': question,
                    'answer': res['answer'],
                })
                if len(st.session_state['rag_history']) > 8:
                    st.session_state['rag_history'].pop(0)

                st.session_state['chat_messages'].append({
                    'role': 'assistant', 'type': 'rag',
                    'content': res['answer'],
                    'sources': res.get('sources', []),
                })


# ══════════════════════════════════════════════════════════════
# 页面 3：Agent 分析
# ══════════════════════════════════════════════════════════════
elif page == "Agent 分析":
    st.title("🤖 Agent 分析")
    st.caption("AI Agent 自主分析实时数据，多步推理，自动生成报告")

    tab1, tab2, tab3 = st.tabs(["异常检测 Agent", "运营快报 Agent", "自由分析 Agent"])

    with tab1:
        st.markdown("**功能**：自动检测分钟级流量异常，分析告警，生成检测报告")
        if st.button("▶ 启动异常检测", key="anomaly", type="primary"):
            from ai_layer.agents import run_anomaly_agent
            with st.spinner("Agent 运行中（约30-60秒）..."):
                try:
                    result = run_anomaly_agent()
                    st.success("检测完成")
                    st.markdown("**最终结论**")
                    st.write(result['output'])
                    steps = result.get('intermediate_steps', [])
                    with st.expander(f"推理步骤（共 {len(steps)} 步）"):
                        for i, (action, obs) in enumerate(steps, 1):
                            st.markdown(f"**步骤 {i}**：调用 `{action.tool}`")
                            st.code(str(obs)[:500])
                except Exception as e:
                    st.error(f"Agent 运行失败：{e}")

    with tab2:
        st.markdown("**功能**：汇总今日各维度实时数据，生成运营快报")
        if st.button("▶ 生成运营快报", key="report", type="primary"):
            from ai_layer.agents import run_report_agent
            with st.spinner("Agent 运行中（约30-60秒）..."):
                try:
                    result = run_report_agent()
                    st.success("快报生成完成")
                    st.write(result['output'])
                except Exception as e:
                    st.error(f"Agent 运行失败：{e}")

    with tab3:
        st.markdown("**功能**：输入任意分析目标，Agent 自主决定查哪些数据、怎么分析")
        goal = st.text_area(
            "分析目标",
            placeholder="例如：分析当前实时取消率异常，找出主要影响品类和地区，给出处理建议",
            height=80,
        )
        if st.button("▶ 开始分析", key="free", type="primary") and goal.strip():
            from ai_layer.agents import run_free_agent
            with st.spinner("Agent 运行中..."):
                try:
                    result = run_free_agent(goal)
                    st.success("分析完成")
                    st.write(result['output'])
                    steps = result.get('intermediate_steps', [])
                    with st.expander(f"推理步骤（共 {len(steps)} 步）"):
                        for i, (action, obs) in enumerate(steps, 1):
                            st.markdown(f"**步骤 {i}**：`{action.tool}`")
                            st.code(str(obs)[:400])
                except Exception as e:
                    st.error(f"Agent 运行失败：{e}")
