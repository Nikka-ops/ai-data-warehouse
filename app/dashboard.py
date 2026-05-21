# -*- coding: utf-8 -*-
"""
AI 智能查询界面
- NL2SQL：自然语言 → ClickHouse SQL，多轮对话，自动图表
- RAG：业务知识库问答（指标定义、字段含义、规则）
- Agent：自主多步分析（异常检测、运营快报、自由分析）
BI 可视化由 Apache Superset 提供（http://localhost:8088）。
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from config import cfg

st.set_page_config(
    page_title="AI 数仓 · 智能分析",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── session_state 初始化 ──────────────────────────────────────
def _init():
    if 'session_id'     not in st.session_state:
        from ai_layer.session_manager import new_session_id
        st.session_state['session_id']     = new_session_id()
    if 'chat_messages'  not in st.session_state:
        st.session_state['chat_messages']  = []
    if 'nl2sql_history' not in st.session_state:
        st.session_state['nl2sql_history'] = []
    if 'rag_history'    not in st.session_state:
        st.session_state['rag_history']    = []
    if 'turn_index'     not in st.session_state:
        st.session_state['turn_index']     = 0

_init()


def _load_session(sid: str):
    from ai_layer.session_manager import load_session
    data = load_session(sid)
    st.session_state.update({
        'session_id':     sid,
        'chat_messages':  data['chat_messages'],
        'nl2sql_history': data['nl2sql_history'],
        'rag_history':    data['rag_history'],
        'turn_index':     len(data['chat_messages']),
    })


def _clear_session():
    from ai_layer.session_manager import new_session_id
    st.session_state.update({
        'session_id':     new_session_id(),
        'chat_messages':  [],
        'nl2sql_history': [],
        'rag_history':    [],
        'turn_index':     0,
    })


# ── 侧边栏 ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🤖 AI 数仓")

    # BI 工具快捷入口
    st.markdown("""
<a href="http://localhost:8088" target="_blank">
  <button style="width:100%;padding:8px;background:#1a73e8;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px">
    📊 打开 Superset BI 看板
  </button>
</a>
""", unsafe_allow_html=True)

    st.markdown("---")
    page = st.radio("", ["💬 智能查询", "🤖 Agent 分析"], label_visibility="collapsed")

    if page == "💬 智能查询":
        st.markdown("---")
        st.markdown("**历史会话**")
        from ai_layer.session_manager import list_recent_sessions
        sessions = list_recent_sessions(limit=10)
        cur_sid = st.session_state.get('session_id', '')
        if not sessions:
            st.caption("暂无历史会话")
        else:
            for s in sessions:
                sid   = s['session_id']
                label = s['session_name'] or s['first_question'] or '新会话'
                ts    = s['started_at'].strftime('%m-%d %H:%M') if s.get('started_at') else ''
                col_b, col_i = st.columns([3, 1])
                if col_b.button(
                    f"{'▶ ' if sid == cur_sid else ''}{label[:28]}",
                    key=f"ses_{sid}", use_container_width=True,
                    type="primary" if sid == cur_sid else "secondary",
                ):
                    if sid != cur_sid:
                        _load_session(sid)
                        st.rerun()
                col_i.caption(f"{s['turn_count']}轮\n{ts}")
        st.markdown("---")
        if st.button("＋ 新建会话", use_container_width=True):
            _clear_session()
            st.rerun()

    st.markdown("---")
    st.caption("""
**数据链路**
Kafka → Flink → ClickHouse
ODS → DWD → DWS → ADS

**AI 服务**
• NL2SQL · RAG 知识库
• 预测（Holt平滑）
• 主动洞察（5分钟）
• AI ETL 质检
""")


# ══════════════════════════════════════════════════════════════
# 页面：智能查询（NL2SQL + RAG 多轮对话）
# ══════════════════════════════════════════════════════════════
if page == "💬 智能查询":
    from ai_layer.session_manager import save_turn

    st.title("💬 智能查询")
    cur_sid = st.session_state['session_id']
    st.caption(f"自然语言 → SQL | 知识库问答 | 多轮上下文 | 会话 `{cur_sid[:8]}...`")

    def _auto_chart(df: pd.DataFrame):
        if df.empty or len(df.columns) < 2:
            st.dataframe(df, use_container_width=True)
            return
        num_cols = df.select_dtypes(include='number').columns.tolist()
        if not num_cols:
            st.dataframe(df, use_container_width=True)
            return
        x_col = df.columns[0]
        x_lower = str(x_col).lower()
        if any(k in x_lower for k in ['time', 'start', 'hour', 'dt', 'date', 'window']):
            fig = px.line(df, x=x_col, y=num_cols, markers=True)
        elif df[x_col].dtype == object:
            fig = px.bar(df.head(20), x=num_cols[0], y=x_col, orientation='h',
                         color=num_cols[0], color_continuous_scale='Blues')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        else:
            fig = px.bar(df.head(20), x=x_col, y=num_cols[0])
        fig.update_layout(margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # 顶栏：快速示例
    top_l, top_r = st.columns([5, 1])
    with top_r:
        if st.button("🗑 新建对话", use_container_width=True):
            _clear_session()
            st.rerun()
    with top_l:
        examples = [
            "最近30分钟每分钟订单量和GMV趋势",
            "今日各品类销售额排行",
            "各州订单量和GMV对比",
            "GMV 和 payment_value 有什么区别？",
        ]
        st.markdown("**快速提问**")
        ecols = st.columns(4)
        for i, ex in enumerate(examples):
            if ecols[i].button(ex[:20] + '...', key=f"ex_{i}", use_container_width=True):
                st.session_state['pending_question'] = ex

    st.markdown("---")

    # 渲染历史消息
    for msg in st.session_state['chat_messages']:
        with st.chat_message(msg['role']):
            if msg['role'] == 'user':
                st.markdown(msg['content'])
            elif msg.get('type') == 'nl2sql':
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
                                f"result_{id(df)}.csv", "text/csv",
                                key=f"dl_h_{id(df)}"
                            )
                    else:
                        st.warning("查询结果为空")
            elif msg.get('type') == 'rag':
                st.markdown(msg['content'])
                if msg.get('sources'):
                    st.caption(f"来源：{', '.join(msg['sources'])}")
            else:
                st.markdown(msg.get('content', ''))

    # 输入框
    pending = st.session_state.pop('pending_question', None)
    question = st.chat_input("输入问题，支持追问（例如：再按州分组 / payment_value 是什么？）")
    question = question or pending

    if question:
        tidx = st.session_state['turn_index']
        save_turn(cur_sid, tidx, 'user', content=question)
        st.session_state['chat_messages'].append({'role': 'user', 'content': question})
        st.session_state['turn_index'] += 1

        with st.chat_message('user'):
            st.markdown(question)

        with st.chat_message('assistant'):
            from ai_layer.rag_engine import route_question, rag_query
            from ai_layer.nl2sql import nl2sql

            # 路由：首轮用 LLM，后续用关键词节省成本
            if not st.session_state['nl2sql_history'] and not st.session_state['rag_history']:
                with st.spinner("路由分析中..."):
                    route = route_question(question)
            else:
                kw_knowledge = any(k in question for k in
                                   ['什么是', '定义', '含义', '区别', '规则', '说明', '怎么', '为什么', '有哪些'])
                route = 'rag' if kw_knowledge else 'nl2sql'

            a_tidx = st.session_state['turn_index']

            if route == 'nl2sql':
                with st.spinner("生成 SQL 并查询..."):
                    res = nl2sql(question, with_insight=True,
                                 history=st.session_state['nl2sql_history'])

                if res['error']:
                    st.error(f"查询失败：{res['error']}")
                    save_turn(cur_sid, a_tidx, 'assistant', 'nl2sql',
                              content=f"查询失败：{res['error']}")
                    st.session_state['chat_messages'].append(
                        {'role': 'assistant', 'type': 'nl2sql', 'error': res['error']})
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
                            st.download_button("下载 CSV",
                                               df.to_csv(index=False, encoding='utf-8-sig'),
                                               "result.csv", "text/csv",
                                               key=f"dl_new_{a_tidx}")
                    else:
                        st.warning("查询结果为空")

                    save_turn(cur_sid, a_tidx, 'assistant', 'nl2sql',
                              content=res.get('insight', ''), sql_text=res['sql'],
                              result_summary=res.get('result_summary', ''))
                    st.session_state['nl2sql_history'].append({
                        'question': question, 'sql': res['sql'],
                        'result_summary': res.get('result_summary', ''),
                    })
                    if len(st.session_state['nl2sql_history']) > 5:
                        st.session_state['nl2sql_history'].pop(0)
                    st.session_state['chat_messages'].append({
                        'role': 'assistant', 'type': 'nl2sql',
                        'sql': res['sql'], 'insight': res.get('insight', ''),
                        'data': df, 'error': None,
                    })

            else:  # RAG
                with st.spinner("检索知识库..."):
                    res = rag_query(question, history=st.session_state['rag_history'])
                st.markdown(res['answer'])
                if res.get('sources'):
                    st.caption(f"来源：{', '.join(res['sources'])}")
                save_turn(cur_sid, a_tidx, 'assistant', 'rag',
                          content=res['answer'], sources=','.join(res.get('sources', [])))
                st.session_state['rag_history'].append(
                    {'question': question, 'answer': res['answer']})
                if len(st.session_state['rag_history']) > 8:
                    st.session_state['rag_history'].pop(0)
                st.session_state['chat_messages'].append({
                    'role': 'assistant', 'type': 'rag',
                    'content': res['answer'], 'sources': res.get('sources', []),
                })

            st.session_state['turn_index'] += 1


# ══════════════════════════════════════════════════════════════
# 页面：Agent 分析
# ══════════════════════════════════════════════════════════════
elif page == "🤖 Agent 分析":
    st.title("🤖 Agent 分析")
    st.caption("AI Agent 自主多步推理，调用 ClickHouse + 知识库 + 预测 + 洞察等工具")

    tab1, tab2, tab3, tab4 = st.tabs(["异常检测", "Lambda 对账", "AI 洞察", "自由分析"])

    with tab1:
        st.markdown("**实时异常检测（2σ基线法）+ AI 告警自动排查结论**")
        if st.button("▶ 启动异常检测", key="anomaly", type="primary"):
            from ai_layer.agents import run_anomaly_agent
            with st.spinner("Agent 运行中（约30-60秒）..."):
                try:
                    result = run_anomaly_agent()
                    st.success("检测完成")
                    st.write(result['output'])
                    steps = result.get('intermediate_steps', [])
                    with st.expander(f"推理步骤（共 {len(steps)} 步）"):
                        for i, (action, obs) in enumerate(steps, 1):
                            st.markdown(f"**步骤 {i}**：调用 `{action.tool}`")
                            st.code(str(obs)[:500])
                except Exception as e:
                    st.error(f"Agent 运行失败：{e}")

    with tab2:
        st.markdown("**Lambda 架构批实时数据一致性校验（离线层 vs 速度层）**")
        col_l, col_r = st.columns(2)
        with col_l:
            if st.button("▶ 运行 Lambda 一致性分析", key="lambda_agent", type="primary"):
                from ai_layer.agents import run_lambda_agent
                with st.spinner("Agent 分析中..."):
                    try:
                        result = run_lambda_agent()
                        st.success("分析完成")
                        st.write(result['output'])
                    except Exception as e:
                        st.error(f"Agent 运行失败：{e}")
        with col_r:
            st.markdown("**对账记录（最近7天）**")
            try:
                rows = clickhouse_connect.get_client(
                    host=cfg.ch_host, port=cfg.ch_port,
                    username=cfg.ch_user, password=cfg.ch_password,
                    connect_timeout=5, send_receive_timeout=15,
                ).query("""
                    SELECT check_date, batch_order_cnt, stream_order_cnt,
                           cnt_diff_pct, check_status
                    FROM stream.lambda_reconciliation
                    ORDER BY check_date DESC LIMIT 7
                """).result_rows
                if rows:
                    import pandas as pd
                    df_rec = pd.DataFrame(rows, columns=['日期','批处理量','实时量','差异%','状态'])
                    st.dataframe(df_rec, use_container_width=True, hide_index=True)
                else:
                    st.info("暂无对账记录（需先运行历史数据加载 + reconciler 服务）")
            except Exception as e:
                st.warning(f"查询失败：{e}")

    with tab3:
        st.markdown("**查看 AI 主动洞察引擎生成的最新洞察报告**")

        import clickhouse_connect
        try:
            ch = clickhouse_connect.get_client(
                host=cfg.ch_host, port=cfg.ch_port,
                username=cfg.ch_user, password=cfg.ch_password,
                connect_timeout=5, send_receive_timeout=15,
            )
            rows = ch.query("""
                SELECT generated_at, insight_type, title, content, priority
                FROM stream.proactive_insights
                ORDER BY generated_at DESC LIMIT 10
            """).result_rows
        except Exception as e:
            rows = []
            st.warning(f"无法连接 ClickHouse：{e}")

        if not rows:
            st.info("暂无洞察数据，洞察引擎每5分钟自动生成（需启动 insight-engine 服务）")
        else:
            _ICON = {'trend_up': '📈', 'trend_down': '📉',
                     'anomaly': '🚨', 'opportunity': '💰', 'summary': '📊'}
            for r in rows:
                icon = _ICON.get(r[1], '💡')
                ts   = str(r[0])[:16]
                with st.expander(f"{icon} {r[2]}  `{ts}`", expanded=False):
                    st.markdown(r[3])
                    st.caption(f"类型：{r[1]}  |  优先级：{r[4]}")

        if st.button("🔄 刷新洞察", key="refresh_insight"):
            st.rerun()

    with tab4:
        st.markdown("**输入任意分析目标，Agent 自主决定调用哪些工具、如何分析**")
        goal = st.text_area(
            "分析目标",
            placeholder="例如：分析当前取消率异常，找出主要影响品类和地区，结合预测数据给出建议",
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
