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
    page = st.radio("", ["💬 智能查询", "🤖 Agent 分析", "🗄️ 特征存储", "📡 业务监控"], label_visibility="collapsed")

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

    # 侧边栏：实时告警处置状态角标
    try:
        import clickhouse_connect as _cc_sb
        _ch_sb = _cc_sb.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password,
            connect_timeout=3, send_receive_timeout=5,
        )
        _cnt = _ch_sb.query(
            "SELECT count() FROM stream.remediation_actions WHERE action_time >= now() - INTERVAL 1 HOUR"
        ).first_row
        _pend = _ch_sb.query(
            "SELECT count() FROM stream.alert_unified WHERE alert_time >= now() - INTERVAL 10 MINUTE"
        ).first_row
        sb_handled = int(_cnt[0] or 0)
        sb_pending = int(_pend[0] or 0)
        if sb_pending > 0:
            st.error(f"🚨 待处置告警 {sb_pending} 条")
        elif sb_handled > 0:
            st.success(f"✅ 近1小时已处置 {sb_handled} 条")
        else:
            st.success("✅ 无活跃告警")
    except Exception:
        pass

    st.caption("""
**Kappa 架构**
Kafka（永久日志）→ Flink → ClickHouse
ODS → DWD → DWS → ADS

**AI 服务**
• NL2SQL · RAG 知识库
• 告警自动处置（30s轮询）
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["异常检测", "自动处置", "Kappa 回放", "AI 洞察", "自由分析"])

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
        st.markdown("**告警自动处置中心** — 检测、分析、修复、反馈全自动闭环")
        st.caption("覆盖：数据质量告警 / Kappa 回放失败 / Kafka Lag 爆发 / ETL 质量劣化")

        import clickhouse_connect as _cc2

        # ── 顶部：处置统计 ───────────────────────────────────────
        try:
            _ch2 = _cc2.get_client(
                host=cfg.ch_host, port=cfg.ch_port,
                username=cfg.ch_user, password=cfg.ch_password,
                connect_timeout=5, send_receive_timeout=15,
            )
            _stats = _ch2.query("""
                SELECT
                    countIf(final_status='resolved')   AS resolved,
                    countIf(final_status='monitoring') AS monitoring,
                    countIf(final_status='escalated')  AS escalated,
                    countIf(action_success=1)          AS success_actions,
                    count()                            AS total
                FROM stream.remediation_actions
                WHERE action_time >= now() - INTERVAL 24 HOUR
            """).first_row
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("✅ 已修复", int(_stats[0] or 0), help="LLM 判断 resolved")
            c2.metric("👁 监控中", int(_stats[1] or 0), help="已处置，观察恢复")
            c3.metric("🚨 需人工", int(_stats[2] or 0), help="超出自动处理范围")
            c4.metric("⚡ 动作执行成功率",
                      f"{int(_stats[3] or 0)}/{int(_stats[4] or 0)}",
                      help="近24小时 Playbook 成功次数")
        except Exception as e:
            st.warning(f"统计查询失败：{e}")

        st.markdown("---")

        # ── 实时处置记录 ─────────────────────────────────────────
        col_feed, col_ctrl = st.columns([3, 1])

        with col_ctrl:
            st.markdown("**操作**")
            if st.button("🔄 刷新", key="refresh_rem", use_container_width=True):
                st.rerun()
            if st.button("▶ AI 分析当前状态", key="rem_agent",
                         type="primary", use_container_width=True):
                from ai_layer.agents import run_anomaly_agent
                with st.spinner("Agent 运行中..."):
                    try:
                        result = run_anomaly_agent()
                        st.success("分析完成")
                        st.write(result['output'])
                        steps = result.get('intermediate_steps', [])
                        with st.expander(f"推理步骤（{len(steps)} 步）"):
                            for i, (action, obs) in enumerate(steps, 1):
                                st.markdown(f"**{i}. {action.tool}**")
                                st.code(str(obs)[:300])
                    except Exception as e:
                        st.error(f"Agent 失败：{e}")

            st.markdown("---")
            st.markdown("**告警类型**")
            try:
                _type_rows = _ch2.query("""
                    SELECT alert_type, count() AS cnt
                    FROM stream.remediation_actions
                    WHERE action_time >= now() - INTERVAL 24 HOUR
                    GROUP BY alert_type ORDER BY cnt DESC
                """).result_rows
                for r in _type_rows:
                    st.caption(f"• {r[0]}：{r[1]} 次")
            except Exception:
                pass

        with col_feed:
            st.markdown("**近期处置记录（自动刷新）**")
            try:
                _rows = _ch2.query("""
                    SELECT action_time, alert_type, alert_severity,
                           action_type, root_cause, action_result,
                           action_success, final_status, confidence
                    FROM stream.remediation_dashboard
                    LIMIT 20
                """).result_rows

                if not _rows:
                    st.info("暂无处置记录（alert-investigator 服务每30秒巡检一次）")
                else:
                    for r in _rows:
                        _status_color = {
                            'resolved':  'success',
                            'monitoring': 'warning',
                            'escalated': 'error',
                        }.get(str(r[7]), 'info')
                        _icon = {'resolved': '✅', 'monitoring': '👁', 'escalated': '🚨'}.get(
                            str(r[7]), '❓')
                        _ok = '✓' if r[6] else '✗'
                        with st.expander(
                            f"{_icon} [{r[2]}] {r[1]}  `{str(r[3])}`  "
                            f"`{str(r[0])[:16]}`",
                            expanded=(str(r[7]) == 'escalated'),
                        ):
                            st.markdown(f"**根因**：{r[4]}")
                            st.markdown(f"**动作**：`{r[3]}` {_ok}")
                            st.markdown(f"**结果**：{r[5]}")
                            st.caption(
                                f"状态：{_icon} {r[7]} | "
                                f"置信度：{float(r[8] or 0):.0%}"
                            )
            except Exception as e:
                st.warning(f"查询失败：{e}")

        # ── 系统告警原始记录 ─────────────────────────────────────
        with st.expander("查看系统告警原始记录（stream.system_alerts）"):
            try:
                _sys = _ch2.query("""
                    SELECT alert_time, alert_type, severity, title, handled
                    FROM stream.system_alerts
                    ORDER BY alert_time DESC LIMIT 20
                """).result_rows
                if _sys:
                    import pandas as pd
                    _df_sys = pd.DataFrame(_sys,
                        columns=['时间', '类型', '级别', '摘要', '已处理'])
                    st.dataframe(_df_sys, use_container_width=True, hide_index=True)
                else:
                    st.info("暂无系统告警记录")
            except Exception as e:
                st.warning(f"查询失败：{e}")

    with tab3:
        st.markdown("**Kappa 架构：Flink 历史回放状态 + AI 驱动重算**")
        st.caption("单一流处理路径：Kafka（永久保留）→ Flink（实时 + 回放）→ ClickHouse")

        import clickhouse_connect as _cc
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown("**回放任务管理**")
            if st.button("▶ AI 分析 Kappa 状态", key="kappa_agent", type="primary"):
                from ai_layer.agents import run_kappa_agent
                with st.spinner("Agent 分析中..."):
                    try:
                        result = run_kappa_agent()
                        st.success("分析完成")
                        st.write(result['output'])
                    except Exception as e:
                        st.error(f"Agent 运行失败：{e}")

            st.markdown("---")
            st.markdown("**历史覆盖概览**")
            try:
                ch = _cc.get_client(
                    host=cfg.ch_host, port=cfg.ch_port,
                    username=cfg.ch_user, password=cfg.ch_password,
                    connect_timeout=5, send_receive_timeout=15,
                )
                cov = ch.query("""
                    SELECT count() AS hours, min(hour_start) AS earliest,
                           max(hour_start) AS latest, sum(order_cnt) AS orders,
                           round(sum(total_gmv), 0) AS gmv
                    FROM dws.kappa_hourly_agg
                """).first_row
                if cov and cov[0] > 0:
                    st.metric("已回放小时数", f"{cov[0]:,}")
                    st.metric("覆盖起点", str(cov[1])[:16] if cov[1] else "—")
                    st.metric("最新时间", str(cov[2])[:16] if cov[2] else "—")
                    st.metric("总订单数", f"{int(cov[3]):,}")
                    st.metric("总 GMV", f"R${float(cov[4]):,.0f}")
                else:
                    st.info("暂无历史聚合数据（可运行 flink-replay 服务执行回放）")
            except Exception as e:
                st.warning(f"查询失败：{e}")

        with col_r:
            st.markdown("**回放任务记录**")
            try:
                rows = _cc.get_client(
                    host=cfg.ch_host, port=cfg.ch_port,
                    username=cfg.ch_user, password=cfg.ch_password,
                    connect_timeout=5, send_receive_timeout=15,
                ).query("""
                    SELECT job_name, triggered_by, start_time,
                           records_processed, status
                    FROM stream.kappa_replay_jobs
                    ORDER BY start_time DESC LIMIT 10
                """).result_rows
                if rows:
                    import pandas as pd
                    df_rep = pd.DataFrame(rows, columns=['任务名','触发方式','开始时间','处理量','状态'])
                    st.dataframe(df_rep, use_container_width=True, hide_index=True)
                else:
                    st.info("暂无回放任务记录")
            except Exception as e:
                st.warning(f"查询失败：{e}")

    with tab4:
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

    with tab5:
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

# ══════════════════════════════════════════════════════════════
# 特征存储页
# ══════════════════════════════════════════════════════════════
elif page == "🗄️ 特征存储":
    st.title("🗄️ 特征存储（Feature Store）")
    st.caption("特征注册、在线/离线查询、漂移监控、训练集构建")

    _tab1, _tab2, _tab3, _tab4 = st.tabs(
        ["📋 特征注册表", "🔍 在线查询", "📉 漂移监控", "🏗️ 训练集构建"]
    )

    @st.cache_resource
    def _ch():
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password,
            connect_timeout=10, send_receive_timeout=60,
        )

    with _tab1:
        st.markdown("所有已注册的特征组和特征定义（来自 `feature_store.feature_definitions`）")
        col1, col2 = st.columns([2, 1])
        with col1:
            try:
                rows = _ch().query("""
                    SELECT group_name, count() AS total,
                           countIf(is_active=1) AS active,
                           max(updated_at) AS last_update
                    FROM feature_store.feature_definitions
                    GROUP BY group_name ORDER BY group_name
                """).result_rows
                if rows:
                    df_groups = pd.DataFrame(rows, columns=["特征组", "总数", "激活", "最后更新"])
                    st.dataframe(df_groups, use_container_width=True, hide_index=True)
                else:
                    st.info("特征注册表为空，请先运行 feature-init 服务加载 YAML 定义")
            except Exception as ex:
                st.error(f"查询失败：{ex}")
        with col2:
            st.metric("刷新频率", "每 5 分钟")
            st.metric("存储引擎", "ClickHouse + Redis")
            st.metric("架构", "Kappa 实时流")

        st.markdown("---")
        group_sel = st.selectbox(
            "选择特征组查看详情",
            ["user_behavior", "category_stats", "seller_stats"],
            key="fs_group_sel",
        )
        if group_sel:
            try:
                detail = _ch().query(f"""
                    SELECT feature_name, feature_type, description,
                           online_ttl, max_staleness_seconds, default_value,
                           arrayStringConcat(tags, ', ') AS tag_str
                    FROM feature_store.feature_definitions
                    WHERE group_name = '{group_sel}' AND is_active = 1
                    ORDER BY feature_name
                """).result_rows
                if detail:
                    df_d = pd.DataFrame(detail,
                        columns=["特征名", "类型", "描述", "在线TTL(s)", "最大陈旧(s)", "默认值", "标签"])
                    st.dataframe(df_d, use_container_width=True, hide_index=True)
                else:
                    st.info(f"{group_sel} 暂无激活特征")
            except Exception as ex:
                st.error(f"查询失败：{ex}")

    with _tab2:
        st.markdown("从离线 ClickHouse 读取实体特征值（在线 Redis 通过 feature-api 访问）")
        col_a, col_b = st.columns(2)
        with col_a:
            q_group = st.selectbox("特征组", ["user_behavior", "category_stats", "seller_stats"],
                                   key="online_group")
        with col_b:
            q_entity = st.text_input("实体ID（逗号分隔，最多10个）",
                                     placeholder="如：customer_abc,customer_xyz",
                                     key="online_entity")

        if st.button("🔍 查询特征", key="online_query", type="primary") and q_entity.strip():
            ids = [e.strip() for e in q_entity.split(',') if e.strip()][:10]
            id_list = ', '.join(f"'{i}'" for i in ids)
            try:
                rows = _ch().query(f"""
                    SELECT entity_id, feature_name, feature_value_str,
                           feature_time, computed_at
                    FROM feature_store.feature_values
                    WHERE group_name = '{q_group}'
                      AND entity_id IN ({id_list})
                    ORDER BY entity_id, feature_name, computed_at DESC
                    LIMIT BY 1 BY (entity_id, feature_name)
                """).result_rows
                if rows:
                    df_r = pd.DataFrame(rows,
                        columns=["实体ID", "特征名", "特征值", "特征时间", "计算时间"])
                    st.dataframe(df_r, use_container_width=True, hide_index=True)
                    st.caption(f"共 {len(rows)} 条特征值（每实体×每特征取最新一条）")
                else:
                    st.warning("未找到特征值，请确认实体ID正确且 feature-pipeline 已运行")
            except Exception as ex:
                st.error(f"查询失败：{ex}")

    with _tab3:
        st.markdown("PSI（Population Stability Index）特征漂移检测，每小时运行")
        st.caption("PSI < 0.10 稳定 | 0.10–0.25 监控 | > 0.25 漂移告警")

        drift_group = st.selectbox("特征组", ["user_behavior", "category_stats", "seller_stats"],
                                   key="drift_group")
        try:
            rows = _ch().query(f"""
                SELECT feature_name, psi_score, drift_detected,
                       mean, std, p50, p95, null_rate, computed_at
                FROM feature_store.drift_stats
                WHERE group_name = '{drift_group}'
                ORDER BY computed_at DESC
                LIMIT BY 1 BY feature_name
            """).result_rows
            if rows:
                df_drift = pd.DataFrame(rows, columns=[
                    "特征名", "PSI", "漂移", "均值", "标准差", "P50", "P95", "空值率", "检测时间"
                ])
                df_drift["状态"] = df_drift["漂移"].apply(lambda x: "🔴 漂移" if x else "🟢 正常")

                col1, col2, col3 = st.columns(3)
                n_drift = df_drift["漂移"].sum()
                col1.metric("监控特征数", len(df_drift))
                col2.metric("漂移特征数", int(n_drift), delta=None if n_drift == 0 else f"+{int(n_drift)}")
                col3.metric("最高 PSI", f"{df_drift['PSI'].max():.4f}")

                st.dataframe(
                    df_drift[["特征名", "PSI", "状态", "均值", "标准差", "P50", "P95", "空值率", "检测时间"]],
                    use_container_width=True, hide_index=True,
                )

                fig = px.bar(df_drift, x="特征名", y="PSI",
                             color="状态",
                             color_discrete_map={"🟢 正常": "#2ecc71", "🔴 漂移": "#e74c3c"},
                             title=f"{drift_group} PSI 分布",
                             text_auto=".4f")
                fig.add_hline(y=0.10, line_dash="dot", line_color="orange",
                              annotation_text="监控阈值(0.10)")
                fig.add_hline(y=0.25, line_dash="dash", line_color="red",
                              annotation_text="告警阈值(0.25)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无漂移统计数据，请等待 feature-drift 服务运行完成")
        except Exception as ex:
            st.error(f"查询失败：{ex}")

    with _tab4:
        st.markdown("通过 Point-in-Time（PIT）正确连接构建无数据泄漏的训练集")
        st.caption("标签 SQL 返回 (entity_id, event_time, label)，ASOF JOIN 特征实现时间点对齐")

        with st.expander("查看历史训练集", expanded=True):
            try:
                rows = _ch().query("""
                    SELECT dataset_name, arrayStringConcat(feature_groups, ', ') AS groups,
                           row_count, file_path, status, created_at
                    FROM feature_store.training_datasets
                    ORDER BY created_at DESC LIMIT 20
                """).result_rows
                if rows:
                    df_ds = pd.DataFrame(rows,
                        columns=["数据集名", "特征组", "行数", "文件路径", "状态", "创建时间"])
                    st.dataframe(df_ds, use_container_width=True, hide_index=True)
                else:
                    st.info("暂无训练集记录")
            except Exception as ex:
                st.error(f"查询失败：{ex}")

        st.markdown("---")
        st.markdown("**通过 API 构建新训练集**")
        ds_name = st.text_input("数据集名称", placeholder="如：churn_model_v1", key="ds_name")
        label_sql = st.text_area(
            "标签 SQL（需返回 entity_id, event_time, label 三列）",
            placeholder="SELECT customer_id AS entity_id,\n       order_time AS event_time,\n       if(cancel_count > 0, 1, 0) AS label\nFROM ods.orders_stream\nWHERE event_time >= now() - INTERVAL 30 DAY",
            height=120,
            key="label_sql",
        )
        feat_groups = st.multiselect(
            "选择特征组",
            ["user_behavior", "category_stats", "seller_stats"],
            default=["user_behavior"],
            key="feat_groups",
        )
        if st.button("🏗️ 构建训练集（调用 API）", key="build_ds", type="primary"):
            if not ds_name.strip() or not label_sql.strip() or not feat_groups:
                st.warning("请填写数据集名称、标签 SQL，并选择至少一个特征组")
            else:
                import json, urllib.request, urllib.error
                payload = json.dumps({
                    "dataset_name": ds_name.strip(),
                    "label_sql": label_sql.strip(),
                    "feature_groups": feat_groups,
                }).encode()
                try:
                    req = urllib.request.Request(
                        "http://feature-api:8000/features/dataset/build",
                        data=payload,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        result = json.loads(resp.read())
                    st.success(f"训练集构建已提交（后台运行）：{result}")
                except urllib.error.URLError as ex:
                    st.error(f"API 调用失败：{ex}（确保 feature-api 服务正常运行）")

# ══════════════════════════════════════════════════════════════
# 业务监控页
# ══════════════════════════════════════════════════════════════
elif page == "📡 业务监控":
    st.title("📡 业务监控")
    st.caption("业务指标告警 · 慢查询诊断 · 数据血缘图谱")

    _m1, _m2, _m3, _m4 = st.tabs(["🚨 业务告警", "🐢 慢查询诊断", "🕸️ 数据血缘", "📊 报告记录"])

    @st.cache_resource
    def _mch():
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password,
            connect_timeout=10, send_receive_timeout=60,
        )

    # ── Tab1：业务告警 ────────────────────────────────────────
    with _m1:
        st.markdown("每5分钟检测 GMV、取消率、品类订单量，与昨日同期对比，异常时触发 LLM 根因分析。")
        try:
            rows = _mch().query("""
                SELECT alert_time, metric_name, current_value, baseline_value,
                       change_pct, severity, detail, root_cause, webhook_sent, resolved
                FROM stream.business_alerts
                ORDER BY alert_time DESC LIMIT 50
            """).result_rows
            if rows:
                df_ba = pd.DataFrame(rows, columns=[
                    "时间", "指标", "当前值", "基准值", "变化%", "严重度", "详情", "根因分析", "已推送", "已解决"
                ])
                active = df_ba[df_ba["已解决"] == 0]
                c1, c2, c3 = st.columns(3)
                c1.metric("未处理告警", len(active))
                c2.metric("CRITICAL", int((active["严重度"] == "CRITICAL").sum()))
                c3.metric("已推送 Webhook", int(df_ba["已推送"].sum()))

                st.markdown("**最近告警（按时间倒序）**")
                for _, r in df_ba.head(20).iterrows():
                    icon = "🔴" if r["严重度"] == "CRITICAL" else "🟡"
                    status = "✅ 已解决" if r["已解决"] else "⏳ 待处理"
                    with st.expander(f"{icon} {r['时间']} — {r['指标']}  {status}"):
                        st.write(f"**当前值**：{r['当前值']:.2f}  **基准值**：{r['基准值']:.2f}  **变化**：{r['变化%']:+.1f}%")
                        st.write(f"**详情**：{r['详情']}")
                        st.info(f"💡 根因分析：{r['根因分析']}")
            else:
                st.success("暂无业务告警，系统运行正常 ✅")
        except Exception as ex:
            st.error(f"查询失败（确认 business-monitor 服务已启动）：{ex}")

        if st.button("🔄 刷新告警", key="refresh_ba"):
            st.rerun()

    # ── Tab2：慢查询诊断 ──────────────────────────────────────
    with _m2:
        st.markdown("每30分钟扫描 `system.query_log`，LLM 自动给出优化建议。")
        try:
            rows = _mch().query("""
                SELECT analyzed_at, query_time, duration_ms, category,
                       suggestion, read_rows, read_bytes, query_sql
                FROM stream.slow_query_analysis
                ORDER BY analyzed_at DESC LIMIT 30
            """).result_rows
            if rows:
                df_sq = pd.DataFrame(rows, columns=[
                    "分析时间", "查询时间", "耗时(ms)", "类别", "优化建议", "读取行数", "读取字节", "SQL"
                ])
                c1, c2, c3 = st.columns(3)
                c1.metric("慢查询总数", len(df_sq))
                c2.metric("最慢查询(ms)", int(df_sq["耗时(ms)"].max()))
                c3.metric("平均耗时(ms)", int(df_sq["耗时(ms)"].mean()))

                cat_counts = df_sq["类别"].value_counts().reset_index()
                cat_counts.columns = ["类别", "数量"]
                fig = px.pie(cat_counts, names="类别", values="数量", title="慢查询分类分布")
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("**慢查询明细**")
                for _, r in df_sq.iterrows():
                    with st.expander(f"⏱️ {r['耗时(ms)']}ms — {r['类别']} — {str(r['查询时间'])[:19]}"):
                        st.code(r["SQL"][:500], language="sql")
                        st.success(f"💡 优化建议：{r['优化建议']}")
                        st.caption(f"读取 {r['读取行数']:,} 行，{r['读取字节']/1024/1024:.1f} MB")
            else:
                st.info("暂无慢查询记录（阈值：执行时间 > 3秒）")
        except Exception as ex:
            st.error(f"查询失败：{ex}")

    # ── Tab3：数据血缘 ────────────────────────────────────────
    with _m3:
        st.markdown("解析 ClickHouse 初始化 SQL 自动生成的表/视图依赖关系图。")
        try:
            from ai_layer.lineage import get_lineage, get_upstream, get_downstream, get_db_color
            lineage = get_lineage()
            nodes = lineage["nodes"]
            edges = lineage["edges"]
            colors = get_db_color()

            c1, c2, c3 = st.columns(3)
            c1.metric("表/视图总数", len(nodes))
            c2.metric("依赖关系总数", len(edges))
            c3.metric("涉及数据库", len(set(n.db for n in nodes)))

            # 节点选择器——查上下游
            node_names = sorted([n.name for n in nodes])
            selected = st.selectbox("选择表/视图查看上下游依赖", ["（请选择）"] + node_names)
            if selected and selected != "（请选择）":
                ups = get_upstream(selected)
                downs = get_downstream(selected)
                col_u, col_d = st.columns(2)
                with col_u:
                    st.markdown(f"**⬆️ 上游（{len(ups)}个）**")
                    for u in ups:
                        st.write(f"- `{u}`")
                    if not ups:
                        st.caption("无上游（数据源头）")
                with col_d:
                    st.markdown(f"**⬇️ 下游（{len(downs)}个）**")
                    for d in downs:
                        st.write(f"- `{d}`")
                    if not downs:
                        st.caption("无下游（末端输出）")

            st.markdown("---")
            st.markdown("**全量依赖关系表**")
            if edges:
                df_edges = pd.DataFrame([
                    {"上游": e.source, "下游": e.target, "关系类型": e.edge_type}
                    for e in edges
                ])
                st.dataframe(df_edges, use_container_width=True, hide_index=True)

            st.markdown("**节点列表**")
            if nodes:
                df_nodes = pd.DataFrame([
                    {"表/视图": n.name, "数据库": n.db, "类型": n.node_type, "来源文件": n.source_file}
                    for n in nodes
                ])
                db_filter = st.multiselect(
                    "按数据库筛选",
                    sorted(df_nodes["数据库"].unique()),
                    default=sorted(df_nodes["数据库"].unique()),
                )
                st.dataframe(
                    df_nodes[df_nodes["数据库"].isin(db_filter)],
                    use_container_width=True, hide_index=True,
                )
        except Exception as ex:
            st.error(f"血缘解析失败：{ex}")

    # ── Tab4：报告记录 ────────────────────────────────────────
    with _m4:
        st.markdown("定时报告调度状态（日报每天09:00 · 周报每周一09:00）")
        import glob
        locks = glob.glob('/tmp/report_*.lock')
        if locks:
            st.success(f"已发送报告记录（{len(locks)} 条）：")
            for lk in sorted(locks, reverse=True):
                name = os.path.basename(lk).replace('.lock', '').replace('report_', '')
                st.write(f"- ✅ {name}")
        else:
            st.info("暂无已发送报告（报告在每天/每周固定时间自动触发）")

        webhook_url = os.getenv('WEBHOOK_URL', '')
        if webhook_url:
            st.success(f"Webhook 已配置：{webhook_url[:30]}...")
        else:
            st.warning("WEBHOOK_URL 未配置，报告只写入日志，不推送外部系统。在 .env 中设置 WEBHOOK_URL 即可启用推送。")
