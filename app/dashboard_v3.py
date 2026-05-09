# -*- coding: utf-8 -*-
"""
AI 数仓助手 v3 - 完整版
NL2SQL + RAG + 异常分析 Agent + 周报 Agent + 自由分析 Agent
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import re, json, time
from openai import OpenAI
import clickhouse_connect

st.set_page_config(page_title="AI 数仓助手 v3", page_icon="🤖", layout="wide")

# ── 侧边栏 ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 配置")
    api_key = st.text_input(
        "DeepSeek API Key",
        value=os.getenv('DEEPSEEK_API_KEY', ''),
        type="password"
    )
    if api_key:
        os.environ['DEEPSEEK_API_KEY'] = api_key

    st.markdown("---")
    st.markdown("### 功能导航")
    page = st.radio("选择功能", [
        "💬 智能问答",
        "🔍 异常检测 Agent",
        "📊 自动周报 Agent",
        "🤖 自由分析 Agent",
    ])

    st.markdown("---")
    # 系统状态
    try:
        ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
        cnt = ch.query("SELECT count() FROM dwd.order_detail").first_row[0]
        st.success(f"ClickHouse 已连接")
        st.metric("订单数据量", f"{cnt:,} 行")
    except:
        st.error("ClickHouse 未连接")

    try:
        import chromadb
        from chromadb.utils import embedding_functions
        chroma = chromadb.PersistentClient(
            path=os.path.join(os.path.dirname(__file__), '..', 'chroma_db')
        )
        names = [c.name for c in chroma.list_collections()]
        if 'ai_dw_knowledge' in names:
            st.success("知识库已就绪")
        else:
            st.warning("知识库未构建")
    except:
        st.warning("ChromaDB 未就绪")

# ── KPI 卡片（全局显示）─────────────────────────────────────
st.title("🤖 AI 数仓助手 v3")
st.caption("NL2SQL · RAG · Agent 三位一体 · Powered by DeepSeek + ClickHouse")

try:
    ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
    kpi = ch.query("SELECT round(sum(gmv),0),sum(order_cnt),sum(user_cnt),round(avg(avg_order_value),2) FROM ads.monthly_kpi").first_row
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("总 GMV", f"R$ {kpi[0]:,.0f}")
    c2.metric("总订单数", f"{kpi[1]:,}")
    c3.metric("总用户数", f"{kpi[2]:,}")
    c4.metric("平均客单价", f"R$ {kpi[3]:.2f}")
except:
    pass

st.markdown("---")

# ══════════════════════════════════════════════════════════════
# 页面1：智能问答（NL2SQL + RAG）
# ══════════════════════════════════════════════════════════════
if page == "💬 智能问答":
    st.subheader("💬 智能问答")
    st.caption("自动判断数据查询或知识问答")

    col1, col2 = st.columns([4,1])
    with col1:
        question = st.text_area("请输入问题", height=80,
            placeholder="数据查询：每月GMV趋势？  |  知识问答：GMV怎么定义？")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        mode = st.selectbox("模式", ["自动", "数据查询", "知识问答"])

    col_btn, col_insight = st.columns([1,3])
    with col_btn:
        run = st.button("🚀 查询", type="primary", use_container_width=True)
    with col_insight:
        show_insight = st.checkbox("生成 AI 洞察", value=True)

    if run and question.strip():
        if not api_key:
            st.error("请输入 API Key")
            st.stop()

        llm = OpenAI(api_key=api_key, base_url='https://api.deepseek.com', timeout=60.0)

        # 自动路由
        actual_mode = mode
        if mode == "自动":
            with st.spinner("判断问题类型..."):
                try:
                    r = llm.chat.completions.create(
                        model='deepseek-chat',
                        messages=[{'role':'user','content':
                            f"判断：查数据回A，问概念回B。问题：{question}\n只回答A或B"}],
                        temperature=0, max_tokens=5
                    )
                    ans = r.choices[0].message.content.strip().upper()
                    actual_mode = "知识问答" if 'B' in ans else "数据查询"
                    st.info(f"自动判断为：{actual_mode}")
                except:
                    actual_mode = "数据查询"

        if actual_mode == "数据查询":
            with st.spinner("生成 SQL 并查询..."):
                try:
                    ch = clickhouse_connect.get_client(host='localhost',port=8123,username='admin',password='admin123')
                    tables = {
                        'dws.order_daily':'每日汇总，dt、gmv、order_cnt、user_cnt、avg_order_value',
                        'dws.category_daily':'品类每日，dt、product_category、gmv、order_cnt',
                        'ads.monthly_kpi':'月度KPI，ym、gmv、order_cnt、user_cnt、mom_gmv_rate',
                        'ads.state_sales_rank':'省份排行，dt_month、state、gmv、order_cnt、rank_by_gmv（无user_cnt）',
                        'dwd.order_detail':'明细宽表，order_date、state、product_category、price、freight_value、order_status、delivery_days（无gmv字段）',
                    }
                    parts=[]
                    for t,desc in tables.items():
                        db,tbl=t.split('.')
                        cols=ch.query(f"SELECT name,type FROM system.columns WHERE database='{db}' AND table='{tbl}' ORDER BY position").result_rows
                        col_lines=[f"  {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
                        parts.append(f"-- {desc}\n{t}:\n"+"\n".join(col_lines))
                    schema="\n\n".join(parts)

                    resp=llm.chat.completions.create(
                        model='deepseek-chat',
                        messages=[
                            {'role':'system','content':f"转为ClickHouse SQL，只返回SELECT不加分号。dwd层用price不用gmv。\n{schema}"},
                            {'role':'user','content':question}
                        ],
                        temperature=0.1,max_tokens=600
                    )
                    sql=resp.choices[0].message.content.strip()
                    sql=re.sub(r'^```sql\s*','',sql,flags=re.IGNORECASE)
                    sql=re.sub(r'^```\s*','',sql)
                    sql=re.sub(r'\s*```$','',sql)
                    sql=sql.strip().rstrip(';')

                    with st.expander("生成的 SQL",expanded=True):
                        st.code(sql,language='sql')

                    df=ch.query_df(sql)

                    if show_insight and len(df)>0:
                        ir=llm.chat.completions.create(
                            model='deepseek-chat',
                            messages=[{'role':'user','content':
                                f"问题：{question}\n数据：\n{df.head(10).to_markdown(index=False)}\n请3-5句中文洞察。"}],
                            temperature=0.7,max_tokens=300
                        )
                        st.info(f"💡 {ir.choices[0].message.content.strip()}")

                    st.markdown(f"**结果（{len(df)}行）**")
                    tab1,tab2=st.tabs(["图表","数据表"])
                    with tab1:
                        cols=df.columns.tolist()
                        num_cols=df.select_dtypes(include='number').columns.tolist()
                        if len(cols)>=2 and len(num_cols)>=1:
                            x_col,y_col=cols[0],num_cols[0]
                            if any(k in str(x_col).lower() for k in ['ym','dt','date','month']):
                                fig=px.line(df,x=x_col,y=num_cols,markers=True)
                            elif df[x_col].dtype==object and len(num_cols)==1:
                                fig=px.bar(df.head(20),x=y_col,y=x_col,orientation='h',
                                          color=y_col,color_continuous_scale='Blues')
                                fig.update_layout(yaxis={'categoryorder':'total ascending'})
                            else:
                                fig=px.bar(df.head(20),x=x_col,y=y_col)
                            st.plotly_chart(fig,use_container_width=True)
                    with tab2:
                        st.dataframe(df,use_container_width=True)
                        st.download_button("下载CSV",df.to_csv(index=False,encoding='utf-8-sig'),"result.csv","text/csv")
                except Exception as e:
                    st.error(f"查询失败：{e}")

        else:  # 知识问答
            with st.spinner("检索知识库..."):
                try:
                    from ai_layer.rag_engine import rag_query
                    result=rag_query(question)
                    st.success(result['answer'])
                    with st.expander("参考来源"):
                        for chunk in result['chunks']:
                            st.markdown(f"**{chunk['source']}**（相似度:{1-chunk['distance']:.3f}）")
                            st.text(chunk['text'][:200]+'...')
                            st.markdown("---")
                except Exception as e:
                    st.error(f"知识问答失败：{e}")

# ══════════════════════════════════════════════════════════════
# 页面2：异常检测 Agent
# ══════════════════════════════════════════════════════════════
elif page == "🔍 异常检测 Agent":
    st.subheader("🔍 销售异常检测 Agent")
    st.markdown("""
    Agent 将自主完成以下步骤：
    1. 查询全量每日销售数据
    2. 统计均值和标准差，识别异常日期
    3. 查询异常日期前后数据进行对比
    4. 结合节日知识库分析原因
    5. 生成并保存分析报告
    """)

    if st.button("🚀 开始异常检测", type="primary", use_container_width=True):
        if not api_key:
            st.error("请输入 API Key")
            st.stop()

        steps_container = st.container()
        with steps_container:
            progress = st.progress(0, text="Agent 启动中...")
            log_area = st.empty()
            logs = []

        try:
            from ai_layer.agents import run_anomaly_agent
            with st.spinner("Agent 正在分析（约1-2分钟）..."):
                result = run_anomaly_agent()

            progress.progress(100, text="分析完成！")

            st.success("异常检测完成！")
            st.markdown("### 分析结论")
            st.markdown(result['output'])

            # 显示推理步骤
            if 'intermediate_steps' in result:
                with st.expander("查看 Agent 推理步骤", expanded=False):
                    for i, (action, obs) in enumerate(result['intermediate_steps']):
                        st.markdown(f"**步骤 {i+1}：{action.tool}**")
                        st.text(f"输入：{str(action.tool_input)[:200]}")
                        st.text(f"结果：{str(obs)[:300]}")
                        st.markdown("---")

        except Exception as e:
            st.error(f"Agent 运行失败：{e}")
            st.info("请检查 API Key 是否正确，网络是否正常")

# ══════════════════════════════════════════════════════════════
# 页面3：自动周报 Agent
# ══════════════════════════════════════════════════════════════
elif page == "📊 自动周报 Agent":
    st.subheader("📊 自动周报 Agent")
    st.markdown("""
    Agent 将自动生成包含以下内容的完整周报：
    - **GMV 趋势**：最近6个月核心指标及环比变化
    - **品类分析**：销售额 Top10 品类排行
    - **地域分析**：各省份最新月度销售排名
    - **综合洞察**：AI 自动生成业务结论与建议
    """)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚀 生成周报", type="primary", use_container_width=True):
            if not api_key:
                st.error("请输入 API Key")
                st.stop()
            st.session_state['run_weekly'] = True

    if st.session_state.get('run_weekly'):
        st.session_state['run_weekly'] = False
        try:
            from ai_layer.agents import run_weekly_report_agent
            with st.spinner("Agent 正在生成周报（约2-3分钟）..."):
                result = run_weekly_report_agent()

            st.success("周报生成完成！")
            st.markdown("### 周报内容")
            st.markdown(result['output'])

            # 查找保存的报告文件
            reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
            if os.path.exists(reports_dir):
                files = sorted(os.listdir(reports_dir), reverse=True)
                if files:
                    latest = os.path.join(reports_dir, files[0])
                    with open(latest, 'r', encoding='utf-8') as f:
                        content = f.read()
                    st.download_button(
                        "⬇️ 下载完整报告 (Markdown)",
                        data=content,
                        file_name=files[0],
                        mime="text/markdown"
                    )

            if 'intermediate_steps' in result:
                with st.expander("查看 Agent 推理步骤"):
                    for i,(action,obs) in enumerate(result['intermediate_steps']):
                        st.markdown(f"**步骤{i+1}：{action.tool}**")
                        st.text(f"输入：{str(action.tool_input)[:150]}")
                        st.text(f"结果：{str(obs)[:200]}")
                        st.markdown("---")

        except Exception as e:
            st.error(f"周报生成失败：{e}")

# ══════════════════════════════════════════════════════════════
# 页面4：自由分析 Agent
# ══════════════════════════════════════════════════════════════
elif page == "🤖 自由分析 Agent":
    st.subheader("🤖 自由分析 Agent")
    st.caption("输入任意分析目标，Agent 自主规划并完成多步骤分析")

    # 示例目标
    examples = [
        "分析2018年上半年销售趋势，找出增长最快的品类，给出运营建议",
        "对比SP州和RJ州的销售数据，分析两个州的消费差异",
        "找出配送时间最长的订单特征，分析是否影响销售",
        "分析黑色星期五（2017-11-24）前后一周的销售变化",
    ]
    st.markdown("**示例分析目标：**")
    cols = st.columns(2)
    for i, ex in enumerate(examples):
        with cols[i % 2]:
            if st.button(ex, key=f"ex_{i}", use_container_width=True):
                st.session_state['free_goal'] = ex

    goal = st.text_area(
        "输入你的分析目标",
        value=st.session_state.get('free_goal', ''),
        height=100,
        placeholder="例如：分析2018年各品类的GMV增长趋势，找出最有潜力的品类",
        key='free_goal'
    )

    if st.button("🚀 启动 Agent 分析", type="primary", use_container_width=True):
        if not api_key:
            st.error("请输入 API Key")
            st.stop()
        if not goal.strip():
            st.warning("请输入分析目标")
            st.stop()

        try:
            from ai_layer.agents import run_free_agent
            with st.spinner(f"Agent 正在分析：{goal[:50]}... （约2-3分钟）"):
                result = run_free_agent(goal)

            st.success("分析完成！")
            st.markdown("### 分析结论")
            st.markdown(result['output'])

            if 'intermediate_steps' in result:
                with st.expander(f"查看推理过程（共{len(result['intermediate_steps'])}步）"):
                    for i,(action,obs) in enumerate(result['intermediate_steps']):
                        st.markdown(f"**步骤{i+1}：调用 `{action.tool}`**")
                        with st.container():
                            col_a,col_b=st.columns(2)
                            with col_a:
                                st.markdown("**输入：**")
                                st.text(str(action.tool_input)[:300])
                            with col_b:
                                st.markdown("**输出：**")
                                st.text(str(obs)[:300])
                        st.markdown("---")

        except Exception as e:
            st.error(f"Agent 运行失败：{e}")
            st.info("如果是超时错误，请尝试简化分析目标或检查网络连接")
