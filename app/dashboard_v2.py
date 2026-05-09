# -*- coding: utf-8 -*-
"""
AI 数仓助手 v2 - 集成 NL2SQL + RAG 双引擎
运行：streamlit run app/dashboard_v2.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import re
from openai import OpenAI
import clickhouse_connect

st.set_page_config(page_title="AI 数仓助手 v2", page_icon="🤖", layout="wide")

# ── 侧边栏 ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 配置")
    api_key = st.text_input("DeepSeek API Key", value=os.getenv('DEEPSEEK_API_KEY',''), type="password")
    if api_key:
        os.environ['DEEPSEEK_API_KEY'] = api_key

    st.markdown("---")
    st.markdown("### 数据查询示例")
    data_examples = [
        "每个月的GMV趋势",
        "销售额最高的前5个品类",
        "哪个州的销售额最高？Top10",
        "2018年每月订单数和环比",
        "最高峰订单日是哪天？",
    ]
    for ex in data_examples:
        if st.button(ex, key=f"d_{ex}", use_container_width=True):
            st.session_state['q'] = ex
            st.session_state['mode'] = 'auto'

    st.markdown("### 知识问答示例")
    knowledge_examples = [
        "GMV和销售额有什么区别？",
        "客单价怎么计算？",
        "delivered状态是什么意思？",
        "为什么统计用户数要用unique_id？",
        "巴西哪个州电商最发达？",
    ]
    for ex in knowledge_examples:
        if st.button(ex, key=f"k_{ex}", use_container_width=True):
            st.session_state['q'] = ex
            st.session_state['mode'] = 'auto'

    st.markdown("---")
    # 知识库状态
    try:
        import chromadb
        from chromadb.utils import embedding_functions
        chroma = chromadb.PersistentClient(
            path=os.path.join(os.path.dirname(__file__), '..', 'chroma_db')
        )
        cols = [c.name for c in chroma.list_collections()]
        if 'ai_dw_knowledge' in cols:
            ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name='paraphrase-multilingual-MiniLM-L12-v2'
            )
            col = chroma.get_collection('ai_dw_knowledge', embedding_function=ef)
            st.success(f"知识库已就绪 ({col.count()} 块)")
        else:
            st.warning("知识库未构建")
            if st.button("构建知识库", use_container_width=True):
                st.session_state['build_kb'] = True
    except Exception as e:
        st.error(f"ChromaDB: {e}")

    # ClickHouse 状态
    try:
        ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
        cnt = ch.query("SELECT count() FROM dwd.order_detail").first_row[0]
        st.success(f"ClickHouse 已连接 ({cnt:,}行)")
    except:
        st.error("ClickHouse 连接失败")

# ── 构建知识库 ────────────────────────────────────────────────
if st.session_state.get('build_kb'):
    with st.spinner("正在构建知识库（首次约需2-3分钟下载模型）..."):
        try:
            from ai_layer.rag_engine import build_knowledge_base
            build_knowledge_base(force_rebuild=True)
            st.success("知识库构建完成！")
            st.session_state['build_kb'] = False
            st.rerun()
        except Exception as e:
            st.error(f"构建失败：{e}")

# ── 主界面 ────────────────────────────────────────────────────
st.title("🤖 AI 数仓助手 v2")
st.caption("数据查询（NL2SQL）+ 知识问答（RAG）双引擎 · Powered by DeepSeek + ClickHouse")

# KPI 卡片
try:
    ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
    kpi = ch.query("SELECT round(sum(gmv),0), sum(order_cnt), sum(user_cnt), round(avg(avg_order_value),2) FROM ads.monthly_kpi").first_row
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("总 GMV", f"R$ {kpi[0]:,.0f}")
    c2.metric("总订单数", f"{kpi[1]:,}")
    c3.metric("总用户数", f"{kpi[2]:,}")
    c4.metric("平均客单价", f"R$ {kpi[3]:.2f}")
except:
    pass

st.markdown("---")

# 输入区域
col_q, col_mode = st.columns([4, 1])
with col_q:
    question = st.text_area(
        "请输入你的问题（数据查询或知识问答均可）",
        value=st.session_state.get('q', ''),
        placeholder="例如：每月GMV趋势？  /  GMV和销售额有什么区别？",
        height=80, key='q'
    )
with col_mode:
    st.markdown("<br>", unsafe_allow_html=True)
    mode = st.selectbox("模式", ["自动判断", "数据查询", "知识问答"],
                        index=0, key='mode_select')

c1, c2 = st.columns([1, 3])
with c1:
    run = st.button("🚀 开始", use_container_width=True, type="primary")
with c2:
    show_insight = st.checkbox("生成 AI 洞察", value=True)

# ── 执行 ──────────────────────────────────────────────────────
if run and question.strip():
    if not api_key:
        st.error("请先在左侧输入 DeepSeek API Key")
        st.stop()

    os.environ['DEEPSEEK_API_KEY'] = api_key
    llm = OpenAI(api_key=api_key, base_url='https://api.deepseek.com', timeout=60.0)

    # 判断模式
    actual_mode = mode
    if mode == "自动判断":
        with st.spinner("判断问题类型..."):
            try:
                resp = llm.chat.completions.create(
                    model='deepseek-chat',
                    messages=[{'role':'user','content':
                        f"判断这个问题是查数据(A)还是问概念(B)：{question}\n只回答A或B"}],
                    temperature=0, max_tokens=5
                )
                r = resp.choices[0].message.content.strip().upper()
                actual_mode = "知识问答" if 'B' in r else "数据查询"
                st.info(f"自动判断：{actual_mode}")
            except Exception as e:
                st.warning(f"自动判断失败，默认数据查询：{e}")
                actual_mode = "数据查询"

    # ── 数据查询（NL2SQL）────────────────────────────────────
    if actual_mode == "数据查询":
        TABLE_DESCRIPTIONS = {
            'dws.order_daily':      '每日订单汇总，含GMV、订单数、用户数、客单价',
            'dws.category_daily':   '每日品类销售汇总，含品类名、GMV、订单数',
            'ads.monthly_kpi':      '月度KPI，含GMV、订单数、环比增长率mom_gmv_rate',
            'ads.state_sales_rank': '各省份每月销售排行，含GMV、订单数、排名',
            'dwd.order_detail':     '订单明细宽表，price=商品价格，freight_value=运费，无gmv字段',
        }
        SYSTEM_PROMPT = """你是精通 ClickHouse SQL 的数据分析师。将用户问题转为可执行 SQL。
【规则】GMV用dws/ads层的gmv字段；dwd层用price字段；查地域用ads.state_sales_rank；只返回SELECT SQL不加分号。
【表结构】{schema}"""

        with st.spinner("生成并执行 SQL..."):
            try:
                ch = clickhouse_connect.get_client(host='localhost',port=8123,username='admin',password='admin123')

                # 获取 schema
                parts = []
                for table, desc in TABLE_DESCRIPTIONS.items():
                    db, tbl = table.split('.')
                    cols = ch.query(f"SELECT name,type FROM system.columns WHERE database='{db}' AND table='{tbl}' ORDER BY position").result_rows
                    col_lines = [f"    {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
                    parts.append(f"-- {desc}\n表名: {table}\n字段:\n"+"\n".join(col_lines))
                schema = "\n\n".join(parts)

                resp = llm.chat.completions.create(
                    model='deepseek-chat',
                    messages=[
                        {'role':'system','content':SYSTEM_PROMPT.format(schema=schema)},
                        {'role':'user','content':question}
                    ],
                    temperature=0.1, max_tokens=800
                )
                sql = resp.choices[0].message.content.strip()
                sql = re.sub(r'^```sql\s*','',sql,flags=re.IGNORECASE)
                sql = re.sub(r'^```\s*','',sql)
                sql = re.sub(r'\s*```$','',sql)
                sql = sql.strip().rstrip(';')

                for kw in ['INSERT','UPDATE','DELETE','DROP','CREATE','ALTER','TRUNCATE']:
                    if re.search(rf'\b{kw}\b', sql.upper()):
                        st.error(f"不允许 {kw} 操作"); st.stop()

                with st.expander("生成的 SQL", expanded=True):
                    st.code(sql, language='sql')

                df = ch.query_df(sql)

                if show_insight and len(df) > 0:
                    insight_resp = llm.chat.completions.create(
                        model='deepseek-chat',
                        messages=[{'role':'user','content':
                            f"问题：{question}\nSQL：{sql}\n数据：\n{df.head(10).to_markdown(index=False)}\n请用3-5句中文给出业务洞察。"}],
                        temperature=0.7, max_tokens=400
                    )
                    st.info(f"💡 AI 洞察：{insight_resp.choices[0].message.content.strip()}")

                st.markdown(f"**查询结果（{len(df)} 行）**")
                tab1, tab2 = st.tabs(["图表", "数据表"])
                with tab1:
                    cols = df.columns.tolist()
                    num_cols = df.select_dtypes(include='number').columns.tolist()
                    if len(cols)>=2 and len(num_cols)>=1:
                        x_col, y_col = cols[0], num_cols[0]
                        if any(k in str(x_col).lower() for k in ['ym','dt','date','month']):
                            fig = px.line(df, x=x_col, y=num_cols, markers=True)
                        elif df[x_col].dtype==object and len(num_cols)==1:
                            fig = px.bar(df.head(20),x=y_col,y=x_col,orientation='h',
                                        color=y_col,color_continuous_scale='Blues')
                            fig.update_layout(yaxis={'categoryorder':'total ascending'})
                        else:
                            fig = px.bar(df.head(20), x=x_col, y=y_col)
                        st.plotly_chart(fig, use_container_width=True)
                with tab2:
                    st.dataframe(df, use_container_width=True)
                    st.download_button("下载 CSV", df.to_csv(index=False,encoding='utf-8-sig'), "result.csv","text/csv")

            except Exception as e:
                st.error(f"查询失败：{e}")

    # ── 知识问答（RAG）──────────────────────────────────────
    elif actual_mode == "知识问答":
        with st.spinner("检索知识库并生成回答..."):
            try:
                from ai_layer.rag_engine import rag_query
                result = rag_query(question)

                st.markdown("### 知识库回答")
                st.success(result['answer'])

                with st.expander("参考来源", expanded=False):
                    for chunk in result['chunks']:
                        st.markdown(f"**来源：{chunk['source']}**（相似度：{1-chunk['distance']:.3f}）")
                        st.text(chunk['text'][:300] + ('...' if len(chunk['text'])>300 else ''))
                        st.markdown("---")

            except Exception as e:
                st.error(f"知识问答失败：{e}")
                st.info("请先在左侧点击「构建知识库」按钮")

elif run:
    st.warning("请输入问题")
