# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="AI 数仓助手", page_icon="🤖", layout="wide")

# ── 侧边栏 ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 配置")
    api_key = st.text_input(
        "DeepSeek API Key",
        value=os.getenv('DEEPSEEK_API_KEY', ''),
        type="password"
    )

    st.markdown("---")
    st.markdown("### 示例问题")
    examples = [
        "每个月的GMV是多少？按时间排序",
        "销售额最高的前5个商品品类",
        "哪个州的销售额最高？Top 10",
        "2018年每月订单数和环比增长",
        "每天平均客单价是多少？",
        "最高峰订单日是哪天？",
    ]
    for ex in examples:
        if st.button(ex, key=ex, use_container_width=True):
            st.session_state['q'] = ex

    st.markdown("---")
    # 数据库状态
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host='localhost', port=8123,
            username='admin', password='admin123'
        )
        cnt = client.query("SELECT count() FROM dwd.order_detail").first_row[0]
        st.success(f"ClickHouse 已连接")
        st.metric("订单明细总量", f"{cnt:,} 行")
    except Exception as e:
        st.error(f"ClickHouse 连接失败: {e}")

# ── 主界面 ────────────────────────────────────────────────────
st.title("🤖 AI 数仓助手")
st.caption("用自然语言查询数据仓库 · Powered by DeepSeek + ClickHouse")

# 顶部 KPI
try:
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host='localhost', port=8123,
        username='admin', password='admin123'
    )
    kpi = client.query("""
        SELECT round(sum(gmv),0), sum(order_cnt), sum(user_cnt), round(avg(avg_order_value),2)
        FROM ads.monthly_kpi
    """).first_row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总 GMV", f"R$ {kpi[0]:,.0f}")
    c2.metric("总订单数", f"{kpi[1]:,}")
    c3.metric("总用户数", f"{kpi[2]:,}")
    c4.metric("平均客单价", f"R$ {kpi[3]:.2f}")
except:
    pass

st.markdown("---")

# 输入框
question = st.text_area(
    "请输入你的问题",
    value=st.session_state.get('q', ''),
    placeholder="例如：每个月的GMV是多少？销售额最高的品类有哪些？",
    height=80,
    key="q"
)

col1, col2 = st.columns([1, 3])
with col1:
    run = st.button("🚀 开始查询", use_container_width=True, type="primary")
with col2:
    show_insight = st.checkbox("生成 AI 洞察", value=True)

# ── 执行查询 ──────────────────────────────────────────────────
if run and question.strip():
    if not api_key:
        st.error("请先在左侧输入 DeepSeek API Key")
        st.stop()

    # 实时设置 API Key
    os.environ['DEEPSEEK_API_KEY'] = api_key

    # 重新初始化 llm 客户端（每次查询都用最新 key）
    from openai import OpenAI
    import re
    import clickhouse_connect

    llm = OpenAI(api_key=api_key, base_url='https://api.deepseek.com', timeout=60.0)

    # 获取表结构
    TABLE_DESCRIPTIONS = {
        'dws.order_daily':      '每日订单汇总，含GMV、订单数、用户数、客单价',
        'dws.category_daily':   '每日品类销售汇总，含品类名、GMV、订单数',
        'ads.monthly_kpi':      '月度KPI，含GMV、订单数、环比增长率',
        'ads.state_sales_rank': '各省份每月销售排行，含GMV、订单数、排名',
        'dwd.order_detail':     '订单明细宽表，price=商品价格，freight_value=运费，无gmv字段',
    }

    def get_schema():
        ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
        parts = []
        for table, desc in TABLE_DESCRIPTIONS.items():
            db, tbl = table.split('.')
            cols = ch.query(
                f"SELECT name, type FROM system.columns WHERE database='{db}' AND table='{tbl}' ORDER BY position"
            ).result_rows
            col_lines = [f"    {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
            parts.append(f"-- {desc}\n表名: {table}\n字段:\n" + "\n".join(col_lines))
        return "\n\n".join(parts)

    SYSTEM_PROMPT = """你是精通 ClickHouse SQL 的数据分析师。将用户问题转为可执行的 ClickHouse SQL。

【业务背景】巴西电商平台，2016-2018年，金额单位巴西雷亚尔(R$)。

【规则】
- GMV用 dws/ads 层的 gmv 字段，不要在 dwd 层找 gmv
- dwd.order_detail 中金额字段是 price 和 freight_value
- 查地域销售用 ads.state_sales_rank 或 dws/dwd 中的 state 字段配合 price
- Top N 用 ORDER BY xxx DESC LIMIT N
- 只返回 SELECT SQL，不加分号，不加解释

【表结构】
{schema}
"""

    with st.spinner("AI 正在分析你的问题..."):
        try:
            schema = get_schema()
            prompt = SYSTEM_PROMPT.format(schema=schema)

            resp = llm.chat.completions.create(
                model='deepseek-chat',
                messages=[
                    {'role': 'system', 'content': prompt},
                    {'role': 'user', 'content': question}
                ],
                temperature=0.1,
                max_tokens=800,
            )
            sql = resp.choices[0].message.content.strip()
            sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'^```\s*', '', sql)
            sql = re.sub(r'\s*```$', '', sql)
            sql = sql.strip().rstrip(';')

            # 安全校验
            for kw in ['INSERT','UPDATE','DELETE','DROP','CREATE','ALTER','TRUNCATE']:
                if re.search(rf'\b{kw}\b', sql.upper()):
                    st.error(f"不允许执行 {kw} 操作")
                    st.stop()

            # 显示 SQL
            with st.expander("生成的 SQL", expanded=True):
                st.code(sql, language='sql')

            # 执行
            ch = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin123')
            df = ch.query_df(sql)

            # AI 洞察
            if show_insight and len(df) > 0:
                insight_resp = llm.chat.completions.create(
                    model='deepseek-chat',
                    messages=[{'role': 'user', 'content':
                        f"用户问题：{question}\nSQL：{sql}\n数据（前10行）：\n{df.head(10).to_markdown(index=False)}\n\n请用3-5句中文给出业务洞察。"}],
                    temperature=0.7,
                    max_tokens=400,
                )
                insight = insight_resp.choices[0].message.content.strip()
                st.info(f"💡 AI 洞察：{insight}")

            # 显示数据和图表
            st.markdown(f"**查询结果（{len(df)} 行）**")
            tab1, tab2 = st.tabs(["图表", "数据表"])

            with tab1:
                cols = df.columns.tolist()
                num_cols = df.select_dtypes(include='number').columns.tolist()
                if len(cols) >= 2 and len(num_cols) >= 1:
                    x_col = cols[0]
                    y_col = num_cols[0]
                    if any(k in str(x_col).lower() for k in ['ym','dt','date','month']):
                        fig = px.line(df, x=x_col, y=num_cols, markers=True)
                    elif df[x_col].dtype == object and len(num_cols) == 1:
                        fig = px.bar(df.head(20), x=y_col, y=x_col, orientation='h',
                                     color=y_col, color_continuous_scale='Blues')
                        fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    else:
                        fig = px.bar(df.head(20), x=x_col, y=y_col)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.write(df)

            with tab2:
                st.dataframe(df, use_container_width=True)
                st.download_button("下载 CSV", df.to_csv(index=False, encoding='utf-8-sig'),
                                   "result.csv", "text/csv")

        except Exception as e:
            st.error(f"查询失败：{e}")

elif run:
    st.warning("请输入问题")
