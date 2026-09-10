# -*- coding: utf-8 -*-
"""
问题路由：判断用户是「查数据」还是「问概念」

原来这段逻辑内联在 app/dashboard_v3.py 的 UI 代码里，
既没法被别的入口复用，也没法单独测。抽出来之后，
评估脚本测的就是线上真实走的这个函数，而不是一份复制品。

路由错误的代价是不对称的：
  把「问概念」判成「查数据」→ 生成一条查不到东西的 SQL，用户拿到空表
  把「查数据」判成「问概念」→ 从知识库里检索一段泛泛的定义，答非所问
两种都难以自动发现，所以这一步值得单独评估。
"""

import os

from openai import OpenAI


_ROUTE_PROMPT = """判断用户问题的类型。

A = 查数据：需要从数据仓库里取具体数值、排名、趋势、明细
B = 问概念：询问指标定义、口径、业务规则、名词解释

示例：
「上个月GMV多少」→ A
「GMV是怎么定义的」→ B
「哪个品类卖得最好」→ A
「什么算已送达」→ B

只回答一个字母 A 或 B，不要任何其他内容。

问题：{question}"""


def _get_llm() -> OpenAI:
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url=os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com'),
        timeout=20.0,
    )


def route(question: str, default: str = 'data') -> str:
    """
    返回 'data' 或 'knowledge'。

    LLM 不可用时回落到 default，而不是抛异常 —— 路由失败不该让整个问答挂掉，
    退化成「按查数据处理」用户至少还能拿到结果。
    """
    if not os.getenv('DEEPSEEK_API_KEY', '').strip():
        return default

    try:
        resp = _get_llm().chat.completions.create(
            model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
            messages=[{'role': 'user', 'content': _ROUTE_PROMPT.format(question=question)}],
            temperature=0,
            max_tokens=4,
        )
        ans = (resp.choices[0].message.content or '').strip().upper()
        return 'knowledge' if ans.startswith('B') else 'data'
    except Exception:
        return default
