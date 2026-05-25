# -*- coding: utf-8 -*-
"""RAG 引擎：知识库构建 + Self-RAG 检索问答"""
import json
import os
import re
import sys
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from openai import OpenAI

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry

log = get_logger('rag_engine')

COLLECTION_NAME = 'ai_dw_knowledge'
_EMBED_MODEL    = 'paraphrase-multilingual-MiniLM-L12-v2'

llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=60.0)

embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name=_EMBED_MODEL
)


def get_chroma_client():
    os.makedirs(cfg.chroma_dir, exist_ok=True)
    return chromadb.PersistentClient(path=cfg.chroma_dir)


# ── 文档处理 ──────────────────────────────────────────────────

def load_documents():
    docs = []
    for md_file in sorted(Path(cfg.knowledge_dir).glob('*.md')):
        content = md_file.read_text(encoding='utf-8')
        docs.append({'filename': md_file.name, 'content': content})
        log.info('已加载知识库文件：%s（%d 字符）', md_file.name, len(content))
    return docs


def split_chunks(text: str, source: str) -> list:
    chunks, current = [], ''
    for para in re.split(r'\n\s*\n', text):
        para = para.strip()
        if not para:
            continue
        if len(current) + len(para) <= cfg.chunk_size:
            current += ('\n\n' + para if current else para)
        else:
            if current:
                chunks.append({'text': current, 'source': source})
            if len(para) > cfg.chunk_size:
                for i in range(0, len(para), cfg.chunk_size - cfg.chunk_overlap):
                    sub = para[i:i + cfg.chunk_size]
                    if sub.strip():
                        chunks.append({'text': sub, 'source': source})
                current = ''
            else:
                overlap = current[-cfg.chunk_overlap:] if current else ''
                current = (overlap + '\n\n' + para).strip() if overlap else para
    if current.strip():
        chunks.append({'text': current, 'source': source})
    return chunks


# ── 知识库构建 ────────────────────────────────────────────────

def build_knowledge_base(force_rebuild: bool = False):
    client = get_chroma_client()
    existing = [c.name for c in client.list_collections()]
    if COLLECTION_NAME in existing:
        if not force_rebuild:
            col = client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)
            log.info('知识库已存在，共 %d 个文本块，跳过构建。', col.count())
            return col
        client.delete_collection(COLLECTION_NAME)
        log.info('已清空旧知识库，重新构建...')

    col = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=embedding_fn,
        metadata={'hnsw:space': 'cosine'},
    )
    docs = load_documents()
    all_chunks = []
    for doc in docs:
        chunks = split_chunks(doc['content'], doc['filename'])
        all_chunks.extend(chunks)
        log.info('%s → %d 个文本块', doc['filename'], len(chunks))

    log.info('正在向量化并写入 %d 个文本块...', len(all_chunks))
    col.add(
        ids=[f"chunk_{i}" for i in range(len(all_chunks))],
        documents=[c['text'] for c in all_chunks],
        metadatas=[{'source': c['source']} for c in all_chunks],
    )
    log.info('知识库构建完成，共 %d 个文本块。', col.count())
    return col


def get_collection():
    client = get_chroma_client()
    existing = [c.name for c in client.list_collections()]
    if COLLECTION_NAME not in existing:
        return build_knowledge_base()
    return client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)


# ── 检索 ──────────────────────────────────────────────────────

def retrieve(question: str, top_k: int | None = None) -> list:
    k = top_k or cfg.rag_top_k
    col = get_collection()
    results = col.query(query_texts=[question], n_results=min(k, col.count()))
    return [
        {
            'text':     doc,
            'source':   results['metadatas'][0][i]['source'],
            'distance': results['distances'][0][i],
        }
        for i, doc in enumerate(results['documents'][0])
    ]


# ── Self-RAG Prompts ──────────────────────────────────────────

RAG_SYSTEM_PROMPT = """你是一位专业的数据分析顾问，熟悉本公司的数据仓库和业务规则。
请根据以下知识库内容回答用户的问题。

【相关知识】
{context}

【回答要求】
- 根据知识库内容准确回答，不要编造知识库中没有的信息
- 如果知识库中没有相关信息，直接说"知识库中暂无此信息"
- 如果用户的问题引用了上文（如"它"、"这个"、"刚才说的"），请结合对话历史理解
- 回答简洁清晰，使用中文
"""

CONSERVATIVE_RAG_SYSTEM_PROMPT = """你是一位严谨的数据分析顾问，熟悉本公司的数据仓库和业务规则。
请严格根据以下知识库内容回答用户的问题。

【相关知识】
{context}

【严格要求】
- 只陈述参考文档中明确存在的信息，不推测、不延伸、不引入文档之外的知识
- 如果知识库中没有相关信息，直接说"知识库中暂无此信息"
- 每个结论必须能直接在参考文档中找到依据
- 如果用户的问题引用了上文（如"它"、"这个"、"刚才说的"），请结合对话历史理解
- 回答简洁清晰，使用中文
"""

RELEVANCE_SCORE_PROMPT = """你是信息检索专家。判断以下每个检索片段与用户问题的相关程度。

用户问题：{question}

检索片段列表（JSON格式）：
{chunks_json}

请对每个片段打分，1=完全相关，0=完全无关。
以 JSON 数组格式输出分数，数组长度与片段数量相同，例如：[0.9, 0.3, 0.7]
只输出 JSON 数组，不要输出其他任何内容。"""

REWRITE_QUERY_PROMPT = """用户原始问题：{question}
第一次检索未找到相关内容，请改写问题使其更适合检索：
要求：更具体、使用不同关键词、保持原意。只输出改写后的问题。"""

GROUNDEDNESS_SCORE_PROMPT = """以下答案是否完全基于给定的参考文档，没有编造或引入文档之外的信息？

参考文档：
{docs}

答案：
{answer}

只输出 0-1 之间的数字，1=完全基于文档，0=大量编造。不要输出其他任何内容。"""


# ── Self-RAG 辅助函数 ─────────────────────────────────────────

def _batch_score_relevance(question: str, chunks: list) -> list[float]:
    """
    一次 LLM 调用批量评估所有检索结果的相关性（0-1）。
    失败时降级：所有片段返回 0.5。
    """
    if not chunks:
        return []
    try:
        chunks_json = json.dumps(
            [{'index': i, 'text': c['text'][:300]} for i, c in enumerate(chunks)],
            ensure_ascii=False,
        )
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[
                {'role': 'user', 'content': RELEVANCE_SCORE_PROMPT.format(
                    question=question,
                    chunks_json=chunks_json,
                )},
            ],
            temperature=0,
            max_tokens=100,
        )
        raw = resp.choices[0].message.content.strip()
        # 提取 JSON 数组
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if not match:
            raise ValueError(f'无法解析相关性评分响应：{raw}')
        scores = json.loads(match.group())
        if len(scores) != len(chunks):
            raise ValueError(f'评分数量 {len(scores)} 与片段数量 {len(chunks)} 不匹配')
        scores = [max(0.0, min(1.0, float(s))) for s in scores]
        log.info('[相关性评分] %s', scores)
        return scores
    except Exception as e:
        log.warning('[相关性评分] 批量评分失败，降级为 0.5：%s', e)
        return [0.5] * len(chunks)


def _rewrite_query(question: str) -> str:
    """让 LLM 改写问题以提升检索效果。失败时返回原问题。"""
    try:
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[
                {'role': 'user', 'content': REWRITE_QUERY_PROMPT.format(question=question)},
            ],
            temperature=0.3,
            max_tokens=200,
        )
        rewritten = resp.choices[0].message.content.strip()
        log.info('[问题改写] 原问题：%s → 改写：%s', question, rewritten)
        return rewritten
    except Exception as e:
        log.warning('[问题改写] 失败，使用原问题：%s', e)
        return question


def _score_groundedness(answer: str, docs_context: str) -> float:
    """评估答案的接地性（是否完全基于参考文档）。失败时降级返回 0.5。"""
    try:
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[
                {'role': 'user', 'content': GROUNDEDNESS_SCORE_PROMPT.format(
                    docs=docs_context[:2000],  # 避免超长 prompt
                    answer=answer,
                )},
            ],
            temperature=0,
            max_tokens=10,
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r'[0-9]*\.?[0-9]+', raw)
        score = float(m.group()) if m else 0.5
        score = max(0.0, min(1.0, score))
        log.info('[接地性评分] %.2f', score)
        return score
    except Exception as e:
        log.warning('[接地性评分] 评分失败，降级为 0.5：%s', e)
        return 0.5


def _generate_answer(question: str, chunks: list, history: list[dict] | None,
                     conservative: bool = False) -> str:
    """根据检索结果生成答案。"""
    context = '\n\n---\n\n'.join(
        [f"来源：{c['source']}\n{c['text']}" for c in chunks]
    )
    system_tpl = CONSERVATIVE_RAG_SYSTEM_PROMPT if conservative else RAG_SYSTEM_PROMPT
    messages: list[dict] = [
        {'role': 'system', 'content': system_tpl.format(context=context)}
    ]
    for turn in (history or [])[-5:]:
        messages.append({'role': 'user',      'content': turn['question']})
        messages.append({'role': 'assistant', 'content': turn['answer']})
    messages.append({'role': 'user', 'content': question})

    resp = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=messages,
        temperature=cfg.rag_temperature,
        max_tokens=800,
    )
    return resp.choices[0].message.content.strip()


# ── 检索词扩展 ────────────────────────────────────────────────

def _build_retrieval_query(question: str, history: list[dict] | None) -> str:
    """结合最近1轮历史扩展检索词，提升上下文相关检索质量"""
    if not history:
        return question
    last = history[-1]
    ctx = last.get('question', '') + ' ' + last.get('answer', '')[:80]
    return f"{ctx} {question}"


# ── 对外接口 ──────────────────────────────────────────────────

@llm_retry
def rag_query(question: str, history: list[dict] | None = None) -> dict:
    """
    Self-RAG RAG 主入口。

    history 格式（每轮一个 dict）：
      [{'question': str, 'answer': str}, ...]

    返回 dict 新增字段：
      retrieval_scores : list[float] — 每个检索结果的相关性分
      answer_confidence: float       — 答案接地性分（0-1）
      query_rewritten  : bool        — 是否经过问题改写
      rewritten_query  : str         — 改写后的问题（若有）
    """
    log.info('[RAG检索] %s', question)

    # ── 步骤1：初次检索 ───────────────────────────────────────
    retrieval_query = _build_retrieval_query(question, history)
    chunks = retrieve(retrieval_query)
    log.info('[RAG检索] 找到 %d 个相关文本块', len(chunks))

    query_rewritten = False
    rewritten_query = ''

    # ── 步骤2：批量相关性评分 ─────────────────────────────────
    retrieval_scores = _batch_score_relevance(question, chunks)
    avg_score = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0.0
    log.info('[相关性] 平均分 %.2f（阈值 0.6）', avg_score)

    # ── 步骤3：若相关性不足，改写问题并二次检索 ──────────────
    if avg_score < 0.6:
        log.info('[Self-RAG] 相关性平均分 %.2f < 0.6，进行问题改写和二次检索', avg_score)
        rewritten_query = _rewrite_query(question)
        query_rewritten = True

        second_query = _build_retrieval_query(rewritten_query, history)
        chunks = retrieve(second_query)
        retrieval_scores = _batch_score_relevance(rewritten_query, chunks)
        avg_score = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0.0
        log.info('[二次检索] 找到 %d 个文本块，平均相关性 %.2f', len(chunks), avg_score)

    # ── 步骤4：生成答案 ───────────────────────────────────────
    answer = _generate_answer(question, chunks, history, conservative=False)

    # ── 步骤5：答案接地性评分 + 必要时保守重生成 ─────────────
    docs_context = '\n\n---\n\n'.join(
        [f"来源：{c['source']}\n{c['text']}" for c in chunks]
    )
    answer_confidence = _score_groundedness(answer, docs_context)

    if answer_confidence < 0.7:
        log.info('[Self-RAG] 接地性评分 %.2f < 0.7，用保守 prompt 重新生成答案', answer_confidence)
        answer = _generate_answer(question, chunks, history, conservative=True)
        answer_confidence = _score_groundedness(answer, docs_context)
        log.info('[Self-RAG] 保守答案接地性评分 %.2f', answer_confidence)

    log.info(
        '[RAG完成] query_rewritten=%s answer_confidence=%.2f',
        query_rewritten, answer_confidence,
    )

    return {
        'answer':            answer,
        'sources':           list({c['source'] for c in chunks}),
        'chunks':            chunks,
        'retrieval_scores':  retrieval_scores,
        'answer_confidence': answer_confidence,
        'query_rewritten':   query_rewritten,
        'rewritten_query':   rewritten_query,
    }


# ── 问题路由 ──────────────────────────────────────────────────

ROUTE_PROMPT = """判断以下问题应该用哪种方式回答：
A. NL2SQL - 需要查询数据库获取具体数字/数据
B. RAG    - 询问概念定义、业务规则、字段含义

问题：{question}
只回答 A 或 B，不要其他内容。"""


@llm_retry
def route_question(question: str) -> str:
    response = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': ROUTE_PROMPT.format(question=question)}],
        temperature=0,
        max_tokens=5,
    )
    answer = response.choices[0].message.content.strip().upper()
    return 'rag' if 'B' in answer else 'nl2sql'


if __name__ == '__main__':
    build_knowledge_base(force_rebuild=True)
    result = rag_query('GMV 和销售额有什么区别？')
    print(result['answer'])
