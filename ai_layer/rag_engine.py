# -*- coding: utf-8 -*-
"""RAG 引擎：知识库构建 + 检索问答"""
import os, re, sys
from pathlib import Path
from openai import OpenAI
import chromadb
from chromadb.utils import embedding_functions

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry

log = get_logger('rag_engine')

COLLECTION_NAME  = 'ai_dw_knowledge'
_EMBED_MODEL     = 'paraphrase-multilingual-MiniLM-L12-v2'

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


# ── RAG 问答 ──────────────────────────────────────────────────

RAG_PROMPT = """你是一位专业的数据分析顾问，熟悉本公司的数据仓库和业务规则。
请根据以下知识库内容回答用户的问题。

【相关知识】
{context}

【用户问题】
{question}

【回答要求】
- 根据知识库内容准确回答，不要编造知识库中没有的信息
- 如果知识库中没有相关信息，直接说"知识库中暂无此信息"
- 回答简洁清晰，使用中文
"""

@llm_retry
def rag_query(question: str) -> dict:
    log.info('[RAG检索] %s', question)
    chunks = retrieve(question)
    log.info('[RAG检索] 找到 %d 个相关文本块', len(chunks))

    context = '\n\n---\n\n'.join(
        [f"来源：{c['source']}\n{c['text']}" for c in chunks]
    )
    prompt = RAG_PROMPT.format(context=context, question=question)
    response = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': prompt}],
        temperature=cfg.rag_temperature,
        max_tokens=800,
    )
    return {
        'answer':  response.choices[0].message.content.strip(),
        'sources': list({c['source'] for c in chunks}),
        'chunks':  chunks,
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
