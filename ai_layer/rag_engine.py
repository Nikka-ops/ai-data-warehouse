# -*- coding: utf-8 -*-
"""
RAG 引擎：知识库构建 + 检索问答
流程：文档 → 切分 → 向量化 → ChromaDB → 检索 → LLM 回答
"""

import os
import re
from pathlib import Path
from openai import OpenAI
import chromadb
from chromadb.utils import embedding_functions

# ── 配置 ──────────────────────────────────────────────────────
DEEPSEEK_API_KEY  = os.getenv('DEEPSEEK_API_KEY', '')
KNOWLEDGE_DIR     = os.path.join(os.path.dirname(__file__), '..', 'knowledge_base')
CHROMA_DIR        = os.path.join(os.path.dirname(__file__), '..', 'chroma_db')
COLLECTION_NAME   = 'ai_dw_knowledge'
CHUNK_SIZE        = 400    # 每个文本块的最大字符数
CHUNK_OVERLAP     = 80     # 相邻块的重叠字符数
TOP_K             = 3      # 检索返回的最相关块数量

# ── 客户端初始化 ──────────────────────────────────────────────
llm = OpenAI(api_key=DEEPSEEK_API_KEY, base_url='https://api.deepseek.com', timeout=60.0)

# 使用多语言 Sentence Transformer 模型（支持中文）
embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name='paraphrase-multilingual-MiniLM-L12-v2'
)

def get_chroma_client():
    os.makedirs(CHROMA_DIR, exist_ok=True)
    return chromadb.PersistentClient(path=CHROMA_DIR)


# ── 文档处理 ──────────────────────────────────────────────────

def load_documents():
    """加载 knowledge_base 目录下所有 .md 文件"""
    docs = []
    kb_path = Path(KNOWLEDGE_DIR)
    for md_file in sorted(kb_path.glob('*.md')):
        content = md_file.read_text(encoding='utf-8')
        docs.append({
            'filename': md_file.name,
            'content': content
        })
        print(f"  已加载：{md_file.name}（{len(content)} 字符）")
    return docs


def split_chunks(text, source):
    """
    将文档切分为小块（Chunk）
    策略：优先按段落切分，超长段落再按字符切分
    """
    chunks = []

    # 先按段落（双换行）切分
    paragraphs = re.split(r'\n\s*\n', text)

    current_chunk = ''
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # 如果当前块加上新段落不超过限制，直接追加
        if len(current_chunk) + len(para) <= CHUNK_SIZE:
            current_chunk += ('\n\n' + para if current_chunk else para)
        else:
            # 保存当前块
            if current_chunk:
                chunks.append({'text': current_chunk, 'source': source})

            # 如果单段落就超过限制，按字符强制切分
            if len(para) > CHUNK_SIZE:
                for i in range(0, len(para), CHUNK_SIZE - CHUNK_OVERLAP):
                    sub = para[i:i + CHUNK_SIZE]
                    if sub.strip():
                        chunks.append({'text': sub, 'source': source})
                current_chunk = ''
            else:
                # 新段落作为新块的开头，携带一点重叠上下文
                overlap = current_chunk[-CHUNK_OVERLAP:] if current_chunk else ''
                current_chunk = (overlap + '\n\n' + para).strip() if overlap else para

    # 保存最后一块
    if current_chunk.strip():
        chunks.append({'text': current_chunk, 'source': source})

    return chunks


# ── 知识库构建 ────────────────────────────────────────────────

def build_knowledge_base(force_rebuild=False):
    """
    构建向量知识库
    force_rebuild=True 时清空重建，否则跳过已存在的
    """
    client = get_chroma_client()

    # 检查是否已存在
    existing = [c.name for c in client.list_collections()]
    if COLLECTION_NAME in existing:
        if not force_rebuild:
            col = client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)
            cnt = col.count()
            print(f"知识库已存在，共 {cnt} 个文本块，跳过构建。")
            print("如需重建，调用 build_knowledge_base(force_rebuild=True)")
            return col
        else:
            client.delete_collection(COLLECTION_NAME)
            print("已清空旧知识库，重新构建...")

    # 创建集合
    col = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=embedding_fn,
        metadata={'hnsw:space': 'cosine'}   # 使用余弦相似度
    )

    print("\n正在构建知识库...")
    docs = load_documents()

    all_chunks = []
    for doc in docs:
        chunks = split_chunks(doc['content'], doc['filename'])
        all_chunks.extend(chunks)
        print(f"  {doc['filename']} → {len(chunks)} 个文本块")

    # 批量写入 ChromaDB
    print(f"\n正在向量化并写入 {len(all_chunks)} 个文本块...")
    col.add(
        ids=[f"chunk_{i}" for i in range(len(all_chunks))],
        documents=[c['text'] for c in all_chunks],
        metadatas=[{'source': c['source']} for c in all_chunks]
    )

    print(f"知识库构建完成，共 {col.count()} 个文本块。")
    return col


def get_collection():
    """获取已存在的知识库集合"""
    client = get_chroma_client()
    existing = [c.name for c in client.list_collections()]
    if COLLECTION_NAME not in existing:
        return build_knowledge_base()
    return client.get_collection(COLLECTION_NAME, embedding_function=embedding_fn)


# ── 检索 ──────────────────────────────────────────────────────

def retrieve(question, top_k=TOP_K):
    """
    向量检索：找到与问题最相关的 top_k 个文本块
    返回：[{'text': ..., 'source': ..., 'distance': ...}, ...]
    """
    col = get_collection()
    results = col.query(
        query_texts=[question],
        n_results=min(top_k, col.count())
    )

    chunks = []
    for i, doc in enumerate(results['documents'][0]):
        chunks.append({
            'text':     doc,
            'source':   results['metadatas'][0][i]['source'],
            'distance': results['distances'][0][i]
        })
    return chunks


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
- 如果涉及计算公式或 SQL，可以给出示例
"""

def rag_query(question):
    """
    RAG 问答主入口
    返回：{'answer': str, 'sources': list, 'chunks': list}
    """
    print(f"[检索] 问题：{question}")

    # 1. 向量检索相关文档块
    chunks = retrieve(question)
    print(f"[检索] 找到 {len(chunks)} 个相关文本块")
    for i, c in enumerate(chunks):
        print(f"  [{i+1}] 来源：{c['source']}，相似度：{1-c['distance']:.3f}")

    # 2. 拼装上下文
    context = '\n\n---\n\n'.join([
        f"来源：{c['source']}\n{c['text']}"
        for c in chunks
    ])

    # 3. 调用 LLM 生成回答
    prompt = RAG_PROMPT.format(context=context, question=question)
    response = llm.chat.completions.create(
        model='deepseek-chat',
        messages=[{'role': 'user', 'content': prompt}],
        temperature=0.3,
        max_tokens=800,
    )
    answer = response.choices[0].message.content.strip()

    sources = list(set(c['source'] for c in chunks))
    return {
        'answer':  answer,
        'sources': sources,
        'chunks':  chunks
    }


# ── 问题路由：判断用 RAG 还是 NL2SQL ─────────────────────────

ROUTE_PROMPT = """判断以下问题应该用哪种方式回答：

A. NL2SQL - 需要查询数据库获取具体数字/数据的问题
   例如：每月GMV是多少、销售额最高的品类、2018年订单数

B. RAG - 询问概念定义、业务规则、字段含义的问题
   例如：GMV怎么计算、delivered是什么意思、客单价的定义

问题：{question}

只回答 A 或 B，不要其他内容。"""

def route_question(question):
    """判断问题类型：返回 'nl2sql' 或 'rag'"""
    response = llm.chat.completions.create(
        model='deepseek-chat',
        messages=[{'role': 'user', 'content': ROUTE_PROMPT.format(question=question)}],
        temperature=0,
        max_tokens=5,
    )
    answer = response.choices[0].message.content.strip().upper()
    return 'rag' if 'B' in answer else 'nl2sql'


# ── 命令行测试 ────────────────────────────────────────────────

if __name__ == '__main__':
    os.environ['DEEPSEEK_API_KEY'] = os.getenv('DEEPSEEK_API_KEY', '')

    print("=" * 60)
    print("  RAG 知识库测试")
    print("=" * 60)

    # 构建知识库
    build_knowledge_base(force_rebuild=True)

    # 测试问题
    test_questions = [
        "GMV 和销售额有什么区别？",
        "customer_id 和 customer_unique_id 有什么不同？",
        "delivered 是什么意思？订单有哪些状态？",
        "客单价怎么计算？",
        "为什么不能在 dwd 层查 gmv 字段？",
        "巴西哪个州电商最发达？",
    ]

    print("\n" + "=" * 60)
    print("  开始问答测试")
    print("=" * 60)

    for q in test_questions:
        print(f"\n[问题] {q}")
        print("-" * 40)
        result = rag_query(q)
        print(f"[回答] {result['answer']}")
        print(f"[来源] {', '.join(result['sources'])}")
        print()
