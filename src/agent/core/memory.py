# -*- coding: utf-8 -*-
"""短期（会话）+ 长期（ChromaDB）记忆管理"""
import time
from collections import deque
from typing import Any

class ShortTermMemory:
    """对话会话短期记忆（环形缓冲，最多保留 20 条）"""
    def __init__(self, max_size: int = 20):
        self._buffer: deque = deque(maxlen=max_size)

    def add(self, role: str, content: str):
        self._buffer.append({"role": role, "content": content, "ts": time.time()})

    def get_recent(self, n: int = 5) -> list[dict]:
        return list(self._buffer)[-n:]

    def clear(self):
        self._buffer.clear()


class LongTermMemory:
    """长期记忆：写入 ChromaDB，跨会话持久化"""
    def __init__(self, collection_name: str = "agent_memory"):
        self._collection = None
        self._collection_name = collection_name

    def _get_collection(self):
        if self._collection is None:
            try:
                import chromadb
                client = chromadb.PersistentClient(path="chroma_db")
                self._collection = client.get_or_create_collection(self._collection_name)
            except Exception:
                pass
        return self._collection

    def store(self, content: str, metadata: dict = None):
        coll = self._get_collection()
        if coll:
            import hashlib
            doc_id = hashlib.md5(content.encode()).hexdigest()[:16]
            coll.upsert(documents=[content], ids=[doc_id], metadatas=[metadata or {}])

    def search(self, query: str, top_k: int = 3) -> list[str]:
        coll = self._get_collection()
        if not coll:
            return []
        result = coll.query(query_texts=[query], n_results=top_k)
        return result.get("documents", [[]])[0]
