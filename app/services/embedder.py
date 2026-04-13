import hashlib
import logging
import math
import re
from collections import Counter

from pinecone import Pinecone, ServerlessSpec

from app.config import get_settings

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSION = 384
_pinecone_index = None


def _tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", text.lower()) if token]


def _hash_embedding(text: str) -> list[float]:
    vector = [0.0] * EMBEDDING_DIMENSION
    token_counts = Counter(_tokenize(text))

    if not token_counts:
        return vector

    for token, count in token_counts.items():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % EMBEDDING_DIMENSION
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[index] += sign * float(count)

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector

    return [value / norm for value in vector]


def _get_index():
    global _pinecone_index
    if _pinecone_index is None:
        settings = get_settings()
        pc = Pinecone(api_key=settings.PINECONE_API_KEY)

        index_name = settings.PINECONE_INDEX_NAME
        existing_indexes = [idx.name for idx in pc.list_indexes()]
        if index_name not in existing_indexes:
            logger.info(f"Creating Pinecone index: {index_name}")
            pc.create_index(
                name=index_name,
                dimension=EMBEDDING_DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )

        _pinecone_index = pc.Index(index_name)
        logger.info(f"Connected to Pinecone index: {index_name}")

    return _pinecone_index


def init_pinecone() -> None:
    _get_index()
    logger.info("Pinecone initialized successfully")


def embed_and_store(reel_id: str, text: str, metadata: dict) -> str:
    index = _get_index()
    embedding = _hash_embedding(text)

    flat_metadata = {
        "reel_id": reel_id,
        "user_id": metadata.get("user_id", ""),
        "title": metadata.get("title", ""),
        "category": metadata.get("category", ""),
        "subcategory": metadata.get("subcategory", ""),
        "summary": metadata.get("summary", "")[:500],
    }

    index.upsert(vectors=[(reel_id, embedding, flat_metadata)])
    logger.info(f"Stored embedding for reel {reel_id}")
    return reel_id


def search_similar(
    query: str,
    user_id: str | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    top_k: int = 5,
) -> list[dict]:
    index = _get_index()
    query_embedding = _hash_embedding(query)

    filter_dict = {}
    if user_id:
        filter_dict["user_id"] = user_id
    if category:
        filter_dict["category"] = category
    if subcategory:
        filter_dict["subcategory"] = subcategory

    results = index.query(
        vector=query_embedding,
        top_k=top_k,
        include_metadata=True,
        filter=filter_dict if filter_dict else None,
    )

    matches = []
    for match in results.matches:
        matches.append(
            {
                "reel_id": match.metadata.get("reel_id", match.id),
                "score": match.score,
                "metadata": match.metadata,
            }
        )

    logger.info(f"Search found {len(matches)} results for query: '{query[:50]}...'")
    return matches
