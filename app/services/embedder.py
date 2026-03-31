import logging
from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer
from app.config import get_settings

logger = logging.getLogger(__name__)

# Module-level cache for the embedding model
_model: SentenceTransformer | None = None
_pinecone_index = None


def _get_model() -> SentenceTransformer:
    """Lazy-load the sentence transformer model."""
    global _model
    if _model is None:
        settings = get_settings()
        logger.info(f"Loading embedding model: {settings.EMBEDDING_MODEL}")
        _model = SentenceTransformer(settings.EMBEDDING_MODEL)
    return _model


def _get_index():
    """Get or create the Pinecone index."""
    global _pinecone_index
    if _pinecone_index is None:
        settings = get_settings()
        pc = Pinecone(api_key=settings.PINECONE_API_KEY)

        index_name = settings.PINECONE_INDEX_NAME

        # Check if index exists, create if not
        existing_indexes = [idx.name for idx in pc.list_indexes()]
        if index_name not in existing_indexes:
            logger.info(f"Creating Pinecone index: {index_name}")
            pc.create_index(
                name=index_name,
                dimension=384,  # all-MiniLM-L6-v2 outputs 384-dim vectors
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )

        _pinecone_index = pc.Index(index_name)
        logger.info(f"Connected to Pinecone index: {index_name}")

    return _pinecone_index


def init_pinecone() -> None:
    """Initialize the Pinecone connection (call on startup)."""
    _get_index()
    logger.info("Pinecone initialized successfully")


def embed_and_store(reel_id: str, text: str, metadata: dict) -> None:
    """
    Generate embedding for text and store in Pinecone.

    Args:
        reel_id: Unique identifier for the reel
        text: Text to embed (transcript + summary combined)
        metadata: Additional metadata to store with the vector
    """
    model = _get_model()
    index = _get_index()

    # Generate embedding
    embedding = model.encode(text).tolist()

    # Flatten metadata for Pinecone (no nested objects allowed)
    flat_metadata = {
        "reel_id": reel_id,
        "user_id": metadata.get("user_id", ""),
        "title": metadata.get("title", ""),
        "category": metadata.get("category", ""),
        "subcategory": metadata.get("subcategory", ""),
        "summary": metadata.get("summary", "")[:500],  # Pinecone metadata size limit
    }

    # Upsert to Pinecone
    index.upsert(vectors=[(reel_id, embedding, flat_metadata)])
    logger.info(f"Stored embedding for reel {reel_id}")


def search_similar(
    query: str,
    user_id: str | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    top_k: int = 5,
) -> list[dict]:
    """
    Search for similar reels using semantic similarity.

    Args:
        query: Natural language search query
        user_id: Optional filter by user
        category: Optional filter by category
        subcategory: Optional filter by subcategory
        top_k: Number of results to return

    Returns:
        List of dicts with 'reel_id', 'score', and 'metadata'
    """
    model = _get_model()
    index = _get_index()

    # Generate query embedding
    query_embedding = model.encode(query).tolist()

    # Build filter
    filter_dict = {}
    if user_id:
        filter_dict["user_id"] = user_id
    if category:
        filter_dict["category"] = category
    if subcategory:
        filter_dict["subcategory"] = subcategory

    # Query Pinecone
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
