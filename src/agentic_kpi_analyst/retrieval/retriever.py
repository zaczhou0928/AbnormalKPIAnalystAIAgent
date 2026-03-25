"""BM25-based retriever for metric definitions and business rules."""

from __future__ import annotations

from pathlib import Path

from rank_bm25 import BM25Okapi

from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import RetrievedDoc
from agentic_kpi_analyst.retrieval.indexer import DocChunk, _tokenize, index_knowledge_dir

logger = get_logger(__name__)


class KnowledgeRetriever:
    """Retrieves relevant knowledge chunks using BM25 ranking."""

    def __init__(self, knowledge_dir: str | Path) -> None:
        self.chunks: list[DocChunk] = index_knowledge_dir(knowledge_dir)
        if self.chunks:
            corpus = [chunk.tokens for chunk in self.chunks]
            self._bm25 = BM25Okapi(corpus)
        else:
            self._bm25 = None
        logger.info("retriever_initialized", n_chunks=len(self.chunks))

    def retrieve(self, query: str, top_k: int = 5) -> list[RetrievedDoc]:
        """Retrieve the most relevant knowledge chunks for a query.

        Args:
            query: Natural language query (e.g., "What is GMV?", "refund rate calculation")
            top_k: Number of results to return.

        Returns:
            List of RetrievedDoc sorted by relevance score (descending).
        """
        if not self.chunks or self._bm25 is None:
            logger.warning("retriever_empty")
            return []

        tokens = _tokenize(query)
        if not tokens:
            return []

        scores = self._bm25.get_scores(tokens)

        # Pair chunks with scores and sort
        scored = sorted(
            zip(self.chunks, scores),
            key=lambda x: x[1],
            reverse=True,
        )

        results: list[RetrievedDoc] = []
        for chunk, score in scored[:top_k]:
            if score <= 0:
                continue
            results.append(RetrievedDoc(
                source=chunk.source,
                section=chunk.section,
                content=chunk.content,
                relevance_score=round(float(score), 4),
            ))

        logger.debug("retrieval_results", query=query[:50], n_results=len(results))
        return results
