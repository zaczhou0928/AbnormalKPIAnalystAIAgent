"""Tests for the knowledge retrieval module."""

import pytest

from agentic_kpi_analyst.retrieval.retriever import KnowledgeRetriever


@pytest.fixture(scope="module")
def retriever() -> KnowledgeRetriever:
    return KnowledgeRetriever("docs/knowledge")


class TestRetrieval:
    """Test knowledge document retrieval."""

    def test_retriever_initializes(self, retriever: KnowledgeRetriever) -> None:
        assert len(retriever.chunks) > 0

    def test_gmv_query(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("GMV definition", top_k=3)
        assert len(docs) > 0
        sources = [d.source for d in docs]
        assert "metric_definitions.md" in sources

    def test_refund_rate_query(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("refund rate calculation", top_k=3)
        assert len(docs) > 0
        assert any("refund" in d.section.lower() for d in docs)

    def test_payment_query(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("payment gateway outage credit card", top_k=3)
        assert len(docs) > 0
        assert any("payment" in d.section.lower() for d in docs)

    def test_campaign_query(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("campaign calendar black friday", top_k=3)
        assert len(docs) > 0
        assert any("campaign" in d.section.lower() for d in docs)

    def test_empty_query(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("", top_k=3)
        assert len(docs) == 0

    def test_relevance_scores_descending(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("order count", top_k=5)
        scores = [d.relevance_score for d in docs]
        assert scores == sorted(scores, reverse=True)

    def test_top_k_limit(self, retriever: KnowledgeRetriever) -> None:
        docs = retriever.retrieve("orders", top_k=2)
        assert len(docs) <= 2
