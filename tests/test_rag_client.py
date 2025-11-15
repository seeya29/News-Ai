from single_pipeline.rag_client import RAGClient


def test_rag_client_initializes():
    client = RAGClient()
    assert client is not None