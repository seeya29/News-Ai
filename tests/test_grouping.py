import os
import time
import datetime

from server import db
from single_pipeline.rag_client import RAGClient


def test_group_key_dedup_and_grouped_feed():
    # Ensure DB initialized
    db.init_db()

    # Use category-specific rows to avoid interfering with other tests
    category = "test"

    # Prepare two similar articles within 24h window
    title = "OpenAI unveils new model with better reasoning"
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    earlier_iso = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)).isoformat()

    rag = RAGClient()
    gk1 = rag.assign_group_key(title=title, body="", published_at_iso=earlier_iso, category=category)
    gk2 = rag.assign_group_key(title=title + "!!!", body="Details soon", published_at_iso=now_iso, category=category)

    art1 = {
        "id": "test:1",
        "title": title,
        "source_name": "unit",
        "source_url": "https://example.com/1",
        "thumbnail_url": None,
        "category": category,
        "published_at": earlier_iso,
        "relevance_score": 0.75,
        "processing_status": "ingested",
        "processing_progress": 10,
        "group_key": gk1,
    }

    art2 = {
        "id": "test:2",
        "title": title + "!!!",
        "source_name": "unit",
        "source_url": "https://example.com/2",
        "thumbnail_url": None,
        "category": category,
        "published_at": now_iso,
        "relevance_score": 0.75,
        "processing_status": "ingested",
        "processing_progress": 10,
        "group_key": gk2,
    }

    db.upsert_article(art1)
    db.upsert_article(art2)

    # Both should be in the same group
    assert gk1 == gk2

    total_groups = db.count_article_groups(category)
    # At least one group (singleton groups may exist in DB from other tests)
    assert total_groups >= 1

    grouped = db.get_grouped_articles(limit=10, offset=0, category=category)
    # Expect one representative for this group, newest should be art2
    ids = [r.get("id") for r in grouped]
    assert "test:2" in ids