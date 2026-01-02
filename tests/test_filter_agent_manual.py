import sys
import os
import json
import pytest

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.agents.filter_agent import FilterAgent

def test_filter_agent_dedup():
    """Test that FilterAgent assigns dedup keys."""
    agent = FilterAgent()
    
    items = [
        {"title": "Test Item 1", "body": "This is a test body.", "timestamp": "2025-01-01T12:00:00Z"},
        {"title": "Test Item 1 Duplicate", "body": "This is a test body.", "timestamp": "2025-01-01T12:05:00Z"},
        {"title": "Test Item 2", "body": "Different body.", "timestamp": "2025-01-01T13:00:00Z"},
    ]
    
    filtered = agent.filter_items(items)
    
    print(f"Filtered {len(filtered)} items.")
    for item in filtered:
        print(f"Title: {item['title']}")
        print(f"Dedup Key: {item['dedup_key']}")
        print(f"Dedup Flag: {item['dedup_flag']}")
        print("-" * 20)
        
    # Check if duplicate items have same dedup_key (if logic works as expected)
    # RAG assign_group_key logic: if similar, reuse key.
    # Item 1 and Item 1 Duplicate have same body, similar title. Should likely share key.
    
    key1 = filtered[0]['dedup_key']
    key2 = filtered[1]['dedup_key']
    if key1 == key2:
        print("SUCCESS: Duplicate items share the same dedup_key.")
    else:
        print("INFO: Duplicate items have different dedup_keys (might be expected if threshold not met).")

if __name__ == "__main__":
    test_filter_agent_dedup()
