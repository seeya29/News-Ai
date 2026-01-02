import sys
import os
import json
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.agents.filter_agent import FilterAgent

def main():
    logging.basicConfig(level=logging.INFO)
    print("Initializing FilterAgent...")
    agent = FilterAgent()

    # Sample items with duplicates
    items = [
        {"title": "SpaceX Launches New Rocket", "body": "SpaceX has successfully launched its new Starship rocket from Texas.", "timestamp": "2025-01-01T10:00:00Z"},
        {"title": "SpaceX Starship Launch Success", "body": "SpaceX has successfully launched its new Starship rocket from Texas.", "timestamp": "2025-01-01T10:05:00Z"},
        {"title": "New AI Model Released", "body": "OpenAI has released a new model with improved reasoning capabilities.", "timestamp": "2025-01-01T11:00:00Z"},
        {"title": "OpenAI Releases GPT-5", "body": "OpenAI has released a new model with improved reasoning capabilities.", "timestamp": "2025-01-01T11:05:00Z"},
        {"title": "Local Weather Update", "body": "It will be sunny today.", "timestamp": "2025-01-01T09:00:00Z"},
    ]

    print(f"\nProcessing {len(items)} items...")
    filtered = agent.filter_items(items)

    print(f"\nFilter complete. Result: {len(filtered)} items.")
    print("-" * 40)
    for item in filtered:
        print(f"Title: {item['title']}")
        print(f"Lang: {item['lang']}")
        print(f"Dedup Key: {item['dedup_key']}")
        print(f"Dedup Flag: {item['dedup_flag']}")
        print("-" * 40)

    print("\nCheck output/rag_cache.json for cached entries.")

if __name__ == "__main__":
    main()
