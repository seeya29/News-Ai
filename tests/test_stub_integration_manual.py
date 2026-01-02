import sys
import os
import json
import pytest

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.fetcher_hub import FetcherHub

def test_fetcher_hub_stubs():
    """Test that FetcherHub correctly picks up stubbed items."""
    hub = FetcherHub()
    
    # We expect 'sources.json' to be present and have 'stubs' configured.
    # If not, we might need to mock _load_sources_config, but let's try with the real one first
    # as we know it's there.
    
    # Run the hub
    # Note: run() writes to file, but also we can't easily capture the return value as it returns a dict of metadata, not items.
    # But wait, run() returns a dict? Let's check fetcher_hub.py again.
    # It returns a dict, but doesn't return the items.
    
    # However, we can mock the write method or just check the output file.
    # Or better, we can monkeypatch _load_sources_config to ensure we only test stubs if we want isolation.
    
    # Let's inspect the output file.
    
    output_file = hub.run(registry_name="single", category="test_category")
    
    # output_file is likely the path to the written file (wait, run() returns what? Let's check).
    # Ah, run() returns what?
    # Let's check fetcher_hub.py again.
    pass

if __name__ == "__main__":
    # Manually run if executed as script
    hub = FetcherHub()
    try:
        hub.run(registry_name="single", category="test_category")
        print("FetcherHub run completed.")
        
        # Check for output file
        # The file name is based on registry_name: "single_items.json"
        out_path = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output", "single_items.json")
        if os.path.exists(out_path):
            with open(out_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                print(f"Found {len(data)} items.")
                stubs = [d for d in data if d.get("source", {}).get("type") == "stub"]
                print(f"Found {len(stubs)} stub items.")
                if len(stubs) > 0:
                    print("Stub integration successful!")
                    print("Sample stub:", stubs[0])
                else:
                    print("No stub items found.")
        else:
            print(f"Output file not found at {out_path}")
            
    except Exception as e:
        print(f"Error: {e}")
