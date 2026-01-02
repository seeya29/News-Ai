import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from single_pipeline.registry import load_registry, validate_feeds, convert_to_sources, write_sources_json, save_registry_yaml

def main():
    print("Loading registry from YAML...")
    try:
        data = load_registry()
        feeds = data["feeds"]
        print(f"Found {len(feeds)} feeds.")
        
        print("Validating feeds...")
        validated, warnings = validate_feeds(feeds)
        for w in warnings:
            print(f"Warning: {w}")
        
        print(f"Validated {len(validated)} feeds.")
        
        print("Converting to sources.json structure...")
        sources = convert_to_sources(validated)
        
        print("Writing sources.json...")
        path = write_sources_json(sources)
        print(f"Successfully updated {path}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
