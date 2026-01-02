import sys
import os
import json
import logging
import shutil
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.fetcher_hub import FetcherHub
from single_pipeline.agents.filter_agent import FilterAgent
from single_pipeline.bucket_orchestrator import BucketOrchestrator
from single_pipeline.logging_utils import PipelineLogger

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("e2e_test.log", encoding='utf-8')
        ]
    )

def main():
    # Force 'stub' provider to avoid pyttsx3 threading deadlocks during E2E test
    os.environ["TTS_PROVIDER"] = "stub"
    
    setup_logging()
    log = logging.getLogger("e2e_test")
    log.info("Starting E2E Pipeline Test")

    # 0. Clean previous outputs
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output"))
    if os.path.exists(output_dir):
        # Don't delete everything, just the files we are about to generate
        # actually, for a clean test, maybe we should clean relevant files
        pass
    os.makedirs(output_dir, exist_ok=True)

    # 1. Fetch
    log.info("Stage 1: Fetching...")
    fetcher = FetcherHub()
    try:
        fetch_res = fetcher.run(registry_name="single")
        items = fetch_res.get("items", [])
        log.info(f"Fetched {len(items)} items. Saved to {fetch_res.get('output_file')}")
    except Exception as e:
        log.error(f"Fetch failed: {e}")
        sys.exit(1)

    if not items:
        log.warning("No items fetched! Aborting.")
        sys.exit(1)

    # Limit to 10 items for speed in E2E test
    items = items[:10]
    log.info(f"Using {len(items)} items for E2E test.")

    # 2. Filter & Process
    log.info("Stage 2: Filtering & Processing...")
    filter_agent = FilterAgent()
    try:
        filtered_items = filter_agent.filter_items(items)
        log.info(f"Filtered to {len(filtered_items)} items.")
        
        # Save filtered items where Orchestrator expects them
        filtered_path = os.path.join(output_dir, "single_filtered.json")
        with open(filtered_path, "w", encoding="utf-8") as f:
            json.dump(filtered_items, f, ensure_ascii=False, indent=2)
        log.info(f"Saved filtered items to {filtered_path}")
    except Exception as e:
        log.error(f"Filter failed: {e}")
        sys.exit(1)

    # 3. Orchestrate (Script -> Voice -> Avatar)
    log.info("Stage 3: Orchestration...")
    orchestrator = BucketOrchestrator(registry="single")
    try:
        orch_res = orchestrator.run()
        log.info("Orchestration completed.")
        log.info(f"Result: {json.dumps(orch_res, indent=2)}")
        
        if not orch_res.get("success"):
            log.error("Orchestration reported failure.")
            sys.exit(1)
            
        results = orch_res.get("results", [])
        log.info(f"Orchestration results paths: {[r.get('files') for r in results]}")

        failed_buckets = [r for r in results if r.get("status") == "failed"]
        if failed_buckets:
            log.warning(f"Some buckets failed: {len(failed_buckets)}")
            for fb in failed_buckets:
                log.warning(f"Bucket {fb.get('bucket')} failed: {fb.get('error')}")
        
    except Exception as e:
        log.error(f"Orchestration failed: {e}")
        sys.exit(1)

    # 4. Verify Outputs
    log.info("Stage 4: Verification...")
    verification_passed = True
    
    # Check script files
    script_files = [r["files"]["scripts"] for r in orch_res.get("results", []) if r.get("status") == "success"]
    for sf in script_files:
        if not os.path.exists(sf):
            log.error(f"Missing script file: {sf}")
            verification_passed = False
        else:
            with open(sf, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    log.error(f"Invalid script file format: {sf}")
                    verification_passed = False
                # Verify structure of first item
                if data:
                    item = data[0]
                    if "variants" not in item:
                        log.error(f"Missing variants in script item: {sf}")
                        verification_passed = False

    # Check voice files
    voice_files = [r["files"]["voice"] for r in orch_res.get("results", []) if r.get("status") == "success"]
    for vf in voice_files:
        if not os.path.exists(vf):
            log.error(f"Missing voice file: {vf}")
            verification_passed = False
        else:
            with open(vf, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Verify audio_path contract
                for item in data:
                    if item.get("status") == "success":
                        if not item.get("audio_path") or not os.path.exists(item["audio_path"]):
                            log.error(f"Audio path missing or invalid for successful item: {item}")
                            verification_passed = False
                    elif item.get("status") == "failed":
                        if item.get("audio_path") is not None:
                            log.error(f"Audio path should be null for failed item: {item}")
                            verification_passed = False
                    else:
                        # If status is missing (old items?), check contract
                        if "audio_path" in item and item["audio_path"] and not os.path.exists(item["audio_path"]):
                             log.error(f"Audio path points to non-existent file: {item}")
                             verification_passed = False

    if verification_passed:
        log.info("E2E Test PASSED")
        # Explicitly check for the elusive file
        target = os.path.join(output_dir, "single_EN-NEWS_voice.json")
        if os.path.exists(target):
            log.info(f"FOUND IT: {target}")
        else:
            log.error(f"STILL MISSING: {target}")
            # List directory content to debug
            log.info(f"Dir content: {os.listdir(output_dir)}")
    else:
        log.error("E2E Test FAILED during verification")
        sys.exit(1)

if __name__ == "__main__":
    main()
