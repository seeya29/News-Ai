import sys
import os
import json
import time
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.cli import run_fetch, run_filter
from single_pipeline.bucket_orchestrator import BucketOrchestrator

def run_e2e():
    print("--- Starting End-to-End Pipeline (Fetch -> Process -> Script -> Voice -> Feedback) ---")
    
    registry = "single"
    category = "general"
    
    # 1. Fetch
    print(f"\n[1/5] Fetching items for registry='{registry}'...")
    fetch_res = run_fetch(registry=registry, category=category)
    fetched_count = len(fetch_res.get("items", []))
    print(f"      Fetched {fetched_count} items. File: {fetch_res.get('output_file')}")
    
    if fetched_count == 0:
        print("WARNING: No items fetched. Pipeline may stop early.")
    
    # 2. Process (Filter)
    print(f"\n[2/5] Filtering/Processing items...")
    filter_res = run_filter(registry=registry, category=category)
    filtered_count = filter_res.get("count", 0)
    print(f"      Filtered down to {filtered_count} valid items. File: {filter_res.get('output_file')}")
    
    if filtered_count == 0:
        print("STOPPING: No items to process.")
        return

    # 3 & 4. Script -> Voice -> Avatar (via BucketOrchestrator)
    print(f"\n[3/5] & [4/5] Running Bucket Orchestrator (Script -> Voice -> Avatar)...")
    orchestrator = BucketOrchestrator(registry=registry, category=category)
    orch_res = orchestrator.run()
    
    # 5. Feedback (Report)
    print(f"\n[5/5] Generating Feedback Report...")
    
    total_processed = 0
    total_success = 0
    break_details = []
    
    for res in orch_res.get("results", []):
        bucket = res.get("bucket")
        counts = res.get("counts", {})
        b_items = counts.get("items", 0)
        b_avatars = counts.get("avatar", 0)
        
        total_processed += b_items
        total_success += b_avatars
        
        if b_items != b_avatars:
            break_details.append({
                "bucket": bucket,
                "input": b_items,
                "output": b_avatars,
                "dropped": b_items - b_avatars
            })

    success_rate = (total_success / total_processed) * 100 if total_processed > 0 else 0
    
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "registry": registry,
        "stages": {
            "fetch": fetched_count,
            "filter": filtered_count,
            "orchestration_input": total_processed,
            "final_output": total_success
        },
        "success_rate": f"{success_rate:.1f}%",
        "break_details": break_details,
        "status": "success" if success_rate > 0 else "failed"  # lenient for e2e test
    }
    
    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output", f"{registry}_e2e_feedback.json"))
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        
    print(f"\nFeedback Report generated: {report_path}")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    run_e2e()
