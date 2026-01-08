import sys
import os
import json
import time
import uuid
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.bucket_orchestrator import BucketOrchestrator

def create_test_item(lang, tone, index):
    return {
        "id": f"burnin_{lang}_{tone}_{index}_{uuid.uuid4().hex[:6]}",
        "title": f"Test Article {index} for {lang} {tone}",
        "body": f"This is a simulated news body for burn-in testing. It contains enough text to generate a script. Language is {lang}. Tone is {tone}. We want to ensure the pipeline processes this correctly.",
        "language": lang,
        "tone": tone,
        "priority_score": 0.5,
        "trend_score": 0.5,
        "audio_path": None,
        "stage_status": {
            "fetch": "success",
            "filter": "success",
            "script": "pending"
        },
        "timestamps": {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
    }

def generate_test_data(count=30):
    items = []
    configs = [
        ("en", "news"),
        ("hi", "news"),
        ("hi", "youth"),
        ("ta", "news"),
        ("bn", "news"),
        ("en", "kids")
    ]
    
    per_config = count // len(configs)
    
    idx = 0
    for lang, tone in configs:
        for _ in range(per_config):
            idx += 1
            items.append(create_test_item(lang, tone, idx))
            
    # Fill remainder
    while len(items) < count:
        idx += 1
        items.append(create_test_item("en", "news", idx))
        
    return items

def run_burn_in():
    print("--- Starting Burn-In & Stability Test ---")
    
    # 1. Generate Data
    items = generate_test_data(30)
    registry = "burnin_v1"
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output"))
    os.makedirs(output_dir, exist_ok=True)
    
    input_file = os.path.join(output_dir, f"{registry}_filtered.json")
    with open(input_file, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
        
    print(f"Generated {len(items)} test items in {input_file}")
    
    # 2. Run Pipeline
    start_time = time.time()
    orchestrator = BucketOrchestrator(registry=registry, category="test")
    result = orchestrator.run()
    duration = time.time() - start_time
    
    # 3. Analyze Results
    print("\n--- Execution Completed ---")
    print(f"Duration: {duration:.2f}s")
    
    total_processed = 0
    total_success = 0
    total_failed = 0
    break_details = []
    
    # The result structure from BucketOrchestrator is:
    # {
    #     "success": bool, 
    #     "buckets_total": int,
    #     "buckets_success": int,
    #     "results": [
    #         {
    #             "bucket": str,
    #             "counts": {"items": N, "scripts": N, "voice": N, "avatar": N},
    #             "files": {...},
    #             "status": "success"
    #         },
    #         ...
    #     ]
    # }

    if not result.get("success") and not result.get("results"):
         print("FATAL: Orchestrator failed to run.")
         total_failed = len(items)
    else:
        for res in result.get("results", []):
            bucket = res.get("bucket")
            if res.get("status") == "failed":
                print(f"Bucket {bucket} FAILED: {res.get('error')}")
                # Assume all items in this bucket failed? 
                # We don't know exactly how many items were in the bucket from the failure result easily
                # unless we parse the input again. But let's assume worst case or just track buckets.
                break_details.append({"bucket": bucket, "error": res.get("error")})
                # For count, we can try to look at input distribution, but simpler to just track success from counts.
            else:
                counts = res.get("counts", {})
                # Total input items for this bucket
                b_items = counts.get("items", 0)
                # Final output (avatar)
                b_avatars = counts.get("avatar", 0)
                
                total_processed += b_items
                total_success += b_avatars
                total_failed += (b_items - b_avatars)
                
                if b_items != b_avatars:
                     break_details.append({
                         "bucket": bucket, 
                         "issue": f"Input {b_items} != Output {b_avatars}",
                         "drop_count": b_items - b_avatars
                     })

    success_rate = (total_success / len(items)) * 100 if items else 0
    break_rate = (total_failed / len(items)) * 100 if items else 0
    
    print(f"\nTotal Items: {len(items)}")
    print(f"Success: {total_success}")
    print(f"Failed/Dropped: {total_failed}")
    print(f"Success Rate: {success_rate:.1f}%")
    print(f"Break Rate: {break_rate:.1f}%")
    
    # 4. Generate Report
    report = {
        "report_version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "test_type": "burn_in_simulation",
        "duration_seconds": duration,
        "metrics": {
            "total_flows": len(items),
            "success_count": total_success,
            "break_count": total_failed,
            "success_percentage": success_rate,
            "break_percentage": break_rate
        },
        "break_details": break_details,
        "fixes_applied": [
            "Implemented strict schema validation in FilterAgent",
            "Added failure modes (degraded/failed) in TTS and ScriptGen",
            "Added skip logic in AvatarAgent for missing audio",
            "Enforced stage_status tracking across all agents"
        ],
        "status": "stable" if success_rate > 90 else "unstable"
    }
    
    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "reports", "orchestration_stability_report_v1.json"))
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        
    print(f"\nReport generated at: {report_path}")

if __name__ == "__main__":
    run_burn_in()
