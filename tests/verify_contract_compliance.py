import json
import sys
import os
from typing import Dict, Any, List

# Add the project root to sys.path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from single_pipeline.agents.script_gen_agent import ScriptGenAgent

def validate_against_schema(data: Dict[str, Any], schema: Dict[str, Any], path: str = "") -> List[str]:
    errors = []
    
    # Check required fields
    if "required" in schema:
        for field in schema["required"]:
            if field not in data:
                errors.append(f"Missing required field: {path}.{field}")
    
    # Check properties
    if "properties" in schema:
        for field, prop_schema in schema["properties"].items():
            if field in data:
                # Type checking (simplified)
                if "type" in prop_schema:
                    expected_type = prop_schema["type"]
                    value = data[field]
                    if expected_type == "string" and not isinstance(value, str):
                        errors.append(f"Type mismatch at {path}.{field}: expected string, got {type(value)}")
                    elif expected_type == "object" and not isinstance(value, dict):
                        errors.append(f"Type mismatch at {path}.{field}: expected object, got {type(value)}")
                    elif expected_type == "array" and not isinstance(value, list):
                        errors.append(f"Type mismatch at {path}.{field}: expected array, got {type(value)}")
                
                # Recursive validation for objects
                if prop_schema.get("type") == "object":
                    errors.extend(validate_against_schema(data[field], prop_schema, path + "." + field))
                
                # Validation for array items (simplified)
                if prop_schema.get("type") == "array" and "items" in prop_schema:
                    item_schema = prop_schema["items"]
                    for i, item in enumerate(data[field]):
                        if item_schema.get("type") == "object":
                            errors.extend(validate_against_schema(item, item_schema, f"{path}.{field}[{i}]"))

    return errors

def main():
    print("Starting Contract Verification...")
    
    # 1. Load Contract
    contract_path = os.path.join(os.path.dirname(__file__), '..', 'single_pipeline', 'orchestration_contract_v1.json')
    try:
        with open(contract_path, 'r') as f:
            contract = json.load(f)
            print(f"Loaded contract version: {contract.get('contract_version')}")
    except Exception as e:
        print(f"Failed to load contract: {e}")
        sys.exit(1)

    # 2. Instantiate Agent
    try:
        agent = ScriptGenAgent()
        print("ScriptGenAgent instantiated.")
    except Exception as e:
        print(f"Failed to instantiate agent: {e}")
        sys.exit(1)

    # 3. Generate Dummy Output
    dummy_input = [{
        "title": "Test News",
        "body": "This is a test news body for verification. It has some details.",
        "lang": "en",
        "audience": "general",
        "tone": "neutral"
    }]
    
    try:
        output = agent.generate(dummy_input, category="test")
        print(f"Generated {len(output)} scripts.")
        if not output:
            print("Error: No output generated.")
            sys.exit(1)
        script_data = output[0]
    except Exception as e:
        print(f"Failed to generate script: {e}")
        sys.exit(1)

    # 4. Validate
    schema = contract["schemas"]["script_output"]
    errors = validate_against_schema(script_data, schema, "root")

    if errors:
        print("\n❌ Verification FAILED with schema errors:")
        for err in errors:
            print(f" - {err}")
        sys.exit(1)
    else:
        print("\n✅ Verification PASSED! Output complies with orchestration_contract_v1.json")

if __name__ == "__main__":
    main()
