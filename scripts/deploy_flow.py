import sys
import os
from pathlib import Path
import prefect
import importlib.util

def validate_flow_name(flow_name):
    """Validate that the flow name contains only valid characters."""
    if not all(c.isalnum() or c == '_' for c in flow_name):
        raise ValueError(f"Invalid flow name '{flow_name}'. Flow name should only contain letters, numbers, and underscores.")

def import_flow_module(file_path, flow_name):
    """Import the flow module and return the flow object."""
    try:
        # Get the absolute path
        abs_path = os.path.abspath(file_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Flow file not found: {abs_path}")

        # Add the directory containing the flow file to Python path
        flow_dir = os.path.dirname(abs_path)
        if flow_dir not in sys.path:
            sys.path.insert(0, flow_dir)

        # Import the module
        module_name = os.path.splitext(os.path.basename(abs_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, abs_path)
        if spec is None:
            raise ImportError(f"Could not load module from {abs_path}")
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Get the flow object
        if not hasattr(module, flow_name):
            raise AttributeError(f"Flow '{flow_name}' not found in {abs_path}")
        
        return getattr(module, flow_name)
    except Exception as e:
        raise ImportError(f"Error importing flow: {str(e)}")

def main():
    if len(sys.argv) == 2:
        flow_path = sys.argv[1]
        if ":" not in flow_path:
            print("Usage: deploy_flow.py <file.py:flow>")
            sys.exit(1)
        file_path, flow_name = flow_path.split(":")
    elif len(sys.argv) == 3:
        file_path, flow_name = sys.argv[1], sys.argv[2]
    else:
        print("Usage: deploy_flow.py <file.py:flow> OR deploy_flow.py <file.py> <flow>")
        sys.exit(1)

    try:
        # Validate flow name
        validate_flow_name(flow_name)
        
        print(f"Importing workflow from: {file_path}")
        
        # Import the flow
        flow = import_flow_module(file_path, flow_name)
        
        # Serve the flow (simpler than deploy)
        flow.serve(
            name=flow_name.replace('_', ' ').title(),
            tags=["file-ingestion"],
            interval=300  # Run every 5 minutes
        )
        
        print(f"Flow '{flow_name}' is now being served")
        print(f"View in UI: http://localhost:4440/flows")

    except Exception as e:
        print(f"Error deploying flow: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
