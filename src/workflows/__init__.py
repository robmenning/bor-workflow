"""
Workflow registry and utilities for BOR workflow service.
"""
from typing import Dict, Type
from prefect import Flow

# Registry of all available workflows
WORKFLOW_REGISTRY: Dict[str, Type[Flow]] = {}

def register_workflow(name: str, flow_class: Type[Flow]) -> None:
    """Register a workflow in the global registry."""
    WORKFLOW_REGISTRY[name] = flow_class

def get_workflow(name: str) -> Type[Flow]:
    """Get a workflow from the registry by name."""
    if name not in WORKFLOW_REGISTRY:
        raise KeyError(f"Workflow '{name}' not found in registry")
    return WORKFLOW_REGISTRY[name]

# Import and register workflows (optional imports)
try:
    from .mellon_holdings_etl import mellon_holdings_etl_flow
    register_workflow("mellon-holdings-etl", mellon_holdings_etl_flow)
except ImportError:
    pass

try:
    from .factset_out_hold import factset_out_hold_flow
    register_workflow("factset-out-hold", factset_out_hold_flow)
except ImportError:
    pass 