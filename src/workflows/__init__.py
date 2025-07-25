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

# Import and register workflows
from .import_web_classfees import import_web_classfees_flow
register_workflow("import_web_classfees", import_web_classfees_flow)

from .mellon_holdings_etl import mellon_holdings_etl_flow
register_workflow("mellon-holdings-etl", mellon_holdings_etl_flow) 