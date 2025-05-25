"""
Base workflow class with common functionality.
"""
from typing import Any, Dict, Optional
from prefect import flow, get_run_logger
from prefect.context import get_run_context
from datetime import datetime

class BaseWorkflow:
    """Base class for all workflows with common functionality."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = get_run_logger()
        
    def log_workflow_start(self, params: Dict[str, Any]) -> None:
        """Log workflow start with parameters."""
        self.logger.info(f"Starting workflow {self.name}")
        self.logger.info(f"Parameters: {params}")
        
    def log_workflow_end(self, success: bool, error: Optional[Exception] = None) -> None:
        """Log workflow completion."""
        if success:
            self.logger.info(f"Workflow {self.name} completed successfully")
        else:
            self.logger.error(f"Workflow {self.name} failed: {str(error)}")
            
    def get_workflow_context(self) -> Dict[str, Any]:
        """Get current workflow context."""
        context = get_run_context()
        return {
            "workflow_id": context.flow_run.id,
            "start_time": context.flow_run.start_time,
            "name": self.name
        }
        
    def handle_workflow_error(self, error: Exception) -> None:
        """Handle workflow errors consistently."""
        self.logger.error(f"Workflow error in {self.name}: {str(error)}")
        # Add any common error handling logic here
        raise error 