"""
Base ingestion workflow with parameterized file loading functionality.
"""


# example of creating a new ingestion workflow:
    # class NewDataIngestionWorkflow(BaseIngestionWorkflow):
    #     def __init__(self):
    #         super().__init__(
    #             name="New Data Ingestion",
    #             target_table="borarch.NewTable",
    #             field_mappings={
    #                 "SourceField1": "TargetColumn1",
    #                 "SourceField2": "TargetColumn2"
    #             },
    #             field_transformations={
    #                 "SourceField1": "UPPER(@SourceField1)"
    #             },
    #             procedure_name="bormeta.usp_NewData_Load"
    #         )


from typing import Dict, Any, Optional, List
from pathlib import Path
import mysql.connector
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from .base_workflow import BaseWorkflow

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_file_exists(file_path: str) -> bool:
    """Check if file exists in the shared volume."""
    if file_path.startswith("/var/lib/mysql-files/"):
        # Optionally, check via MySQL if needed
        return True  # Assume file is present for MySQL server
    else:
        return Path(file_path).exists()

@task(retries=3, retry_delay_seconds=60)
def load_data_to_staging(
    file_path: str,
    db_config: dict,
    target_table: str,
    field_mappings: Dict[str, str],
    field_transformations: Optional[Dict[str, str]] = None,
    delimiter: str = ',',
    quote_char: str = '"',
    line_terminator: str = '\n',
    skip_lines: int = 1
) -> bool:
    """
    Load data from file into staging table using LOAD DATA INFILE.
    
    Args:
        file_path: Path to the input file
        db_config: Database connection configuration
        target_table: Target table name (format: database.table)
        field_mappings: Dictionary mapping source fields to target columns
        field_transformations: Optional dictionary of field transformations
        delimiter: Field delimiter character
        quote_char: Quote character
        line_terminator: Line terminator character
        skip_lines: Number of header lines to skip
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Build field list and transformations
        fields = []
        transformations = []
        
        for source_field, target_field in field_mappings.items():
            if field_transformations and source_field in field_transformations:
                fields.append(f"@{source_field}")
                transformations.append(f"{target_field} = {field_transformations[source_field]}")
            else:
                fields.append(target_field)
        
        # Build LOAD DATA INFILE command
        load_query = f"""
        LOAD DATA INFILE '{file_path}'
        INTO TABLE {target_table}
        FIELDS TERMINATED BY '{delimiter}'
        ENCLOSED BY '{quote_char}'
        LINES TERMINATED BY '{line_terminator}'
        IGNORE {skip_lines} LINES
        ({', '.join(fields)})
        """
        
        if transformations:
            load_query += f"\nSET {', '.join(transformations)}"
        
        cursor.execute(load_query)
        conn.commit()
        
        return True
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@task(retries=3, retry_delay_seconds=60)
def execute_stored_procedure(
    db_config: dict,
    procedure_name: str,
    procedure_params: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Execute a stored procedure for data processing.
    
    Args:
        db_config: Database connection configuration
        procedure_name: Name of the stored procedure (format: database.procedure)
        procedure_params: Optional dictionary of procedure parameters
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        if procedure_params:
            # Build parameter list for procedure call
            param_names = list(procedure_params.keys())
            param_values = list(procedure_params.values())
            cursor.callproc(procedure_name, param_values)
        else:
            cursor.callproc(procedure_name)
            
        conn.commit()
        return True
    except Exception as e:
        print(f"Error executing stored procedure: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

class BaseIngestionWorkflow(BaseWorkflow):
    """Base class for file ingestion workflows."""
    
    def __init__(
        self,
        name: str,
        target_table: str,
        field_mappings: Dict[str, str],
        field_transformations: Optional[Dict[str, str]] = None,
        procedure_name: Optional[str] = None,
        procedure_params: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name)
        self.target_table = target_table
        self.field_mappings = field_mappings
        self.field_transformations = field_transformations
        self.procedure_name = procedure_name
        self.procedure_params = procedure_params
        
    @flow(name="File Ingestion Workflow")
    def execute(
        self,
        file_path: str,
        db_host: str = "bor-db",
        db_port: int = 3306,
        db_user: str = "borAllSvc",
        db_password: str = None,
        db_name: str = "borarch",
        delimiter: str = ',',
        quote_char: str = '"',
        line_terminator: str = '\n',
        skip_lines: int = 1
    ) -> bool:
        """
        Main workflow for file ingestion process.
        
        Args:
            file_path: Path to the input file in the shared volume
            db_host: Database host
            db_port: Database port
            db_user: Database user
            db_password: Database password
            db_name: Database name
            delimiter: Field delimiter character
            quote_char: Quote character
            line_terminator: Line terminator character
            skip_lines: Number of header lines to skip
        
        Returns:
            bool: True if workflow completed successfully, False otherwise
        """
        try:
            # Log workflow start
            self.log_workflow_start({
                "file_path": file_path,
                "target_table": self.target_table,
                "db_host": db_host,
                "db_port": db_port,
                "db_user": db_user,
                "db_name": db_name
            })
            
            # Configure database connection
            db_config = {
                "host": db_host,
                "port": db_port,
                "user": db_user,
                "password": db_password,
                "database": db_name
            }
            
            # Check if file exists
            if not check_file_exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Load data to staging
            if not load_data_to_staging(
                file_path=file_path,
                db_config=db_config,
                target_table=self.target_table,
                field_mappings=self.field_mappings,
                field_transformations=self.field_transformations,
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=skip_lines
            ):
                raise Exception("Failed to load data to staging")
            
            # Execute stored procedure if specified
            if self.procedure_name:
                if not execute_stored_procedure(
                    db_config=db_config,
                    procedure_name=self.procedure_name,
                    procedure_params=self.procedure_params
                ):
                    raise Exception("Failed to execute stored procedure")
            
            # Log successful completion
            self.log_workflow_end(True)
            return True
            
        except Exception as e:
            self.handle_workflow_error(e)
            return False 