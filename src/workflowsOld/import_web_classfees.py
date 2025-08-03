import os
from pathlib import Path
from typing import Optional, Dict, Any

import mysql.connector
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from src.utils.base_ingestion import BaseIngestionWorkflow

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_file_exists(file_path: str, db_config: dict = None) -> bool:
    """Check if file exists in the shared volume."""
    if file_path.startswith("/var/lib/mysql-files/"):
        # Optionally, check via MySQL if needed
        return True  # Assume file is present for MySQL server
    else:
        return Path(file_path).exists()

@task(retries=3, retry_delay_seconds=60)
def load_data_to_staging(file_path: str, db_config: dict) -> bool:
    """Load data from file into staging table using LOAD DATA INFILE."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute LOAD DATA INFILE command
        load_query = f"""
        TRUNCATE TABLE borarch.FundClassFee;
        
        LOAD DATA INFILE '{file_path}'
        INTO TABLE borarch.FundClassFee
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (FundCode, FundName, Class, Description, Mer, @Trailer, @PerformanceFee, @MinInvestmentInitial, @MinInvestmentSubsequent, Currency)
        SET
            Trailer = NULLIF(@Trailer, ''),
            PerformanceFee = NULLIF(@PerformanceFee, ''),
            MinInvestmentInitial = NULLIF(@MinInvestmentInitial, ''),
            MinInvestmentSubsequent = NULLIF(@MinInvestmentSubsequent, '');
        """
        
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
def execute_stored_procedure(db_config: dict) -> bool:
    """Execute the stored procedure for data processing."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute stored procedure
        cursor.callproc('bormeta.usp_FundClassFee_Load')
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

class ImportWebClassFeesWorkflow(BaseIngestionWorkflow):
    """Import Web ClassFees file ingestion workflow implementation."""
    
    def __init__(self):
        super().__init__(
            name="Import Web ClassFees",
            target_table="borarch.FundClassFee",
            field_mappings={
                "FundCode": "FundCode",
                "FundName": "FundName",
                "Class": "Class",
                "Description": "Description",
                "Mer": "Mer",
                "Trailer": "Trailer",
                "PerformanceFee": "PerformanceFee",
                "MinInvestmentInitial": "MinInvestmentInitial",
                "MinInvestmentSubsequent": "MinInvestmentSubsequent",
                "Currency": "Currency"
            },
            field_transformations={
                "Trailer": "NULLIF(@Trailer, '')",
                "PerformanceFee": "NULLIF(@PerformanceFee, '')",
                "MinInvestmentInitial": "NULLIF(@MinInvestmentInitial, '')",
                "MinInvestmentSubsequent": "NULLIF(@MinInvestmentSubsequent, '')"
            },
            procedure_name="bormeta.usp_FundClassFee_Load",
            truncate_before_load=True
        )

@flow
def import_web_classfees_flow(
    file_path: str,
    db_host: str,
    db_port: str,  # Accept as string for env var compatibility
    db_user: str,
    db_password: str,
    db_name: str,
    delimiter: str = ',',
    quote_char: str = '"',
    line_terminator: str = '\n',
    skip_lines: int = 1,
    truncate_before_load: bool = True,  # <-- Add this line, default matches YAML
) -> bool:
    """
    Top-level Prefect flow for Import Web ClassFees.
    """
    wf = ImportWebClassFeesWorkflow()
    return wf.execute(
        file_path=file_path,
        db_host=db_host,
        db_port=db_port,  # Will be cast to int in workflow
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        delimiter=delimiter,
        quote_char=quote_char,
        line_terminator=line_terminator,
        skip_lines=skip_lines,
        truncate_before_load=truncate_before_load,  # <-- Pass it through
    )

# Create workflow instance
# Create workflow instance