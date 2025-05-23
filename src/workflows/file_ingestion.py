import os
from pathlib import Path
from typing import Optional

import mysql.connector
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_file_exists(file_path: str, db_config: dict = None) -> bool:
    if file_path.startswith("/var/lib/mysql-files/"):
        # Optionally, check via MySQL if needed
        return True  # Assume file is present for MySQL server
    else:
        return Path(file_path).exists()

@task
def load_data_to_staging(file_path: str, db_config: dict) -> bool:
    """Load data from file into staging table using LOAD DATA INFILE."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute LOAD DATA INFILE command
        load_query = f"""
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

@task
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

@flow(name="File Ingestion Workflow")
def file_ingestion_workflow(
    file_path: str,
    db_host: str = "bor-db",
    db_port: int = 3306,
    db_user: str = "borAllSvc",
    db_password: str = None,
    db_name: str = "borarch"
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
    
    Returns:
        bool: True if workflow completed successfully, False otherwise
    """
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
        print(f"File not found: {file_path}")
        return False
    
    # Load data to staging
    if not load_data_to_staging(file_path, db_config):
        print("Failed to load data to staging")
        return False
    
    # Execute stored procedure
    if not execute_stored_procedure(db_config):
        print("Failed to execute stored procedure")
        return False
    
    return True 