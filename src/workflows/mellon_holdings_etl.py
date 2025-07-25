"""
Mellon Holdings ETL Workflow

This workflow processes Mellon holdings CSV files and loads them into the bor system databases.
Based on specifications in ref/mellon-prefect-flow-specs.txt
"""

import os
import shutil
from pathlib import Path
from prefect import flow, task
from src.utils.base_ingestion import BaseIngestionWorkflow

class MellonHoldingsETLWorkflow(BaseIngestionWorkflow):
    def __init__(self):
        super().__init__(
            name="Mellon Holdings ETL",
            target_table="borarch.MellonHoldingsStaging",
            field_mappings={
                "AccountNumber": "AccountNumber",
                "AccountName": "AccountName", 
                "AccountType": "AccountType",
                "SourceAccountNumber": "SourceAccountNumber",
                "SourceAccountName": "SourceAccountName",
                "AsOfDate": "AsOfDate",
                "MellonSecurityId": "MellonSecurityId",
                "CountryCode": "CountryCode",
                "Country": "Country",
                "Segment": "Segment",
                "Category": "Category",
                "Sector": "Sector",
                "Industry": "Industry",
                "SecurityDescription1": "SecurityDescription1",
                "SecurityDescription2": "SecurityDescription2",
                "AcctBaseCurrencyCode": "AcctBaseCurrencyCode",
                "ExchangeRate": "ExchangeRate",
                "IssueCurrencyCode": "IssueCurrencyCode",
                "SharesPar": "SharesPar",
                "BaseCost": "BaseCost",
                "LocalCost": "LocalCost",
                "BasePrice": "BasePrice",
                "LocalPrice": "LocalPrice",
                "BaseMarketValue": "BaseMarketValue",
                "LocalMarketValue": "LocalMarketValue",
                "BaseNetIncomeReceivable": "BaseNetIncomeReceivable",
                "LocalNetIncomeReceivable": "LocalNetIncomeReceivable",
                "BaseMarketValueWithAccrual": "BaseMarketValueWithAccrual",
                "CouponRate": "CouponRate",
                "MaturityDate": "MaturityDate",
                "BaseUnrealizedGainLoss": "BaseUnrealizedGainLoss",
                "LocalUnrealizedGainLoss": "LocalUnrealizedGainLoss",
                "BaseUnrealizedCurrencyGainLoss": "BaseUnrealizedCurrencyGainLoss",
                "BaseNetUnrealizedGainLoss": "BaseNetUnrealizedGainLoss",
                "PercentOfTotal": "PercentOfTotal",
                "ISIN": "ISIN",
                "SEDOL": "SEDOL",
                "CUSIP": "CUSIP",
                "Ticker": "Ticker",
                "CMSAccountNumber": "CMSAccountNumber",
                "IncomeCurrency": "IncomeCurrency",
                "SecurityIdentifier": "SecurityIdentifier",
                "UnderlyingSecurity": "UnderlyingSecurity",
                "FairValuePriceLevel": "FairValuePriceLevel",
                "ReportRunDateTime": "ReportRunDateTime"
            },
            field_transformations={
                "AsOfDate": "STR_TO_DATE(TRIM(@AsOfDate), '%m/%d/%Y')",
                "MaturityDate": "CASE WHEN TRIM(@MaturityDate) = '' OR TRIM(@MaturityDate) = ' ' THEN NULL ELSE STR_TO_DATE(TRIM(@MaturityDate), '%m/%d/%Y') END",
                "ReportRunDateTime": "CASE WHEN TRIM(@ReportRunDateTime) = '' OR TRIM(@ReportRunDateTime) = ' ' THEN NULL ELSE STR_TO_DATE(TRIM(@ReportRunDateTime), '%m/%d/%Y %h:%i:%s %p') END",
                "SharesPar": "NULLIF(REPLACE(TRIM(@SharesPar), '\"', ''), '')",
                "BaseCost": "NULLIF(REPLACE(TRIM(@BaseCost), '\"', ''), '')",
                "LocalCost": "NULLIF(REPLACE(TRIM(@LocalCost), '\"', ''), '')",
                "BaseMarketValue": "NULLIF(REPLACE(TRIM(@BaseMarketValue), '\"', ''), '')",
                "LocalMarketValue": "NULLIF(REPLACE(TRIM(@LocalMarketValue), '\"', ''), '')",
                "BaseNetIncomeReceivable": "NULLIF(REPLACE(TRIM(@BaseNetIncomeReceivable), '\"', ''), '')",
                "LocalNetIncomeReceivable": "NULLIF(REPLACE(TRIM(@LocalNetIncomeReceivable), '\"', ''), '')",
                "BaseMarketValueWithAccrual": "NULLIF(REPLACE(TRIM(@BaseMarketValueWithAccrual), '\"', ''), '')",
                "BaseUnrealizedGainLoss": "NULLIF(REPLACE(TRIM(@BaseUnrealizedGainLoss), '\"', ''), '')",
                "LocalUnrealizedGainLoss": "NULLIF(REPLACE(TRIM(@LocalUnrealizedGainLoss), '\"', ''), '')",
                "BaseUnrealizedCurrencyGainLoss": "NULLIF(REPLACE(TRIM(@BaseUnrealizedCurrencyGainLoss), '\"', ''), '')",
                "BaseNetUnrealizedGainLoss": "NULLIF(REPLACE(TRIM(@BaseNetUnrealizedGainLoss), '\"', ''), '')",
                "PercentOfTotal": "NULLIF(REPLACE(REPLACE(TRIM(@PercentOfTotal), ' ', ''), '(', ''), '')"
            },
            procedure_name=None,  # No stored procedure for data loading
            procedure_params=None,
            truncate_before_load=False
        )

@task(name="update-file-tracking")
def update_file_tracking(file_source: str, db_config: dict) -> bool:
    """
    Update the MellonFileImport tracking table
    """
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Insert or update file import record
        cursor.execute("""
            INSERT INTO borarch.MellonFileImport (FileName, Status) 
            VALUES (%s, 'IMPORTED')
            ON DUPLICATE KEY UPDATE 
                ImportDate = CURRENT_TIMESTAMP,
                Status = 'IMPORTED',
                ErrorMessage = NULL
        """, (file_source,))
        
        conn.commit()
        print(f"Updated file tracking for {file_source}")
        return True
        
    except Exception as e:
        print(f"Error updating file tracking: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@flow(name="mellon-holdings-etl")
def mellon_holdings_etl_flow(
    source_files: list,
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_name: str = "borarch",
    target_dir: str = "/var/lib/mysql-files/ftpetl/incoming/",
    delimiter: str = ',',
    quote_char: str = '"',
    line_terminator: str = '\n',
    skip_lines: int = 1,
    truncate_before_load: bool = False
) -> bool:
    """
    Main ETL flow for processing Mellon holdings CSV files
    
    Args:
        source_files: List of source file paths (already in container)
        db_host: Database host
        db_port: Database port (as string)
        db_user: Database user
        db_password: Database password
        db_name: Database name
        target_dir: Target directory in container
        delimiter: Field delimiter character
        quote_char: Quote character
        line_terminator: Line terminator character
        skip_lines: Number of header lines to skip
        truncate_before_load: Boolean indicating whether to truncate the table before loading
    
    Returns:
        bool: True if workflow completed successfully
    """
    try:
        print("Starting Mellon Holdings ETL Flow")
        
        # Process each file
        print("Processing files...")
        wf = MellonHoldingsETLWorkflow()
        
        for source_file in source_files:
            file_name = Path(source_file).name
            target_file = f"{target_dir}{file_name}"
            
            print(f"Processing {file_name}...")
            
            # Configure database connection
            db_config = {
                "host": db_host,
                "port": int(db_port),
                "user": db_user,
                "password": db_password,
                "database": db_name
            }
            
            # Step 1: Update file tracking
            print(f"Step 1: Updating file tracking for {file_name}...")
            if not update_file_tracking(file_name, db_config):
                raise Exception(f"Failed to update file tracking for {file_name}")
            
            # Step 2: Load data using BaseIngestionWorkflow (like import_web_hold.py)
            print(f"Step 2: Loading data for {file_name}...")
            success = wf.execute(
                file_path=target_file,
                db_host=db_host,
                db_port=db_port,
                db_user=db_user,
                db_password=db_password,
                db_name=db_name,
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=skip_lines,
                truncate_before_load=truncate_before_load
            )
            
            if not success:
                raise Exception(f"Failed to load data for {file_name}")
            
            print(f"Successfully processed {file_name}")
        
        print("Mellon Holdings ETL completed successfully")
        return True
        
    except Exception as e:
        print(f"Mellon Holdings ETL failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Example usage
    source_files = [
        "/var/lib/mysql-files/ftpetl/incoming/mellon-660600017-AAD-20250414.csv"
    ]
    
    mellon_holdings_etl_flow(
        source_files=source_files,
        db_host="bor-db",
        db_port="3306",
        db_user="borAllAdmin",
        db_password="kBu9pjz2vi",
        db_name="borarch"
    ) 