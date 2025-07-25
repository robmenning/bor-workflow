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
                            "Account Number": "AccountNumber",
                            "Account Name": "AccountName", 
                            "Account Type": "AccountType",
                            "Source Account Number": "SourceAccountNumber",
                            "Source Account Name": "SourceAccountName",
                            "As-Of Date": "AsOfDate",
                            "Mellon Security ID": "MellonSecurityId",
                            "Country Code": "CountryCode",
                            "Country": "Country",
                            "Segment": "Segment",
                            "Category": "Category",
                            "Sector": "Sector",
                            "Industry": "Industry",
                            "Security Description 1": "SecurityDescription1",
                            "Security Description 2": "SecurityDescription2",
                            "@dummy": "@dummy",  # Skip empty column 15
                            "Acct Base Currency Code": "AcctBaseCurrencyCode",
                            "Exchange Rate": "ExchangeRate",
                            "Issue Currency Code": "IssueCurrencyCode",
                            "Shares/Par": "SharesPar",
                            "Base Cost": "BaseCost",
                            "Local Cost": "LocalCost",
                            "Base Price": "BasePrice",
                            "Local Price": "LocalPrice",
                            "Base Market Value": "BaseMarketValue",
                            "Local Market Value": "LocalMarketValue",
                            "Base Net Income Receivable": "BaseNetIncomeReceivable",
                            "Local Net Income Receivable": "LocalNetIncomeReceivable",
                            "Base Market Value with Accrual": "BaseMarketValueWithAccrual",
                            "Coupon Rate": "CouponRate",
                            "Maturity Date": "MaturityDate",
                            "Base Unrealized Gain/Loss": "BaseUnrealizedGainLoss",
                            "Local Unrealized Gain/Loss": "LocalUnrealizedGainLoss",
                            "Base Unrealized Currency Gain/Loss": "BaseUnrealizedCurrencyGainLoss",
                            "Base Net Unrealized Gain/Loss": "BaseNetUnrealizedGainLoss",
                            "Percent of Total": "PercentOfTotal",
                            "ISIN": "ISIN",
                            "SEDOL": "SEDOL",
                            "CUSIP": "CUSIP",
                            "Ticker": "Ticker",
                            "CMS Account Number": "CMSAccountNumber",
                            "Income Currency": "IncomeCurrency",
                            "Security Identifier": "SecurityIdentifier",
                            "Underlying Security": "UnderlyingSecurity",
                            "Fair Value Price Level": "FairValuePriceLevel",
                            "Report Run Date and Time (EDT)": "ReportRunDateTime"
                        },
            field_transformations={
                # Date columns - handle empty/invalid dates
                "As-Of Date": "STR_TO_DATE(@`As-Of Date`, '%m/%d/%Y')",
                "Maturity Date": "CASE WHEN TRIM(@`Maturity Date`) = '' OR TRIM(@`Maturity Date`) = ' ' OR TRIM(@`Maturity Date`) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@`Maturity Date`), '%m/%d/%Y') END",
                "Report Run Date and Time (EDT)": "CASE WHEN TRIM(@`Report Run Date and Time (EDT)`) = '' OR TRIM(@`Report Run Date and Time (EDT)`) = ' ' OR TRIM(@`Report Run Date and Time (EDT)`) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@`Report Run Date and Time (EDT)`), '%m/%d/%Y %h:%i:%s %p') END",
                
                # Decimal columns - remove quotes, commas, handle empty/dash values
                "ExchangeRate": "CASE WHEN TRIM(@ExchangeRate) = '' OR TRIM(@ExchangeRate) = ' ' OR TRIM(@ExchangeRate) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@ExchangeRate), '\"', ''), ',', ''), '') END",
                "SharesPar": "CASE WHEN TRIM(@SharesPar) = '' OR TRIM(@SharesPar) = ' ' OR TRIM(@SharesPar) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@SharesPar), '\"', ''), ',', ''), '') END",
                "BaseCost": "CASE WHEN TRIM(@BaseCost) = '' OR TRIM(@BaseCost) = ' ' OR TRIM(@BaseCost) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseCost), '\"', ''), ',', ''), '') END",
                "LocalCost": "CASE WHEN TRIM(@LocalCost) = '' OR TRIM(@LocalCost) = ' ' OR TRIM(@LocalCost) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalCost), '\"', ''), ',', ''), '') END",
                "BasePrice": "CASE WHEN TRIM(@BasePrice) = '' OR TRIM(@BasePrice) = ' ' OR TRIM(@BasePrice) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BasePrice), '\"', ''), ',', ''), '') END",
                "LocalPrice": "CASE WHEN TRIM(@LocalPrice) = '' OR TRIM(@LocalPrice) = ' ' OR TRIM(@LocalPrice) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalPrice), '\"', ''), ',', ''), '') END",
                "BaseMarketValue": "CASE WHEN TRIM(@BaseMarketValue) = '' OR TRIM(@BaseMarketValue) = ' ' OR TRIM(@BaseMarketValue) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseMarketValue), '\"', ''), ',', ''), '') END",
                "LocalMarketValue": "CASE WHEN TRIM(@LocalMarketValue) = '' OR TRIM(@LocalMarketValue) = ' ' OR TRIM(@LocalMarketValue) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalMarketValue), '\"', ''), ',', ''), '') END",
                "BaseNetIncomeReceivable": "CASE WHEN TRIM(@BaseNetIncomeReceivable) = '' OR TRIM(@BaseNetIncomeReceivable) = ' ' OR TRIM(@BaseNetIncomeReceivable) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseNetIncomeReceivable), '\"', ''), ',', ''), '') END",
                "LocalNetIncomeReceivable": "CASE WHEN TRIM(@LocalNetIncomeReceivable) = '' OR TRIM(@LocalNetIncomeReceivable) = ' ' OR TRIM(@LocalNetIncomeReceivable) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalNetIncomeReceivable), '\"', ''), ',', ''), '') END",
                "BaseMarketValueWithAccrual": "CASE WHEN TRIM(@BaseMarketValueWithAccrual) = '' OR TRIM(@BaseMarketValueWithAccrual) = ' ' OR TRIM(@BaseMarketValueWithAccrual) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseMarketValueWithAccrual), '\"', ''), ',', ''), '') END",
                "CouponRate": "CASE WHEN TRIM(@CouponRate) = '' OR TRIM(@CouponRate) = ' ' OR TRIM(@CouponRate) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@CouponRate), '\"', ''), ',', ''), '') END",
                "BaseUnrealizedGainLoss": "CASE WHEN TRIM(@BaseUnrealizedGainLoss) = '' OR TRIM(@BaseUnrealizedGainLoss) = ' ' OR TRIM(@BaseUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                "LocalUnrealizedGainLoss": "CASE WHEN TRIM(@LocalUnrealizedGainLoss) = '' OR TRIM(@LocalUnrealizedGainLoss) = ' ' OR TRIM(@LocalUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                "BaseUnrealizedCurrencyGainLoss": "CASE WHEN TRIM(@BaseUnrealizedCurrencyGainLoss) = '' OR TRIM(@BaseUnrealizedCurrencyGainLoss) = ' ' OR TRIM(@BaseUnrealizedCurrencyGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseUnrealizedCurrencyGainLoss), '\"', ''), ',', ''), '') END",
                "BaseNetUnrealizedGainLoss": "CASE WHEN TRIM(@BaseNetUnrealizedGainLoss) = '' OR TRIM(@BaseNetUnrealizedGainLoss) = ' ' OR TRIM(@BaseNetUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseNetUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                "PercentOfTotal": "CASE WHEN TRIM(@PercentOfTotal) = '' OR TRIM(@PercentOfTotal) = ' ' OR TRIM(@PercentOfTotal) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(REPLACE(TRIM(@PercentOfTotal), ' ', ''), '(', ''), ')', ''), '') END",
                
                # Integer column - handle empty/dash values
                "FairValuePriceLevel": "CASE WHEN TRIM(@FairValuePriceLevel) = '' OR TRIM(@FairValuePriceLevel) = ' ' OR TRIM(@FairValuePriceLevel) = '-' THEN NULL ELSE CAST(TRIM(@FairValuePriceLevel) AS SIGNED) END",
                
                # String columns - trim whitespace, handle empty as NULL for optional fields
                "SourceAccountNumber": "CASE WHEN TRIM(@SourceAccountNumber) = '' OR TRIM(@SourceAccountNumber) = ' ' OR TRIM(@SourceAccountNumber) = '-' THEN NULL ELSE TRIM(@SourceAccountNumber) END",
                "SourceAccountName": "CASE WHEN TRIM(@SourceAccountName) = '' OR TRIM(@SourceAccountName) = ' ' OR TRIM(@SourceAccountName) = '-' THEN NULL ELSE TRIM(@SourceAccountName) END",
                "CountryCode": "CASE WHEN TRIM(@CountryCode) = '' OR TRIM(@CountryCode) = ' ' OR TRIM(@CountryCode) = '-' THEN NULL ELSE TRIM(@CountryCode) END",
                "Country": "CASE WHEN TRIM(@Country) = '' OR TRIM(@Country) = ' ' OR TRIM(@Country) = '-' THEN NULL ELSE TRIM(@Country) END",
                "Segment": "CASE WHEN TRIM(@Segment) = '' OR TRIM(@Segment) = ' ' OR TRIM(@Segment) = '-' THEN NULL ELSE TRIM(@Segment) END",
                "Category": "CASE WHEN TRIM(@Category) = '' OR TRIM(@Category) = ' ' OR TRIM(@Category) = '-' THEN NULL ELSE TRIM(@Category) END",
                "Sector": "CASE WHEN TRIM(@Sector) = '' OR TRIM(@Sector) = ' ' OR TRIM(@Sector) = '-' THEN NULL ELSE TRIM(@Sector) END",
                "Industry": "CASE WHEN TRIM(@Industry) = '' OR TRIM(@Industry) = ' ' OR TRIM(@Industry) = '-' THEN NULL ELSE TRIM(@Industry) END",
                "SecurityDescription1": "CASE WHEN TRIM(@SecurityDescription1) = '' OR TRIM(@SecurityDescription1) = ' ' OR TRIM(@SecurityDescription1) = '-' THEN NULL ELSE TRIM(@SecurityDescription1) END",
                "SecurityDescription2": "CASE WHEN TRIM(@SecurityDescription2) = '' OR TRIM(@SecurityDescription2) = ' ' OR TRIM(@SecurityDescription2) = '-' THEN NULL ELSE TRIM(@SecurityDescription2) END",
                "IssueCurrencyCode": "CASE WHEN TRIM(@IssueCurrencyCode) = '' OR TRIM(@IssueCurrencyCode) = ' ' OR TRIM(@IssueCurrencyCode) = '-' THEN NULL ELSE TRIM(@IssueCurrencyCode) END",
                "ISIN": "CASE WHEN TRIM(@ISIN) = '' OR TRIM(@ISIN) = ' ' OR TRIM(@ISIN) = '-' THEN NULL ELSE TRIM(@ISIN) END",
                "SEDOL": "CASE WHEN TRIM(@SEDOL) = '' OR TRIM(@SEDOL) = ' ' OR TRIM(@SEDOL) = '-' THEN NULL ELSE TRIM(@SEDOL) END",
                "CUSIP": "CASE WHEN TRIM(@CUSIP) = '' OR TRIM(@CUSIP) = ' ' OR TRIM(@CUSIP) = '-' THEN NULL ELSE TRIM(@CUSIP) END",
                "Ticker": "CASE WHEN TRIM(@Ticker) = '' OR TRIM(@Ticker) = ' ' OR TRIM(@Ticker) = '-' THEN NULL ELSE TRIM(@Ticker) END",
                "CMSAccountNumber": "CASE WHEN TRIM(@CMSAccountNumber) = '' OR TRIM(@CMSAccountNumber) = ' ' OR TRIM(@CMSAccountNumber) = '-' THEN NULL ELSE TRIM(@CMSAccountNumber) END",
                "IncomeCurrency": "CASE WHEN TRIM(@IncomeCurrency) = '' OR TRIM(@IncomeCurrency) = ' ' OR TRIM(@IncomeCurrency) = '-' THEN NULL ELSE TRIM(@IncomeCurrency) END",
                "SecurityIdentifier": "CASE WHEN TRIM(@SecurityIdentifier) = '' OR TRIM(@SecurityIdentifier) = ' ' OR TRIM(@SecurityIdentifier) = '-' THEN NULL ELSE TRIM(@SecurityIdentifier) END",
                "UnderlyingSecurity": "CASE WHEN TRIM(@UnderlyingSecurity) = '' OR TRIM(@UnderlyingSecurity) = ' ' OR TRIM(@UnderlyingSecurity) = '-' THEN NULL ELSE TRIM(@UnderlyingSecurity) END",
                "@file_source": "mellon-660600017-AAD-20250414.csv"
            },
            procedure_name=None,  # No stored procedure for data loading
            procedure_params=None,
            truncate_before_load=False
        )

@task(name="delete-existing-account-data")
def delete_existing_account_data(file_path: str, db_config: dict) -> bool:
    """
    Delete existing records for accounts found in the incoming file
    """
    try:
        import csv
        
        # Read the first few rows to get account numbers
        account_numbers = set()
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'Account Number' in row and row['Account Number'].strip():
                    account_numbers.add(row['Account Number'].strip())
                # Limit to first 10 rows to get a sample of accounts
                if len(account_numbers) >= 10:
                    break
        
        if not account_numbers:
            print("No account numbers found in file")
            return True
        
        # Delete existing records for these accounts
        import mysql.connector
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Build the DELETE query with placeholders
        placeholders = ', '.join(['%s'] * len(account_numbers))
        delete_sql = f"""
            DELETE FROM borarch.MellonHoldingsStaging 
            WHERE AccountNumber IN ({placeholders})
        """
        
        cursor.execute(delete_sql, list(account_numbers))
        deleted_count = cursor.rowcount
        
        conn.commit()
        print(f"Deleted {deleted_count} existing records for accounts: {', '.join(account_numbers)}")
        return True
        
    except Exception as e:
        print(f"Error deleting existing account data: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

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
            
            # Step 2: Delete existing data for accounts in this file
            print(f"Step 2: Deleting existing data for accounts in {file_name}...")
            if not delete_existing_account_data(target_file, db_config):
                raise Exception(f"Failed to delete existing account data for {file_name}")
            
            # Step 3: Create workflow instance with dynamic FileSource
            print(f"Step 3: Loading data for {file_name}...")
            
            # Create a custom workflow class for this specific file
            class DynamicMellonWorkflow(BaseIngestionWorkflow):
                def __init__(self, file_source: str):
                    super().__init__(
                        name="Mellon Holdings ETL",
                        target_table="borarch.MellonHoldingsStaging",
                        field_mappings={
                            "Account Number": "AccountNumber",
                            "Account Name": "AccountName", 
                            "Account Type": "AccountType",
                            "Source Account Number": "SourceAccountNumber",
                            "Source Account Name": "SourceAccountName",
                            "As-Of Date": "@AsOfDate",
                            "Mellon Security ID": "MellonSecurityId",
                            "Country Code": "CountryCode",
                            "Country": "Country",
                            "Segment": "Segment",
                            "Category": "Category",
                            "Sector": "Sector",
                            "Industry": "Industry",
                            "Security Description 1": "SecurityDescription1",
                            "Security Description 2": "SecurityDescription2",
                            "@dummy": "@dummy",  # Skip empty column 15
                            "Acct Base Currency Code": "AcctBaseCurrencyCode",
                            "Exchange Rate": "@ExchangeRate",
                            "Issue Currency Code": "IssueCurrencyCode",
                            "Shares/Par": "@SharesPar",
                            "Base Cost": "@BaseCost",
                            "Local Cost": "@LocalCost",
                            "Base Price": "@BasePrice",
                            "Local Price": "@LocalPrice",
                            "Base Market Value": "@BaseMarketValue",
                            "Local Market Value": "@LocalMarketValue",
                            "Base Net Income Receivable": "@BaseNetIncomeReceivable",
                            "Local Net Income Receivable": "@LocalNetIncomeReceivable",
                            "Base Market Value with Accrual": "@BaseMarketValueWithAccrual",
                            "Coupon Rate": "@CouponRate",
                            "Maturity Date": "@MaturityDate",
                            "Base Unrealized Gain/Loss": "@BaseUnrealizedGainLoss",
                            "Local Unrealized Gain/Loss": "@LocalUnrealizedGainLoss",
                            "Base Unrealized Currency Gain/Loss": "@BaseUnrealizedCurrencyGainLoss",
                            "Base Net Unrealized Gain/Loss": "@BaseNetUnrealizedGainLoss",
                            "Percent of Total": "@PercentOfTotal",
                            "ISIN": "ISIN",
                            "SEDOL": "SEDOL",
                            "CUSIP": "CUSIP",
                            "Ticker": "Ticker",
                            "CMS Account Number": "CMSAccountNumber",
                            "Income Currency": "IncomeCurrency",
                            "Security Identifier": "SecurityIdentifier",
                            "Underlying Security": "UnderlyingSecurity",
                            "Fair Value Price Level": "@FairValuePriceLevel",
                            "Report Run Date and Time (EDT)": "@ReportRunDateTime",
                            "FileSource": "FileSource"
                        },
                        field_transformations={
                            # Date transformations - convert MM/DD/YYYY to MySQL date format
                            "AsOfDate": "STR_TO_DATE(@AsOfDate, '%m/%d/%Y')",
                            "MaturityDate": "CASE WHEN TRIM(@MaturityDate) = '' OR TRIM(@MaturityDate) = ' ' OR TRIM(@MaturityDate) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@MaturityDate), '%m/%d/%Y') END",
                            "ReportRunDateTime": "CASE WHEN TRIM(@ReportRunDateTime) = '' OR TRIM(@ReportRunDateTime) = ' ' OR TRIM(@ReportRunDateTime) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@ReportRunDateTime), '%m/%d/%Y %h:%i:%s %p') END",
                            
                            # Decimal transformations - remove quotes, commas, handle special values
                            "ExchangeRate": "CASE WHEN TRIM(@ExchangeRate) = '' OR TRIM(@ExchangeRate) = ' ' OR TRIM(@ExchangeRate) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@ExchangeRate), '\"', ''), ',', ''), '') END",
                            "SharesPar": "CASE WHEN TRIM(@SharesPar) = '' OR TRIM(@SharesPar) = ' ' OR TRIM(@SharesPar) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@SharesPar), '\"', ''), ',', ''), '') END",
                            "BaseCost": "CASE WHEN TRIM(@BaseCost) = '' OR TRIM(@BaseCost) = ' ' OR TRIM(@BaseCost) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseCost), '\"', ''), ',', ''), '') END",
                            "LocalCost": "CASE WHEN TRIM(@LocalCost) = '' OR TRIM(@LocalCost) = ' ' OR TRIM(@LocalCost) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalCost), '\"', ''), ',', ''), '') END",
                            "BasePrice": "CASE WHEN TRIM(@BasePrice) = '' OR TRIM(@BasePrice) = ' ' OR TRIM(@BasePrice) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BasePrice), '\"', ''), ',', ''), '') END",
                            "LocalPrice": "CASE WHEN TRIM(@LocalPrice) = '' OR TRIM(@LocalPrice) = ' ' OR TRIM(@LocalPrice) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalPrice), '\"', ''), ',', ''), '') END",
                            "BaseMarketValue": "CASE WHEN TRIM(@BaseMarketValue) = '' OR TRIM(@BaseMarketValue) = ' ' OR TRIM(@BaseMarketValue) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseMarketValue), '\"', ''), ',', ''), '') END",
                            "LocalMarketValue": "CASE WHEN TRIM(@LocalMarketValue) = '' OR TRIM(@LocalMarketValue) = ' ' OR TRIM(@LocalMarketValue) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalMarketValue), '\"', ''), ',', ''), '') END",
                            "BaseNetIncomeReceivable": "CASE WHEN TRIM(@BaseNetIncomeReceivable) = '' OR TRIM(@BaseNetIncomeReceivable) = ' ' OR TRIM(@BaseNetIncomeReceivable) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseNetIncomeReceivable), '\"', ''), ',', ''), '') END",
                            "LocalNetIncomeReceivable": "CASE WHEN TRIM(@LocalNetIncomeReceivable) = '' OR TRIM(@LocalNetIncomeReceivable) = ' ' OR TRIM(@LocalNetIncomeReceivable) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalNetIncomeReceivable), '\"', ''), ',', ''), '') END",
                            "BaseMarketValueWithAccrual": "CASE WHEN TRIM(@BaseMarketValueWithAccrual) = '' OR TRIM(@BaseMarketValueWithAccrual) = ' ' OR TRIM(@BaseMarketValueWithAccrual) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseMarketValueWithAccrual), '\"', ''), ',', ''), '') END",
                            "CouponRate": "CASE WHEN TRIM(@CouponRate) = '' OR TRIM(@CouponRate) = ' ' OR TRIM(@CouponRate) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@CouponRate), '\"', ''), ',', ''), '') END",
                            "BaseUnrealizedGainLoss": "CASE WHEN TRIM(@BaseUnrealizedGainLoss) = '' OR TRIM(@BaseUnrealizedGainLoss) = ' ' OR TRIM(@BaseUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                            "LocalUnrealizedGainLoss": "CASE WHEN TRIM(@LocalUnrealizedGainLoss) = '' OR TRIM(@LocalUnrealizedGainLoss) = ' ' OR TRIM(@LocalUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@LocalUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                            "BaseUnrealizedCurrencyGainLoss": "CASE WHEN TRIM(@BaseUnrealizedCurrencyGainLoss) = '' OR TRIM(@BaseUnrealizedCurrencyGainLoss) = ' ' OR TRIM(@BaseUnrealizedCurrencyGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseUnrealizedCurrencyGainLoss), '\"', ''), ',', ''), '') END",
                            "BaseNetUnrealizedGainLoss": "CASE WHEN TRIM(@BaseNetUnrealizedGainLoss) = '' OR TRIM(@BaseNetUnrealizedGainLoss) = ' ' OR TRIM(@BaseNetUnrealizedGainLoss) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@BaseNetUnrealizedGainLoss), '\"', ''), ',', ''), '') END",
                            "PercentOfTotal": "CASE WHEN TRIM(@PercentOfTotal) = '' OR TRIM(@PercentOfTotal) = ' ' OR TRIM(@PercentOfTotal) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(REPLACE(TRIM(@PercentOfTotal), ' ', ''), '(', ''), ')', ''), '') END",
                            
                            # Integer transformation - handle empty/dash values
                            "FairValuePriceLevel": "CASE WHEN TRIM(@FairValuePriceLevel) = '' OR TRIM(@FairValuePriceLevel) = ' ' OR TRIM(@FairValuePriceLevel) = '-' THEN NULL ELSE CAST(TRIM(@FairValuePriceLevel) AS SIGNED) END",
                            
                            # Set FileSource dynamically
                            "FileSource": f"'{file_source}'"
                        },
                        procedure_name=None, # No stored procedure for data loading
                        procedure_params=None,
                        truncate_before_load=False
                    )
            
            # Create workflow instance for this file
            wf = DynamicMellonWorkflow(file_name)
            
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