"""
Mellon Holdings ETL Workflow

This workflow processes Mellon holdings CSV files and loads them into the bor system databases.
Based on specifications in ref/mellon-prefect-flow-specs.txt
"""

import os
import shutil
from pathlib import Path
from prefect import flow, task
from src.utils.base_ingestion import BaseIngestionWorkflow, load_data_to_staging

class MellonHoldingsETLWorkflow(BaseIngestionWorkflow):
    def __init__(self):
        super().__init__(
            name="Mellon Holdings ETL",
                                    target_table="borarch.MellonHoldings",
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
                "As-Of Date": "STR_TO_DATE(@`As-Of Date`, '%c/%e/%Y')",
                "Maturity Date": "CASE WHEN TRIM(@`Maturity Date`) = '' OR TRIM(@`Maturity Date`) = ' ' OR TRIM(@`Maturity Date`) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@`Maturity Date`), '%c/%e/%Y') END",
                "Report Run Date and Time (EDT)": "CASE WHEN TRIM(@`Report Run Date and Time (EDT)`) = '' OR TRIM(@`Report Run Date and Time (EDT)`) = ' ' OR TRIM(@`Report Run Date and Time (EDT)`) = '-' THEN NULL ELSE STR_TO_DATE(TRIM(@`Report Run Date and Time (EDT)`), '%c/%e/%Y %h:%i:%s %p') END",
                
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
    Delete existing records for accounts and dates found in the incoming file
    """
    try:
        import csv
        from datetime import datetime
        
        # Read the file to get account numbers and dates
        account_date_pairs = set()
        print(f"Reading file: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8-sig') as file:  # utf-8-sig handles BOM
            csv_reader = csv.DictReader(file)
            print(f"CSV headers: {csv_reader.fieldnames}")
            
            for row_num, row in enumerate(csv_reader, 1):
                # Handle BOM in column names by stripping it
                account_number_key = 'Account Number'
                as_of_date_key = 'As-Of Date'
                
                # Check if BOM is present in the first column name
                if csv_reader.fieldnames and csv_reader.fieldnames[0].startswith('\ufeff'):
                    account_number_key = '\ufeffAccount Number'
                
                if (account_number_key in row and row[account_number_key].strip() and 
                    as_of_date_key in row and row[as_of_date_key].strip()):
                    account_number = row[account_number_key].strip()
                    as_of_date = row[as_of_date_key].strip()
                    
                    print(f"Row {row_num}: Account='{account_number}', Date='{as_of_date}'")
                    
                    # Convert date from M/D/YYYY to YYYY-MM-DD format
                    try:
                        date_obj = datetime.strptime(as_of_date, '%m/%d/%Y')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                        account_date_pairs.add((account_number, formatted_date))
                        print(f"  -> Parsed date: {formatted_date}")
                    except ValueError as e:
                        print(f"Warning: Could not parse date '{as_of_date}' for account {account_number}: {e}")
                        continue
        
        if not account_date_pairs:
            print("No valid account/date pairs found in file")
            return True
        
        # Delete existing records for these account/date combinations
        import mysql.connector
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Build the DELETE query with placeholders
        placeholders = ', '.join(['(%s, %s)'] * len(account_date_pairs))
        delete_sql = f"""
            DELETE FROM borarch.MellonHoldings 
            WHERE (AccountNumber, AsOfDate) IN ({placeholders})
        """
        
        # Flatten the pairs for the query
        flat_params = []
        for account, date in account_date_pairs:
            flat_params.extend([account, date])
        
        # Log the delete statement and parameters for debugging
        print("=" * 60)
        print("------------------- DELETE STATEMENT ---------------------")
        print(delete_sql)
        print("------------------- PARAMETERS ---------------------")
        print(f"Parameters: {flat_params}")
        print(f"Account/Date pairs found: {list(account_date_pairs)}")
        print("=" * 60)
        
        cursor.execute(delete_sql, flat_params)
        deleted_count = cursor.rowcount
        
        conn.commit()
        print(f"Deleted {deleted_count} existing records for {len(account_date_pairs)} account/date combinations")
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

@task(name="update-filesource-for-loaded-data")
def update_filesource_for_loaded_data(file_source: str, db_config: dict) -> bool:
    """
    Update FileSource for all rows in MellonRawStaging that don't have it set
    """
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Updating FileSource for loaded data: {file_source}...")
        
        # Update FileSource for all rows that don't have it set
        cursor.execute("UPDATE borarch.MellonRawStaging SET FileSource = %s WHERE FileSource IS NULL", (file_source,))
        updated_rows = cursor.rowcount
        
        conn.commit()
        print(f"Updated FileSource for {updated_rows} rows in MellonRawStaging")
        return True
        
    except Exception as e:
        print(f"Error updating FileSource: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@task(name="transform-loaded-data")
def transform_loaded_data(file_source: str, db_config: dict) -> bool:
    """
    Transform data from MellonRawStaging to MellonHoldings with proper type conversions
    """
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Transforming data from MellonRawStaging to MellonHoldings for {file_source}...")
        
        # Insert data from raw staging to typed staging with transformations
        insert_sql = f"""
        INSERT INTO borarch.MellonHoldings (
            AccountNumber, AccountName, AccountType, SourceAccountNumber, SourceAccountName,
            AsOfDate, MellonSecurityId, CountryCode, Country, Segment, Category, Sector,
            Industry, SecurityDescription1, SecurityDescription2, AcctBaseCurrencyCode,
            ExchangeRate, IssueCurrencyCode, SharesPar, BaseCost, LocalCost, BasePrice,
            LocalPrice, BaseMarketValue, LocalMarketValue, BaseNetIncomeReceivable,
            LocalNetIncomeReceivable, BaseMarketValueWithAccrual, CouponRate, MaturityDate,
            BaseUnrealizedGainLoss, LocalUnrealizedGainLoss, BaseUnrealizedCurrencyGainLoss,
            BaseNetUnrealizedGainLoss, PercentOfTotal, ISIN, SEDOL, CUSIP, Ticker,
            CMSAccountNumber, IncomeCurrency, SecurityIdentifier, UnderlyingSecurity,
            FairValuePriceLevel, ReportRunDateTime, FileSource
        )
        SELECT 
            TRIM(AccountNumber),
            TRIM(AccountName),
            TRIM(AccountType),
            NULLIF(TRIM(SourceAccountNumber), ''),
            NULLIF(TRIM(SourceAccountName), ''),
            STR_TO_DATE(TRIM(AsOfDate), '%c/%e/%Y'),
            TRIM(MellonSecurityId),
            NULLIF(TRIM(CountryCode), ''),
            NULLIF(TRIM(Country), ''),
            NULLIF(TRIM(Segment), ''),
            NULLIF(TRIM(Category), ''),
            NULLIF(TRIM(Sector), ''),
            NULLIF(TRIM(Industry), ''),
            NULLIF(TRIM(SecurityDescription1), ''),
            NULLIF(TRIM(SecurityDescription2), ''),
            TRIM(AcctBaseCurrencyCode),
            CASE 
                WHEN TRIM(ExchangeRate) = '' OR TRIM(ExchangeRate) = ' ' OR TRIM(ExchangeRate) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(ExchangeRate), '"', ''), ',', ''), '')
            END,
            NULLIF(TRIM(IssueCurrencyCode), ''),
            CASE 
                WHEN TRIM(SharesPar) = '' OR TRIM(SharesPar) = ' ' OR TRIM(SharesPar) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(SharesPar), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseCost) = '' OR TRIM(BaseCost) = ' ' OR TRIM(BaseCost) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseCost), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(LocalCost) = '' OR TRIM(LocalCost) = ' ' OR TRIM(LocalCost) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(LocalCost), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BasePrice) = '' OR TRIM(BasePrice) = ' ' OR TRIM(BasePrice) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BasePrice), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(LocalPrice) = '' OR TRIM(LocalPrice) = ' ' OR TRIM(LocalPrice) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(LocalPrice), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseMarketValue) = '' OR TRIM(BaseMarketValue) = ' ' OR TRIM(BaseMarketValue) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseMarketValue), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(LocalMarketValue) = '' OR TRIM(LocalMarketValue) = ' ' OR TRIM(LocalMarketValue) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(LocalMarketValue), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseNetIncomeReceivable) = '' OR TRIM(BaseNetIncomeReceivable) = ' ' OR TRIM(BaseNetIncomeReceivable) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseNetIncomeReceivable), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(LocalNetIncomeReceivable) = '' OR TRIM(LocalNetIncomeReceivable) = ' ' OR TRIM(LocalNetIncomeReceivable) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(LocalNetIncomeReceivable), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseMarketValueWithAccrual) = '' OR TRIM(BaseMarketValueWithAccrual) = ' ' OR TRIM(BaseMarketValueWithAccrual) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseMarketValueWithAccrual), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(CouponRate) = '' OR TRIM(CouponRate) = ' ' OR TRIM(CouponRate) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(CouponRate), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(MaturityDate) = '' OR TRIM(MaturityDate) = ' ' OR TRIM(MaturityDate) = '-' THEN NULL
                ELSE STR_TO_DATE(TRIM(MaturityDate), '%c/%e/%Y')
            END,
            CASE 
                WHEN TRIM(BaseUnrealizedGainLoss) = '' OR TRIM(BaseUnrealizedGainLoss) = ' ' OR TRIM(BaseUnrealizedGainLoss) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseUnrealizedGainLoss), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(LocalUnrealizedGainLoss) = '' OR TRIM(LocalUnrealizedGainLoss) = ' ' OR TRIM(LocalUnrealizedGainLoss) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(LocalUnrealizedGainLoss), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseUnrealizedCurrencyGainLoss) = '' OR TRIM(BaseUnrealizedCurrencyGainLoss) = ' ' OR TRIM(BaseUnrealizedCurrencyGainLoss) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseUnrealizedCurrencyGainLoss), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(BaseNetUnrealizedGainLoss) = '' OR TRIM(BaseNetUnrealizedGainLoss) = ' ' OR TRIM(BaseNetUnrealizedGainLoss) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(TRIM(BaseNetUnrealizedGainLoss), '"', ''), ',', ''), '')
            END,
            CASE 
                WHEN TRIM(PercentOfTotal) = '' OR TRIM(PercentOfTotal) = ' ' OR TRIM(PercentOfTotal) = '-' THEN NULL
                ELSE NULLIF(REPLACE(REPLACE(REPLACE(TRIM(PercentOfTotal), ' ', ''), '(', ''), ')', ''), '')
            END,
            NULLIF(TRIM(ISIN), ''),
            NULLIF(TRIM(SEDOL), ''),
            NULLIF(TRIM(CUSIP), ''),
            NULLIF(TRIM(Ticker), ''),
            NULLIF(TRIM(CMSAccountNumber), ''),
            NULLIF(TRIM(IncomeCurrency), ''),
            NULLIF(TRIM(SecurityIdentifier), ''),
            NULLIF(TRIM(UnderlyingSecurity), ''),
            CASE 
                WHEN TRIM(FairValuePriceLevel) = '' OR TRIM(FairValuePriceLevel) = ' ' OR TRIM(FairValuePriceLevel) = '-' THEN NULL
                ELSE CAST(TRIM(FairValuePriceLevel) AS SIGNED)
            END,
            CASE 
                WHEN TRIM(ReportRunDateTime) = '' OR TRIM(ReportRunDateTime) = ' ' OR TRIM(ReportRunDateTime) = '-' THEN NULL
                ELSE STR_TO_DATE(TRIM(ReportRunDateTime), '%c/%e/%Y %h:%i:%s %p')
            END,
            FileSource
        FROM borarch.MellonRawStaging
        WHERE FileSource = '{file_source}'
        """
        
        print(f"Executing INSERT with file_source: '{file_source}'")
        print(f"SQL query: {insert_sql}")
        cursor.execute(insert_sql)
        inserted_rows = cursor.rowcount
        
        # Clear the raw staging table for this file
        cursor.execute("DELETE FROM borarch.MellonRawStaging WHERE FileSource = %s", (file_source,))
        deleted_rows = cursor.rowcount
        
        conn.commit()
        print(f"Transformed and inserted {inserted_rows} rows from MellonRawStaging to MellonHoldings")
        print(f"Cleared {deleted_rows} rows from MellonRawStaging")
        return True
        
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@task(name="run-mellon-integration-proc")
def run_mellon_integration_proc(db_config: dict) -> bool:
    """
    Run the Mellon holdings integration stored procedure in the bormeta database
    """
    try:
        import mysql.connector
        
        # Create a new connection config for bormeta database
        bormeta_db_config = db_config.copy()
        bormeta_db_config["database"] = "bormeta"
        
        conn = mysql.connector.connect(**bormeta_db_config)
        cursor = conn.cursor()
        
        print("Running Mellon integration stored procedure...")
        
        # Execute the stored procedure
        cursor.callproc('usp_mellon_hold_integrate', (1, 'FULL_INTEGRATION', None, None, None, None))
        
        # Fetch any results if the procedure returns them
        for result in cursor.stored_results():
            if result:
                rows = result.fetchall()
                print(f"Stored procedure returned {len(rows)} result rows")
        
        conn.commit()
        print("Mellon integration stored procedure completed successfully")
        return True
        
    except Exception as e:
        print(f"Error running Mellon integration stored procedure: {str(e)}")
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
        print(f"Source files: {source_files}")
        print(f"DB config: {db_host}:{db_port}, user: {db_user}, db: {db_name}")
        
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
                        target_table="borarch.MellonRawStaging",  # Load to raw staging first
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
                            "": "@dummy",  # Skip empty column - use @dummy to ignore it
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
                        field_transformations=None,  # No transformations during load
                        procedure_name=None, # No stored procedure for data loading
                        procedure_params=None,
                        truncate_before_load=False
                    )
            
            # Step 3: Load data to raw staging using BaseIngestionWorkflow
            print(f"Step 3: Loading data to raw staging for {file_name}...")
            
            # Create workflow instance for this file
            wf = DynamicMellonWorkflow(file_name)
            
            # Call the load_data_to_staging task directly instead of the execute method
            success = load_data_to_staging(
                file_path=target_file,
                db_config=db_config,
                target_table=wf.target_table,
                field_mappings=wf.field_mappings,
                field_transformations=None,  # No transformations during load
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=skip_lines,
                truncate_before_load=truncate_before_load
            )
            
            print(f"load_data_to_staging() returned: {success}")
            
            if not success:
                raise Exception(f"Failed to load data for {file_name}")
            
            # Step 3.5: Update FileSource for loaded data
            print(f"Step 3.5: Updating FileSource for {file_name}...")
            update_result = update_filesource_for_loaded_data(file_name, db_config)
            print(f"update_filesource_for_loaded_data() returned: {update_result}")
            
            if not update_result:
                raise Exception(f"Failed to update FileSource for {file_name}")
            
            # Step 4: Transform the loaded data
            print(f"Step 4: Starting transformation for {file_name}...")
            transform_result = transform_loaded_data(file_name, db_config)
            print(f"transform_loaded_data() returned: {transform_result}")
            
            if not transform_result:
                raise Exception(f"Failed to transform data for {file_name}")
            
            print(f"Successfully processed {file_name}")
        
        # Step 5: Run integration stored procedure after all files are processed
        print("Step 5: Running Mellon integration stored procedure...")
        integration_result = run_mellon_integration_proc(db_config)
        print(f"run_mellon_integration_proc() returned: {integration_result}")
        
        if not integration_result:
            raise Exception("Failed to run Mellon integration stored procedure")
        
        print("Mellon Holdings ETL completed successfully")
        return True
        
    except Exception as e:
        print(f"Mellon Holdings ETL failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
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