"""
Mellon Duration ETL Workflow

This workflow processes Mellon duration CSV files and loads them into the bor system databases.
The files have a two-header row format with Account Name and Date information.
"""

import os
import shutil
import csv
from pathlib import Path
from prefect import flow, task
from src.utils.base_ingestion import BaseIngestionWorkflow, load_data_to_staging, check_file_exists, execute_stored_procedure

class MellonDurationETLWorkflow(BaseIngestionWorkflow):
    def __init__(self):
        super().__init__(
            name="Mellon Duration ETL",
            target_table="borarch.MellonDuration",
            field_mappings={
                # Header information (added by custom processing)
                "AccountName": "AccountName",
                "ReportDate": "ReportDate",
                # Data columns from CSV (starting from row 3)
                "Security Number": "SecurityNumber",
                "Security Description": "SecurityDescription", 
                "CUSIP": "CUSIP",
                "Shares Oustanding": "SharesOutstanding",
                "Asset Group": "AssetGroup",
                "Traded Market Value (Base)": "TradedMarketValue",
                "Maturity Date": "MaturityDate",
                "Duration": "Duration",
                "Yield (To Maturity)": "YieldToMaturity",
                "Yield (Current)": "YieldCurrent",
                "Months to Maturity": "MonthsToMaturity",
                "Days to Maturity": "DaysToMaturity",
                "FileSource": "FileSource"
            },
            field_transformations={
                # String columns - trim whitespace, handle empty as NULL for optional fields
                "SecurityNumber": "CASE WHEN TRIM(@`Security Number`) = '' OR TRIM(@`Security Number`) = ' ' OR TRIM(@`Security Number`) = '-' THEN NULL ELSE TRIM(@`Security Number`) END",
                "SecurityDescription": "CASE WHEN TRIM(@`Security Description`) = '' OR TRIM(@`Security Description`) = ' ' OR TRIM(@`Security Description`) = '-' THEN NULL ELSE TRIM(@`Security Description`) END",
                "CUSIP": "CASE WHEN TRIM(@CUSIP) = '' OR TRIM(@CUSIP) = ' ' OR TRIM(@CUSIP) = '-' THEN NULL ELSE TRIM(@CUSIP) END",
                "SharesOutstanding": "CASE WHEN TRIM(@`Shares Oustanding`) = '' OR TRIM(@`Shares Oustanding`) = ' ' OR TRIM(@`Shares Oustanding`) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Shares Oustanding`), '\"', ''), ',', ''), '') END",
                "AssetGroup": "CASE WHEN TRIM(@`Asset Group`) = '' OR TRIM(@`Asset Group`) = ' ' OR TRIM(@`Asset Group`) = '-' THEN NULL ELSE TRIM(@`Asset Group`) END",
                "TradedMarketValue": "CASE WHEN TRIM(@`Traded Market Value (Base)`) = '' OR TRIM(@`Traded Market Value (Base)`) = ' ' OR TRIM(@`Traded Market Value (Base)`) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Traded Market Value (Base)`), '\"', ''), ',', ''), '') END",
                "MaturityDate": "CASE WHEN TRIM(@`Maturity Date`) = '' OR TRIM(@`Maturity Date`) = ' ' OR TRIM(@`Maturity Date`) = '-' OR TRIM(@`Maturity Date`) = '0-Jan-00' THEN NULL ELSE TRIM(@`Maturity Date`) END",
                "Duration": "CASE WHEN TRIM(@Duration) = '' OR TRIM(@Duration) = ' ' OR TRIM(@Duration) = '-' OR TRIM(@Duration) = '-999' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@Duration), '\"', ''), ',', ''), '') END",
                "YieldToMaturity": "CASE WHEN TRIM(@`Yield (To Maturity)`) = '' OR TRIM(@`Yield (To Maturity)`) = ' ' OR TRIM(@`Yield (To Maturity)`) = '-' OR TRIM(@`Yield (To Maturity)`) = '0' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Yield (To Maturity)`), '\"', ''), ',', ''), '') END",
                "YieldCurrent": "CASE WHEN TRIM(@`Yield (Current)`) = '' OR TRIM(@`Yield (Current)`) = ' ' OR TRIM(@`Yield (Current)`) = '-' OR TRIM(@`Yield (Current)`) = '0' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Yield (Current)`), '\"', ''), ',', ''), '') END",
                "MonthsToMaturity": "CASE WHEN TRIM(@`Months to Maturity`) = '' OR TRIM(@`Months to Maturity`) = ' ' OR TRIM(@`Months to Maturity`) = '-' OR TRIM(@`Months to Maturity`) = '0' THEN NULL ELSE CAST(TRIM(@`Months to Maturity`) AS SIGNED) END",
                "DaysToMaturity": "CASE WHEN TRIM(@`Days to Maturity`) = '' OR TRIM(@`Days to Maturity`) = ' ' OR TRIM(@`Days to Maturity`) = '-' OR TRIM(@`Days to Maturity`) = '0' THEN NULL ELSE CAST(TRIM(@`Days to Maturity`) AS SIGNED) END",
                "FileSource": "'mellon_duration_default'"
            }
        )
    
    def execute(
        self,
        file_path: str,
        db_host: str,
        db_port: str,
        db_user: str,
        db_password: str,
        db_name: str,
        delimiter: str = ',',
        quote_char: str = '"',
        line_terminator: str = '\n',
        skip_lines: int = 1,
        truncate_before_load: bool = None
    ) -> bool:
        """
        Override the base execute method to handle the two-header row format for Mellon Duration files.
        Extracts AccountName and ReportDate from the first two rows and adds them to each data row.
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
                "port": int(db_port),
                "user": db_user,
                "password": db_password,
                "database": db_name
            }
            
            # Defensive cast for truncate_before_load
            if isinstance(truncate_before_load, str):
                truncate_before_load = truncate_before_load.lower() == "true"
            if truncate_before_load is None:
                truncate_before_load = self.truncate_before_load
            
            # Check if file exists
            if not check_file_exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Extract account name and date from the first two lines of the file
            account_name = None
            report_date = None
            
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    # Parse Account Name from line 1: "#Account Name,PENDER SM CAP OPP FD"
                    if lines[0].startswith('#Account Name,'):
                        account_name = lines[0].split(',', 1)[1].strip()
                    
                    # Parse Date from line 2: "#Date,14-Apr-25"
                    if lines[1].startswith('#Date,'):
                        report_date = lines[1].split(',', 1)[1].strip()
            
            if not account_name or not report_date:
                raise Exception(f"Could not extract account name or date from file: {file_path}")
            
            print(f"INFO: Extracted Account: {account_name}, Date: {report_date}")
            
            # Create a temporary file with the header information added to each data row
            temp_file_path = file_path + '.temp'
            
            with open(file_path, 'r', encoding='utf-8-sig') as infile, open(temp_file_path, 'w', encoding='utf-8', newline='') as outfile:
                lines = infile.readlines()
                
                # Write the column header row with AccountName, ReportDate, and FileSource added
                if len(lines) >= 3:
                    # Get the original column headers (line 3) using CSV parsing
                    csv_reader = csv.reader([lines[2]])
                    original_headers = next(csv_reader)
                    # Add AccountName, ReportDate, and FileSource at the beginning
                    new_headers = ['AccountName', 'ReportDate'] + original_headers + ['FileSource']
                    
                    csv_writer = csv.writer(outfile)
                    csv_writer.writerow(new_headers)
                    
                    # Process data rows (starting from line 4) using CSV parsing
                    for i in range(3, len(lines)):
                        line = lines[i].strip()
                        if line and not line.startswith('Total Investments') and not line.startswith(',,,,,,,,,,,'):
                            # Parse the line using CSV reader
                            csv_reader = csv.reader([line])
                            row_data = next(csv_reader)
                            
                            # Add AccountName, ReportDate, and FileSource to each data row
                            file_source = f"mellon_duration_{os.path.basename(file_path)}"
                            new_row = [account_name, report_date] + row_data + [file_source]
                            csv_writer.writerow(new_row)
            
            # Debug: Print first few lines of temporary file
            print(f"DEBUG: Temporary file created: {temp_file_path}")
            with open(temp_file_path, 'r') as debug_file:
                debug_lines = debug_file.readlines()
                print(f"DEBUG: First 5 lines of temp file:")
                for i, line in enumerate(debug_lines[:5]):
                    print(f"DEBUG: Line {i+1}: {line.strip()}")
                print(f"DEBUG: Total lines in temp file: {len(debug_lines)}")
            
            # Load data to staging using the temporary file
            if not load_data_to_staging(
                file_path=temp_file_path,
                db_config=db_config,
                target_table=self.target_table,
                field_mappings=self.field_mappings,
                field_transformations=self.field_transformations,
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=0,  # No need to skip lines since we've already processed them
                truncate_before_load=truncate_before_load
            ):
                raise Exception("Failed to load data to staging")
            
            # Clean up temporary file (commented out for debugging)
            # if os.path.exists(temp_file_path):
            #     os.remove(temp_file_path)
            
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

@task(name="delete-existing-account-data")
def delete_existing_account_data(file_path: str, db_config: dict) -> bool:
    """
    Delete existing data for the account and date from the MellonDuration table.
    
    Args:
        file_path: Path to the CSV file
        db_config: Database configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract account name and date from the first two lines of the file
        account_name = None
        report_date = None
        
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
            if len(lines) >= 2:
                # Parse Account Name from line 1: "#Account Name,PENDER SM CAP OPP FD"
                if lines[0].startswith('#Account Name,'):
                    account_name = lines[0].split(',', 1)[1].strip()
                
                # Parse Date from line 2: "#Date,14-Apr-25"
                if lines[1].startswith('#Date,'):
                    report_date = lines[1].split(',', 1)[1].strip()
        
        if not account_name or not report_date:
            print(f"ERROR: Could not extract account name or date from file: {file_path}")
            return False
        
        print(f"INFO: Deleting existing data for Account: {account_name}, Date: {report_date}")
        
        # Connect to database and delete existing data
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Delete existing records for this account and date
            delete_query = """
                DELETE FROM MellonDuration 
                WHERE AccountName = %s AND ReportDate = %s
            """
            
            cursor.execute(delete_query, (account_name, report_date))
            deleted_count = cursor.rowcount
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"INFO: Deleted {deleted_count} existing records for Account: {account_name}, Date: {report_date}")
            return True
            
        else:
            print("ERROR: Could not connect to database")
            return False
            
    except Error as e:
        print(f"ERROR: Database error during deletion: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during deletion: {e}")
        return False

@task(name="update-file-tracking")
def update_file_tracking(file_source: str, db_config: dict) -> bool:
    """
    Update file tracking information in the MellonFileImport table.
    
    Args:
        file_source: Source file name
        db_config: Database configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Insert or update file tracking record
            insert_query = """
                INSERT INTO MellonFileImport (FileName, ImportDate, Status)
                VALUES (%s, NOW(), 'IMPORTED')
                ON DUPLICATE KEY UPDATE 
                    ImportDate = NOW(),
                    Status = 'IMPORTED'
            """
            
            cursor.execute(insert_query, (file_source,))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"INFO: Updated file tracking for {file_source}")
            return True
            
        else:
            print("ERROR: Could not connect to database")
            return False
            
    except Error as e:
        print(f"ERROR: Database error during file tracking update: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during file tracking update: {e}")
        return False

@task(name="update-filesource-for-loaded-data")
def update_filesource_for_loaded_data(file_source: str, db_config: dict) -> bool:
    """
    Update FileSource for all records loaded from this file.
    
    Args:
        file_source: Source file name
        db_config: Database configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Update FileSource for records without a FileSource
            update_query = """
                UPDATE MellonDuration 
                SET FileSource = %s 
                WHERE FileSource IS NULL OR FileSource = ''
            """
            
            cursor.execute(update_query, (f"mellon_duration_{file_source}",))
            updated_count = cursor.rowcount
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"INFO: Updated FileSource for {updated_count} records")
            return True
            
        else:
            print("ERROR: Could not connect to database")
            return False
            
    except Error as e:
        print(f"ERROR: Database error during FileSource update: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during FileSource update: {e}")
        return False

@task(name="transform-loaded-data")
def transform_loaded_data(file_source: str, db_config: dict) -> bool:
    """
    Apply any additional transformations to the loaded data.
    
    Args:
        file_source: Source file name
        db_config: Database configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Mark records as processed
            update_query = """
                UPDATE MellonDuration 
                SET Processed = 1 
                WHERE FileSource = %s AND Processed = 0
            """
            
            cursor.execute(update_query, (f"mellon_duration_{file_source}",))
            updated_count = cursor.rowcount
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"INFO: Marked {updated_count} records as processed")
            return True
            
        else:
            print("ERROR: Could not connect to database")
            return False
            
    except Error as e:
        print(f"ERROR: Database error during data transformation: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during data transformation: {e}")
        return False

@task(name="run-mellon-duration-integration-proc")
def run_mellon_duration_integration_proc(db_config: dict) -> bool:
    """
    Run the Mellon Duration integration stored procedure (placeholder for now).
    
    Args:
        db_config: Database configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print("INFO: Mellon Duration integration procedure - SKIPPED FOR NOW")
        print("INFO: This is a placeholder for future integration logic")
        return True
        
    except Exception as e:
        print(f"ERROR: Unexpected error during integration procedure: {e}")
        return False

@flow(name="mellon-duration-etl")
def mellon_duration_etl_flow(
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
    skip_lines: int = 3,  # Skip two header rows + column header
    truncate_before_load: bool = False
) -> bool:
    """
    Main flow for Mellon Duration ETL processing.
    
    Args:
        source_files: List of CSV file paths to process
        db_host: Database host
        db_port: Database port (as string, will be cast to int)
        db_user: Database user
        db_password: Database password
        db_name: Database name
        target_dir: Directory containing the source files
        delimiter: Field delimiter character
        quote_char: Quote character
        line_terminator: Line terminator character
        skip_lines: Number of header lines to skip
        truncate_before_load: Boolean indicating whether to truncate the table before loading
        
    Returns:
        bool: True if all files processed successfully, False otherwise
    """
    
    # Configure database connection
    db_config = {
        "host": db_host,
        "port": int(db_port),
        "user": db_user,
        "password": db_password,
        "database": db_name
    }
    
    success_count = 0
    error_count = 0
    
    for file_path in source_files:
        try:
            print(f"INFO: Processing file: {file_path}")
            
            # Extract file name for tracking
            file_source = os.path.basename(file_path)
            
            # Step 1: Delete existing data for this account/date
            if not delete_existing_account_data(file_path, db_config):
                print(f"ERROR: Failed to delete existing data for {file_path}")
                error_count += 1
                continue
            
            # Step 2: Update file tracking
            if not update_file_tracking(file_source, db_config):
                print(f"ERROR: Failed to update file tracking for {file_path}")
                error_count += 1
                continue
            
            # Step 3: Load data to staging
            print(f"DEBUG: Creating DynamicMellonDurationWorkflow for {file_source}")
            workflow = DynamicMellonDurationWorkflow(file_source)
            print(f"DEBUG: Workflow class: {type(workflow).__name__}")
            print(f"DEBUG: Workflow has custom execute: {hasattr(workflow, 'execute')}")
            print(f"DEBUG: About to call workflow.execute() for {file_path}")
            if not workflow.execute(
                file_path=file_path,
                db_host=db_config['host'],
                db_port=str(db_config['port']),
                db_user=db_config['user'],
                db_password=db_config['password'],
                db_name=db_config['database'],
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=skip_lines,
                truncate_before_load=truncate_before_load
            ):
                print(f"ERROR: Failed to load data to staging for {file_path}")
                error_count += 1
                continue
            
            # Step 4: Update FileSource for loaded data
            if not update_filesource_for_loaded_data(file_source, db_config):
                print(f"ERROR: Failed to update FileSource for {file_path}")
                error_count += 1
                continue
            
            # Step 5: Transform loaded data
            if not transform_loaded_data(file_source, db_config):
                print(f"ERROR: Failed to transform data for {file_path}")
                error_count += 1
                continue
            
            # Step 6: Run integration procedure (placeholder)
            if not run_mellon_duration_integration_proc(db_config):
                print(f"ERROR: Failed to run integration procedure for {file_path}")
                error_count += 1
                continue
            
            print(f"INFO: Successfully processed {file_path}")
            success_count += 1
            
        except Exception as e:
            print(f"ERROR: Unexpected error processing {file_path}: {e}")
            error_count += 1
            continue
    
    print(f"INFO: Mellon Duration ETL flow completed")
    print(f"INFO: Successfully processed: {success_count} files")
    print(f"INFO: Failed to process: {error_count} files")
    
    return error_count == 0

# Dynamic workflow class for individual file processing
class DynamicMellonDurationWorkflow(BaseIngestionWorkflow):
    def __init__(self, file_source: str):
        super().__init__(
            name=f"Mellon Duration ETL - {file_source}",
            target_table="borarch.MellonDuration",
            field_mappings={
                # Header information (added by custom processing)
                "AccountName": "AccountName",
                "ReportDate": "ReportDate",
                # Data columns from CSV (starting from row 3)
                "Security Number": "SecurityNumber",
                "Security Description": "SecurityDescription", 
                "CUSIP": "CUSIP",
                "Shares Oustanding": "SharesOutstanding",
                "Asset Group": "AssetGroup",
                "Traded Market Value (Base)": "TradedMarketValue",
                "Maturity Date": "MaturityDate",
                "Duration": "Duration",
                "Yield (To Maturity)": "YieldToMaturity",
                "Yield (Current)": "YieldCurrent",
                "Months to Maturity": "MonthsToMaturity",
                "Days to Maturity": "DaysToMaturity",
                "FileSource": "FileSource"
            },
            field_transformations={
                # String columns - trim whitespace, handle empty as NULL for optional fields
                "SecurityNumber": "CASE WHEN TRIM(@`Security Number`) = '' OR TRIM(@`Security Number`) = ' ' OR TRIM(@`Security Number`) = '-' THEN NULL ELSE TRIM(@`Security Number`) END",
                "SecurityDescription": "CASE WHEN TRIM(@`Security Description`) = '' OR TRIM(@`Security Description`) = ' ' OR TRIM(@`Security Description`) = '-' THEN NULL ELSE TRIM(@`Security Description`) END",
                "CUSIP": "CASE WHEN TRIM(@CUSIP) = '' OR TRIM(@CUSIP) = ' ' OR TRIM(@CUSIP) = '-' THEN NULL ELSE TRIM(@CUSIP) END",
                "SharesOutstanding": "CASE WHEN TRIM(@`Shares Oustanding`) = '' OR TRIM(@`Shares Oustanding`) = ' ' OR TRIM(@`Shares Oustanding`) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Shares Oustanding`), '\"', ''), ',', ''), '') END",
                "AssetGroup": "CASE WHEN TRIM(@`Asset Group`) = '' OR TRIM(@`Asset Group`) = ' ' OR TRIM(@`Asset Group`) = '-' THEN NULL ELSE TRIM(@`Asset Group`) END",
                "TradedMarketValue": "CASE WHEN TRIM(@`Traded Market Value (Base)`) = '' OR TRIM(@`Traded Market Value (Base)`) = ' ' OR TRIM(@`Traded Market Value (Base)`) = '-' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Traded Market Value (Base)`), '\"', ''), ',', ''), '') END",
                "MaturityDate": "CASE WHEN TRIM(@`Maturity Date`) = '' OR TRIM(@`Maturity Date`) = ' ' OR TRIM(@`Maturity Date`) = '-' OR TRIM(@`Maturity Date`) = '0-Jan-00' THEN NULL ELSE TRIM(@`Maturity Date`) END",
                "Duration": "CASE WHEN TRIM(@Duration) = '' OR TRIM(@Duration) = ' ' OR TRIM(@Duration) = '-' OR TRIM(@Duration) = '-999' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@Duration), '\"', ''), ',', ''), '') END",
                "YieldToMaturity": "CASE WHEN TRIM(@`Yield (To Maturity)`) = '' OR TRIM(@`Yield (To Maturity)`) = ' ' OR TRIM(@`Yield (To Maturity)`) = '-' OR TRIM(@`Yield (To Maturity)`) = '0' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Yield (To Maturity)`), '\"', ''), ',', ''), '') END",
                "YieldCurrent": "CASE WHEN TRIM(@`Yield (Current)`) = '' OR TRIM(@`Yield (Current)`) = ' ' OR TRIM(@`Yield (Current)`) = '-' OR TRIM(@`Yield (Current)`) = '0' THEN NULL ELSE NULLIF(REPLACE(REPLACE(TRIM(@`Yield (Current)`), '\"', ''), ',', ''), '') END",
                "MonthsToMaturity": "CASE WHEN TRIM(@`Months to Maturity`) = '' OR TRIM(@`Months to Maturity`) = ' ' OR TRIM(@`Months to Maturity`) = '-' OR TRIM(@`Months to Maturity`) = '0' THEN NULL ELSE CAST(TRIM(@`Months to Maturity`) AS SIGNED) END",
                "DaysToMaturity": "CASE WHEN TRIM(@`Days to Maturity`) = '' OR TRIM(@`Days to Maturity`) = ' ' OR TRIM(@`Days to Maturity`) = '-' OR TRIM(@`Days to Maturity`) = '0' THEN NULL ELSE CAST(TRIM(@`Days to Maturity`) AS SIGNED) END",
                "FileSource": "'mellon_duration_default'"
            }
        )
    
    def execute(
        self,
        file_path: str,
        db_host: str,
        db_port: str,
        db_user: str,
        db_password: str,
        db_name: str,
        delimiter: str = ',',
        quote_char: str = '"',
        line_terminator: str = '\n',
        skip_lines: int = 1,
        truncate_before_load: bool = None
    ) -> bool:
        """
        Override the base execute method to handle the two-header row format for Mellon Duration files.
        Extracts AccountName and ReportDate from the first two rows and adds them to each data row.
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
                "port": int(db_port),
                "user": db_user,
                "password": db_password,
                "database": db_name
            }
            
            # Defensive cast for truncate_before_load
            if isinstance(truncate_before_load, str):
                truncate_before_load = truncate_before_load.lower() == "true"
            if truncate_before_load is None:
                truncate_before_load = self.truncate_before_load
            
            # Check if file exists
            if not check_file_exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Extract account name and date from the first two lines of the file
            account_name = None
            report_date = None
            
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    # Parse Account Name from line 1: "#Account Name,PENDER SM CAP OPP FD"
                    if lines[0].startswith('#Account Name,'):
                        account_name = lines[0].split(',', 1)[1].strip()
                    
                    # Parse Date from line 2: "#Date,14-Apr-25"
                    if lines[1].startswith('#Date,'):
                        report_date = lines[1].split(',', 1)[1].strip()
            
            if not account_name or not report_date:
                raise Exception(f"Could not extract account name or date from file: {file_path}")
            
            print(f"INFO: Extracted Account: {account_name}, Date: {report_date}")
            
            # Create a temporary file with the header information added to each data row
            temp_file_path = file_path + '.temp'
            
            with open(file_path, 'r', encoding='utf-8-sig') as infile, open(temp_file_path, 'w', encoding='utf-8', newline='') as outfile:
                lines = infile.readlines()
                
                # Write the column header row with AccountName, ReportDate, and FileSource added
                if len(lines) >= 3:
                    # Get the original column headers (line 3) using CSV parsing
                    csv_reader = csv.reader([lines[2]])
                    original_headers = next(csv_reader)
                    # Add AccountName, ReportDate, and FileSource at the beginning
                    new_headers = ['AccountName', 'ReportDate'] + original_headers + ['FileSource']
                    
                    csv_writer = csv.writer(outfile)
                    csv_writer.writerow(new_headers)
                    
                    # Process data rows (starting from line 4) using CSV parsing
                    for i in range(3, len(lines)):
                        line = lines[i].strip()
                        if line and not line.startswith('Total Investments') and not line.startswith(',,,,,,,,,,,'):
                            # Parse the line using CSV reader
                            csv_reader = csv.reader([line])
                            row_data = next(csv_reader)
                            
                            # Add AccountName, ReportDate, and FileSource to each data row
                            file_source = f"mellon_duration_{os.path.basename(file_path)}"
                            new_row = [account_name, report_date] + row_data + [file_source]
                            csv_writer.writerow(new_row)
            
            # Debug: Print first few lines of temporary file
            print(f"DEBUG: Temporary file created: {temp_file_path}")
            with open(temp_file_path, 'r') as debug_file:
                debug_lines = debug_file.readlines()
                print(f"DEBUG: First 5 lines of temp file:")
                for i, line in enumerate(debug_lines[:5]):
                    print(f"DEBUG: Line {i+1}: {line.strip()}")
                print(f"DEBUG: Total lines in temp file: {len(debug_lines)}")
            
            # Load data to staging using the temporary file
            if not load_data_to_staging(
                file_path=temp_file_path,
                db_config=db_config,
                target_table=self.target_table,
                field_mappings=self.field_mappings,
                field_transformations=self.field_transformations,
                delimiter=delimiter,
                quote_char=quote_char,
                line_terminator=line_terminator,
                skip_lines=0,  # No need to skip lines since we've already processed them
                truncate_before_load=truncate_before_load
            ):
                raise Exception("Failed to load data to staging")
            
            # Clean up temporary file (commented out for debugging)
            # if os.path.exists(temp_file_path):
            #     os.remove(temp_file_path)
            
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