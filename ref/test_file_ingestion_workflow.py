import os
import pytest
import pandas as pd
import mysql.connector
from datetime import datetime
from pathlib import Path
from ftplib import FTP, error_perm, error_temp
from dotenv import load_dotenv
from prefect import flow
from prefect.exceptions import PrefectException
from workflows.import_web_classfees import file_ingestion_workflow

# Load environment variables
load_dotenv()

# Constants
FTP_HOST = "bor-files"
FTP_PORT = 21
FTP_USER = "ftpetl"
FTP_PASS = "lteptf"
FTP_INCOMING_DIR = "/home/vsftpd/incoming"
FTP_PROCESSED_DIR = "/home/vsftpd/processed"

DB_HOST = "bor-db"
DB_PORT = 3306
DB_USER = os.getenv("DB_BORINST_SVC_USER")
DB_PASSWORD = os.getenv("DB_BORINST_SVC_USER_PASSWORD")
DB_NAME = os.getenv("DB_BORARCH_NAME")

class WorkflowTestError(Exception):
    """Base exception for workflow test errors"""
    pass

class FTPError(WorkflowTestError):
    """Exception for FTP-related errors"""
    pass

class DatabaseError(WorkflowTestError):
    """Exception for database-related errors"""
    pass

class WorkflowExecutionError(WorkflowTestError):
    """Exception for workflow execution errors"""
    pass

@pytest.fixture
def setup_test_data():
    """Create test data from the actual fund class fees file."""
    # Read the actual fund class fees data
    data_file = Path(__file__).parent / "data" / "fund-class-fees.csv"
    df = pd.read_csv(data_file)
    
    # Create a unique filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"fund-class-fees_{timestamp}.csv"
    filepath = f"/tmp/{filename}"
    
    # Save the file
    df.to_csv(filepath, index=False)
    
    yield filepath, df
    
    # Cleanup
    if os.path.exists(filepath):
        os.remove(filepath)

@pytest.fixture
def setup_database():
    """Setup and cleanup database tables."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()
    
    # Create staging table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FundClassFee (
            fund_code VARCHAR(20),
            fund_name VARCHAR(100),
            class VARCHAR(10),
            description VARCHAR(50),
            mer DECIMAL(10,4),
            trailer DECIMAL(10,4),
            performance_fee VARCHAR(100),
            min_investment_initial VARCHAR(20),
            min_investment_subsequent VARCHAR(20),
            currency VARCHAR(3)
        )
    """)
    
    # Create target table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS borinst.FundClassFee (
            fund_code VARCHAR(20),
            fund_name VARCHAR(100),
            class VARCHAR(10),
            description VARCHAR(50),
            mer DECIMAL(10,4),
            trailer DECIMAL(10,4),
            performance_fee VARCHAR(100),
            min_investment_initial VARCHAR(20),
            min_investment_subsequent VARCHAR(20),
            currency VARCHAR(3),
            PRIMARY KEY (fund_code, class)
        )
    """)
    
    conn.commit()
    
    yield conn
    
    # Cleanup
    cursor.execute("TRUNCATE TABLE FundClassFee")
    cursor.execute("TRUNCATE TABLE borinst.FundClassFee")
    conn.commit()
    cursor.close()
    conn.close()

def test_file_ingestion_workflow(setup_test_data, setup_database):
    """Test the complete file ingestion workflow with actual fund class fees data."""
    filepath, original_df = setup_test_data
    conn = setup_database
    
    try:
        # 1. Upload file to FTP
        try:
            with FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASS)
                
                # Upload file to incoming directory
                with open(filepath, 'rb') as file:
                    ftp.storbinary(f'STOR {FTP_INCOMING_DIR}/{os.path.basename(filepath)}', file)
        except error_perm as e:
            raise FTPError(f"FTP permission error: {str(e)}")
        except error_temp as e:
            raise FTPError(f"FTP temporary error: {str(e)}")
        except Exception as e:
            raise FTPError(f"FTP upload failed: {str(e)}")
        
        # 2. Trigger the workflow
        try:
            workflow_result = file_ingestion_workflow(
                file_path=filepath,
                ftp_host=FTP_HOST,
                ftp_port=FTP_PORT,
                ftp_user=FTP_USER,
                ftp_pass=FTP_PASS,
                db_host=DB_HOST,
                db_port=DB_PORT,
                db_user=DB_USER,
                db_pass=DB_PASSWORD,
                db_name=DB_NAME,
                use_direct_access=True
            )
            
            if not workflow_result:
                raise WorkflowExecutionError("Workflow returned False")
                
        except PrefectException as e:
            raise WorkflowExecutionError(f"Prefect workflow error: {str(e)}")
        except Exception as e:
            raise WorkflowExecutionError(f"Workflow execution failed: {str(e)}")
        
        # 3. Verify file was moved to processed directory
        try:
            with FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASS)
                
                # List files in processed directory
                processed_files = []
                ftp.cwd(FTP_PROCESSED_DIR)
                ftp.retrlines('LIST', processed_files.append)
                
                if not any(os.path.basename(filepath) in f for f in processed_files):
                    raise FTPError("File was not moved to processed directory")
        except Exception as e:
            raise FTPError(f"Failed to verify processed file: {str(e)}")
        
        # 4. Verify data was loaded into staging table
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM FundClassFee")
            staging_data = cursor.fetchall()
            staging_df = pd.DataFrame(staging_data)
            
            if len(staging_df) != len(original_df):
                raise DatabaseError(
                    f"Staging table record count mismatch. Expected {len(original_df)}, got {len(staging_df)}"
                )
        except mysql.connector.Error as e:
            raise DatabaseError(f"Database error while checking staging table: {str(e)}")
        
        # 5. Verify data was loaded into target table
        try:
            cursor.execute("SELECT * FROM borinst.FundClassFee")
            target_data = cursor.fetchall()
            target_df = pd.DataFrame(target_data)
            
            if len(target_df) != len(original_df):
                raise DatabaseError(
                    f"Target table record count mismatch. Expected {len(original_df)}, got {len(target_df)}"
                )
        except mysql.connector.Error as e:
            raise DatabaseError(f"Database error while checking target table: {str(e)}")
        
        # 6. Verify data integrity
        try:
            # Convert numeric columns to float for comparison
            numeric_cols = ['mer', 'trailer']
            for col in numeric_cols:
                original_df[col] = pd.to_numeric(original_df[col], errors='coerce')
                target_df[col] = pd.to_numeric(target_df[col], errors='coerce')
            
            # Compare key columns
            key_cols = ['fund_code', 'class', 'mer', 'trailer', 'currency']
            for col in key_cols:
                original_values = original_df[col].fillna('').astype(str).tolist()
                target_values = target_df[col].fillna('').astype(str).tolist()
                if sorted(original_values) != sorted(target_values):
                    raise DatabaseError(f"Data mismatch in column {col}")
        except Exception as e:
            raise DatabaseError(f"Data integrity check failed: {str(e)}")
        
        # 7. Verify specific records
        try:
            test_records = [
                {
                    'fund_code': 'PGF 1100',
                    'class': 'A',
                    'mer': 2.47,
                    'trailer': 1.00,
                    'currency': 'CAD'
                },
                {
                    'fund_code': 'PGF 1113',
                    'class': 'F2',
                    'mer': 1.41,
                    'trailer': None,
                    'currency': 'CAD'
                }
            ]
            
            for record in test_records:
                cursor.execute("""
                    SELECT * FROM borinst.FundClassFee 
                    WHERE fund_code = %s AND class = %s
                """, (record['fund_code'], record['class']))
                result = cursor.fetchone()
                
                if result is None:
                    raise DatabaseError(f"Record not found: {record['fund_code']} - {record['class']}")
                if float(result['mer']) != record['mer']:
                    raise DatabaseError(f"MER mismatch for {record['fund_code']} - {record['class']}")
                if record['trailer'] is not None and float(result['trailer']) != record['trailer']:
                    raise DatabaseError(f"Trailer mismatch for {record['fund_code']} - {record['class']}")
                if result['currency'] != record['currency']:
                    raise DatabaseError(f"Currency mismatch for {record['fund_code']} - {record['class']}")
        except mysql.connector.Error as e:
            raise DatabaseError(f"Database error while verifying specific records: {str(e)}")
            
    except WorkflowTestError as e:
        pytest.fail(f"Workflow test failed: {str(e)}")
    except Exception as e:
        pytest.fail(f"Unexpected error in workflow test: {str(e)}")
    finally:
        # Cleanup
        try:
            if 'cursor' in locals():
                cursor.close()
        except Exception:
            pass 