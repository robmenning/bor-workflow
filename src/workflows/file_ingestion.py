from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import os
from ftplib import FTP
import mysql.connector
from dotenv import load_dotenv
import shutil
import subprocess

# Load environment variables
load_dotenv()

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_local_file(file_path: str) -> bool:
    """Check if file exists in local directory"""
    return os.path.exists(file_path)

@task(retries=3, retry_delay_seconds=60)
def upload_to_ftp(file_path: str, ftp_host: str, ftp_port: int, 
                 ftp_user: str, ftp_pass: str) -> bool:
    """Upload file to FTP server"""
    try:
        with FTP() as ftp:
            ftp.connect(host=ftp_host, port=ftp_port)
            ftp.login(user=ftp_user, passwd=ftp_pass)
            
            with open(file_path, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(file_path)}', file)
        return True
    except Exception as e:
        print(f"FTP upload failed: {str(e)}")
        return False

@task(retries=3, retry_delay_seconds=60)
def move_file_to_processed(file_path: str, ftp_user: str = "ftpetl") -> bool:
    """Move file from incoming to processed directory using direct volume access"""
    try:
        filename = os.path.basename(file_path)
        incoming_path = f"/home/vsftpd/{ftp_user}/incoming/{filename}"
        processed_path = f"/home/vsftpd/{ftp_user}/processed/{filename}"
        
        if not os.path.exists(incoming_path):
            raise FileNotFoundError(f"File not found in incoming directory: {incoming_path}")
            
        shutil.move(incoming_path, processed_path)
        return True
    except Exception as e:
        print(f"File move failed: {str(e)}")
        return False

@task(retries=3, retry_delay_seconds=60)
def load_file_to_borarch(file_path: str, db_host: str, db_port: int, db_user: str, db_pass: str, db_name: str) -> bool:
    """Load file into borarch.FundClassFee using mysqlimport via shared volume."""
    try:
        # Convert 'Closed' and empty strings to NULL in a temp file
        import pandas as pd
        temp_file = f"/tmp/cleaned_{os.path.basename(file_path)}"
        df = pd.read_csv(file_path)
        for col in ['min_investment_initial', 'min_investment_subsequent']:
            if col in df.columns:
                df[col] = df[col].replace({'Closed': None, '': None})
        for col in ['mer', 'trailer']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.to_csv(temp_file, index=False)
        # Run mysqlimport
        cmd = [
            'mysqlimport',
            f'--host={db_host}',
            f'--port={db_port}',
            f'--user={db_user}',
            f'--password={db_pass}',
            '--fields-terminated-by=,',
            '--ignore-lines=1',
            db_name,
            temp_file
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"mysqlimport failed: {result.stderr}")
            return False
        os.remove(temp_file)
        return True
    except Exception as e:
        print(f"Load to borarch failed: {str(e)}")
        return False

@task(retries=3, retry_delay_seconds=60)
def execute_stored_procedure(proc_name: str, db_host: str, db_port: int,
                           db_user: str, db_pass: str, db_name: str) -> bool:
    """Execute stored procedure in MySQL database"""
    try:
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        cursor = conn.cursor()
        cursor.callproc(proc_name)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database procedure execution failed: {str(e)}")
        return False

@flow(name="File Ingestion Workflow")
def file_ingestion_workflow(
    file_path: str,
    ftp_host: str = "bor-files",
    ftp_port: int = 21,
    ftp_user: str = None,
    ftp_pass: str = None,
    db_host: str = "bor-db",
    db_port: int = 3306,
    db_user: str = None,
    db_pass: str = None,
    db_name: str = "borarch",
    proc_name: str = "usp_FundClassFee_Load",
    use_direct_access: bool = True
):
    """
    Main workflow for file ingestion process:
    1. Check if file exists locally
    2. Upload file to FTP server or move directly if using shared volume
    3. Load file into borarch.FundClassFee
    4. Move file to processed
    5. Execute stored procedure to process the file
    """
    # Use environment variables if not provided
    ftp_user = ftp_user or os.getenv("FTP_ETL_USER")
    ftp_pass = ftp_pass or os.getenv("FTP_ETL_PASS")
    db_user = db_user or os.getenv("DB_ALL_SVC_USER")
    db_pass = db_pass or os.getenv("DB_ALL_SVC_USER_PASSWORD")

    # Step 1: Check if file exists
    file_exists = check_local_file(file_path)
    if not file_exists:
        raise FileNotFoundError(f"File not found: {file_path}")

    # Step 2: Handle file transfer
    if use_direct_access:
        # Move file to incoming directory for borarch
        filename = os.path.basename(file_path)
        incoming_path = f"/home/vsftpd/{ftp_user}/incoming/{filename}"
        shutil.copy(file_path, incoming_path)
        # Step 3: Load file into borarch.FundClassFee
        load_success = load_file_to_borarch(
            incoming_path,
            db_host,
            db_port,
            db_user,
            db_pass,
            db_name
        )
        if not load_success:
            raise Exception("Load to borarch failed")
        # Step 4: Move file to processed
        move_success = move_file_to_processed(file_path, ftp_user)
        if not move_success:
            raise Exception("File move to processed failed")
    else:
        upload_success = upload_to_ftp(
            file_path=file_path,
            ftp_host=ftp_host,
            ftp_port=ftp_port,
            ftp_user=ftp_user,
            ftp_pass=ftp_pass
        )
        if not upload_success:
            raise Exception("FTP upload failed")
        # Assume FTP puts file in incoming, then load and move as above
        filename = os.path.basename(file_path)
        incoming_path = f"/home/vsftpd/{ftp_user}/incoming/{filename}"
        load_success = load_file_to_borarch(
            incoming_path,
            db_host,
            db_port,
            db_user,
            db_pass,
            db_name
        )
        if not load_success:
            raise Exception("Load to borarch failed")
        move_success = move_file_to_processed(file_path, ftp_user)
        if not move_success:
            raise Exception("File move to processed failed")

    # Step 5: Execute stored procedure
    proc_success = execute_stored_procedure(
        proc_name=proc_name,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_pass=db_pass,
        db_name=db_name
    )
    if not proc_success:
        raise Exception("Stored procedure execution failed")

    return True 