import pytest
from prefect.testing.utilities import prefect_test_harness
from src.workflows.file_ingestion import file_ingestion_workflow
import os
import tempfile
import shutil

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

@pytest.fixture
def test_file():
    # Create a temporary test file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Test file content")
        return f.name

@pytest.fixture
def setup_test_directories():
    # Create test directories in the shared volume
    base_dir = "/home/vsftpd/ftpetl"
    incoming_dir = f"{base_dir}/incoming"
    processed_dir = f"{base_dir}/processed"
    
    os.makedirs(incoming_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    yield
    
    # Cleanup
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

def test_file_ingestion_workflow(test_file):
    """Test the complete file ingestion workflow using FTP"""
    try:
        # Run the workflow with test parameters
        result = file_ingestion_workflow(
            file_path=test_file,
            ftp_host="localhost",  # Use localhost for testing
            ftp_port=4470,  # Development port from system scheme
            ftp_user="ftpetl",  # Test user
            ftp_pass="lteptf",  # Test password
            db_host="localhost",
            db_port=4420,  # Development port from system scheme
            db_user="borAllSvc",
            db_pass="u67nyNgomZ",
            db_name="bormeta",
            proc_name="usp_FundClassFee_Load",
            use_direct_access=False  # Use FTP for this test
        )
        assert result is True
    finally:
        # Clean up test file
        if os.path.exists(test_file):
            os.unlink(test_file)

def test_direct_file_access(test_file, setup_test_directories):
    """Test the workflow using direct volume access"""
    try:
        # Copy test file to incoming directory
        incoming_path = "/home/vsftpd/ftpetl/incoming/test_file.txt"
        shutil.copy2(test_file, incoming_path)
        
        # Run the workflow with direct access
        result = file_ingestion_workflow(
            file_path=incoming_path,
            db_host="localhost",
            db_port=4420,
            db_user="borAllSvc",
            db_pass="u67nyNgomZ",
            db_name="bormeta",
            proc_name="usp_FundClassFee_Load",
            use_direct_access=True
        )
        assert result is True
        
        # Verify file was moved to processed directory
        processed_path = "/home/vsftpd/ftpetl/processed/test_file.txt"
        assert os.path.exists(processed_path)
        assert not os.path.exists(incoming_path)
    finally:
        # Clean up test file
        if os.path.exists(test_file):
            os.unlink(test_file)

def test_file_not_found():
    """Test workflow with non-existent file"""
    with pytest.raises(FileNotFoundError):
        file_ingestion_workflow(
            file_path="nonexistent_file.txt",
            ftp_host="localhost",
            ftp_port=4470,
            ftp_user="ftpetl",
            ftp_pass="lteptf"
        ) 