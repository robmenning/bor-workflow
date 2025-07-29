"""
Factset Out Holdings Workflow

This workflow retrieves holdings data from the bor-api and produces output files
in the specified format for external consumption.
"""

import os
import csv
import requests
from typing import List, Dict, Optional
from datetime import datetime
from prefect import flow, task, get_run_logger
from src.utils.jwt_auth import get_service_token, get_auth_headers


@task(name="fetch-holdings-data")
def fetch_holdings_data(
    date_valid: str,
    port_id: Optional[int] = None,
    api_host: str = "bor-api",
    api_port: str = "4410"  # Default for dev, overridden by deployment
) -> List[Dict]:
    """
    Fetch holdings data from bor-api endpoint with proper JWT authentication.
    
    Args:
        date_valid: Date for which to fetch holdings data (YYYY-MM-DD format)
        port_id: Optional portfolio ID to filter by
        api_host: bor-api hostname
        api_port: bor-api port number
    
    Returns:
        List of holdings data dictionaries
    
    Raises:
        Exception: If API request fails or returns error
    """
    logger = get_run_logger()
    
    try:
        # Get JWT token for authentication
        logger.info("Getting JWT token for API authentication...")
        jwt_token = get_service_token()
        
        # Build API URL and parameters
        base_url = f"http://{api_host}:{api_port}/data/factset-holdings"
        params = {"dateValId": date_valid}
        if port_id is not None:
            params["portId"] = port_id
        
        # Get authentication headers
        headers = get_auth_headers(jwt_token)
        
        logger.info(f"Fetching data from: {base_url}")
        logger.info(f"Parameters: {params}")
        
        # Make authenticated API request
        response = requests.get(
            base_url, 
            params=params, 
            headers=headers, 
            timeout=30
        )
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Handle bor-api response format: {"success": true, "data": [...]}
        if isinstance(data, dict):
            if 'success' in data and 'data' in data:
                if data['success']:
                    holdings_data = data['data']
                    if not isinstance(holdings_data, list):
                        logger.error(f"Expected list in 'data' field, got: {type(holdings_data)}")
                        raise Exception(f"Invalid response format: expected list in 'data' field, got {type(holdings_data)}")
                else:
                    logger.error(f"API returned success: false")
                    raise Exception("API request failed: success is false")
            else:
                logger.error(f"Unexpected response format: {list(data.keys())}")
                raise Exception(f"Unexpected response format: expected 'success' and 'data' fields, got {list(data.keys())}")
        elif isinstance(data, list):
            holdings_data = data
        else:
            logger.error(f"Expected dict or list response, got: {type(data)}")
            raise Exception(f"Invalid response format: expected dict or list, got {type(data)}")
        
        logger.info(f"Successfully fetched {len(holdings_data)} holdings records")
        return holdings_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching data: {str(e)}")
        raise


@task(name="write-holdings-file")
def write_holdings_file(
    holdings_data: List[Dict], 
    output_path: str, 
    delimiter: str = "\t", 
    include_header: bool = True
) -> bool:
    """
    Write holdings data to a delimited file.
    
    Args:
        holdings_data: List of holdings data dictionaries
        output_path: Full path to output file
        delimiter: Field delimiter (must be exactly 1 character)
        include_header: Whether to include header row
    
    Returns:
        True if successful, False otherwise
    """
    logger = get_run_logger()
    
    # Validate delimiter
    if not delimiter:
        logger.error("Error: Delimiter cannot be empty")
        return False
    
    if len(delimiter) != 1:
        logger.error(f"Error: Delimiter must be exactly 1 character. Got '{delimiter}' (length: {len(delimiter)})")
        return False
    
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        
        # Write data to file
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            if not holdings_data:
                logger.warning("No holdings data to write")
                return True
            
            # Get field names from first record
            fieldnames = list(holdings_data[0].keys())
            
            writer = csv.DictWriter(
                csvfile, 
                fieldnames=fieldnames, 
                delimiter=delimiter
            )
            
            # Write header if requested
            if include_header:
                writer.writeheader()
            
            # Write data rows
            writer.writerows(holdings_data)
        
        logger.info(f"Successfully wrote {len(holdings_data)} records to {output_path}")
        logger.info(f"File size: {os.path.getsize(output_path)} bytes")
        return True
        
    except Exception as e:
        logger.error(f"Error writing file: {str(e)}")
        return False


@flow(name="factset-out-hold")
def factset_out_hold_flow(
    date_valid: str,
    port_id: Optional[int] = None,
    file_name: Optional[str] = None,
    delimiter: str = "|",
    api_host: str = "bor-api",
    api_port: str = "4410"  # Default for dev, overridden by deployment
) -> str:
    """
    Main workflow for generating Factset Out Holdings files.
    
    Args:
        date_valid: Date for which to fetch holdings data (YYYY-MM-DD format)
        port_id: Optional portfolio ID to filter by
        file_name: Output filename (if None, auto-generated)
        delimiter: Field delimiter for output file
        api_host: bor-api hostname
        api_port: bor-api port number
    
    Returns:
        Path to the generated output file
    
    Raises:
        Exception: If workflow fails
    """
    logger = get_run_logger()
    
    logger.info("Starting Factset Out Holdings Flow")
    logger.info(f"Date: {date_valid}")
    logger.info(f"Port ID: {port_id}")
    logger.info(f"Delimiter: '{delimiter}' (type: {type(delimiter)}, length: {len(delimiter)})")
    
    # Step 1: Fetch holdings data from API
    logger.info("Step 1: Fetching holdings data...")
    holdings_data = fetch_holdings_data(
        date_valid=date_valid,
        port_id=port_id,
        api_host=api_host,
        api_port=api_port
    )
    
    # Step 2: Generate output filename if not provided
    if file_name is None:
        # Convert date from YYYY-MM-DD to YYYYMMDD format
        date_obj = datetime.strptime(date_valid, "%Y-%m-%d")
        date_str = date_obj.strftime("%Y%m%d")
        file_name = f"holdings_{date_str}.txt"
    
    # Step 3: Write the file
    output_path = f"/var/lib/mysql-files/ftpetl/outgoing/{file_name}"
    logger.info(f"Step 2: Writing file to {output_path}")
    
    success = write_holdings_file(
        holdings_data=holdings_data,
        output_path=output_path,
        delimiter=delimiter,
        include_header=True
    )
    
    if not success:
        raise Exception("Failed to write holdings file")
    
    logger.info("Factset Out Holdings Flow completed successfully")
    return output_path


if __name__ == "__main__":
    # Test the workflow locally
    try:
        result = factset_out_hold_flow(
            date_valid="2025-04-14",
            port_id=None,
            file_name="test_holdings.txt",
            delimiter="|"
        )
        print(f"Workflow completed successfully. Output: {result}")
    except Exception as e:
        print(f"Workflow failed: {str(e)}")
        exit(1) 