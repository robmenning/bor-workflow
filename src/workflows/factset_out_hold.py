"""


Factset Out Holdings Workflow

This workflow retrieves holdings data and creates output files for external systems.
Based on the Mellon holdings ETL workflow structure but working in reverse (producing files instead of ingesting).
"""

import os
import json
import csv
import requests
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from typing import List, Dict, Optional

@task(name="fetch-holdings-data")
def fetch_holdings_data(
    date_valid: str,
    port_id: Optional[int] = None,
    api_host: str = "bor-api",
    api_port: str = "4000"
) -> List[Dict]:
    """
    Fetch holdings data from the API endpoint
    
    Args:
        date_valid: Date in YYYY-MM-DD format
        port_id: Optional portfolio ID to filter by
        api_host: API hostname
        api_port: API port
    
    Returns:
        List of holdings records
    """
    try:
        # Build the API URL
        base_url = f"http://{api_host}:{api_port}/data/factset-holdings"
        params = {"dateValId": date_valid}
        
        if port_id is not None:
            params["portId"] = port_id
        
        print(f"Fetching data from: {base_url}")
        print(f"Parameters: {params}")
        
        # Make the API request
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        if isinstance(data, list):
            holdings_data = data
        elif isinstance(data, dict) and 'data' in data:
            holdings_data = data['data']
        else:
            holdings_data = [data]
        
        print(f"Retrieved {len(holdings_data)} holdings records")
        return holdings_data
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error fetching data: {str(e)}")
        raise

@task(name="write-holdings-file")
def write_holdings_file(
    holdings_data: List[Dict],
    output_path: str,
    delimiter: str = "\t",
    include_header: bool = True
) -> bool:
    """
    Write holdings data to a delimited text file
    
    Args:
        holdings_data: List of holdings records
        output_path: Path to the output file
        delimiter: Field delimiter (tab, pipe, comma, etc.)
        include_header: Whether to include column headers
    
    Returns:
        bool: True if file was written successfully
    """
    try:
        if not holdings_data:
            print("No data to write")
            return False
        
        # Get field names from the first record
        fieldnames = list(holdings_data[0].keys())
        
        # Validate delimiter
        if not delimiter or len(delimiter) != 1:
            print(f"Invalid delimiter: '{delimiter}' (length: {len(delimiter) if delimiter else 0})")
            return False
        
        print(f"Using delimiter: '{delimiter}' (length: {len(delimiter)})")
        
        # Write the file
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(
                csvfile, 
                fieldnames=fieldnames, 
                delimiter=delimiter,
                quoting=csv.QUOTE_MINIMAL
            )
            
            if include_header:
                writer.writeheader()
            
            for record in holdings_data:
                writer.writerow(record)
        
        print(f"Successfully wrote {len(holdings_data)} records to {output_path}")
        return True
        
    except Exception as e:
        print(f"Error writing file: {str(e)}")
        return False

@flow(name="factset-out-hold")
def factset_out_hold_flow(
    date_valid: str,
    port_id: Optional[int] = None,
    file_name: Optional[str] = None,
    delimiter: str = "\t",
    output_dir: str = "/var/lib/mysql-files/ftpetl/outgoing/",
    api_host: str = "bor-api",
    api_port: str = "4000"
) -> bool:
    """
    Main flow for creating Factset holdings output files
    
    Args:
        date_valid: Date in YYYY-MM-DD format
        port_id: Optional portfolio ID to filter by
        file_name: Output filename (if None, auto-generated)
        delimiter: Field delimiter character
        output_dir: Output directory path
        api_host: API hostname
        api_port: API port
    
    Returns:
        bool: True if workflow completed successfully
    """
    try:
        print("Starting Factset Out Holdings Flow")
        print(f"Date: {date_valid}")
        print(f"Port ID: {port_id}")
        print(f"Output directory: {output_dir}")
        print(f"Delimiter: '{delimiter}' (type: {type(delimiter)}, length: {len(delimiter) if delimiter else 0})")
        
        # Step 1: Fetch holdings data
        print("Step 1: Fetching holdings data...")
        holdings_data = fetch_holdings_data(
            date_valid=date_valid,
            port_id=port_id,
            api_host=api_host,
            api_port=api_port
        )
        
        if not holdings_data:
            print("No holdings data retrieved")
            return False
        
        # Step 2: Generate filename if not provided
        if file_name is None or file_name == "":
            port_suffix = f"_port{port_id}" if port_id else ""
            file_name = f"factset-holdings_{date_valid}{port_suffix}.txt"
        
        output_path = os.path.join(output_dir, file_name)
        
        # Step 3: Write the file
        print(f"Step 2: Writing file to {output_path}...")
        if not write_holdings_file(
            holdings_data=holdings_data,
            output_path=output_path,
            delimiter=delimiter,
            include_header=True
        ):
            raise Exception("Failed to write holdings file")
        
        print("Factset Out Holdings Flow completed successfully")
        return True
        
    except Exception as e:
        print(f"Factset Out Holdings Flow failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    # Example usage
    factset_out_hold_flow(
        date_valid="2025-04-14",
        port_id=1,
        file_name="factset-holdings-20250414-port1.txt",
        delimiter="\t"
    ) 