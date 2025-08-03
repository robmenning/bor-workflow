#!/usr/bin/env python3
"""
Test script for Mellon Duration ETL workflow - step by step testing
"""

import os
import sys
import csv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_file_processing():
    """Test the file processing logic step by step"""
    
    # Test file
    file_path = "/var/lib/mysql-files/ftpetl/incoming/660600017Duration_Report.csv"
    
    print("=== Step 1: Extract Account Name and Date ===")
    
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
    
    print(f"Account Name: {account_name}")
    print(f"Report Date: {report_date}")
    
    if not account_name or not report_date:
        print("ERROR: Could not extract account name or date from file")
        return False
    
    print("=== Step 2: Create Temporary File with Proper CSV Parsing ===")
    
    # Create a temporary file with the header information added to each data row
    temp_file_path = file_path + '.temp'
    
    with open(file_path, 'r', encoding='utf-8-sig') as infile, open(temp_file_path, 'w', encoding='utf-8', newline='') as outfile:
        lines = infile.readlines()
        
        # Write the column header row with AccountName and ReportDate added
        if len(lines) >= 3:
            # Get the original column headers (line 3) using CSV parsing
            csv_reader = csv.reader([lines[2]])
            original_headers = next(csv_reader)
            # Add AccountName and ReportDate at the beginning
            new_headers = ['AccountName', 'ReportDate'] + original_headers
            
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(new_headers)
            
            # Process data rows (starting from line 4) using CSV parsing
            for i in range(3, len(lines)):
                line = lines[i].strip()
                if line and not line.startswith('Total Investments') and not line.startswith(',,,,,,,,,,,'):
                    # Parse the line using CSV reader
                    csv_reader = csv.reader([line])
                    row_data = next(csv_reader)
                    
                    # Add AccountName and ReportDate to each data row
                    new_row = [account_name, report_date] + row_data
                    csv_writer.writerow(new_row)
    
    print(f"Temporary file created: {temp_file_path}")
    
    print("=== Step 3: Inspect Temporary File ===")
    
    # Debug: Print first few lines of temporary file
    with open(temp_file_path, 'r') as debug_file:
        debug_lines = debug_file.readlines()
        print(f"Total lines in temp file: {len(debug_lines)}")
        print(f"First 5 lines of temp file:")
        for i, line in enumerate(debug_lines[:5]):
            print(f"Line {i+1}: {line.strip()}")
    
    print("=== Step 4: Check AssetGroup Values with CSV Parsing ===")
    
    # Check the AssetGroup column (7th column, index 6) in the data rows using CSV parsing
    with open(temp_file_path, 'r') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)  # Skip header
        print(f"Header columns: {header}")
        
        for i, row in enumerate(csv_reader):
            if i >= 5:  # Only check first 5 data rows
                break
            if len(row) > 6:
                asset_group = row[6].strip()
                print(f"Row {i+1} AssetGroup: '{asset_group}' (length: {len(asset_group)})")
                print(f"Row {i+1} full row: {row}")
    
    # Clean up temporary file
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
        print(f"Cleaned up temporary file: {temp_file_path}")
    
    return True

if __name__ == "__main__":
    test_file_processing() 