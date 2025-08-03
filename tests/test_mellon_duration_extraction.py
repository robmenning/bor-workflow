#!/usr/bin/env python3
"""
Test script for Mellon Duration ETL workflow
Tests the extraction of account name and date from CSV files
"""

import os

def test_account_date_extraction():
    """Test extraction of account name and date from CSV files"""
    
    # Test file paths
    test_files = [
        "tests/data/660600017Duration_Report.csv",
        "tests/data/660600027Duration_Report.csv",
        "tests/data/660610007Duration_Report.csv"
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            print(f"\nTesting file: {file_path}")
            
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
                
                if account_name and report_date:
                    print(f"  ✓ Account: {account_name}")
                    print(f"  ✓ Date: {report_date}")
                    
                    # Also test the column headers
                    if len(lines) >= 3:
                        headers = lines[2].strip().split(',')
                        print(f"  ✓ Column headers: {len(headers)} columns")
                        print(f"     First few: {headers[:3]}")
                else:
                    print(f"  ✗ Failed to extract account name or date")
                    
            except Exception as e:
                print(f"  ✗ Error processing file: {e}")
        else:
            print(f"\nFile not found: {file_path}")

if __name__ == "__main__":
    test_account_date_extraction() 