import os
from typing import Dict, List, Optional
import pandas as pd
import pdfplumber
# from prefect import flow, task
# from prefect.logging import get_run_logger


# to run stand alone, decorators and logging have been commented out

# Fund code lookup dictionary
FUND_CODES = {
    "Pender Partners Fund": "1100",
    "Pender Small Cap Opportunities Fund": "300",
    "Pender Value Fund": "200",
    "Pender US Small/Mid Cap Equity Fund": "1800",
    "Pender Strategic Growth and Income Fund": "1000",
    "Pender Corporate Bond Fund": "500",
    "Pender Bond Universe Fund": "1400",
    "Pender Alternative Absolute Return Fund": "2000",
    "Pender Alternative Arbitrage Fund": "2100",
    "Pender Alternative Arbitrage Plus Fund": "2200",
    "Pender Alternative Multi-Strategy Income Fund": "1200",
    "Pender Alternative Special Situations Fund": "1500"
}

# @task
def extract_text_from_pdf(pdf_path: str) -> List[Dict]:
    """Extract text and tables from PDF file."""
    # logger = get_run_logger()
    # logger.info(f"Processing PDF file: {pdf_path}")
    
    holdings_data = []
    current_fund = None
    current_date = None
    in_holdings_section = False
    
    print(f"Opening PDF file: {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        print(f"PDF has {len(pdf.pages)} pages")
        
        for page_num, page in enumerate(pdf.pages, 1):
            print(f"\nProcessing page {page_num}")
            text = page.extract_text()
            if not text:
                print(f"No text found on page {page_num}")
                continue
                
            lines = text.split('\n')
            print(f"Found {len(lines)} lines of text")
            
            for i, line in enumerate(lines):
                # Look for fund name
                if line in FUND_CODES:
                    current_fund = line
                    in_holdings_section = False
                    print(f"Found fund: {current_fund}")
                    continue
                    
                # Look for "Schedule of Investment Portfolio"
                if "Schedule of Investment Portfolio" in line:
                    in_holdings_section = True
                    print(f"Found holdings section on page {page_num}")
                    # Get date from next line
                    if i + 1 < len(lines):
                        current_date = lines[i + 1].strip()
                        print(f"Found date: {current_date}")
                    # Print raw tables for this page and exit for debug
                    tables = page.extract_tables()
                    print(f"\nDEBUG: Raw tables from extract_tables() on page {page_num}:")
                    for t in tables:
                        print(t)
                    print("\nExiting after debug print.")
                    exit(0)
                    # continue
                
                # Extract table data
                if in_holdings_section and current_fund and current_date:
                    tables = page.extract_tables()
                    print(f"Found {len(tables)} tables on page {page_num}")
                    
                    for table_num, table in enumerate(tables, 1):
                        if not table or len(table) < 2:
                            print(f"Skipping empty table or table without headers (table {table_num})")
                            continue
                            
                        print(f"Processing table {table_num} with {len(table)} rows")
                        
                        # Process table rows
                        for row_num, row in enumerate(table[1:], 1):  # Skip header row
                            if not row or len(row) < 4:
                                print(f"Skipping invalid row {row_num}: {row}")
                                continue
                                
                            # Skip total rows
                            if "Total net assets attributable to holders of redeemable" in str(row):
                                in_holdings_section = False
                                print("Found end of holdings section")
                                continue
                                
                            # Skip rows that are totals or subtotals
                            if any(x in str(row[0]).lower() for x in ['total', 'subtotal']):
                                print(f"Skipping total/subtotal row: {row[0]}")
                                continue
                                
                            # Extract data
                            description = row[0] if row[0] else ""
                            currency = row[1] if row[1] else ""
                            units = row[2] if row[2] else ""
                            cost = row[3] if row[3] else ""
                            mv = row[4] if len(row) > 4 else ""
                            
                            # Skip if this is a total row
                            if description.strip() == "Total":
                                print(f"Skipping total row: {description}")
                                continue
                            
                            # Extract issuer and issue from description
                            issuer = ""
                            issue = ""
                            if description:
                                parts = description.split()
                                if parts:
                                    issuer = parts[0]
                                    issue = parts[-1] if len(parts) > 1 else ""
                            
                            holding = {
                                "date": current_date,
                                "fund_name": current_fund,
                                "fund_code": FUND_CODES.get(current_fund, "###"),
                                "issuer": issuer,
                                "issue": issue,
                                "currency": currency,
                                "units": units,
                                "cost": cost,
                                "mv": mv
                            }
                            holdings_data.append(holding)
                            print(f"Added holding: {holding}")
    
    print(f"\nTotal holdings extracted: {len(holdings_data)}")
    return holdings_data

# @task
def save_to_csv(data: List[Dict], output_path: str):
    """Save extracted data to CSV file."""
    # logger = get_run_logger()
    # logger.info(f"Saving data to CSV: {output_path}")
    
    print(f"\nSaving {len(data)} records to {output_path}")
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    # logger.info(f"Successfully saved {len(data)} records to {output_path}")
    print("Save complete")

# @flow(name="PDF Holdings Extraction", persist_result=False)
def extract_holdings_workflow(
    input_pdf: str = "tests/data/01-Pender-Mutual-Funds-FS-ENG-2024.12.31-conformed.pdf",
    output_csv: str = "tests/data/holdweb-20241231.csv"
):
    """Extract holdings data from PDF and save to CSV."""
    # logger = get_run_logger()
    # logger.info("Starting PDF holdings extraction workflow")
    
    print(f"Starting extraction from {input_pdf}")
    
    # Extract data from PDF
    holdings_data = extract_text_from_pdf(input_pdf)
    
    # Save to CSV
    save_to_csv(holdings_data, output_csv)
    
    # logger.info("PDF holdings extraction workflow completed")
    return len(holdings_data)

if __name__ == "__main__":
    extract_holdings_workflow() 