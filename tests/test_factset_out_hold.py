#!/usr/bin/env python3
"""
Test script for the Factset Out Holdings workflow
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.workflows.factset_out_hold import factset_out_hold_flow

def test_factset_out_hold():
    """Test the Factset Out Holdings workflow"""
    
    print("Testing Factset Out Holdings workflow...")
    print("========================================")
    
    try:
        # Test with default parameters
        result = factset_out_hold_flow(
            date_valid="2025-04-14",
            port_id=None,
            file_name="",
            delimiter="\t",
            output_dir="/var/lib/mysql-files/ftpetl/outgoing/",
            api_host="bor-api",
            api_port=4000
        )
        
        if result:
            print("✅ Factset Out Holdings workflow completed successfully")
        else:
            print("❌ Factset Out Holdings workflow failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Factset Out Holdings workflow: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_factset_out_hold()
    sys.exit(0 if success else 1) 