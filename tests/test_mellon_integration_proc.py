#!/usr/bin/env python3
"""
Test script for the Mellon integration stored procedure task
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.workflows.mellon_holdings_etl import run_mellon_integration_proc

def test_integration_proc():
    """Test the integration stored procedure task"""
    
    # Database configuration for testing
    db_config = {
        "host": "bor-db",
        "port": 3306,
        "user": "borAllAdmin", 
        "password": "kBu9pjz2vi",
        "database": "borarch"  # This will be changed to bormeta in the task
    }
    
    print("Testing Mellon integration stored procedure task...")
    print(f"DB config: {db_config['host']}:{db_config['port']}, user: {db_config['user']}")
    
    try:
        # Run the integration procedure task
        result = run_mellon_integration_proc(db_config)
        
        if result:
            print("✅ Integration procedure task completed successfully")
        else:
            print("❌ Integration procedure task failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing integration procedure task: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_integration_proc()
    sys.exit(0 if success else 1) 