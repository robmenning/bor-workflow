#!/usr/bin/env python3
"""
Test Factset Out Holdings Workflow

This script tests the Factset Out Holdings workflow with proper JWT authentication.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.workflows.factset_out_hold import factset_out_hold_flow


def test_factset_out_hold_workflow():
    """Test the Factset Out Holdings workflow"""
    print("Testing Factset Out Holdings workflow...")
    print("=" * 50)
    
    # Set required environment variables for testing
    os.environ['NEXTAUTH_SECRET'] = 'test-secret-for-jwt-validation'
    os.environ['API_PORT'] = '4410'  # Dev port
    
    try:
        # Test parameters
        test_params = {
            'date_valid': '2025-04-14',
            'port_id': None,
            'file_name': 'test_holdings.txt',
            'delimiter': '|',
            'api_host': 'bor-api',
            'api_port': '4410'
        }
        
        print(f"Test parameters: {test_params}")
        
        # Run the workflow
        result = factset_out_hold_flow(**test_params)
        
        print(f"✅ Workflow completed successfully")
        print(f"Output file: {result}")
        
        # Check if file was created
        if os.path.exists(result):
            file_size = os.path.getsize(result)
            print(f"✅ Output file created: {result} ({file_size} bytes)")
            
            # Read first few lines to verify content
            with open(result, 'r') as f:
                lines = f.readlines()
                print(f"✅ File contains {len(lines)} lines")
                if lines:
                    print(f"First line: {lines[0].strip()}")
        else:
            print(f"❌ Output file not found: {result}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Workflow test failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False


def main():
    """Run the Factset Out Holdings workflow test"""
    print("🧪 Testing Factset Out Holdings Workflow")
    print("=" * 50)
    
    success = test_factset_out_hold_workflow()
    
    print("=" * 50)
    if success:
        print("🎉 Factset Out Holdings workflow test passed!")
    else:
        print("❌ Factset Out Holdings workflow test failed!")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 