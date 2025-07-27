#!/bin/bash
# Test script for Mellon integration stored procedure

echo "Testing Mellon integration stored procedure task..."
echo "=================================================="

# Run the test
python3 tests/test_mellon_integration_proc.py

if [ $? -eq 0 ]; then
    echo "✅ Test completed successfully"
else
    echo "❌ Test failed"
    exit 1
fi 