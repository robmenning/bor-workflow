#!/bin/bash
# Test JWT Authentication Utilities

echo "🧪 Testing JWT Authentication Utilities"
echo "======================================"

# Check if Python script exists
if [ ! -f "tests/test_jwt_auth.py" ]; then
    echo "❌ JWT auth test script not found: tests/test_jwt_auth.py"
    exit 1
fi

# Run the JWT authentication test
echo "Running JWT authentication tests..."
python3 tests/test_jwt_auth.py

if [ $? -eq 0 ]; then
    echo "✅ JWT authentication tests completed successfully"
else
    echo "❌ JWT authentication tests failed"
    exit 1
fi

echo ""
echo "🎉 All JWT authentication tests passed!" 