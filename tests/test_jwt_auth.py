#!/usr/bin/env python3
"""
Test JWT Authentication Utilities

This script tests the JWT authentication utilities that match the bor-api specifications.
"""

import sys
import os
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.utils.jwt_auth import (
    generate_jwt_token,
    validate_jwt_token,
    get_auth_headers,
    is_token_expired,
    get_service_token
)


def test_jwt_token_generation():
    """Test JWT token generation with bor-api compatible payload"""
    print("Testing JWT token generation...")
    
    # Set required environment variable for testing
    os.environ['NEXTAUTH_SECRET'] = 'test-secret-for-jwt-validation'
    
    try:
        # Generate token with default parameters
        token = generate_jwt_token()
        print(f"✅ Generated token: {token[:50]}...")
        
        # Validate the token
        payload = validate_jwt_token(token)
        print(f"✅ Token validated successfully")
        print(f"   User ID: {payload.get('id')}")
        print(f"   Email: {payload.get('email')}")
        print(f"   Roles: {payload.get('roleIds')}")
        print(f"   Permissions: {payload.get('permissionIds')}")
        print(f"   Approved: {payload.get('isApproved')}")
        
        # Check required fields
        assert payload.get('id') == 'workflow-service'
        assert payload.get('email') == 'workflow-service@bor-system.com'
        assert payload.get('roleIds') == ['dataadmin']
        assert payload.get('permissionIds') == ['port:read', 'instr:read']
        assert payload.get('isApproved') is True
        assert 'iat' in payload
        assert 'exp' in payload
        
        print("✅ All required fields present and correct")
        return True
        
    except Exception as e:
        print(f"❌ Token generation failed: {str(e)}")
        return False


def test_auth_headers():
    """Test authentication headers generation"""
    print("\nTesting authentication headers...")
    
    try:
        token = "test-jwt-token"
        headers = get_auth_headers(token)
        
        expected_headers = {
            'Authorization': 'Bearer test-jwt-token',
            'Content-Type': 'application/json'
        }
        
        assert headers == expected_headers
        print("✅ Authentication headers generated correctly")
        return True
        
    except Exception as e:
        print(f"❌ Auth headers test failed: {str(e)}")
        return False


def test_token_expiration():
    """Test token expiration checking"""
    print("\nTesting token expiration...")
    
    try:
        # Test with expired token
        expired_token = generate_jwt_token(expiration_days=-1)  # Expired
        assert is_token_expired(expired_token)
        print("✅ Expired token correctly identified")
        
        # Test with valid token
        valid_token = generate_jwt_token(expiration_days=1)  # Valid for 1 day
        assert not is_token_expired(valid_token)
        print("✅ Valid token correctly identified")
        
        return True
        
    except Exception as e:
        print(f"❌ Token expiration test failed: {str(e)}")
        return False


def test_service_token():
    """Test service token retrieval"""
    print("\nTesting service token retrieval...")
    
    try:
        # Test without environment token
        if 'BOR_API_TOKEN' in os.environ:
            del os.environ['BOR_API_TOKEN']
        
        token = get_service_token()
        assert token is not None
        assert len(token) > 0
        print("✅ Service token generated when none exists")
        
        # Test with environment token
        test_token = "test-environment-token"
        os.environ['BOR_API_TOKEN'] = test_token
        token = get_service_token()
        assert token == test_token
        print("✅ Service token retrieved from environment")
        
        return True
        
    except Exception as e:
        print(f"❌ Service token test failed: {str(e)}")
        return False


def main():
    """Run all JWT authentication tests"""
    print("🧪 Testing JWT Authentication Utilities")
    print("=" * 50)
    
    tests = [
        test_jwt_token_generation,
        test_auth_headers,
        test_token_expiration,
        test_service_token
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All JWT authentication tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 