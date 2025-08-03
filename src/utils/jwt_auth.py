"""JWT Authentication Utilities for BOR Workflow"""
import os
import time
import jwt
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from prefect import task


@task(name="generate-jwt-token")
def generate_jwt_token(
    user_id: str = "workflow-service",
    email: str = "workflow-service@bor-system.com",
    name: str = "BOR Workflow Service",
    bor_user_id: int = 999,
    role_ids: List[str] = None,
    permission_ids: List[str] = None,
    expiration_days: int = 30
) -> str:
    """
    Generate a JWT token for bor-workflow service authentication.
    
    Args:
        user_id: User identifier (required)
        email: User email address
        name: User display name
        bor_user_id: Numeric user ID
        role_ids: Array of role identifiers (required for authorization)
        permission_ids: Array of permission identifiers (required for authorization)
        expiration_days: Token expiration in days
    
    Returns:
        JWT token string
    
    Raises:
        ValueError: If JWT secret is not configured
    """
    # Get JWT secret from environment (same as bor-api uses)
    jwt_secret = os.getenv('NEXTAUTH_SECRET')
    if not jwt_secret:
        raise ValueError("NEXTAUTH_SECRET environment variable not set")
    
    # Set default roles and permissions for workflow service
    if role_ids is None:
        role_ids = ["dataadmin"]
    if permission_ids is None:
        permission_ids = ["port:read", "instr:read"]
    
    # Create payload according to bor-api specifications
    current_time = int(time.time())
    payload = {
        "id": user_id,
        "name": name,
        "email": email,
        "borUserId": bor_user_id,
        "roleIds": role_ids,
        "permissionIds": permission_ids,
        "isApproved": True,
        "iat": current_time,
        "exp": current_time + (expiration_days * 24 * 60 * 60)
    }
    
    # Generate token using HS256 algorithm (same as bor-api)
    return jwt.encode(payload, jwt_secret, algorithm="HS256")


@task(name="validate-jwt-token")
def validate_jwt_token(token: str) -> Dict:
    """
    Validate a JWT token using the same logic as bor-api.
    
    Args:
        token: JWT token string
    
    Returns:
        Decoded token payload
    
    Raises:
        ValueError: If JWT secret is not configured
        jwt.InvalidTokenError: If token is invalid or expired
    """
    jwt_secret = os.getenv('NEXTAUTH_SECRET')
    if not jwt_secret:
        raise ValueError("NEXTAUTH_SECRET environment variable not set")
    
    # Decode and validate token
    payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
    
    # Additional validation checks (matching bor-api logic)
    if not payload.get("isApproved", False):
        raise ValueError("Account not approved")
    
    if not payload.get("roleIds"):
        raise ValueError("No roles assigned")
    
    return payload


def get_auth_headers(token: str) -> Dict[str, str]:
    """
    Get authentication headers for API requests.
    
    Args:
        token: JWT token string
    
    Returns:
        Dictionary with Authorization and Content-Type headers
    """
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def is_token_expired(token: str) -> bool:
    """
    Check if a JWT token is expired.
    
    Args:
        token: JWT token string
    
    Returns:
        True if token is expired, False otherwise
    """
    try:
        # Decode without verification to check expiration
        payload = jwt.decode(token, options={"verify_signature": False})
        return time.time() >= payload.get('exp', 0)
    except Exception:
        return True


def get_service_token() -> str:
    """
    Get the service token from environment or generate a new one.
    
    Returns:
        JWT token string for service authentication
    """
    # First, try to get existing token from environment
    token = os.getenv('BOR_API_TOKEN')
    
    if token and not is_token_expired(token):
        return token
    
    # Generate new token if none exists or current one is expired
    return generate_jwt_token() 