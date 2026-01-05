import os
import jwt
from jwt import PyJWKClient
from mcp.server.fastmcp import Context
from typing import Any, Optional

# Load env variables
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

def create_auth0_verifier():
    """
    Creates a verifier function that checks JWT tokens against Auth0.
    """
    if not AUTH0_DOMAIN or not AUTH0_AUDIENCE:
        return None

    jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    jwk_client = PyJWKClient(jwks_url)

    async def verify_token(token: str) -> Optional[Any]:
        try:
            signing_key = jwk_client.get_signing_key_from_jwt(token)
            
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=AUTH0_AUDIENCE,
                issuer=f"https://{AUTH0_DOMAIN}/",
            )
            return payload
        except Exception as e:
            return None

    return verify_token
