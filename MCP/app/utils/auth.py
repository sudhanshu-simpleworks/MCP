import os
import jwt
from jwt import PyJWKClient
from typing import Any, Optional

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

def create_auth0_verifier():
    if not AUTH0_DOMAIN or not AUTH0_AUDIENCE:
        return None

    url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    jwk_client = PyJWKClient(url)

    # Make this async to be compatible with middleware calls if needed, 
    # though PyJWKClient is synchronous, wrapping it is fine.
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
            print(f"Auth Error: {e}")
            return None

    return verify_token
