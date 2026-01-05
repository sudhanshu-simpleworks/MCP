import os
import json
import sys
from fastmcp import Context
from app.utils.logger import logger

def verify_access(ctx: Context) -> str:
    """
    Verifies the API Key against the registry.
    """
    registry_str = os.getenv("MCP_CLIENT_REGISTRY", "{}")
    try:
        client_registry = json.loads(registry_str)
    except json.JSONDecodeError:
        logger.error("❌ MCP_CLIENT_REGISTRY is not valid JSON.")
        raise ValueError("Server Configuration Error")

    request_key = None
    
    if not request_key:
        request_key = os.getenv("MCP_SERVER_API_KEY")

    if not request_key and ctx.meta and "headers" in ctx.meta:
        headers = {k.lower(): v for k, v in ctx.meta["headers"].items()}
        request_key = headers.get("authorization") or headers.get("x-api-key")

    if request_key and request_key.startswith("Bearer "):
        request_key = request_key.split(" ")[1]


    if not request_key:
        raise ValueError("Unauthorized: No API Key provided.")

    # Verify Key
    key_lookup = {v: k for k, v in client_registry.items()}

    if request_key in key_lookup:
        client_name = key_lookup[request_key]
        logger.info(f"🔓 Authorized access: {client_name}")
        return client_name

    logger.warning(f"🚫 Unauthorized access attempt using key: {request_key}")
    raise ValueError("Unauthorized: Invalid API Key")