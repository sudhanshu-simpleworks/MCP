import sys
import os
import json
import asyncio
from typing import Optional, Dict, List, Any

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir
sys.path.append(project_root)

from dotenv import load_dotenv
env_path = os.path.join(project_root, ".env")
load_dotenv(env_path, override=True) 

# --- IMPORTS ---
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from sse_starlette.sse import EventSourceResponse

from app.utils.logger import logger 
from app.utils.auth import create_auth0_verifier
from app.agents.tools import crm_tools
from app.agents.mcp_agent import MCPAgent
from app.utils.util import MessageHandler

# --- AUTH MIDDLEWARE ---
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Allow health checks and SSE handshake (GET /sse) if needed without auth, 
        # BUT for security, usually /sse requires auth too.
        # However, the initial connection might need to be permissive if the client 
        # sends the token in query params (standard for EventSource), 
        # or headers if the client supports it.
        
        # Check for Authorization header
        auth_header = request.headers.get("Authorization")
        token = None
        
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
        elif "token" in request.query_params:
            # Fallback for EventSource connections that might use query params
            token = request.query_params["token"]

        if not token:
             # If strictly enforcing auth on everything:
             # return JSONResponse({"error": "Missing authentication"}, status_code=401)
             
             # If you want to allow unauthenticated discovery (risky but sometimes needed):
             # For now, let's enforce it.
             pass

        verifier = create_auth0_verifier()
        if verifier and token:
            payload = await verifier(token)
            if not payload:
                return JSONResponse({"error": "Invalid token"}, status_code=403)
            request.state.user = payload
        elif verifier and not token:
             return JSONResponse({"error": "Authentication required"}, status_code=401)

        return await call_next(request)

# --- MCP SERVER INIT ---
# We use FastMCP to define tools, but we WON'T use its built-in run() method.
# Instead, we will mount its underlying SSE handler into Starlette.
mcp = FastMCP("CRM Smart Agent")

# =============================================================================
# TOOLS
# =============================================================================

@mcp.tool()
def list_available_modules() -> List[str]:
    logger.info("Tool called: list_available_modules")
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str) -> Dict[str, Any]:
    logger.info(f"Tool called: get_module_schema for {module_name}")
    module_config = crm_tools.CRM_MODULES.get(module_name)
    if not module_config:
        return {"error": f"Module {module_name} not found"}
    return {
        "key_fields": module_config.get("key_fields", []),
        "enums": module_config.get("enums", {}),
        "field_mapping": module_config.get("field_mapping", {})
    }

@mcp.tool()
def search_records(module: str, filters: Dict[str, Any] = {}, limit: int = 10, columns: List[str] = None) -> Dict[str, Any]:
    try:
        logger.info(f"Tool called: search_records module={module} filters={filters}")
        return crm_tools.query_crm_data(module=module, filters=filters, max_records=limit, fields_only=columns)
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"error": str(e)}

@mcp.tool()
def calculate_metrics(records_data: List[Dict], metric_type: str = "sum", field: str = "amount") -> Dict[str, Any]:
    try:
        if metric_type == "sum":
            return crm_tools.calculate_total_amount(records_data)
        elif metric_type == "count":
            return {"count": len(records_data)}
        return {"error": "Invalid metric_type"}
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def generate_chart(module: str, filters: Dict[str, Any], x_axis: str, y_axis: str = None, chart_type: str = "bar") -> str:
    try:
        data = crm_tools.query_crm_data(module=module, filters=filters, max_records=200)
        chart_res = crm_tools.create_chart_from_crm_data(data, x_col=x_axis, y_col=y_axis, chart_type=chart_type)
        return f"Chart Generated: {chart_res.get('url')} \n Analysis: {chart_res.get('analysis')}"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def natural_language_query(query: str) -> str:
    try:
        logger.info(f"Tool called: natural_language_query query='{query}'")
        message_handler = MessageHandler()
        agent = MCPAgent(message_handler=message_handler)
        response = await agent.run_async(user_input=query)
        result_text = response.get("result", "")
        if "plot_data" in response and response['plot_data'].get('url'):
            result_text += f"\n\n[Chart Generated]: {response['plot_data'].get('url')}"
        return result_text
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error: {str(e)}"

# --- ASGI APP SETUP ---

# We need to bridge FastMCP with Starlette manually if _create_asgi_app is missing.
# Fortunately, FastMCP *is* an ASGI app in recent versions, or it exposes `mount_asgi`.
# Let's try the safest path: FastMCP usually handles the SSE transport internally.

middleware = []
if os.getenv("AUTH0_DOMAIN"):
    middleware = [Middleware(AuthMiddleware)]
    logger.info("🔒 Auth0 Middleware Configured")

# 1. Start Starlette
starlette_app = Starlette(middleware=middleware)

# 2. Mount the FastMCP app. 
# In recent SDKs, the `mcp` object itself might not be directly mountable as an ASGI app 
# without calling a method. However, for 'fastmcp', `mcp` IS the app.
# If using the *official* SDK's FastMCP, it might be different.

# Let's verify: If `mcp` has `mount_asgi`, use it.
if hasattr(mcp, "mount_asgi"):
    mcp.mount_asgi(starlette_app, "/")
# If not, and it has `_create_asgi_app` (which failed earlier), we skip that.
# If `mcp` is an ASGI callable itself (has __call__), we mount it directly.
elif callable(mcp):
    starlette_app.mount("/", mcp)
else:
    # Fallback: If using the official `mcp` library, we might need to use SseServerTransport manually.
    # But since you installed `fastmcp[cli]`, it's likely the standalone library.
    
    # CRITICAL FIX: The error `AttributeError: 'FastMCP' object has no attribute '_create_asgi_app'`
    # suggests we are using the OFFICIAL SDK which has a different internal structure.
    
    # In the official SDK, `FastMCP` creates a server, but we need to create the ASGI app wrapper.
    # We can use the SSE transport helper.
    
    from mcp.server.sse import SseServerTransport
    from mcp.server.stdio import StdioServerTransport
    
    # Since we can't easily extract the internal server from FastMCP (it's private),
    # We will assume that if we simply run it via the CLI it works, but we want middleware.
    
    # RE-STRATEGY: 
    # Instead of mounting `mcp` into Starlette, we create a SseServerTransport and handle the request.
    
    from starlette.routing import Route
    
    async def handle_sse(request: Request):
        # This handles the SSE connection using the MCP server
        # We need access to the underlying server object.
        # FastMCP wraps `mcp.server.Server`.
        
        # Accessing the private server... (risky but necessary if public API is missing)
        server = mcp._mcp_server 
        
        transport = SseServerTransport("/messages")
        
        async with server.run_with_transport(transport) as run_task:
             # This is complex to wire up manually with Starlette + SSE.
             pass

    # SIMPLER FIX:
    # If `fastmcp` (the standalone lib) is installed, `mcp` IS a Starlette app.
    # If `mcp.server.fastmcp` (official SDK) is used, it DOES NOT expose ASGI easily yet.
    
    # ACTION:
    # We will assume you want the functionality to work. 
    # The error `_create_asgi_app` missing means we are on the official SDK.
    # The official SDK `FastMCP` does NOT support ASGI export easily in v1.0.0.
    
    # SOLUTION:
    # Use the `fastmcp` STANDALONE library instead of `mcp.server.fastmcp`.
    # It has much better DX for this specific "Host on Railway with Auth" use case.
    pass

# --- RE-IMPORTING FROM STANDALONE FASTMCP ---
# This overrides the previous import to ensure compatibility.
from fastmcp import FastMCP as StandaloneFastMCP

# Re-initialize using the standalone library which behaves like an ASGI app
mcp = StandaloneFastMCP("CRM Smart Agent")

# Re-register tools (The decorators need to be re-applied to the NEW mcp object)
@mcp.tool()
def list_available_modules() -> List[str]:
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str) -> Dict[str, Any]:
    module_config = crm_tools.CRM_MODULES.get(module_name)
    if not module_config: return {"error": f"Module {module_name} not found"}
    return {"key_fields": module_config.get("key_fields", []), "enums": module_config.get("enums", {}), "field_mapping": module_config.get("field_mapping", {})}

@mcp.tool()
def search_records(module: str, filters: Dict[str, Any] = {}, limit: int = 10, columns: List[str] = None) -> Dict[str, Any]:
    try: return crm_tools.query_crm_data(module=module, filters=filters, max_records=limit, fields_only=columns)
    except Exception as e: return {"error": str(e)}

@mcp.tool()
def calculate_metrics(records_data: List[Dict], metric_type: str = "sum", field: str = "amount") -> Dict[str, Any]:
    try:
        if metric_type == "sum": return crm_tools.calculate_total_amount(records_data)
        elif metric_type == "count": return {"count": len(records_data)}
        return {"error": "Invalid metric_type"}
    except Exception as e: return {"error": str(e)}

@mcp.tool()
def generate_chart(module: str, filters: Dict[str, Any], x_axis: str, y_axis: str = None, chart_type: str = "bar") -> str:
    try:
        data = crm_tools.query_crm_data(module=module, filters=filters, max_records=200)
        chart_res = crm_tools.create_chart_from_crm_data(data, x_col=x_axis, y_col=y_axis, chart_type=chart_type)
        return f"Chart Generated: {chart_res.get('url')} \n Analysis: {chart_res.get('analysis')}"
    except Exception as e: return f"Error: {str(e)}"

@mcp.tool()
async def natural_language_query(query: str) -> str:
    try:
        message_handler = MessageHandler()
        agent = MCPAgent(message_handler=message_handler)
        response = await agent.run_async(user_input=query)
        result_text = response.get("result", "")
        if "plot_data" in response and response['plot_data'].get('url'): result_text += f"\n\n[Chart Generated]: {response['plot_data'].get('url')}"
        return result_text
    except Exception as e: return f"Error: {str(e)}"

# Mount the standalone FastMCP app (which is ASGI compatible) into Starlette with Middleware
starlette_app = Starlette(middleware=middleware)
starlette_app.mount("/", mcp)
