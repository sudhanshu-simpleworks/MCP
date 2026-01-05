import sys
import os
import json
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
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.utils.logger import logger 
from app.utils.auth import create_auth0_verifier
from app.agents.tools import crm_tools
from app.agents.mcp_agent import MCPAgent
from app.utils.util import MessageHandler

# --- AUTH MIDDLEWARE ---
# This class intercepts every request to check for the Auth0 token
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Allow health checks or root access without token if needed
        if request.url.path == "/health":
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"error": "Missing or invalid Authorization header"}, 
                status_code=401
            )

        token = auth_header.split(" ")[1]
        verifier = create_auth0_verifier()
        
        if verifier:
            # Verify the token
            payload = await verifier(token)
            if not payload:
                return JSONResponse({"error": "Invalid or expired token"}, status_code=403)
            # Store payload in request state if needed
            request.state.user = payload
        
        return await call_next(request)

# --- MCP SERVER INIT ---
mcp = FastMCP("CRM Smart Agent")

# =============================================================================
# TOOLS
# =============================================================================

@mcp.tool()
def list_available_modules() -> List[str]:
    """Lists all available CRM modules."""
    logger.info("Tool called: list_available_modules")
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str) -> Dict[str, Any]:
    """Get the schema for a specific CRM module."""
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
    """Search for records in a CRM module."""
    try:
        logger.info(f"Tool called: search_records module={module} filters={filters}")
        return crm_tools.query_crm_data(module=module, filters=filters, max_records=limit, fields_only=columns)
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"error": str(e)}

@mcp.tool()
def calculate_metrics(records_data: List[Dict], metric_type: str = "sum", field: str = "amount") -> Dict[str, Any]:
    """Calculate totals or counts."""
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
    """Generate a chart URL."""
    try:
        data = crm_tools.query_crm_data(module=module, filters=filters, max_records=200)
        chart_res = crm_tools.create_chart_from_crm_data(data, x_col=x_axis, y_col=y_axis, chart_type=chart_type)
        return f"Chart Generated: {chart_res.get('url')} \n Analysis: {chart_res.get('analysis')}"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def natural_language_query(query: str) -> str:
    """Complex natural language query."""
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

# --- ASGI APP EXPORT ---
# This matches the MCP server to the Starlette SSE endpoint
# We apply the AuthMiddleware if configured
middleware = []
if os.getenv("AUTH0_DOMAIN"):
    middleware = [Middleware(AuthMiddleware)]
    logger.info("🔒 Auth0 Middleware Enabled")

# Extract the SSE handler from the official MCP SDK
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route

# Create the Starlette app
# Note: mcp.create_asgi_app() creates a full app, but we want to wrap it with auth
# The simplest way with the official SDK is to let it create the app, then wrap that.
internal_mcp_app = mcp._create_asgi_app() # Uses the internal helper to get the ASGI app

# We wrap the internal MCP app with our Auth Middleware
starlette_app = Starlette(middleware=middleware, routes=[
    Mount("/", app=internal_mcp_app)
])