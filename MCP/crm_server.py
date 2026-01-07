import sys
import os
from typing import Dict, List, Any

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from dotenv import load_dotenv
env_path = os.path.join(current_dir, ".env")
load_dotenv(env_path, override=True) 

from fastmcp import FastMCP, Context 
from app.agents.tools.crm_tools import user_token_ctx
from app.utils.logger import logger 
from app.agents.tools import crm_tools
from app.agents.mcp_agent import MCPAgent
from app.utils.util import MessageHandler

mcp = FastMCP("CRM Smart Agent")

def set_request_context(ctx: Context):
    """
    Extracts user CRM token from headers and sets the ContextVar.
    Useful if a client sends the token, but optional now that we have login_to_crm.
    """
    token = None
    meta = getattr(ctx, "meta", {})
    headers = meta.get("headers", {})
    clean_headers = {k.lower(): v for k, v in headers.items()}
    
    token = clean_headers.get("x-crm-token")
    if not token:
        auth = clean_headers.get("authorization")
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ")[1]
            
    if token:
        user_token_ctx.set(token)
        logger.info(f"User Context: Token found in headers.")


# =============================================================================
# AUTHENTICATION TOOL
# =============================================================================

@mcp.tool()
def login_to_crm(username: str, password: str, ctx: Context) -> str:
    """
    Authenticates a user against the CRM. 
    Call this FIRST to establish a session for subsequent queries.
    """
    try:
        token = crm_tools.perform_user_login(username, password)
        return f"Login Successful for user '{username}'. Session active."
    except Exception as e:
        return f"Login Failed: {str(e)}"

@mcp.tool()
def debug_auth_status(ctx: Context) -> str:
    """Checks if there is an active user session."""
    try:
        set_request_context(ctx)
        token = crm_tools.resolve_auth_token()
        return f"Authenticated. Active Token: {token[:10]}..."
    except Exception as e:
        return f"Not Authenticated. Please use 'login_to_crm' tool."

# =============================================================================
# EXISTING TOOLS
# =============================================================================

@mcp.tool()
def list_available_modules(ctx: Context) -> List[str]:
    set_request_context(ctx)
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str, ctx: Context) -> Dict[str, Any]:
    set_request_context(ctx)
    module_config = crm_tools.CRM_MODULES.get(module_name)
    if not module_config: return {"error": f"Module {module_name} not found"}
    return {
        "key_fields": module_config.get("key_fields", []),
        "enums": module_config.get("enums", {}),
        "field_mapping": module_config.get("field_mapping", {})
    }

@mcp.tool()
def search_records(
    module: str, 
    ctx: Context,
    filters: Dict[str, Any] = {}, 
    start_date: str = None,
    end_date: str = None,
    limit: int = 20,
    columns: List[str] = None
) -> Dict[str, Any]:
    set_request_context(ctx)
    try:
        result = crm_tools.query_crm_data(
            module=module,
            filters=filters,
            start_date=start_date,
            end_date=end_date,
            max_records=limit,
            fields_only=columns
        )
        return result
    except Exception as e:
        logger.error(f"Error in search_records: {e}", exc_info=True)
        return {"error": f"Search Failed: {str(e)}"}

@mcp.tool()
def get_record_details(module: str, record_id: str, ctx: Context) -> Dict[str, Any]:
    set_request_context(ctx)
    try:
        filters = {"id": record_id}
        result = crm_tools.query_crm_data(module=module, filters=filters, max_records=1)
        if result.get("count", 0) > 0:
            return result["records"][0]
        return {"error": "Record not found"}
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def calculate_metrics(
    ctx: Context,
    data_id: str = None,
    records_data: List[Dict] = None, 
    metric_type: str = "sum", 
    field: str = "amount"
) -> Dict[str, Any]:
    set_request_context(ctx)
    try:
        data_source = data_id if data_id else records_data
        if not data_source: return {"error": "No data provided."}

        if metric_type == "sum":
            return crm_tools.calculate_total_amount(data_source)
        elif metric_type == "count":
            if isinstance(data_source, list): return {"count": len(data_source)}
            res = crm_tools.calculate_total_amount(data_source)
            return {"count": res.get("records_processed", 0)}
        return {"error": "Invalid metric_type"}
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def generate_chart(
    module: str,
    filters: Dict[str, Any],
    x_axis: str,
    ctx: Context,
    y_axis: str = None,
    chart_type: str = "bar"
) -> str:
    set_request_context(ctx)
    try:
        data = crm_tools.query_crm_data(module=module, filters=filters, max_records=200)
        chart_res = crm_tools.create_chart_from_crm_data(
            data_id_or_records=data,
            x_col=x_axis,
            y_col=y_axis,
            chart_type=chart_type
        )
        return f"Chart Generated: {chart_res.get('url')} \n Analysis: {chart_res.get('analysis')}"
    except Exception as e:
        return f"Error creating chart: {str(e)}"

@mcp.tool()
async def natural_language_query(query: str, ctx: Context) -> str:
    set_request_context(ctx)
    try:
        message_handler = MessageHandler()
        agent = MCPAgent(message_handler=message_handler)
        response = await agent.run_async(user_input=query)
        result_text = str(response.get("result", ""))
        if "plot_data" in response:
            plot_data = response['plot_data']
            if isinstance(plot_data, dict) and plot_data.get('url'):
                result_text += f"\n\n[Chart Generated]: {plot_data.get('url')}"
        return result_text
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="sse")
