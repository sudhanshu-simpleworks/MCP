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

from mcp.server.fastmcp import FastMCP
from app.utils.logger import logger 
from app.utils.auth import create_auth0_verifier
from app.agents.tools import crm_tools
from app.agents.mcp_agent import MCPAgent
from app.utils.util import MessageHandler

# --- AUTH SETUP ---
auth_verifier = create_auth0_verifier()
auth_settings = None

if auth_verifier:
    auth_settings = FastMCP(
        # The client (ChatGPT) calls this to start the flow
        url=f"https://{os.getenv('AUTH0_DOMAIN')}/authorize", 
        # The function that checks the token
        verifier=auth_verifier 
    )
    logger.info("🔒 Auth0 Security Enabled")
else:
    logger.warning("⚠️  Auth0 config missing. Server running in insecure mode.")

# --- FAST MCP INIT ---
mcp = FastMCP(
    "CRM Smart Agent",
    auth=auth_settings # Injecting the Auth middleware
)

# =============================================================================
# 1. DISCOVERY TOOLS
# =============================================================================

@mcp.tool()
def list_available_modules() -> List[str]:
    """
    Lists all available CRM modules (e.g., Leads, Opportunities, Cases).
    """
    logger.info("Tool called: list_available_modules")
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str) -> Dict[str, Any]:
    """
    Get the schema for a specific CRM module.
    """
    logger.info(f"Tool called: get_module_schema for {module_name}")
    module_config = crm_tools.CRM_MODULES.get(module_name)
    if not module_config:
        return {"error": f"Module {module_name} not found"}
    
    return {
        "key_fields": module_config.get("key_fields", []),
        "enums": module_config.get("enums", {}),
        "field_mapping": module_config.get("field_mapping", {})
    }

# =============================================================================
# 2. DATA RETRIEVAL TOOLS
# =============================================================================

@mcp.tool()
def search_records(
    module: str, 
    filters: Dict[str, Any] = {}, 
    limit: int = 10,
    columns: List[str] = None
) -> Dict[str, Any]:
    """
    Search for records in a CRM module with specific filters.
    """
    try:
        logger.info(f"Tool called: search_records module={module} filters={filters}")
        result = crm_tools.query_crm_data(
            module=module,
            filters=filters,
            max_records=limit,
            fields_only=columns
        )
        return result
    except Exception as e:
        logger.error(f"Error in search_records: {e}", exc_info=True)
        return {"error": str(e)}

@mcp.tool()
def get_record_details(module: str, record_id: str) -> Dict[str, Any]:
    """
    Get full details for a single record by ID.
    """
    try:
        logger.info(f"Tool called: get_record_details id={record_id}")
        filters = {"id": record_id}
        result = crm_tools.query_crm_data(module=module, filters=filters, max_records=1)
        if result.get("count", 0) > 0:
            return result["records"][0]
        return {"error": "Record not found"}
    except Exception as e:
        logger.error(f"Error in get_record_details: {e}", exc_info=True)
        return {"error": str(e)}

# =============================================================================
# 3. ANALYSIS TOOLS
# =============================================================================

@mcp.tool()
def calculate_metrics(
    records_data: List[Dict], 
    metric_type: str = "sum", 
    field: str = "amount"
) -> Dict[str, Any]:
    """
    Calculate totals or counts on a list of records.
    """
    try:
        logger.info(f"Tool called: calculate_metrics type={metric_type}")
        if metric_type == "sum":
            return crm_tools.calculate_total_amount(records_data)
        elif metric_type == "count":
            return {"count": len(records_data)}
        return {"error": "Invalid metric_type"}
    except Exception as e:
        logger.error(f"Error in calculate_metrics: {e}", exc_info=True)
        return {"error": str(e)}

@mcp.tool()
def generate_chart(
    module: str,
    filters: Dict[str, Any],
    x_axis: str,
    y_axis: str = None,
    chart_type: str = "bar"
) -> str:
    """
    Generates a chart URL for the data.
    """
    try:
        logger.info(f"Tool called: generate_chart {chart_type}")
        data = crm_tools.query_crm_data(module=module, filters=filters, max_records=200)
        
        chart_res = crm_tools.create_chart_from_crm_data(
            data_id_or_records=data,
            x_col=x_axis,
            y_col=y_axis,
            chart_type=chart_type
        )
        return f"Chart Generated: {chart_res.get('url')} \n Analysis: {chart_res.get('analysis')}"
    except Exception as e:
        logger.error(f"Error in generate_chart: {e}", exc_info=True)
        return f"Error creating chart: {str(e)}"

# =============================================================================
# 4. FALLBACK "SMART" TOOL
# =============================================================================

@mcp.tool()
async def natural_language_query(query: str) -> str:
    """
    Use this ONLY if the user provides a complex request that requires multi-step reasoning.
    """
    try:
        logger.info(f"Tool called: natural_language_query query='{query}'")
        message_handler = MessageHandler()
        agent = MCPAgent(message_handler=message_handler)
        
        response = await agent.run_async(user_input=query)
        result_text = response.get("result", "")
        
        if "plot_data" in response:
            plot_data = response['plot_data']
            if isinstance(plot_data, dict) and plot_data.get('url'):
                result_text += f"\n\n[Chart Generated]: {plot_data.get('url')}"
        return result_text
    
    except Exception as e:
        logger.error(f"Error in natural_language_query: {e}", exc_info=True)
        return f"Error: {str(e)}"

if __name__ == "__main__":
    mcp.run()