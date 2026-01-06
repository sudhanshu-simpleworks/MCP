import sys
import os
from typing import Dict, List, Any

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from dotenv import load_dotenv
env_path = os.path.join(current_dir, ".env")
load_dotenv(env_path, override=True) 

from fastmcp import FastMCP, Context 
from app.utils.auth import verify_access
from app.agents.tools.crm_tools import user_token_ctx
from app.utils.logger import logger 
from app.agents.tools import crm_tools
from app.agents.mcp_agent import MCPAgent
from app.utils.util import MessageHandler

mcp = FastMCP("CRM Smart Agent")

def set_request_context(ctx: Context):
    """
    Extracts user CRM token and sets it for the tools to use.
    Robustly handles cases where ctx.meta might be missing.
    """
    verify_access(ctx)
    token = None
    
    meta = getattr(ctx, "meta", None)
    
    if meta and isinstance(meta, dict) and "headers" in meta:
        headers = {k.lower(): v for k, v in meta["headers"].items()}
        token = headers.get("x-crm-token")
        if not token:
            auth = headers.get("authorization")
            if auth and auth.startswith("Bearer "):
                token = auth.split(" ")[1]
    
    if not token:
        token = os.environ.get("CRM_ACCESS_TOKEN")

    if not token:
        try:
            if hasattr(crm_tools, "get_crm_token"): 
                token = crm_tools.get_crm_token()
        except:
            pass

    if not token:
        raise ValueError("User Login Required: No CRM Access Token found.")

    user_token_ctx.set(token)

# =============================================================================
# 1. DISCOVERY TOOLS
# =============================================================================

@mcp.tool()
def list_available_modules(ctx: Context) -> List[str]:
    """Lists all available CRM modules."""
    set_request_context(ctx)
    logger.info("Tool called: list_available_modules")
    return list(crm_tools.CRM_MODULES.keys())

@mcp.tool()
def get_module_schema(module_name: str, ctx: Context) -> Dict[str, Any]:
    """Get the schema for a specific CRM module."""
    set_request_context(ctx)
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
    ctx: Context,
    filters: Dict[str, Any] = {}, 
    start_date: str = None,
    end_date: str = None,
    limit: int = 10,
    columns: List[str] = None
) -> Dict[str, Any]:
    """
    Search for records in a CRM module.
    
    Args:
        module: The module name (e.g. 'Opportunities', 'Leads')
        filters: Dictionary of field filters (e.g. {'stage': 'Closed Won'})
        start_date: Filter records created after this date (YYYY-MM-DD)
        end_date: Filter records created before this date (YYYY-MM-DD)
        limit: Max records to return (default 10)
        columns: Specific columns to retrieve
    """
    set_request_context(ctx)
    try:
        logger.info(f"Tool called: search_records module={module} start={start_date} end={end_date}")
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
        return {"error": str(e)}

@mcp.tool()
def get_record_details(module: str, record_id: str, ctx: Context) -> Dict[str, Any]:
    """Get full details for a single record by ID."""
    set_request_context(ctx)
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
    ctx: Context,
    data_id: str = None,
    records_data: List[Dict] = None, 
    metric_type: str = "sum", 
    field: str = "amount"
) -> Dict[str, Any]:
    """
    Calculate totals or counts.
    
    Args:
        data_id: The ID string returned by search_records (Preferred for large datasets).
        records_data: List of record dictionaries (Only use for small datasets).
        metric_type: 'sum' or 'count'.
        field: The field to sum (default 'amount').
    """
    set_request_context(ctx)
    try:
        logger.info(f"Tool called: calculate_metrics type={metric_type} data_id={data_id}")
        
        data_source = None
        if data_id:
            data_source = data_id
        elif records_data:
            data_source = records_data
        
        if not data_source:
             return {"error": "No data provided. Please provide either data_id or records_data."}

        if metric_type == "sum":
            return crm_tools.calculate_total_amount(data_source)
        elif metric_type == "count":
            if isinstance(data_source, list):
                return {"count": len(data_source)}
            res = crm_tools.calculate_total_amount(data_source)
            return {"count": res.get("records_processed", 0)}
            
        return {"error": "Invalid metric_type"}
    except Exception as e:
        logger.error(f"Error in calculate_metrics: {e}", exc_info=True)
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
    """Generates a chart URL for the data."""
    set_request_context(ctx)
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
async def natural_language_query(query: str, ctx: Context) -> str:
    """Use this for complex queries involving dates, multiple steps, or reasoning."""
    set_request_context(ctx)
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
    mcp.run(transport="sse")
