# --- START OF FILE c:\MCP\gemini_client.py ---
import asyncio
import os
import google.generativeai as genai
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# =============================================================================
# CONFIGURATION
# =============================================================================
PYTHON_PATH = r"C:\Users\simpl\anaconda3\envs\mcp-env\python.exe"
SCRIPT_PATH = r"c:\MCP\crm_server.py"
ENV_VARS = {
    "PYTHONPATH": "c:\\MCP",
    "ROOT_PATH": "c:\\MCP",
    "PATH": os.environ.get("PATH", "")
}

GEMINI_API_KEY = "AIzaSyBwtTzYuRdZ1oaNXACiFS5YwM4TYeLZeDc"
genai.configure(api_key=GEMINI_API_KEY)

# =============================================================================
# HELPER: SCHEMA SANITIZER (The Fix)
# =============================================================================
def clean_schema(schema):
    """
    Recursively fixes JSON Schema for Gemini:
    1. Converts types to UPPERCASE (string -> STRING).
    2. Handles Optional types (['string', 'null'] -> STRING).
    3. Recursively cleans properties and items.
    """
    if not isinstance(schema, dict):
        return schema
    
    cleaned = {}
    for key, value in schema.items():
        if key == "type":
            if isinstance(value, str):
                # Fix: Gemini requires UPPERCASE types (e.g., "STRING")
                cleaned[key] = value.upper()
            elif isinstance(value, list):
                # Fix: Handle Pydantic optional types like ["string", "null"]
                valid_types = [t for t in value if t != "null"]
                if valid_types:
                    cleaned[key] = valid_types[0].upper()
                else:
                    cleaned[key] = "OBJECT" # Fallback
        
        elif key == "properties" and isinstance(value, dict):
            cleaned[key] = {k: clean_schema(v) for k, v in value.items()}
        
        elif key == "items" and isinstance(value, dict):
            cleaned[key] = clean_schema(value)
            
        elif key == "parameters" and isinstance(value, dict):
            cleaned[key] = clean_schema(value)
            
        # Pass through other valid keys
        elif key in ["description", "required", "enum", "format"]:
            cleaned[key] = value
            
    return cleaned

def convert_mcp_tool_to_gemini(mcp_tool):
    """
    Converts an MCP tool definition into a format Gemini understands.
    """
    # 1. Get the raw schema from MCP
    raw_schema = mcp_tool.inputSchema
    
    # 2. Sanitize it for Gemini (Fix capitalization issues)
    sanitized_schema = clean_schema(raw_schema)
    
    function_decl = {
        "name": mcp_tool.name,
        "description": mcp_tool.description,
        "parameters": sanitized_schema
    }
    return function_decl

# =============================================================================
# MAIN CLIENT LOOP
# =============================================================================
async def run_gemini_client():
    server_params = StdioServerParameters(
        command=PYTHON_PATH,
        args=[SCRIPT_PATH],
        env=ENV_VARS
    )

    print(f"🔌 Connecting to CRM Server at: {SCRIPT_PATH}...")

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            
            await session.initialize()
            
            # List and Convert Tools
            mcp_tools = await session.list_tools()
            gemini_tools = [convert_mcp_tool_to_gemini(tool) for tool in mcp_tools.tools]
            print(f"🛠️  Loaded {len(gemini_tools)} tools from CRM Agent.")

            # Initialize Gemini
            model = genai.GenerativeModel(
                model_name='gemini-2.5-flash', 
                tools=gemini_tools
            )
            
            chat = model.start_chat(enable_automatic_function_calling=False)
            
            print("\n🤖 Gemini CRM Agent is Ready! (Type 'quit' to exit)")
            print("-" * 50)

            while True:
                try:
                    user_input = input("\nYou: ")
                    if user_input.lower() in ["quit", "exit"]:
                        break

                    response = chat.send_message(user_input)
                    
                    # Handle Function Calls
                    while response.parts and response.parts[0].function_call:
                        part = response.parts[0]
                        fc = part.function_call
                        tool_name = fc.name
                        tool_args = dict(fc.args)

                        print(f"   > ⚙️  Gemini is calling tool: {tool_name}...")
                        
                        try:
                            # Execute via MCP
                            mcp_result = await session.call_tool(tool_name, arguments=tool_args)
                            
                            # Format Output
                            if not mcp_result.isError:
                                tool_output = "\n".join([c.text for c in mcp_result.content if c.type == 'text'])
                            else:
                                tool_output = f"Error: {mcp_result.content}"
                        except Exception as e:
                            tool_output = f"Tool Execution Failed: {str(e)}"

                        # Send result back to Gemini
                        response = chat.send_message({
                            "function_response": {
                                "name": tool_name,
                                "response": {"result": tool_output}
                            }
                        })

                    print(f"Gemini: {response.text}")
                
                except Exception as e:
                    print(f"\n❌ Error encountered: {e}")
                    # Only break if it's critical, otherwise let the loop continue
                    # break 

if __name__ == "__main__":
    asyncio.run(run_gemini_client())