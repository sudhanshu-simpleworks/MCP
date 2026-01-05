import asyncio
import os
import sys
from dotenv import load_dotenv
import google.generativeai as genai
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# =============================================================================
# 1. PATH SETUP (Fixes the "Not Found" Error)
# =============================================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

MCP_DIR = os.path.join(CURRENT_DIR, "MCP")
ENV_PATH = os.path.join(MCP_DIR, ".env")
load_dotenv(ENV_PATH, override=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print(f"❌ Error: Could not find .env file at: {ENV_PATH}")
    print("   Make sure the file exists and contains GEMINI_API_KEY")
    sys.exit(1)

# Ignore the deprecation warning for now; it works fine.
genai.configure(api_key=GEMINI_API_KEY)

# =============================================================================
# 2. CLIENT CONFIGURATION & AUTH
# =============================================================================
PYTHON_PATH = os.getenv("PYTHON_PATH")
SCRIPT_PATH = os.path.join(MCP_DIR, "crm_server.py")

CLIENT_IDENTITY_KEY = "my_secure_dev_key_123"

ENV_VARS = {
    "PYTHONPATH": MCP_DIR,
    "ROOT_PATH": MCP_DIR,
    "PATH": os.environ.get("PATH", ""),
    "MCP_SERVER_API_KEY": CLIENT_IDENTITY_KEY 
}

# =============================================================================
# HELPER: SCHEMA SANITIZER
# =============================================================================

def clean_schema(schema):
    """Recursively fixes JSON Schema for Gemini."""
    if not isinstance(schema, dict):
        return schema
    
    cleaned = {}
    for key, value in schema.items():
        if key == "type":
            if isinstance(value, str):
                cleaned[key] = value.upper()
            elif isinstance(value, list):
                valid_types = [t for t in value if t != "null"]
                cleaned[key] = valid_types[0].upper() if valid_types else "OBJECT"
        elif key == "properties" and isinstance(value, dict):
            cleaned[key] = {k: clean_schema(v) for k, v in value.items()}
        elif key in ["items", "parameters"] and isinstance(value, dict):
            cleaned[key] = clean_schema(value)
        elif key in ["description", "required", "enum", "format"]:
            cleaned[key] = value
            
    return cleaned

def convert_mcp_tool_to_gemini(mcp_tool):
    return {
        "name": mcp_tool.name,
        "description": mcp_tool.description,
        "parameters": clean_schema(mcp_tool.inputSchema)
    }

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
            
            mcp_tools = await session.list_tools()
            gemini_tools = [convert_mcp_tool_to_gemini(tool) for tool in mcp_tools.tools]
            print(f"🛠️  Loaded {len(gemini_tools)} tools from CRM Agent.")

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
                    
                    while response.parts and response.parts[0].function_call:
                        part = response.parts[0]
                        fc = part.function_call
                        tool_name = fc.name
                        tool_args = dict(fc.args)

                        print(f"   > ⚙️  Gemini is calling tool: {tool_name}...")
                        
                        try:
                            mcp_result = await session.call_tool(tool_name, arguments=tool_args)
                            
                            if not mcp_result.isError:
                                tool_output = "\n".join([c.text for c in mcp_result.content if c.type == 'text'])
                            else:
                                tool_output = f"Error: {mcp_result.content}"
                        except Exception as e:
                            tool_output = f"Tool Execution Failed: {str(e)}"

                        response = chat.send_message({
                            "function_response": {
                                "name": tool_name,
                                "response": {"result": tool_output}
                            }
                        })

                    print(f"Gemini: {response.text}")
                
                except Exception as e:
                    print(f"\n❌ Error encountered: {e}")

if __name__ == "__main__":
    asyncio.run(run_gemini_client())
