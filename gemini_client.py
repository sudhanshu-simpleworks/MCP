import asyncio
import os
import sys
import requests
import getpass
import json
import base64
from dotenv import load_dotenv
import google.generativeai as genai
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client, sse_client

# --- Cryptography Imports ---
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# =============================================================================
# 1. PATH SETUP
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

genai.configure(api_key=GEMINI_API_KEY)

# =============================================================================
# 2. CLIENT CONFIGURATION & AUTH
# =============================================================================

PYTHON_PATH = os.getenv("PYTHON_PATH", r"C:\Users\simpl\anaconda3\envs\mcp-env\python.exe")
SCRIPT_PATH = os.path.join(MCP_DIR, "crm_server.py")
CLIENT_IDENTITY_KEY = "my_secure_dev_key_123"

ENV_VARS = {
    "PYTHONPATH": MCP_DIR,
    "ROOT_PATH": MCP_DIR,
    "PATH": os.environ.get("PATH", ""),
    "MCP_SERVER_API_KEY": CLIENT_IDENTITY_KEY 
}

# =============================================================================
# ENCRYPTION LOGIC
# =============================================================================

def encrypt_password(plain_password):
    try:
        passphrase = "373632764d5243706c706d6973"
        iterations = 999
        key_length = 32
        salt_length = 32
        iv_length = 16

        salt = os.urandom(salt_length)
        iv = os.urandom(iv_length)

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA512(),
            length=key_length,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        derived_key = kdf.derive(passphrase.encode('utf-8'))

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plain_password.encode('utf-8')) + padder.finalize()

        cipher = Cipher(algorithms.AES(derived_key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        payload = {
            "amtext": base64.b64encode(ciphertext).decode('utf-8'),
            "slam_ltol": salt.hex(),
            "iavmol": iv.hex()
        }
        return base64.b64encode(json.dumps(payload).encode('utf-8')).decode('utf-8')

    except Exception as e:
        print(f"Encryption Failed: {e}")
        sys.exit(1)

# =============================================================================
# CRM LOGIN LOGIC
# =============================================================================

def perform_crm_login():
    """
    Dynamically logs into SimpleCRM.
    - If Username is provided -> Uses Password Grant (User context).
    - If Username is skipped -> Uses Client Credentials Grant (System context).
    """
    print("\n🔐 CRM AUTHENTICATION")
    print("-" * 30)
    
    login_url = os.getenv("CRM_LOGIN_ENDPOINT")
    client_id = os.getenv("CRM_CLIENT_ID")
    client_secret = os.getenv("CRM_CLIENT_SECRET")
    
    if not login_url or not client_id or not client_secret:
        print("❌ Error: Missing CRM_LOGIN_ENDPOINT, CLIENT_ID, or CLIENT_SECRET in .env")
        sys.exit(1)
    
    print("👉 Press [Enter] to login as System (Client Credentials)")
    username = input("👤 CRM Username: ").strip()

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
    }

    if username:
        print(f"🔑 Logging in as user: {username}")
        password_plain = getpass.getpass("🔑 CRM Password: ")
        
        print("🔐 Encrypting password...")
        encrypted_password = encrypt_password(password_plain)
        
        payload["grant_type"] = "password"
        payload["username"] = username
        payload["password"] = encrypted_password
    else:
        print("⚙️  Logging in with Client Credentials...")
        payload["grant_type"] = "client_credentials"

    try:
        print(f"⏳ Authenticating...")
        response = requests.post(login_url, data=payload, timeout=15, verify=False)
        
        if response.status_code >= 400:
            response = requests.post(login_url, json=payload, headers={"Content-Type": "application/json"}, timeout=15, verify=False)

        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        
        if not token:
            print(f"❌ Error: Response did not contain access_token. Data: {data}")
            sys.exit(1)
            
        print(f"✅ Login Successful! (Mode: {payload['grant_type']} | Session: {token[:8]}...)")
        return token
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ Login Failed: {e}")
        try:
            print(f"Server Response: {response.text}")
        except: pass
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        sys.exit(1)

# =============================================================================
# HELPER: SCHEMA & DATA CLEANING
# =============================================================================

def clean_schema(schema):
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

def proto_to_dict(obj):
    """Recursively converts Google Protobuf Map/List objects to Python dict/list."""
    if hasattr(obj, "items"): 
        return {k: proto_to_dict(v) for k, v in obj.items()}
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
        return [proto_to_dict(v) for v in obj]
    else:
        return obj

# =============================================================================
# MAIN CLIENT LOOP
# =============================================================================

async def run_gemini_client():
    user_crm_token = perform_crm_login()
    
    server_env = ENV_VARS.copy()
    server_env["CRM_ACCESS_TOKEN"] = user_crm_token
    
    server_params = StdioServerParameters(
        command=PYTHON_PATH,
        args=[SCRIPT_PATH],
        env=server_env
    )

    print(f"\n🔌 Connecting to CRM Server at: {SCRIPT_PATH}...")

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            mcp_tools = await session.list_tools()
            gemini_tools = [convert_mcp_tool_to_gemini(tool) for tool in mcp_tools.tools]
            print(f"🛠️  Loaded {len(gemini_tools)} tools from CRM Agent.")

            model = genai.GenerativeModel(model_name='gemini-2.5-flash', tools=gemini_tools)
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
                        
                        # FIX: Recursively convert args to pure dict to avoid serialization errors
                        tool_args = proto_to_dict(fc.args)

                        print(f"   > ⚙️  Gemini is calling tool: {tool_name}...")
                        
                        try:
                            mcp_result = await session.call_tool(tool_name, arguments=tool_args)
                            
                            if not mcp_result.isError:
                                # Clean extraction of text content
                                tool_output = "\n".join([c.text for c in mcp_result.content if c.type == 'text'])
                            else:
                                tool_output = f"Tool Error: {mcp_result.content}"
                        except Exception as e:
                            tool_output = f"Tool Execution Exception: {str(e)}"

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
