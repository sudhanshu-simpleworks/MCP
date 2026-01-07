import re
import os
import json
import hashlib
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Generator, Optional, Dict, Any, List
from llama_index.llms.openai import OpenAI
from llama_index.core.memory import ChatMemoryBuffer
from llama_index.core.llms import ChatMessage, MessageRole

from app.agents.tools import crm_tools
from app.agents.tools.mcp_logger import mcp_logger
from app.utils.logger import logger
from app.utils.util import MessageHandler


# ============================================================================
# MCP AGENT (Multi-Task Async)
# ============================================================================


class MCPAgent:

    _MARKDOWN_PATTERN = re.compile(
        r"\[([^\]]+)\]\((https?://[^\s)]+/api/v1/plots/(plot_[\w\d]+\.json))\)"
    )

    def __init__(self, message_handler: MessageHandler, llm_model: str = "gpt-4o"):
        self.message_handler = message_handler
        self.llm = OpenAI(model=llm_model)
        self.plot_dir = (
            Path(os.getenv("ROOT_PATH")) / "app" / "agents" / "tools" / "plots"
        )
        self.plot_dir.mkdir(parents=True, exist_ok=True)

    def _map_user_terms_to_api_fields(self, module: str, inputs: Any) -> Any:
        """Helper to map user friendly terms to API keys"""
        module_config = crm_tools.CRM_MODULES.get(module, {})
        input_mapping = module_config.get("field_mapping", {})
        renames = module_config.get("column_renames", {})

        def resolve_key(term):
            if not isinstance(term, str):
                return term
            term_lower = term.lower().strip()
            term_normalized = term_lower.replace(" ", "_")

            if term_lower in input_mapping:
                return input_mapping[term_lower]
            if term_normalized in input_mapping:
                return input_mapping[term_normalized]
            for disp_name, api_key in renames.items():
                if disp_name.lower() == term_lower:
                    return api_key
            return term_normalized

        if isinstance(inputs, list):
            return [resolve_key(x) for x in inputs]
        if isinstance(inputs, dict):
            return {resolve_key(k): v for k, v in inputs.items()}

        return inputs

    def _filter_columns(
        self, all_keys: List[str], requested_columns: List[str] = None
    ) -> List[str]:
        """
        Filters out ID columns and internal fields unless explicitly requested.
        """
        ignored_suffixes = ("_id", "_link", "id_c", "aclacces", "aclaccess")
        ignored_exact = {"id", "deleted", "module", "filename", "uuid"}

        final_keys = []

        is_filtering_mode = requested_columns is None or requested_columns == [
            "__ALL__"
        ]
        source_keys = all_keys if is_filtering_mode else requested_columns

        for key in source_keys:
            key_lower = key.lower()
            if is_filtering_mode:
                if key_lower in ignored_exact:
                    continue
                if key_lower.endswith(ignored_suffixes):
                    continue

            final_keys.append(key)

        return final_keys

    async def _classify_query(
        self,
        user_input: str,
        chat_history: List[ChatMessage] = [],
        user_date_format: str = "YYYY-MM-DD",
    ) -> Dict[str, Any]:
        """
        Classifies query into a list of executable tasks, using chat history for context.
        """
        history_context = ""
        if chat_history:
            history_context = "\n--- CONVERSATION HISTORY ---\n"
            for msg in chat_history[-5:]:
                role_label = (
                    msg.role.value if hasattr(msg.role, "value") else str(msg.role)
                )
                history_context += f"{role_label.upper()}: {msg.content}\n"
            history_context += "--- END HISTORY ---\n"
        modules_list = list(crm_tools.CRM_MODULES.keys())
        aliases_json = json.dumps(
            {k: v["aliases"] for k, v in crm_tools.CRM_MODULES.items()}
        )

        module_prompt = f"""
        {history_context}
                Identify the CRM modules relevant to the User's latest input:
                "{user_input}"
                Instructions:
                - If the user query is generic (e.g. "filter by high priority"),
                    infer the module from the CONVERSATION HISTORY above.
        - Available Modules: {modules_list}
        - Aliases: {aliases_json}

        Return a JSON List of strings. Example: ["Cases", "Opportunities"].
        If unsure or general, return empty list [].
        """
        target_modules = []
        try:
            mod_resp = await self.llm.acomplete(module_prompt)
            cleaned_resp = re.sub(r"```json\s*|\s*```", "", str(mod_resp).strip())
            detected_list = json.loads(cleaned_resp)

            if isinstance(detected_list, list):
                for m in detected_list:
                    m_clean = m.strip().replace('"', "").replace("'", "")
                    found = next(
                        (
                            k
                            for k in crm_tools.CRM_MODULES
                            if k.lower() == m_clean.lower()
                        ),
                        None,
                    )
                    if found and found not in target_modules:
                        target_modules.append(found)
        except Exception as e:
            logger.error(f"Module identification error: {e}")

        if not target_modules:
            return {"tasks": [], "error": "Could not identify any valid CRM module."}

        schema_context = ""
        for mod in target_modules:
            config = crm_tools.CRM_MODULES.get(mod, {})
            schema_context += f"""
            --- SCHEMA FOR MODULE: {mod} ---
            Allowed API Keys: {json.dumps(config.get("key_fields", []))}
            Field Mappings: {json.dumps(config.get("field_mapping", {}))}
            Enum Values: {json.dumps(config.get("enums", {}))}
            """

        classification_prompt = f"""
        You are a strict CRM Query Parser.
        {history_context}
        USER QUERY: "{user_input}"
        CURRENT DATE: {datetime.now().strftime('%Y-%m-%d')}
        USER DATE FORMAT PREFERENCE: {user_date_format}

        FISCAL YEAR RULES (INDIAN FY):
        The Financial Year starts on April 1st.
        - Q1: April 1 to June 30
        - Q2: July 1 to September 30
        - Q3: October 1 to December 31
        - Q4: January 1 to March 31 (of the **next** calendar year)

        EXAMPLES:
        - "Q3 2025": Start="10/01/2025", End="12/31/2025"
        - "Q4 2025": Start="01/01/2026", End="03/31/2026"
        - "First quarter 2025": Start="04/01/2025", End="06/30/2025"

          DATE PARSING RULES:
          - Interpret any specific dates in the User Query according to the format: "{user_date_format}".
          - Example: If format is DD-MM-YYYY, "01-02-2025" is February 1st. If MM-DD-YYYY, it is January 2nd.
          - Convert all extracted dates to standardized "YYYY-MM-DD" in the JSON output.
          {schema_context}

          RULES:
          1. "columns": List specific columns requested.
              - If user explicitly says "all columns", "everything", or "details", return ["__ALL__"].
              - If user DOES NOT specify columns (general view), return empty [].
          2. "display_format": Defaults to "table".
              - Use "list" ONLY if the user explicitly uses the word "list" (e.g. "List opportunities").
              - If user says "Show", "Get", "Display", "Find", or "View", use "table".
          3. "filters": Use ONLY keys from 'Allowed API Keys' for the specific module.
              - Do NOT invent fields.
              - Look at 'Enum Values' to infer correct fields.
              - If the user refers to previous context (e.g., "Add a filter for..."), combine with inferred context.
          4. "filters" (Date Values): Use a dictionary for detailed date logic.
              - Specific Date: {{ "operator": "=", "value": "MM/DD/YYYY" }}
              - Before/After: use an operator ("less_than" or "greater_than") and a date value.
                 e.g. {{ "operator": "less_than", "value": "MM/DD/YYYY" }}
              - Between: use operator "between" and provide start/end dates, e.g.:
                 {{ "operator": "between", "start": "MM/DD/YYYY", "end": "MM/DD/YYYY" }}
              - Predefined: use a named operator such as "today", "this_week", "next_month",
                 or "last_7_days".
          5. "limit": Extract integer if user specifies quantity (e.g. "top 10", "5 latest"). Default null.
          6. "chart_config":
              - "x_col": The categorical field (e.g., "sales_stage", "user_name").
              - "y_col": The numerical metric if specified (e.g., "amount", "revenue",
                 or "probability"). If just counting, leave null.
              - "chart_type": "bar", "pie", "line", or "scatter".
          7. "type" Selection:
              - "sum": Use ONLY if user explicitly asks for "total amount", "financial sum",
                 or "calculate total value".
                 Treat generic "summary", "overview", "brief", or "analysis" as "list".
              - "list": For "list", "show", "tabulate", "summary", "overview", "details".
              - "count": For "how many", "count of".
              - "chart": For "graph", "plot", "chart".

                CRITICAL:
                - Return a "tasks" array.
                - Each task MUST specify its own "module" from the identified list: {target_modules}.
                - Separate distinct actions (e.g. "List Tasks and Opportunities") into separate task objects.

        OUTPUT JSON:
        {{
            "tasks": [
                {{
                    "type": "sum|list|count|chart",
                    "module": "ModuleName",
                    "filters": {{ ... }},
                    "limit": 10,
                    "columns": [],
                    "display_format": "list",
                    "chart_config": {{ "x_col": "...", "y_col": "...", "chart_type": "..." }}
                }}
            ]
        }}
        """
        try:
            response = await self.llm.acomplete(classification_prompt)
            parsed = json.loads(re.sub(r"```json\s*|\s*```", "", str(response).strip()))

            if "tasks" not in parsed:
                if "type" in parsed:
                    parsed = {"tasks": [parsed]}
                else:
                    parsed = {"tasks": []}
            final_tasks = []
            for task in parsed["tasks"]:
                mod = task.get("module")
                if mod not in target_modules:
                    if len(target_modules) == 1:
                        mod = target_modules[0]
                        task["module"] = mod
                    else:
                        continue

                if "columns" not in task:
                    task["columns"] = []
                if "display_format" not in task:
                    task["display_format"] = "table"
                if "chart_config" not in task:
                    task["chart_config"] = {}
                if "filters" not in task:
                    task["filters"] = {}
                if "limit" not in task:
                    task["limit"] = None
                mapped_filters = {}
                raw_filters = task.get("filters", {})
                for k, v in raw_filters.items():
                    mapped_k = self._map_user_terms_to_api_fields(mod, {k: ""})
                    mapped_key_str = (
                        list(mapped_k.keys())[0] if isinstance(mapped_k, dict) else k
                    )
                    mapped_filters[mapped_key_str] = v
                task["filters"] = mapped_filters
                final_tasks.append(task)

            return {"tasks": final_tasks}
        except Exception as e:
            logger.error(f"Classify error: {e}")
            if target_modules:
                return {
                    "tasks": [
                        {
                            "type": "list",
                            "module": target_modules[0],
                            "columns": [],
                            "display_format": "table",
                            "filters": {},
                            "limit": None,
                        }
                    ]
                }
            return {"tasks": [], "error": "Classification failed."}

    async def _resolve_smart_filters(self, params: Dict[str, Any]):
        filters = params.get("filters", {})
        if "filter_display_values" not in params:
            params["filter_display_values"] = {}
        resolvable_fields = {
            "created_by_name": "created_by",
            "created_by_user": "created_by",
            "creator": "created_by",
            "assigned_user_name": "assigned_user_id",
            "assigned_user": "assigned_user_id",
            "assigned_to": "assigned_user_id",
        }
        for filter_key in list(filters.keys()):
            if filter_key in resolvable_fields:
                val = filters[filter_key]
                if isinstance(val, list):
                    continue
                if isinstance(val, dict) and isinstance(val.get("value"), list):
                    continue

                target_api_key = resolvable_fields[filter_key]
                name_query = filters.pop(filter_key)
                if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}", str(name_query)):
                    filters[target_api_key] = name_query
                    continue

                self.message_handler.add_message(
                    f"Resolving User ID for '{name_query}'..."
                )
                target_id = None
                search_fields = ["name", "user_name", "first_name", "last_name"]
                user_res_fields = ["id", "name", "first_name", "last_name", "user_name"]
                for field in search_fields:
                    try:
                        users = await asyncio.to_thread(
                            crm_tools.query_crm_data,
                            module="Users",
                            filters={field: name_query},
                            max_records=1,
                            fields_only=user_res_fields,
                        )
                        if users.get("count", 0) > 0:
                            target_id = users["records"][0]["id"]
                            break
                    except Exception as e:
                        logger.warning(
                            f"Could not resolve user ID (likely 403 Forbidden): {e}"
                        )

                if (
                    not target_id
                    and isinstance(name_query, str)
                    and " " in name_query.strip()
                ):
                    parts = name_query.strip().split()
                    if len(parts) >= 2:
                        try:
                            users = await asyncio.to_thread(
                                crm_tools.query_crm_data,
                                module="Users",
                                filters={
                                    "first_name": parts[0],
                                    "last_name": " ".join(parts[1:]),
                                },
                                max_records=1,
                                fields_only=user_res_fields,
                            )
                            if users.get("count", 0) > 0:
                                target_id = users["records"][0]["id"]
                        except Exception as e:
                            logger.warning(f"Split name resolution failed: {e}")

                if target_id:
                    filters[target_api_key] = target_id
                    params["filter_display_values"][target_api_key] = name_query
                    self.message_handler.add_message(f"Resolved to ID: {target_id}")
                else:
                    original_val = str(name_query).strip()
                    if " " in original_val:
                        first_part = original_val.split(" ")[0]
                        self.message_handler.add_message(
                            f"User resolution failed. Will search for both: '{original_val}' AND '{first_part}'"
                        )
                        filters[filter_key] = [original_val, first_part]
                        params["_merge_fallback_results"] = True
                    else:
                        filters[filter_key] = name_query

    async def _fetch_data_handling_multi_values(
        self,
        module: str,
        filters: Dict,
        limit: int = None,
        page: int = 1,
        iterate_pages: bool = False,
    ) -> Dict:
        """Handles cases where 'in' operator isn't supported by making parallel queries."""

        multi_value_key = None
        multi_values = []

        for k, v in filters.items():
            if isinstance(v, list):
                multi_value_key = k
                multi_values = v
                break
            elif isinstance(v, dict) and isinstance(v.get("value"), list):
                multi_value_key = k
                multi_values = v["value"]
                break

        if not multi_value_key:
            return await asyncio.to_thread(
                crm_tools.query_crm_data,
                module=module,
                filters=filters,
                max_records=limit,
                page_number=page,
                iterate_pages=iterate_pages,
            )

        self.message_handler.add_message(
            f"Splitting query for {len(multi_values)} values in '{multi_value_key}'..."
        )

        tasks = []
        for val in multi_values:
            single_filter = filters.copy()
            single_filter[multi_value_key] = val

            tasks.append(
                asyncio.to_thread(
                    crm_tools.query_crm_data,
                    module=module,
                    filters=single_filter,
                    max_records=limit,
                    page_number=page,
                    iterate_pages=iterate_pages,
                )
            )

        results = await asyncio.gather(*tasks)
        all_records = []
        seen_ids = set()

        for res in results:
            if "records" in res:
                for rec in res["records"]:
                    if rec["id"] not in seen_ids:
                        all_records.append(rec)
                        seen_ids.add(rec["id"])

        return {
            "count": len(all_records),
            "records": all_records,
            "module": module,
            "data_id": crm_tools._cache_data(all_records),
        }

    # =========================================================================
    # FORMATTING LAYER (LLM)
    # =========================================================================

    async def _format_response_with_llm(
        self,
        query: str,
        task_config: Dict,
        task_result: Dict,
        chat_history: List[ChatMessage] = [],
    ) -> str:
        """
        Full LLM Formatting Layer.
        - Processes ENTIRE dataset (no sampling).
        - Generates Header, Summary, AND Table/List.
        - Strictly preserves hyperlinks.
        """
        if task_result.get("type") == "error":
            return f"**Error:** {task_result.get('message')}"

        module = task_config.get("module")
        fetched_count = task_result.get("count", 0)
        total_count = task_result.get("total_count", fetched_count)

        records = task_result.get("records", [])
        module_config = crm_tools.CRM_MODULES.get(module, {})
        enums = module_config.get("enums", {})
        if enums:
            processed_records = []
            for rec in records:
                new_rec = rec.copy()
                for key, val in rec.items():
                    if key in enums and isinstance(enums[key], dict):
                        new_rec[key] = enums[key].get(val, val)
                processed_records.append(new_rec)
            records = processed_records
        records_context = json.dumps(records, default=str, separators=(",", ":"))
        task_type = task_result.get("type", "list")
        calculated_context = ""
        if task_type == "sum" and task_result.get("summary"):
            calculated_context = f"Calculated Total: {task_result.get('summary')}"
        elif task_type == "count":
            calculated_context = f"Calculated Count: {task_result.get('message')}"

        summary_keywords = [
            "summar",
            "overview",
            "analy",
            "insight",
            "explain",
            "detail",
            "description",
        ]
        show_narrative = any(k in query.lower() for k in summary_keywords)

        narrative_instruction = ""
        if show_narrative:
            narrative_instruction = """
            3. **Deep Summarization & Narrative (CRITICAL - REQUESTED)**:
               - The user explicitly asked for a summary/overview.
               - **Action**: Analyze the 'description' (or 'company_research_c') text and
                 write a **rich, narrative summary**.
               - **Be specific**: Mention client concerns, key topics, dates, and names.
               - This is a priority section since the user asked for it.
            """
        else:
            narrative_instruction = """
            3. **Deep Summarization & Narrative (SKIP)**:
               - The user did NOT explicitly ask for a summary.
               - **Action**: **SKIP** the narrative summary.
               - **Do NOT** output any analysis paragraph. Go straight to the Table/List.
            """

        module_config = crm_tools.CRM_MODULES.get(module, {})
        default_cols = module_config.get("default_display_columns", [])
        renames = module_config.get("column_renames", {})
        user_requested_cols = task_config.get("columns", [])

        target_cols = []
        col_instruction_intro = ""

        if not user_requested_cols:
            target_cols = default_cols
            col_instruction_intro = "Strictly use the following Default Columns:"
        elif user_requested_cols == ["__ALL__"]:
            col_instruction_intro = "Include all non-internal data columns."
        else:
            target_cols = user_requested_cols
            col_instruction_intro = (
                "Display ONLY these explicitly requested columns by user:"
            )

        col_list_str = ""
        if target_cols:
            lines = []
            for col in target_cols:
                api_key = renames.get(col)
                if not api_key:
                    for k, v in renames.items():
                        if k.lower() == col.lower():
                            api_key = v
                            break
                mapping_hint = f"(Data Field: `{api_key}`)" if api_key else ""
                currency_hint = ""
                check_term = (col + " " + (api_key or "")).lower()
                if any(
                    x in check_term
                    for x in [
                        "amount",
                        "price",
                        "cost",
                        "revenue",
                        "margin",
                        "total",
                        "value",
                    ]
                ):
                    currency_hint = " **(Format as ₹)**"
                lines.append(f"   - **{col}** {mapping_hint}{currency_hint}")
            col_list_str = "\n".join(lines)

        prompt = f"""
        You are a CRM Insight Analyst.

        USER QUERY: "{query}"
        SYSTEM DATA:
        - Module: {module}
        - Records Fetched (Visible to you): {fetched_count}
        - Total Records Existent in CRM: {total_count}
        - Raw Records (JSON): {records_context}
        {calculated_context}

        INSTRUCTIONS:
        1. **Goal**: Generate the FINAL markdown response.

        2. **Response Style by Task Type (CRITICAL)**:
                     - **'sum'**: The USER wants a number (Financial Total).
                         Output the "Calculated Total" string provided in system data
                         VERBATIM as a prominent header. Do NOT convert currency or assume USD.
                     - **'count'**: The USER wants a number (Count). State the count clearly.
                         Do NOT show a table if the user only asked for a count.
                     - **'list'**: The USER wants to see the data. Output a Markdown Table.
                         Example: "List opportunities", "Give me a list".
                     - **'chart'**: The USER wants a graph. Output a chart.

                3. Structure (CRITICAL)*:
                     - **Header**: ALWAYS start with a Level 3 Header (e.g., `### Calls for Prateek...`).
                     - **Count Statement**: State how many records were found.
                     - **Narrative**: Include ONLY if instructed below.
                {narrative_instruction}

                4. **Table Formatting**:
                     - **Hyperlinks**: The JSON contains hyperlinks in format `[Name](URL)`.
                         **YOU MUST PRESERVE** these exact links in your output table/list. Do not strip the URLs.
                     - **Currency**: **ALWAYS** prefix monetary values (e.g. Amount, Price)
                         with the Rupee symbol (**₹**). Example: `₹1,500.00`.

                     - **Columns (STRICT)**:
                         {col_instruction_intro}
                         {col_list_str}
                     - **Do NOT rename headers**. Use the exact headers listed above.
                     - **Do NOT drop columns** listed above unless data is completely missing.

                     - **Description Column Handling**: **Exclude** the 'description'
                         (or 'company_research_c') column from the table to keep it clean,
                         **UNLESS** the user explicitly asked for that column to be shown
                         or it is in the list above.

                5. **Counts & Footer**:
                     - If 'Total Records Existent' ({total_count}) > 'Records Fetched' ({fetched_count}):
                         - Header/Intro: "Found **{total_count}** records. Displaying the latest **{fetched_count}**."
                         - Footer: "_Displaying {fetched_count} of {total_count} records._"
                     - Else:
                         - Header/Intro: "Found **{fetched_count}** records."
                         - Footer: (None required)

                OUTPUT FORMAT:
                [Header]
                [Total Count Statement]
                [Rich Narrative Summary (ONLY IF enabled in instruction #3)]

                [Markdown Table (ONLY if Task Type is 'list' OR user explicitly asked for details)]
                [Footer Note (if applicable)]
                """
        try:
            start_time = datetime.now()
            response = await self.llm.acomplete(prompt, temperature=0.0)
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"LLM Full Response Generation: {duration:.4f}s | Records processed: {len(records)}"
            )
            return str(response).strip()
        except Exception as e:
            logger.error(f"Formatting failed: {e}")
            return f"### {module}\nFound **{fetched_count}** records."

    async def _execute_sum_workflow(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self.message_handler.add_message(f"Querying {params['module']}...")
        result = await self._fetch_data_handling_multi_values(
            module=params["module"],
            filters=params.get("filters", {}),
            limit=params.get("limit") or 40000,
            page=1,
            iterate_pages=True,
        )
        count = result["count"]
        self.message_handler.add_message(f" Found {count:,} records")

        base_resp = {"type": "sum", "module": params["module"], "count": count}
        base_resp["records"] = result.get("records", [])[:20]

        if count == 0:
            base_resp["summary"] = f"No records found for {params['module']}."
            return base_resp

        self.message_handler.add_message("Calculating totals...")
        total_result = await asyncio.to_thread(
            crm_tools.calculate_total_amount, data_id_or_records=result
        )
        summary = self._format_sum_response(total_result, count, params)

        base_resp["summary"] = summary
        base_resp["metadata"] = {"total_records": count}
        return base_resp

    async def _execute_count_workflow(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self.message_handler.add_message(f"🔍 Counting {params['module']}...")
        result = await self._fetch_data_handling_multi_values(
            module=params["module"],
            filters=params.get("filters", {}),
            limit=params.get("limit") or 40000,
            page=1,
            iterate_pages=True,
        )
        count = result["count"]
        self.message_handler.add_message(f"Found {count:,} records")
        return {
            "type": "count",
            "module": params["module"],
            "count": count,
            "message": f"Total Count: **{count:,}**",
            "records": result.get("records", [])[:20],
        }

    async def _execute_list_workflow(self, params: Dict[str, Any]) -> Dict[str, Any]:
        module_name = params["module"]
        filters = params.get("filters", {})
        display_lookups = params.get("filter_display_values", {})
        page_num = params.get("page", 1)
        user_limit = params.get("limit")
        limit = user_limit or 40000

        module_config = crm_tools.CRM_MODULES.get(module_name, {})
        renames = module_config.get("column_renames", {})
        api_to_display_map = {v: k for k, v in renames.items()}
        enums = module_config.get("enums", {})
        requested_raw = params.get("columns", [])
        fetch_fields = []
        if requested_raw and requested_raw == ["__ALL__"]:
            fetch_fields = ["__ALL__"]
        elif requested_raw:
            fetch_fields = self._map_user_terms_to_api_fields(
                module_name, requested_raw
            )
            if not isinstance(fetch_fields, list):
                fetch_fields = [fetch_fields]

        split_key = None
        split_values = []
        for k, v in filters.items():
            if isinstance(v, list):
                split_key = k
                split_values = v
                break
            elif isinstance(v, dict) and isinstance(v.get("value"), list):
                split_key = k
                split_values = v["value"]
                break

        if split_key:
            self.message_handler.add_message(
                f"Generating separate reports for: {', '.join(map(str, split_values))}..."
            )
            sub_results = []

            for val in split_values:
                sub_filters = filters.copy()
                sub_filters[split_key] = val
                label = str(val)

                result = await asyncio.to_thread(
                    crm_tools.query_crm_data,
                    module=module_name,
                    filters=sub_filters,
                    max_records=limit,
                    page_number=page_num,
                    iterate_pages=False,
                    fields_only=fetch_fields,
                )
                count = result["count"]
                crm_link = (
                    crm_tools.generate_crm_link(module_name, sub_filters)
                    if count > 0
                    else None
                )
                section = {
                    "label": label,
                    "count": count,
                    "records": result.get("records", []),
                    "table": "_No records found_",
                    "crm_link": crm_link,
                }

                if count > 0:
                    records = result["records"][:10]

                    requested_raw = params.get("columns", [])
                    if requested_raw == ["__ALL__"]:
                        all_keys = list(records[0].keys())
                        display_keys = self._filter_columns(all_keys, ["__ALL__"])
                    elif requested_raw:
                        display_keys = self._map_user_terms_to_api_fields(
                            module_name, requested_raw
                        )
                    else:
                        defaults = module_config.get("default_display_columns", [])
                        if defaults:
                            display_keys = self._map_user_terms_to_api_fields(
                                module_name, defaults
                            )
                        else:
                            all_keys = list(records[0].keys())
                            display_keys = self._filter_columns(all_keys)[:8]

                    display_format = params.get("display_format", "table")

                    if display_format == "table":
                        tbl = await asyncio.to_thread(
                            crm_tools.get_table_from_query,
                            data_id_or_records=result,
                            columns=display_keys,
                            module=module_name,
                            max_rows=10,
                            filters=sub_filters,
                            display_lookups=display_lookups,
                        )
                        section["table"] = tbl["table"]
                    else:
                        lines = []
                        for i, rec in enumerate(records, 1):
                            parts = []
                            for key in display_keys:
                                val = rec.get(key, "-")
                                if key in enums and isinstance(enums[key], dict):
                                    val = enums[key].get(val, val)
                                if display_lookups and key in display_lookups:
                                    val = display_lookups[key]
                                label_display = api_to_display_map.get(
                                    key, key.replace("_", " ").title()
                                )
                                parts.append(f"{label_display}: {val}")

                            lines.append(f"{i}. " + " | ".join(parts))
                        section["table"] = "\n".join(lines)

                sub_results.append(section)
            return {
                "type": "multi_list",
                "module": module_name,
                "sub_results": sub_results,
            }

        self.message_handler.add_message(
            f"Querying {module_name} (Page {page_num})..."
        )
        result = await asyncio.to_thread(
            crm_tools.query_crm_data,
            module=module_name,
            filters=filters,
            max_records=limit,
            page_number=page_num,
            fields_only=fetch_fields,
        )
        fetched_count = result["count"]
        meta_total = result.get("total_records_from_meta")
        final_total = fetched_count

        should_verify_total = (fetched_count >= 20) and (
            user_limit is None or user_limit > 20
        )

        if meta_total is not None:
            final_total = meta_total
        elif should_verify_total:
            self.message_handler.add_message("Verifying total count...")
            try:
                count_result = await asyncio.to_thread(
                    crm_tools.query_crm_data,
                    module=module_name,
                    filters=filters,
                    iterate_pages=True,
                    fields_only=["id"],
                )
                final_total = count_result["count"]
            except Exception as e:
                logger.warning(f"Background count failed: {e}")
                final_total = fetched_count

        crm_link = crm_tools.generate_crm_link(module_name, filters)
        base_resp = {
            "type": "list",
            "module": module_name,
            "count": fetched_count,
            "total_count": final_total,
            "records": result.get("records", []),
            "crm_link": crm_link,
        }

        if final_total == 0:
            base_resp["table"] = "_No records found_"
            base_resp["message"] = "No records found."
            return base_resp

        display_format = params.get("display_format", "table")

        records = result["records"][:20]

        requested_raw = params.get("columns", [])
        if requested_raw == ["__ALL__"]:
            all_keys = list(records[0].keys())
            display_keys = self._filter_columns(all_keys, ["__ALL__"])
        elif requested_raw:
            display_keys = self._map_user_terms_to_api_fields(
                module_name, requested_raw
            )
        else:
            defaults = module_config.get("default_display_columns", [])
            if defaults:
                display_keys = self._map_user_terms_to_api_fields(module_name, defaults)
            else:
                all_keys = list(records[0].keys())
                display_keys = self._filter_columns(all_keys)[:8]

        if display_format == "table":
            table_result = await asyncio.to_thread(
                crm_tools.get_table_from_query,
                data_id_or_records=result,
                columns=display_keys,
                module=module_name,
                max_rows=20,
                filters=filters,
                display_lookups=display_lookups,
            )
            base_resp["table"] = table_result["table"]
        else:
            lines = []
            start_index = (page_num - 1) * 20 + 1

            for i, rec in enumerate(records, start_index):
                parts = []
                for key in display_keys:
                    val = rec.get(key, "-")
                    if key in enums and isinstance(enums[key], dict):
                        val = enums[key].get(val, val)
                    if display_lookups and key in display_lookups:
                        val = display_lookups[key]

                    label_display = api_to_display_map.get(
                        key, key.replace("_", " ").title()
                    )
                    parts.append(f"{label_display}: {val}")

                lines.append(f"{i}. " + " | ".join(parts))

            text_out = "\n".join(lines)
            if final_total > 20:
                text_out += (
                    f"\n_...and {final_total - 20} more records available in CRM._"
                )
            base_resp["table"] = text_out
        return base_resp

    async def _execute_chart_workflow(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self.message_handler.add_message(f"Querying {params['module']}...")
        result = await self._fetch_data_handling_multi_values(
            module=params["module"],
            filters=params.get("filters", {}),
            limit=params.get("limit"),
            page=1,
            iterate_pages=True,
        )
        base_resp = {"type": "chart", "module": params["module"]}

        if result["count"] == 0:
            base_resp["message"] = f"No data found for {params['module']}."
            return base_resp

        self.message_handler.add_message(f"Found {result['count']:,} records")

        chart_config = params.get("chart_config", {})
        x_col = chart_config.get("x_col")
        y_col = chart_config.get("y_col")

        mappings = crm_tools.CRM_MODULES.get(params["module"], {}).get(
            "field_mapping", {}
        )
        if x_col and x_col.lower() in mappings:
            x_col = mappings[x_col.lower()]
        if y_col and y_col.lower() in mappings:
            y_col = mappings[y_col.lower()]

        self.message_handler.add_message(f"Generating chart by '{x_col}'...")

        try:
            chart_result = await asyncio.to_thread(
                crm_tools.create_chart_from_crm_data,
                data_id_or_records=result,
                x_col=x_col,
                y_col=y_col,
                chart_type=chart_config.get("chart_type", "bar"),
                title=f"{params['module']} Analysis",
            )
            message = chart_result.get("analysis", "Chart created.")
            if chart_result.get("warning"):
                message += "\n\n" + chart_result["warning"]

            base_resp["plot_data"] = chart_result
            base_resp["message"] = message
            return base_resp
        except Exception as e:
            base_resp["message"] = f"Chart error: {e}"
            return base_resp

    def _format_sum_response(self, total_result: Dict, count: int, params: Dict) -> str:
        formatted_total = total_result["formatted_total"]

        lines = ["### Financial Summary", ""]
        lines.append(f"### Total Amount: **{formatted_total}**")
        lines.append(f"**Total Records:** {count:,}")

        records_without = total_result.get("records_without_amount", 0)
        if records_without > 0:
            lines.append(
                f"> **Note:** {records_without} records were excluded from calculation (missing amount data)."
            )

        return "\n".join(lines)

    def _generate_task_id(self, module: str, filters: Dict) -> str:
        """Generates a deterministic hash based on module and filters."""
        filter_str = json.dumps(filters, sort_keys=True, default=str)
        raw_string = f"{module}|{filter_str}"
        return hashlib.md5(raw_string.encode("utf-8")).hexdigest()

    async def _process_task(
        self,
        task_config: Dict[str, Any],
        user_query: str,
        page: int = 1,
        target_task_id: Optional[str] = None,
        chat_history: List[ChatMessage] = [],
        user_date_format: str = "YYYY-MM-DD",
    ) -> Dict[str, Any]:
        """
        Execute workflow and assemble the final response programmatically.
        """
        try:
            if task_config.get("filters"):
                await self._resolve_smart_filters(task_config)

            current_module = task_config.get("module")
            filters = task_config.get("filters", {})
            current_task_id = self._generate_task_id(current_module, filters)

            if target_task_id:
                if current_task_id == target_task_id:
                    task_page = page
                else:
                    task_page = 1
            else:
                task_page = page
            task_config["page"] = task_page
            q_type = task_config.get("type")
            result = None

            if q_type == "list":
                result = await self._execute_list_workflow(task_config)
            elif q_type == "sum":
                result = await self._execute_sum_workflow(task_config)
            elif q_type == "count":
                result = await self._execute_count_workflow(task_config)
            elif q_type == "chart":
                result = await self._execute_chart_workflow(task_config)
            else:
                return {"type": "error", "message": f"Unknown query type: {q_type}"}

            final_content = ""
            final_content += f"<!-- task_id: {current_task_id} -->\n"

            if result.get("type") == "multi_list":
                sub_results = result.get("sub_results", [])
                if task_config.get("_merge_fallback_results"):
                    successful_subs = [s for s in sub_results if s["count"] > 0]
                    if successful_subs:
                        sub_results = successful_subs
                    else:
                        sub_results = sub_results[:1]
                for sub in sub_results:
                    sub_config = task_config.copy()
                    sub_config["filters"] = sub.get("filters", {})
                    sub_result = {
                        "type": "list",
                        "count": sub["count"],
                        "records": sub.get("records", []),
                        "crm_link": sub.get("crm_link"),
                    }

                    self.message_handler.add_message(
                        f"Generating full response for: {sub['label']}..."
                    )
                    formatted_part = await self._format_response_with_llm(
                        user_query, sub_config, sub_result, chat_history
                    )
                    final_content += f"{formatted_part}\n"

                    if sub.get("crm_link") and sub.get("count", 0) >= 20:
                        final_content += f"\n **[View in CRM]({sub['crm_link']})**\n"
                    final_content += "\n---\n"
            else:
                self.message_handler.add_message("Generating full response...")
                formatted_part = await self._format_response_with_llm(
                    user_query, task_config, result, chat_history
                )
                final_content += f"{formatted_part}\n"

                if result.get("crm_link") and result.get("count", 0) >= 20:
                    final_content += f"\n<!-- [View in CRM]({result['crm_link']}) -->\n"

            return {
                "type": "final_text",
                "content": final_content,
                "plot_data": result.get("plot_data"),
                "task_info": {
                    "task_id": current_task_id,
                    "module": current_module,
                    "page": task_page,
                },
            }

        except Exception as e:
            logger.error(f"Task failed: {e}")
            return {"type": "error", "content": f"Task failed: {str(e)}"}

    async def run_async(
        self,
        user_input: str,
        memory: Optional[ChatMemoryBuffer] = None,
        page: int = 1,
        target_task_id: Optional[str] = None,
        user_date_format: str = "YYYY-MM-DD",
    ):
        try:
            mcp_logger.clear()
            mcp_logger.log_step(
                "prompt",
                "User Request",
                {
                    "query": user_input,
                    "page": page,
                    "target_task_id": target_task_id,
                    "date_format": user_date_format,
                },
            )
            chat_history = []
            if memory:
                chat_history = memory.get_all()

            parsed_response = await self._classify_query(
                user_input, chat_history, user_date_format
            )
            tasks = parsed_response.get("tasks", [])
            response_payload = {"messages": [], "result": "", "metadata": {}}

            final_text = ""
            plot_data = None
            task_metadata = []
            if not tasks:
                final_text = (
                    "Could not identify module from query and conversation history."
                )
            else:
                if len(tasks) > 1:
                    self.message_handler.add_message(
                        f"Detected {len(tasks)} requests. Executing in parallel..."
                    )
                results = await asyncio.gather(
                    *(
                        self._process_task(
                            task,
                            user_input,
                            page,
                            target_task_id,
                            chat_history,
                            user_date_format,
                        )
                        for task in tasks
                    )
                )
                final_text = "\n\n---\n\n".join([r.get("content", "") for r in results])
                task_metadata = [
                    r.get("task_info") for r in results if r.get("task_info")
                ]
                for res in results:
                    if res.get("plot_data"):
                        plot_data = res["plot_data"]
                        break
            if memory:
                memory.put(ChatMessage(role=MessageRole.USER, content=user_input))
                memory.put(ChatMessage(role=MessageRole.ASSISTANT, content=final_text))
            self.message_handler.set_final_result(
                final_text,
                metadata={
                    "execution_log": mcp_logger.get_execution_log(),
                    "task_list": task_metadata,
                },
            )
            response_payload["messages"] = self.message_handler.get_messages()
            response_payload["result"] = self.message_handler.get_final_result()
            response_payload["metadata"] = self.message_handler.get_metadata()

            if plot_data:
                response_payload["plot_data"] = plot_data

            return response_payload
        except Exception as e:
            logger.error(f"Agent error: {e}")
            return {"messages": [], "result": f"Error: {e}"}

    # Plot loading removed — previously unused

    def run(
        self,
        user_input: str,
        memory: Optional[ChatMemoryBuffer] = None,
        page: int = 1,
        target_task_id: Optional[str] = None,
        user_date_format: str = "YYYY-MM-DD",
    ) -> Generator[Dict, None, None]:
        """
        Sync wrapper that yields results as generator for streaming response.
        """
        import asyncio

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self.run_async(
                    user_input, memory, page, target_task_id, user_date_format
                )
            )
            loop.close()
        except Exception as e:
            logger.error(f"Failed to run async agent: {e}", exc_info=True)
            result = {
                "messages": [f"Error: {str(e)}"],
                "result": f"Failed to process query: {str(e)}",
                "metadata": None,
            }
        for msg in result.get("messages", []):
            yield {"message": msg}

        final_dict = {"result": result.get("result", "")}
        if result.get("plot_data"):
            final_dict["plot_data"] = result["plot_data"]
        if result.get("metadata"):
            final_dict["metadata"] = result["metadata"]

        yield final_dict
