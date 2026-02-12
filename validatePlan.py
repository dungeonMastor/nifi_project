import json
import os
import requests
import urllib3
from dotenv import load_dotenv
from google import genai
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

load_dotenv()

class PlanValidator:

    def __init__(self, plan_path="plan.json"):
        self.console = Console()
        self.plan_path = plan_path
        self.data = None
        self.errors = []
        self.nifi_base_url = os.getenv("NIFI_BASE_URL")
        self.nifi_auth_token = os.getenv("NIFI_AUTH")
        self.verify_ssl = os.getenv("NIFI_VERIFY_SSL", "true").lower() == "true"
        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.available_processor_types = {}

        self.changes_made = False

        self.sandbox_pg_id = os.getenv("SANDBOX_VALIDATION_PROCESSOR_GROUP")
        
        # LLM Configuration
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.llm_model = os.getenv("LLM_MODEL")
        self.max_retries = int(os.getenv("MAX_VALIDATION_FIX_RETRIES", "3"))
        
        self.client = None
        if self.gemini_api_key:
            self.client = genai.Client(api_key=self.gemini_api_key)
        else:
            print("Warning: GEMINI_API_KEY not set. Auto-fixes via LLM will be disabled.")

    def load_plan(self):
        if not os.path.exists(self.plan_path):
            self.errors.append(f"File not found: {self.plan_path}")
            return False
        try:
            with open(self.plan_path, 'r') as f:
                self.data = json.load(f)
            return True
        except json.JSONDecodeError as e:
            self.errors.append(f"JSON Parse Error: {e}")
            return False

    def validate_structure(self):
        if not self.data:
            return

        data = self.data

        # 1. plan_summary
        if "plan_summary" not in data:
            self.errors.append("Missing required field: `plan_summary`")
        elif not isinstance(data["plan_summary"], str):
            self.errors.append("Field `plan_summary` must be a string (markdown)")

        # 2. plan_details
        if "plan_details" not in data:
            self.errors.append("Missing required field: `plan_details`")
            return 
        
        details = data["plan_details"]
        if not isinstance(details, dict):
            self.errors.append("Field `plan_details` must be an object (dict)")
            return

        # a. processors
        if "processors" not in details:
            self.errors.append("Missing required field in plan_details: `processors`")
        elif not isinstance(details["processors"], list):
            self.errors.append("Field `plan_details.processors` must be an array (list)")
        
        if isinstance(details.get("processors"), list):
             for index, processor in enumerate(details["processors"]):
                proc_prefix = f"Processor[{index}]"
                required_processor_fields = {
                    "id": (str, "string"),
                    "name": (str, "string"),
                    "properties": (dict, "object"),
                    "scheduling": (dict, "object"),
                    "auto_terminated_relationships": (list, "array"),
                    "type": (str, "string")
                }
                
                for field, (expected_type, type_name) in required_processor_fields.items():
                    if field not in processor:
                        self.errors.append(f"{proc_prefix}: Missing required field `{field}`")
                    elif not isinstance(processor[field], expected_type):
                        self.errors.append(f"{proc_prefix}: Field `{field}` must be a {type_name}, got {type(processor[field]).__name__}")

        # b. connections
        if "connections" not in details:
            self.errors.append("Missing required field in plan_details: `connections`")
        elif not isinstance(details["connections"], list):
            self.errors.append("Field `plan_details.connections` must be an array (list)")
        else:
            for index, connection in enumerate(details["connections"]):
                conn_prefix = f"Connection[{index}]"
                required_connection_fields = {
                    "from_id": (str, "string"),
                    "to_id": (str, "string"),
                    "relationships": (list, "array")
                }

                for field, (expected_type, type_name) in required_connection_fields.items():
                    if field not in connection:
                        self.errors.append(f"{conn_prefix}: Missing required field `{field}`")
                    elif not isinstance(connection[field], expected_type):
                        self.errors.append(f"{conn_prefix}: Field `{field}` must be a {type_name}, got {type(connection[field]).__name__}")

    def fetch_nifi_processor_types(self):
        if not self.nifi_base_url:
            self.errors.append("NIFI_BASE_URL not set in .env")
            return False

        headers = {}
        if self.nifi_auth_token:
            headers["Authorization"] = f"Bearer {self.nifi_auth_token}"

        try:
            url = f"{self.nifi_base_url}/nifi-api/flow/processor-types"
            response = requests.get(url, headers=headers, verify=self.verify_ssl, timeout=10)
            response.raise_for_status()
            
            types_data = response.json()
            if "processorTypes" in types_data:
                for pt in types_data["processorTypes"]:
                    self.available_processor_types[pt["type"]] = pt
            else:
                 self.errors.append("Unexpected response format from NiFi processor-types endpoint")
                 return False
            
            return True

        except requests.exceptions.RequestException as e:
            self.errors.append(f"Failed to fetch processor types from NiFi: {e}")
            return False

    def save_plan(self):
        try:
            with open(self.plan_path, 'w') as f:
                json.dump(self.data, f, indent=2)
            self.console.print(f"[green]Updated {self.plan_path} with corrected types.[/green]")
        except IOError as e:
            self.errors.append(f"Failed to save updated plan: {e}")

    def validate_processor_types(self):
        if not self.data or "plan_details" not in self.data or "processors" not in self.data["plan_details"]:
            return

        if not self.available_processor_types:
            success = self.fetch_nifi_processor_types()
            if not success:
               return

        processors = self.data["plan_details"]["processors"]
        changes_made = False

        for index, processor in enumerate(processors):
            p_type = processor.get("type")
            p_name = processor.get("name", f"Processor-{index}")
            
            if not p_type:
                continue

            if p_type in self.available_processor_types:
                continue

            matched_fqcn = None
            possible_matches = []
            
            for fqcn in self.available_processor_types:
                if fqcn.endswith(f".{p_type}") or fqcn == p_type:
                     possible_matches.append(fqcn)
            
            if len(possible_matches) == 1:
                matched_fqcn = possible_matches[0]
            elif len(possible_matches) > 1:
                self.errors.append(f"Processor[{index}] '{p_name}': Type '{p_type}' is ambiguous. Matches: {possible_matches}")
                continue
            
            if matched_fqcn:
                if processor["type"] != matched_fqcn:
                    self.console.print(f"[cyan]Updating processor '{p_name}': '{p_type}' -> '{matched_fqcn}'[/cyan]")
                    processor["type"] = matched_fqcn
                    changes_made = True
            else:
                self.errors.append(f"Processor[{index}] '{p_name}': Type '{p_type}' does not exist in your NiFi instance.")
        
        self.changes_made = changes_made

    def validate_processor_configuration(self):
        if not self.sandbox_pg_id:
            self.errors.append("SANDBOX_VALIDATION_PROCESSOR_GROUP not set in .env")
            return

        if not self.data or "plan_details" not in self.data or "processors" not in self.data["plan_details"]:
            return

        headers = {
            "Content-Type": "application/json"
        }
        if self.nifi_auth_token:
            headers["Authorization"] = f"Bearer {self.nifi_auth_token}"

        connections_map = {} 
        if self.data.get("plan_details") and "connections" in self.data["plan_details"]:
             for conn in self.data["plan_details"]["connections"]:
                 src = conn.get("from_id")
                 rels = conn.get("relationships", [])
                 if src:
                     if src not in connections_map:
                         connections_map[src] = set()
                     connections_map[src].update(rels)

        processors = self.data["plan_details"]["processors"]
        processors = self.data["plan_details"]["processors"]
        self.console.print(f"[bold]Validating configuration for {len(processors)} processors in sandbox...[/bold]")

        for index, processor in enumerate(processors):
            p_name = processor.get("name", f"Processor-{index}")
            p_type = processor.get("type")
            p_id = processor.get("id")
            
            retry_count = 0
            self.console.print(f"\n[bold]Validating {p_name} ({p_type})[/bold]")
            
            max_attempts = self.max_retries + 1 if self.gemini_api_key else 1

            while retry_count < max_attempts:
                # Refresh properties from self.data as they might have been updated
                current_processor_data = self.data["plan_details"]["processors"][index]
                p_properties = current_processor_data.get("properties", {})
                clean_properties = {k: v for k, v in p_properties.items() if isinstance(v, str)}
                
                scheduling = current_processor_data.get("scheduling", {})
                
                # 1. Create Processor
                create_url = f"{self.nifi_base_url}/nifi-api/process-groups/{self.sandbox_pg_id}/processors"
                payload = {
                    "revision": { "version": 0 },
                    "component": {
                        "type": p_type,
                        "name": f"VALIDATION-{p_name}",
                        "position": { "x": index * 500, "y": 0 },
                        "config": { 
                            "properties": clean_properties,
                            "schedulingStrategy": scheduling.get("strategy"),
                            "schedulingPeriod": scheduling.get("period"),
                            "concurrentlySchedulableTaskCount": scheduling.get("concurrent_tasks")
                        }
                    }
                }

                try:
                    response = requests.post(create_url, json=payload, headers=headers, verify=self.verify_ssl, timeout=15)
                    
                    if response.status_code != 201:
                        error_text = response.text
                        if response.status_code == 400 and "scheduling period" in error_text.lower():
                            # Attempt LLM Fix for Scheduling
                             if retry_count < self.max_retries and self.gemini_api_key:
                                self.console.print(f"[red]Scheduling Error: {error_text}[/red]")
                                scheduling_config = current_processor_data.get("scheduling", {})
                                fixes = self.resolve_scheduling_errors_with_llm(scheduling_config, error_text)
                                
                                if fixes:
                                    self.console.print(f"[cyan]Applying LLM scheduling fixes for '{p_name}'...[/cyan]")
                                    self.data["plan_details"]["processors"][index]["scheduling"] = fixes
                                    self.changes_made = True
                                    retry_count += 1
                                    continue
                        
                        self.errors.append(f"Processor[{index}] '{p_name}': Failed to create in sandbox. Status: {response.status_code}, Response: {error_text}")
                        break 

                    proc_data = response.json()
                    proc_id = proc_data["component"]["id"]
                    proc_revision = proc_data["revision"]["version"]
                    
                    # --- Auto-Correct Controller Service Properties ---
                    if "config" in proc_data["component"] and "descriptors" in proc_data["component"]["config"]:
                        descriptors = proc_data["component"]["config"]["descriptors"]
                        for prop_name, descriptor in descriptors.items():
                            if "identifiesControllerService" in descriptor:
                                 allowable_values = descriptor.get("allowableValues")
                                 if allowable_values and len(allowable_values) == 1:
                                     single_value = allowable_values[0]["allowableValue"]["value"]
                                     current_val = p_properties.get(prop_name)
                                     
                                     if current_val != single_value:
                                         print(f"Auto-correcting '{p_name}' property '{prop_name}': {current_val} -> {single_value}")
                                         if "properties" not in self.data["plan_details"]["processors"][index]:
                                              self.data["plan_details"]["processors"][index]["properties"] = {}
                                         
                                         self.data["plan_details"]["processors"][index]["properties"][prop_name] = single_value
                                         self.changes_made = True

                    # --- Cleanup Dynamic Properties ---
                    supports_dynamic_props = proc_data["component"].get("supportsDynamicProperties", False)
                    if not supports_dynamic_props and "config" in proc_data["component"] and "descriptors" in proc_data["component"]["config"]:
                        valid_descriptors = set(proc_data["component"]["config"]["descriptors"].keys())
                        
                        current_props_dict = self.data["plan_details"]["processors"][index].get("properties", {})
                    for prop_key in current_props_dict.keys():
                            if prop_key not in valid_descriptors:
                                # Mark as invalid for LLM context
                                filtered_errors.append(f"Property '{prop_key}' is invalid because the processor does not support dynamic properties. It must be removed.")

                    validation_errors = proc_data["component"].get("validationErrors", [])

                    # 2. Check Errors
                    filtered_errors = []
                    for error in validation_errors:
                        if error.startswith("'Relationship") or error.startswith("'Upstream Connections"):
                            continue
                        filtered_errors.append(error)
                    
                    # 3. Check Relationship Errors
                    available_rels = set()
                    if "relationships" in proc_data["component"]:
                        for r in proc_data["component"]["relationships"]:
                            available_rels.add(r["name"])
                    
                    auto_terminated = set(current_processor_data.get("auto_terminated_relationships", []))
                    connected = connections_map.get(p_id, set())
                    
                    relationship_errors = []
                    if p_id in connections_map:
                         unaccounted = available_rels - (auto_terminated | connected)
                         if unaccounted:
                              relationship_errors.append(f"Relationships {unaccounted} are not auto-terminated or connected.")
                             
                    # Combine all errors found
                    all_errors_found = []
                    if filtered_errors:
                        for err in filtered_errors:
                            all_errors_found.append(f"Processor[{index}] '{p_name}' Config Error: {err}")
                    if relationship_errors:
                         for err in relationship_errors:
                             all_errors_found.append(f"Processor[{index}] '{p_name}' Relationship Error: {err}")

                    # 4. Delete Processor
                    delete_url = f"{self.nifi_base_url}/nifi-api/processors/{proc_id}"
                    delete_params = { "version": proc_revision }
                    client_id = proc_data["revision"].get("clientId")
                    if client_id: delete_params["clientId"] = client_id
                    
                    try:
                        requests.delete(delete_url, params=delete_params, headers=headers, verify=self.verify_ssl, timeout=10)
                    except Exception:
                        pass 

                    if not all_errors_found:
                        self.console.print("[green]All Validation Passed[/green]")
                        self.console.print("[dim]Moving to next validation[/dim]")
                        break # Success!

                    # If errors exist and we have retries left
                    if retry_count < self.max_retries and self.gemini_api_key and (filtered_errors or relationship_errors):
                        # Attempt LLM Fix (Config + Relationships)
                        descriptors = proc_data["component"]["config"]["descriptors"]
                        
                        # Combine errors for LLM context
                        combined_errors = filtered_errors + relationship_errors
                        
                        self.console.print(f"[yellow]Validation Errors ({len(combined_errors)}):[/yellow]")
                        for err in combined_errors:
                            self.console.print(f"- {err}")

                        fixes = self.resolve_validation_errors_with_llm(current_processor_data, combined_errors, descriptors)
                        
                        if fixes:
                            self.console.print(f"[cyan]Applying LLM fixes for {p_name} ({p_type})[/cyan]")
                            
                            # Apply properties
                            if "properties" in fixes:
                                self.data["plan_details"]["processors"][index]["properties"] = fixes["properties"]
                                self.changes_made = True
                            
                            # Apply auto-terminated relationships
                            if "auto_terminated_relationships" in fixes:
                                self.data["plan_details"]["processors"][index]["auto_terminated_relationships"] = fixes["auto_terminated_relationships"]
                                self.changes_made = True
                            
                            retry_count += 1
                            continue # Retry loop
                    
                    # No fixes or retries exhausted
                    self.errors.extend(all_errors_found)
                    break 

                except requests.exceptions.RequestException as e:
                    self.errors.append(f"Processor[{index}] '{p_name}': NiFi API error during sandbox validation: {e}")
                    break

    def resolve_validation_errors_with_llm(self, processor_config, validation_errors, descriptors):
        if not self.gemini_api_key:
            return False

        # self.console.print(f"[yellow]--- Attempting to fix validation errors with LLM ({self.llm_model}) ---[/yellow]")

        # 1. Structure Errors with Context
        error_context = []
        for err in validation_errors:
            import re
            match = re.search(r"'([^']+)' is invalid", err)
            prop_details = {}
            if match:
                prop_name = match.group(1)
                if prop_name in descriptors:
                    desc = descriptors[prop_name]
                    prop_details = {
                        "description": desc.get("description"),
                        "required": desc.get("required"),
                        "sensitive": desc.get("sensitive"),
                        "supportsEl": desc.get("supportsEl") # Expression Language
                    }
                    if "allowableValues" in desc and desc["allowableValues"]:
                        prop_details["allowableValues"] = [
                            {"value": av["allowableValue"]["value"], "displayName": av["allowableValue"]["displayName"]}
                            for av in desc["allowableValues"]
                        ]
            
            error_context.append({
                "error_message": err,
                "property_details": prop_details
            })

        # 2. Build Prompt
        system_prompt = """You are a NiFi expert. Your goal is to fix validation errors in a generic Processor configuration.
You will receive the current processor configuration and a list of validation errors.
You must output a JSON object containing the COMPLETE 'properties' object AND the 'auto_terminated_relationships' list.
Rules:
1. Return ONLY valid JSON. No markdown formatting, no explanations.
2. The JSON must have two top-level keys: 'properties' (dict) and 'auto_terminated_relationships' (list).
3. For 'properties': Remove invalid/dynamic props, correct values, provide defaults.
4. For 'auto_terminated_relationships': Include any relationships that should be auto-terminated (e.g., 'success', 'failure', 'original') based on the errors and common processor patterns.
5. If relationships are missing, add them to 'auto_terminated_relationships' if they are not meant to be connected.
"""
        
        user_prompt = f"""
Processor Configuration:
{json.dumps(processor_config, indent=2)}

Validation Errors:
{json.dumps(error_context, indent=2)}

Please provide the corrected configuration JSON with 'properties' and 'auto_terminated_relationships'.
"""
        
        full_prompt = system_prompt + "\n" + user_prompt
        # print(f"\n[LLM PROMPT]:\n{full_prompt}\n")

        try:
            # 3. Call LLM
            if not self.client:
                return False

            response = self.client.models.generate_content(
                model=self.llm_model,
                contents=full_prompt,
                config={
                    'response_mime_type': 'application/json'
                }
            )
            
            # 4. Parse Response
            try:
                text = response.text.strip()
                if text.startswith("```json"):
                    text = text[7:]
                if text.endswith("```"):
                    text = text[:-3]
                
                fixes = json.loads(text)
                
                if not isinstance(fixes, dict):
                    print("LLM Error: Response is not a JSON object.")
                    return False
                
                properties_to_update = fixes.get("properties")
                relationships_to_update = fixes.get("auto_terminated_relationships")
                
                if not properties_to_update and not relationships_to_update:
                    print("LLM Response contained no properties or relationships to update.")
                    return False

                # 5. Update Configuration
                return fixes

            except json.JSONDecodeError as e:
                print(f"LLM Error: Failed to parse JSON response: {e}")
                return False

        except Exception as e:
            print(f"LLM Error: API call failed: {e}")
            return False

    def resolve_scheduling_errors_with_llm(self, scheduling_config, error_message):
        if not self.gemini_api_key:
            return False
        
        system_prompt = """You are a NiFi expert. Your goal is to fix a Processor's scheduling configuration based on an error message.
You will receive the current scheduling configuration and the error message returned by NiFi.
You must output a JSON object containing the corrected 'scheduling' configuration.
Rules:
1. Return ONLY valid JSON. No markdown.
2. The JSON keys must match the standard NiFi scheduling keys (strategy, period, concurrent_tasks).
3. Fix the specific error mentioned (e.g., invalid time duration format).
4. For CRON_DRIVEN strategies, you MUST use a 6-field Quartz cron expression (e.g. "0 */15 * * * ?").
"""
        
        user_prompt = f"""
Current Configuration:
{json.dumps(scheduling_config, indent=2)}

Error Message:
{error_message}

Please provide the corrected scheduling JSON object.
"""

        full_prompt = system_prompt + "\n" + user_prompt
        # print(f"\n[LLM PROMPT]:\n{full_prompt}\n")

        try:
            if not self.client:
                return False

            response = self.client.models.generate_content(
                model=self.llm_model,
                contents=full_prompt,
                config={
                    'response_mime_type': 'application/json'
                }
            )
            
            text = response.text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            
            fixes = json.loads(text)
            
            return fixes.get("scheduling", fixes)

        except Exception as e:
            print(f"LLM Error: {e}")
            return False

    def resolve_structure_errors_with_llm(self):
        if not self.gemini_api_key:
            print("Error: GEMINI_API_KEY not set. Cannot use LLM to fix plan.")
            return False

        print(f"\n--- Attempting to fix plan structure/types with LLM ({self.llm_model}) ---")
        
        # Ensure we have processor types for context
        if not self.available_processor_types:
            self.console.print("[dim]Fetching available processor types for LLM context...[/dim]")
            self.fetch_nifi_processor_types()

        # Simplify available types for prompt to reduce token count
        available_types_summary = {
            t: {"description": d.get("description", "")[:100]} 
            for t, d in self.available_processor_types.items()
        }

        system_prompt = """You are a NiFi expert. Your goal is to fix structural validation errors in a NiFi plan.json file.
You will receive the current 'plan_details' and a list of validation errors.
You must output a JSON object representing the CORRECTED 'plan_details' object.
Rules:
1. Return ONLY valid JSON. No markdown.
2. The output must be the full 'plan_details' object with fixes applied.
3. Fix issues like invalid processor types by finding the closest match from the provided available types.
4. Do not invent new processor types. Use only what is available or standard NiFi types.
"""

        user_prompt = f"""
Current Plan Details:
{json.dumps(self.data.get("plan_details", {}), indent=2)}

Validation Errors:
{json.dumps(self.errors, indent=2)}

Available Processor Types (Reference):
{json.dumps(list(available_types_summary.keys()), indent=2)}

Please provide the corrected 'plan_details' JSON object.
"""
        
        full_prompt = system_prompt + "\n" + user_prompt
        full_prompt = system_prompt + "\n" + user_prompt

        try:
            if not self.client:
                return False

            response = self.client.models.generate_content(
                model=self.llm_model,
                contents=full_prompt,
                config={
                    'response_mime_type': 'application/json'
                }
            )
            
            
            # self.console.print(Panel(response.text[:500] + "...", title="LLM Response", style="dim"))
            
            text = response.text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            
            fixed_details = json.loads(text)
            
            if "plan_details" in fixed_details:
                fixed_details = fixed_details["plan_details"]
            
            self.data["plan_details"] = fixed_details
            self.changes_made = True
            return True

        except Exception as e:
            print(f"LLM Error during structure fix: {e}")
            return False

    def validate_controller_services(self):
        if not self.data or "plan_details" not in self.data or "processors" not in self.data["plan_details"]:
            return

        processors = self.data["plan_details"]["processors"]
        for index, processor in enumerate(processors):
            p_name = processor.get("name", f"Processor-{index}")
            properties = processor.get("properties", {})
            
            for prop_name, prop_value in properties.items():
                if prop_value == "CREATE_NEW_CS":
                    self.errors.append(f"Processor[{index}] '{p_name}': Property '{prop_name}' requires a new Controller Service (value is 'CREATE_NEW_CS'). Please create it manually in NiFi and update the plan.json with the actual UUID.")

    def run(self):
        while True:
            self.errors = []
            if not self.load_plan():
                self._print_errors()
                return

            self.changes_made = False
            
            # Initial Validation Checks
            initial_validations = [
                self.validate_structure,
                self.validate_processor_types,
                self.validate_controller_services
            ]

            print(f"Starting initial validaton for {self.plan_path}...")
            for validation_method in initial_validations:
                validation_method()

            # Check if initial validation passed
            if not self.errors:
                print("\nInitial checks passed.")
                break # Exit the loop and proceed to sandbox validation
            
            # Errors found: prompting user
            self.console.print(f"\n[bold red][STOP] Critical validation errors found (Total: {len(self.errors)})[/bold red]")
            for err in self.errors:
                 self.console.print(f" - {err}", style="red")
            
            choice = Prompt.ask("Select an option", choices=["Retry initial validation", "Fix the plan using LLM", "Exit"], default="Retry initial validation")
            
            if choice == "Retry initial validation":
                self.console.print("\n[yellow]Retrying validation...[/yellow]")
                continue # Loop back to start
            elif choice == "Fix the plan using LLM":
                if self.resolve_structure_errors_with_llm():
                    self.console.print("\n[green]LLM fixes applied. Saving plan and retrying validation...[/green]")
                    self.save_plan()
                    continue # Loop back to start (re-validate updated plan)
                else:
                    self.console.print("\n[red]Failed to fix plan with LLM. Please try manual fix.[/red]")
                    continue
            else:
                self.console.print("Exiting.")
                exit(1)

        # Sandbox & Advanced Validation
        advanced_validations = [
            self.validate_processor_configuration
        ]

        self.console.print(f"\n[bold]Proceeding to Sandbox validation and LLM fixes...[/bold]")
        with self.console.status("[bold green]Running Sandbox Validation...[/bold green]", spinner="dots"):
            for validation_method in advanced_validations:
                validation_method()

        if self.changes_made:
            self.save_plan()

        self._print_errors()

    def _print_errors(self):
        if self.errors:
            self.console.print(f"[bold red]Validation failed (Total Errors: {len(self.errors)}):[/bold red]")
            for err in self.errors:
                self.console.print(f" - {err}", style="red")
            exit(1)
        else:
            self.console.print(Panel("Validation successful! Plan is valid.", style="bold green", title="Success"))
            
            # Interactive Chain to Build Pipeline
            self.console.print()
            if Prompt.ask("Proceed to build pipeline?", choices=["y", "n"], default="y") == "y":
                self.console.print("[bold green]Starting build process...[/bold green]")
                os.system("python buildPipeline.py")
            else:
                self.console.print("[yellow]Exiting. Run 'python buildPipeline.py' manually to continue.[/yellow]")
            
            exit(0)

if __name__ == "__main__":
    validator = PlanValidator()
    validator.run()