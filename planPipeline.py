#!/usr/bin/env python3
import os
import sys
import json
import requests
from dotenv import load_dotenv
from google import genai
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt

load_dotenv()
console = Console()

# Configuration
NIFI_BASE_URL = os.getenv("NIFI_BASE_URL")
NIFI_AUTH = os.getenv("NIFI_AUTH")
NIFI_VERIFY_SSL = os.getenv("NIFI_VERIFY_SSL", "false").lower() == "true"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = os.getenv("LLM_MODEL")

def get_nifi_controller_services():
    """Fetches enabled controller services from NiFi root process group."""
    if not NIFI_AUTH:
        console.print("[yellow]Warning:[/yellow] NIFI_AUTH not set. Cannot fetch controller services.")
        return []

    headers = {
        "Authorization": f"Bearer {NIFI_AUTH}",
        "Content-Type": "application/json"
    }
    
    try:
        url = f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/root/controller-services"
        response = requests.get(url, headers=headers, verify=NIFI_VERIFY_SSL, timeout=10)
        response.raise_for_status()
        
        services = []
        data = response.json()
        for cs in data.get("controllerServices", []):
            if cs["status"]["runStatus"] == "ENABLED":
                 services.append({
                    "id": cs["id"],
                    "name": cs["component"]["name"],
                    "type": cs["component"]["type"],
                    "state": cs["status"]["runStatus"]
                })
        return services
    except Exception as e:
        console.print(f"[red]Error fetching controller services:[/red] {e}")
        return []

def generate_plan(pipeline_spec, controller_services):
    """Generates the plan using Gemini."""
    if not GEMINI_API_KEY:
        console.print("[red]Error:[/red] GEMINI_API_KEY not set.")
        sys.exit(1)

    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # Format controller services for the prompt
    cs_text = "No controller services available."
    if controller_services:
        cs_lines = []
        for cs in controller_services:
            cs_lines.append(f'- ID: "{cs["id"]}", Name: "{cs["name"]}", Type: "{cs["type"]}"')
        cs_text = "\n".join(cs_lines)

    system_prompt = f"""You are a senior Apache NiFi Architect. Your goal is to generate a detailed NiFi pipeline plan based on the provided Pipeline Specification.

Output:
Produce a single valid JSON object containing exactly two keys:
1. "plan_summary": A markdown string describing the pipeline strategy, components, and rationale.
2. "plan_details": A JSON object defining the actual NiFi flow structure.

Requirements for "plan_details":
- "flow_name": Name of the pipeline.
- "processors": A list of processor objects. Each must have:
    - "id": A unique placeholder ID (e.g., "proc-1").
    - "name": A descriptive name.
    - "type": The fully qualified class name (FQCN) (e.g., org.apache.nifi.processors.mongodb.RunMongoAggregation).
    - "properties": Key-value pairs for configuration. 
      IMPORTANT: when referencing a Controller Service, use the exact ID provided in the "Available Controller Services" list if a relevant service exists (e.g., for DBCPConnectionPool or MongoDBControllerService). If no matching service exists, output a placeholder string like "CREATE_NEW_CS".
    - "scheduling": {{ "strategy": "TIMER_DRIVEN", "period": "..." }}
    - "auto_terminated_relationships": List of relationships to terminate (e.g., ["failure"]).
- "connections": [
      {{ "id": "conn-001", "from_id": "processor-id-1", "to_id": "processor-id-2", "relationships": ["success", "failure"] }}
    ]

Pipeline Specification:
{json.dumps(pipeline_spec, indent=2)}

Available Controller Services:
{cs_text}

Output strictly valid JSON. Do not use markdown fencing."""

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=system_prompt,
            config={'response_mime_type': 'application/json'}
        )
        return response.text
    except Exception as e:
        console.print(f"[red]Error calling LLM:[/red] {e}")
        sys.exit(1)

def main():
    console.rule("[bold blue]NiFi Pipeline Planner[/bold blue]")

    # 1. Read Pipeline Spec
    spec_path = "pipeline_spec.json"
    if len(sys.argv) > 1:
        spec_path = sys.argv[1]
    
    if not os.path.exists(spec_path):
        console.print(f"[red]Error:[/red] Pipeline spec file '{spec_path}' not found.")
        sys.exit(1)
        
    try:
        with open(spec_path, 'r') as f:
            pipeline_spec = json.load(f)
        console.print(Panel(f"Loaded Spec: [bold]{spec_path}[/bold]\nPipeline: {pipeline_spec.get('PipelineSpec', {}).get('pipelineName', 'Unknown')}", style="cyan"))
    except Exception as e:
        console.print(f"[red]Error reading pipeline spec:[/red] {e}")
        sys.exit(1)

    # 2. Fetch Controller Services
    with console.status("[bold green]Fetching NiFi Controller Services...[/bold green]", spinner="dots"):
        controller_services = get_nifi_controller_services()

    console.print(f"Found [bold]{len(controller_services)}[/bold] enabled controller services.")

    # 3. Generate Plan
    with console.status("[bold magenta]Generating execution plan with Gemini...[/bold magenta]", spinner="dots"):
         plan_json_str = generate_plan(pipeline_spec, controller_services)
    
    # 4. Save Output
    try:
        plan_data = json.loads(plan_json_str)
        
        output_path = "plan.json"
        with open(output_path, 'w') as f:
            json.dump(plan_data, f, indent=2)
            
        console.print(Panel(f"Plan generated successfully and saved to [bold]{output_path}[/bold]", style="bold green", title="Success"))
        
        console.rule("[bold]Plan Summary[/bold]")
        console.print(plan_data.get("plan_summary", "No summary provided."))
        
    except json.JSONDecodeError as e:
        console.print(f"[red]Error: LLM returned invalid JSON. Saving raw output to 'plan_error.txt'.[/red]")
        with open("plan_error.txt", "w") as f:
            f.write(plan_json_str)
        sys.exit(1)

    console.print()
    if Prompt.ask("Proceed to validation?", choices=["y", "n"], default="y") == "y":
        console.print("[bold green]Starting validation...[/bold green]")
        os.system("python validatePlan.py")
    else:
        console.print("[yellow]Exiting. Run 'python validatePlan.py' manually to continue.[/yellow]")

if __name__ == "__main__":
    main()
