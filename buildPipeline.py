import requests
import json
import os
import sys
from dotenv import load_dotenv

load_dotenv()

NIFI_BASE_URL = os.getenv("NIFI_BASE_URL")
NIFI_AUTH = os.getenv("NIFI_AUTH")
PLAN_PATH = os.getenv("PLAN_JSON_PATH", "plan.json")
VERIFY_SSL = os.getenv("NIFI_VERIFY_SSL", "true").lower() == "true"

if not VERIFY_SSL:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

console = Console()

def get_headers():
    headers = {"Content-Type": "application/json"}
    if NIFI_AUTH:
        headers["Authorization"] = f"Bearer {NIFI_AUTH}"
    return headers

def fetch_processor_types():
    """Fetch all available processor types to resolve bundles."""
    url = f"{NIFI_BASE_URL}/nifi-api/flow/processor-types"
    try:
        resp = requests.get(url, headers=get_headers(), verify=VERIFY_SSL)
        resp.raise_for_status()
        types = {}
        for t in resp.json().get("processorTypes", []):
            types[t["type"]] = t["bundle"]
        return types
    except Exception as e:
        console.print(f"[bold red]Error fetching processor types:[/bold red] {e}")
        sys.exit(1)

def create_process_group(root_id, name):
    url = f"{NIFI_BASE_URL}/nifi-api/process-groups/{root_id}/process-groups"
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": 0, "y": 0}
        }
    }
    resp = requests.post(url, json=payload, headers=get_headers(), verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()

def create_processor(pg_id, processor_config, bundle, index):
    url = f"{NIFI_BASE_URL}/nifi-api/process-groups/{pg_id}/processors"
    
    # Simple vertical layout
    y_pos = 100 + (index * 200)
    
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": processor_config["type"],
            "name": processor_config["name"],
            "bundle": bundle,
            "position": {"x": 400, "y": y_pos},
            "config": {
                "properties": processor_config.get("properties", {}),
                "schedulingStrategy": processor_config.get("scheduling", {}).get("strategy", "TIMER_DRIVEN"),
                "schedulingPeriod": processor_config.get("scheduling", {}).get("period", "0 sec"),
                "concurrentlySchedulableTaskCount": processor_config.get("scheduling", {}).get("concurrent_tasks", 1),
                "autoTerminatedRelationships": processor_config.get("auto_terminated_relationships", [])
            }
        }
    }
    resp = requests.post(url, json=payload, headers=get_headers(), verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()

def update_processor_auto_termination(proc_id, relationships, revision_version):
    """Update processor to auto-terminate specified relationships."""
    url = f"{NIFI_BASE_URL}/nifi-api/processors/{proc_id}"
    payload = {
        "revision": {
            "version": revision_version
        },
        "component": {
            "id": proc_id,
            "config": {
                "autoTerminatedRelationships": relationships
            }
        }
    }
    resp = requests.put(url, json=payload, headers=get_headers(), verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()

def create_connection(pg_id, source_id, dest_id, relationships, index):
    url = f"{NIFI_BASE_URL}/nifi-api/process-groups/{pg_id}/connections"
    
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {
                "id": source_id,
                "groupId": pg_id,
                "type": "PROCESSOR"
            },
            "destination": {
                "id": dest_id,
                "groupId": pg_id,
                "type": "PROCESSOR"
            },
            "selectedRelationships": relationships
        }
    }
    resp = requests.post(url, json=payload, headers=get_headers(), verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()

def main():
    if not os.path.exists(PLAN_PATH):
        console.print(f"[bold red]Plan file not found:[/bold red] {PLAN_PATH}")
        sys.exit(1)
        
    console.print(f"[blue]Reading plan from {PLAN_PATH}...[/blue]")
    with open(PLAN_PATH, 'r') as f:
        plan = json.load(f)
    
    details = plan.get("plan_details", {})
    flow_name = details.get("flow_name", "NiFi Pipeline")
    processors = details.get("processors", [])
    connections = details.get("connections", [])
    
    # Identify non-leaf (source) processors
    source_processor_ids = set()
    for conn in connections:
        if "from_id" in conn:
            source_processor_ids.add(conn["from_id"])

    print("Fetching NiFi processor types...")
    with console.status("[bold green]Fetching NiFi processor types...[/bold green]", spinner="dots"):
        proc_types_map = fetch_processor_types()
    
    # Get Root PG ID
    try:
        root_resp = requests.get(f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/root", headers=get_headers(), verify=VERIFY_SSL)
        root_resp.raise_for_status()
        root_id = root_resp.json()["processGroupFlow"]["id"]
    except Exception as e:
        print(f"Error fetching root process group: {e}")
        sys.exit(1)
    
    console.print(f"[bold]Creating Process Group '{flow_name}' under root ({root_id})...[/bold]")
    try:
        pg = create_process_group(root_id, flow_name)
        pg_id = pg["component"]["id"]
        console.print(f"[green]Successfully created Process Group used ID: {pg_id}[/green]")
    except Exception as e:
        console.print(f"[bold red]Error creating process group:[/bold red] {e}")
        sys.exit(1)
    
    plan_id_to_nifi_id = {}
    
    print(f"Creating {len(processors)} processors...")
    for index, proc in enumerate(processors):
        p_type = proc["type"]
        p_name = proc.get("name", f"Processor-{index}")
        p_plan_id = proc.get("id")
        
        if p_type not in proc_types_map:
            console.print(f"[red]Error: Processor type '{p_type}' not found in NiFi installation. Skipping '{p_name}'.[/red]")
            continue
            
        bundle = proc_types_map[p_type]
        try:
            nifi_proc = create_processor(pg_id, proc, bundle, index)
            nifi_id = nifi_proc["component"]["id"]
            plan_id_to_nifi_id[p_plan_id] = nifi_id
            console.print(f" - Created '{p_name}' ({nifi_id})")

            # Check if leaf node (not a source for any connection)
            if p_plan_id not in source_processor_ids:
                available_rels = [r["name"] for r in nifi_proc["component"].get("relationships", [])]
                if available_rels:
                    console.print(f"   -> [dim]Leaf node detected. Auto-terminating relationships:[/dim] {available_rels}")
                    version = nifi_proc["revision"]["version"]
                    update_processor_auto_termination(nifi_id, available_rels, version)

        except Exception as e:
            console.print(f" - [red]Failed to create/update '{p_name}': {e}[/red]")
            
    print(f"Creating {len(connections)} connections...")
    for index, conn in enumerate(connections):
        src_plan_id = conn["from_id"]
        dst_plan_id = conn["to_id"]
        rels = conn.get("relationships", [])
        
        src_nifi_id = plan_id_to_nifi_id.get(src_plan_id)
        dst_nifi_id = plan_id_to_nifi_id.get(dst_plan_id)
        
        if not src_nifi_id or not dst_nifi_id:
            console.print(f" - [yellow]Skipping connection {index}: Source or Destination processor not created/found.[/yellow]")
            continue
            
        try:
            create_connection(pg_id, src_nifi_id, dst_nifi_id, rels, index)
            console.print(f" - Created connection: {src_nifi_id} -> {dst_nifi_id}")
        except Exception as e:
             console.print(f" - [red]Failed to create connection: {e}[/red]")
            
    console.print(Panel(f"View in NiFi: {NIFI_BASE_URL}/nifi/?processGroupId={pg_id}", title="Pipeline Built", style="bold green"))

if __name__ == "__main__":
    main()
