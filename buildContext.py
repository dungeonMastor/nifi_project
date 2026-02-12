import asyncio
import sys
import os
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from google import genai
from google.genai import types

load_dotenv()
console = Console()

# --- State Model ---
class ProjectState(BaseModel):
    user_request: str
    pipeline_type: str = "Unknown"
    known_facts: Dict[str, Any] = Field(default_factory=dict)
    technical_unknowns: List[str] = Field(default_factory=list)
    clarification_questions: List[str] = Field(default_factory=list)
    status: str = "IN_PROGRESS"
    architect_feedback: Optional[str] = None

# --- Helper Functions ---
def mcp_tool_to_openai_function(mcp_tool) -> dict:
    """Converts an MCP tool to an OpenAI-compatible function definition."""
    return {
        "type": "function",
        "function": {
            "name": mcp_tool.name,
            "description": mcp_tool.description,
            "parameters": mcp_tool.inputSchema
        }
    }

# --- Agents ---

async def run_supervisor(state: ProjectState, tools_list: List[dict], client: genai.Client, model_name: str) -> dict:
    """
    Supervisor Agent: Decides the next step (Thinker).
    """
    console.print(Panel("Supervisor Agent: Analyzing state...", style="bold blue"))
    
    system_prompt = (
        "You are the Project Supervisor. Your goal is to fill the 'known_facts' dictionary. "
        "Analyze the state. If technical facts are missing, output specific instructions for the Researcher. "
        "If subjective ambiguity exists, output a question for the User. "
        "If all facts are present, output 'CALL_ARCHITECT'.\n"
        "If the Architect rejects your plan with specific errors, your TOP priority is to resolve those specific missing facts immediately.\n"
        "Available Tools (for Researcher use only): " + ", ".join([t['name'] for t in tools_list]) + "\n"
        "Output a JSON object with the following structure:\n"
        "{\n"
        '  "next_action": "RESEARCH" | "ASK" | "ARCHITECT",\n'
        '  "payload": "Specific instruction for Researcher OR Question for User OR None"\n'
        "}"
    )
    
    user_prompt = f"Current State:\n{state.model_dump_json(indent=2)}"

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=system_prompt + "\n\n" + user_prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        
        result_json = json.loads(response.text)
        return result_json
    except Exception as e:
        console.print(f"[red]Supervisor Error:[/red] {e}")
        return {"next_action": "ASK", "payload": "Supervisor encountered an error. Please try again."}


async def run_researcher(task: str, session: ClientSession, client: genai.Client, model_name: str, tools_map: Dict[str, Any], gemini_tools: list) -> str:
    """
    Researcher Agent: Executes technical lookups using MCP tools (Doer).
    """
    console.print(Panel(f"Researcher Agent: Working on task: {task}", style="bold green"))
    
    system_prompt = (
        "You are a Technical Researcher. Use the available MCP tools to answer the Supervisor's specific question. "
        "Output strictly the facts found. Do not guess."
    )
    
    # Create chat for tool use loop
    chat = client.chats.create(
        model=model_name,
        config=types.GenerateContentConfig(
            tools=gemini_tools,
            system_instruction=system_prompt
        )
    )

    try:
        response = chat.send_message(task)
        
        # Handle tool calls loop
        while response.function_calls:
            for fc in response.function_calls:
                tool_name = fc.name
                tool_args = fc.args
                
                console.print(f"[green]Researcher calling tool:[/green] {tool_name}")
                
                # Simplify args
                if hasattr(tool_args, "to_dict"):
                    tool_args = tool_args.to_dict()
                elif not isinstance(tool_args, dict):
                    try:
                        tool_args = dict(tool_args)
                    except:
                        pass
                
                # Execute tool
                try:
                    tool_result = await session.call_tool(tool_name, arguments=tool_args)
                    
                    result_text = ""
                    if tool_result.content:
                        for content in tool_result.content:
                            if content.type == "text":
                                result_text += content.text
                    
                    console.print(f"[dim]Tool Result: {result_text[:100]}...[/dim]")

                    # Send result back
                    response_part = types.Part.from_function_response(
                        name=tool_name,
                        response={"result": result_text}
                    )
                    
                    response = chat.send_message(response_part)

                except Exception as e:
                    console.print(f"[red]Tool Execution Error:[/red] {e}")
                    response_part = types.Part.from_function_response(
                        name=tool_name,
                        response={"error": str(e)}
                    )
                    response = chat.send_message(response_part)

        return response.text

    except Exception as e:
        console.print(f"[red]Researcher Error:[/red] {e}")
        return f"Researcher failed: {e}"


async def run_architect(state: ProjectState, client: genai.Client, model_name: str) -> Optional[Dict[str, Any]]:
    """
    Architect Agent: Generates the final JSON spec (Formatter).
    Returns the parsed JSON response upon success, or None on failure.
    """
    console.print(Panel("Architect Agent: Generating pipeline spec...", style="bold magenta"))
    
    system_prompt = (
        "You are the Solutions Architect. Convert the final state into a valid 'PipelineSpec' JSON. "
        "If critical info is missing, mark validation as FAILED."
    )
    
    user_prompt = f"Final State:\n{state.model_dump_json(indent=2)}"

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=system_prompt + "\n\n" + user_prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        
        spec_json = response.text
        
        # Save to file
        with open("pipeline_spec.json", "w") as f:
            f.write(spec_json)
        
        console.print(Panel(f"Pipeline Spec generated:\n{spec_json}", title="Final Output", style="bold white"))
        
        return json.loads(spec_json)

    except Exception as e:
        console.print(f"[red]Architect Error:[/red] {e}")
        return None


# --- Main Orchestrator ---

async def main():
    # Setup
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        console.print("[red]Error:[/red] GEMINI_API_KEY not set.")
        return
    
    client = genai.Client(api_key=api_key)
    model_name = os.getenv("LLM_MODEL", "gemini-2.0-flash-exp")

    # Connect to MCP Server
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mcp_server.main"],
        env=None
    )

    console.print("[bold]Connecting to MCP Server...[/bold]")
    
    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # List tools
                result = await session.list_tools()
                tools_list = []
                gemini_tools = []
                tools_map = {}

                for tool in result.tools:
                    tools_list.append({"name": tool.name, "description": tool.description})
                    tools_map[tool.name] = tool
                    
                    # Gemini Tool Object
                    openai_tool = mcp_tool_to_openai_function(tool)
                    func_def = openai_tool["function"]
                    gemini_tools.append(types.FunctionDeclaration(
                        name=func_def["name"],
                        description=func_def["description"],
                        parameters=func_def["parameters"]
                    ))

                gemini_tool_obj = types.Tool(function_declarations=gemini_tools)
                
                # Initial User Request
                initial_request = Prompt.ask("[bold yellow]Enter your pipeline request[/bold yellow]")
                
                # Initialize State
                state = ProjectState(user_request=initial_request)

                # Main Loop
                while state.status != "COMPLETED":
                    console.rule(f"[bold]State Update[/bold]")
                    
                    # 1. Supervisor Turn
                    decision = await run_supervisor(state, tools_list, client, model_name)
                    next_action = decision.get("next_action")
                    payload = decision.get("payload")
                    
                    console.print(f"[blue]Supervisor Decision:[/blue] {next_action} -> {payload}")
                    
                    # 2. Routing
                    if next_action == "RESEARCH":
                        # Call Researcher
                        research_result = await run_researcher(
                            task=payload,
                            session=session,
                            client=client,
                            model_name=model_name,
                            tools_map=tools_map,
                            gemini_tools=[gemini_tool_obj]
                        )
                        
                        # Update State
                        state.known_facts[payload] = research_result
                        state.architect_feedback = None
                    
                    elif next_action == "ASK":
                        # Ask User
                        user_answer = Prompt.ask(f"[bold yellow]{payload}[/bold yellow]")
                        
                        # Update State
                        state.known_facts[payload] = user_answer
                        state.architect_feedback = None
                    
                    elif next_action == "ARCHITECT":
                        # Call Architect
                        architect_result = await run_architect(state, client, model_name)
                        
                        if architect_result:
                            validation = architect_result.get("validation")
                            if not validation:
                                pipe_spec = architect_result.get("pipelineSpec")
                                if not pipe_spec:
                                    pipe_spec = architect_result.get("PipelineSpec", {})
                                
                                validation = pipe_spec.get("validation", {})
                            
                            status = validation.get("status")
                            
                            if status == "FAILED":
                                missing_info = validation.get("missing_info")
                                reason = validation.get("reason")
                                
                                errors = []
                                if missing_info:
                                    errors.append(f"Missing items: {missing_info}")
                                if reason:
                                    errors.append(f"Reason: {reason}")
                                
                                feedback_msg = f"Architect rejected the plan. {', '.join(errors)}. You must find these facts."
                                console.print(f"[bold red]Architect Rejection:[/bold red] {feedback_msg}")
                                state.architect_feedback = feedback_msg
                                # Loop continues
                            elif status == "VALID" or status == "SUCCESS":
                                console.print("[bold green]Architect accepted the plan![/bold green]")
                                state.status = "COMPLETED"
                                break
                            else:
                                console.print(f"[yellow]Unknown validation status: {status}. Treating as success.[/yellow]")
                                state.status = "COMPLETED"
                                break
                        else:
                             console.print("[red]Architect failed to generate a result.[/red]")
                             break
                    
                    else:
                        console.print(f"[red]Unknown action:[/red] {next_action}")
                        break

    except Exception as e:
        console.print(f"[red]Orchestrator Error:[/red] {e}")

if __name__ == "__main__":
    asyncio.run(main())
