# 🧞‍♂️ NiFi AI Pipeline Generator

This project is an **AI-powered automation tool** for Apache NiFi. Instead of manually dragging, dropping, and configuring processors on the NiFi canvas, you simply define *what* you want to do in a JSON file (e.g., "Move data from MongoDB to ClickHouse every 15 minutes"). The system uses LLM to gather context, design a pipeline plan, validates it in a real NiFi environment, and then builds it for you.

---

## 🚀 How It Works

The workflow consists of three distinct stages, ensuring that no broken or invalid code reaches your production canvas.

0.  **Context Build (`buildContext.py`)**:
    -   An interactive CLI tool where you describe your goal in plain English (e.g., "I need to ingest CSVs from an FTP server").
    -   Agents (Supervisor, Researcher, Architect) use MCP tools to research your environment (e.g., list database tables, check API schemas) and generate the `pipeline_spec.json` for you.

1.  **Plan (`planPipeline.py`)**: 
    -   Reads your `pipeline_spec.json`.
    -   Fetches available Controller Services from your NiFi instance to reuse existing connections.
    -   Asks Gemini to design a NiFi flow that meets the requirements, outputting a `plan.json` containing specific processor types, property values, and relationships.

2.  **Validate & Heal (`validatePlan.py`)**:
    -   **Static Checks**: Verifies that the Spec and Plan structures are valid.
    -   **Dynamic Checks**: Actually **creates** the processors in a dedicated "Sandbox" Process Group in your running NiFi instance.
    -   **Self-Healing**: If NiFi rejects a configuration (e.g., "Batch Size must be an integer", "Invalid Scheduling Period"), the error is fed back to the LLM, which generates a fix. The script applies the fix and retries automatically.

3.  **Build (`buildPipeline.py`)**:
    -   Takes the fully validated `plan.json`.
    -   Deploys the final Process Group, Processors, and Connections to your root NiFi canvas, ready to start.

---

## 🛠️ Installation & Setup

### Prerequisites
-   **Python 3.8+**
-   **Apache NiFi**
-   **API Key** 

### 1. Clone & Install
```bash
git clone https://github.com/dungeonMastor/nifi_project.git
cd nifi_project

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure Environment
Copy the example config and edit it with your details.
```bash
cp .env.example .env
```

**Critical `.env` Variables:**
-   `NIFI_BASE_URL`: e.g., `https://localhost:8443`
-   `NIFI_AUTH`: Your Bearer token (if NiFi is secured).
-   `SANDBOX_VALIDATION_PROCESSOR_GROUP`: **Crucial Step!** Go to your NiFi UI, create a new Process Group named "Sandbox" (or similar), copy its UUID, and paste it here. **The validator will create and delete processors in this group.**
-   `GEMINI_API_KEY`: Your LLM API key.

---

## 📖 Usage Example

Let's say you want to move data from **MongoDB** to **ClickHouse** incrementally.

### 1. Define your Spec
**Option A: Interactive Generation (Recommended)**
Run the context builder to have the AI interview you and generate the spec:
```bash
python buildContext.py
```
*Follow the prompts. The AI will inspect your connected systems (if tools are available via MCP) and produce `pipeline_spec.json`.*

**Option B: Manual Creation**
Create `pipeline_spec.json` manually. You don't need to know NiFi processor class names. Just describe the intent:

```json
{
  "PipelineSpec": {
    "pipelineName": "MongoToClickHouse_Incremental",
    "pipelineType": "INCREMENTAL",
    "schedule": "*/15 * * * *",
    "source": {
      "type": "MONGODB",
      "collection": "users",
      "incrementalField": "updatedAt"
    },
    "destination": {
      "type": "CLICKHOUSE",
      "table": "users",
      "loadMethod": "UPSERT"
    }
  }
}
```

### 2. Generate the Plan
```bash
python planPipeline.py
```
**Output**: A `plan.json` file is created. It will select `RunMongoAggregation` for the source (better for state management) and `PutDatabaseRecord` for the destination. It also writes a human-readable summary in the terminal.

### 3. Validate
```bash
python validatePlan.py
```
**Process**: 
-   The script connects to NiFi.
-   It tries to create the `RunMongoAggregation` processor in your Sandbox group.
-   *Scenario*: If Gemini set a property "Batch" instead of "Batch Size", NiFi errors out.
-   *Auto-Fix*: The script catches the 400 error, sends it to Gemini, gets the correct property name "Batch Size", updates `plan.json`, and succeeds.
-   It cleans up the Sandbox group when finished.

### 4. Build
```bash
python buildPipeline.py
```
**Result**: A new Process Group "MongoToClickHouse_Incremental" appears on your NiFi root canvas, fully connected and ready to run!

---

## 📂 Project Structure

-   `buildContext.py`: **The Consultant.** Uses a multi-agent system (Supervisor, Researcher, Architect) to interactively interview you and research your environment to generate the `pipeline_spec.json`.
-   `planPipeline.py`:  **The Architect.** Interface with LLM to design the flow based on the spec.
-   `validatePlan.py`: **The Engineer.** Tests the design against reality and fixes bugs.
-   `buildPipeline.py`: **The Construction Crew.** Deploys the blueprint.
-   `mcp_server/`:  Contains tools for the Model Context Protocol (used by `buildContext.py`).
-   `pipeline_spec.json`:  Input requirements.
-   `plan.json`: Output blueprint (machine and human readable).