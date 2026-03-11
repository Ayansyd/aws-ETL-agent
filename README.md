# 🤖 AWS ETL Agent

A modular, conversational AI agent that manages AWS S3 and ETL pipelines through natural language. Built with a tool-calling loop, human-in-the-loop confirmation for destructive operations, and a dry-run safety layer.

> Originally built as a single-file S3 agent, now modularised into a clean tool-registry architecture. LLM-agnostic — ships with Ollama/Llama 3.2 locally but can be swapped for any LLM (OpenAI, Bedrock, Groq, etc.)

---

## How It Works
```
You: "copy all objects from bucket-a to bucket-b"
              │
              ▼
   ┌─────────────────────┐
   │      S3 Agent       │  ←── Any LLM backend
   │  (Conversation +    │       (Ollama / OpenAI
   │   Tool-call Loop)   │        / Bedrock / etc.)
   └──────────┬──────────┘
              │  selects tool + args
              ▼
   ┌─────────────────────────────────────┐
   │           Tool Registry             │
   │                                     │
   │  s3_tools  │ glue_tools │  schema   │
   │  etl_orch  │  upload    │  infer    │
   └──────────┬──────────────────────────┘
              │
              ▼
   ┌─────────────────────┐
   │   Safety Layer      │  ←── Dry-run first
   │   (Confirmation)    │       then confirm
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │    AWS Execution    │
   │   S3  │  Glue       │
   └─────────────────────┘
```

---

## Features

- **Natural language interface** — plain English commands, no boto3 scripting needed
- **LLM-agnostic** — swap the LLM backend with a single config change
- **Dry-run safety** — all destructive operations simulate first, then ask for confirmation
- **Human-in-the-loop** — dangerous ops (delete bucket, wipe objects) require explicit `CONFIRM`
- **Conversation memory** — maintains context across multiple commands in a session
- **Pagination aware** — handles large buckets with 1000-key batch operations
- **Schema inference** — auto-detects data types and structure from S3 objects
- **ETL orchestration** — Glue job creation and execution

---

## Safety Model

Destructive operations follow a strict 3-step flow:
```
1. Agent selects tool
        │
        ▼
2. Dry-run executes     → shows what WOULD happen (no AWS changes)
        │
        ▼
3. User types CONFIRM   → only then does real execution happen
```

Covered operations: `delete_bucket`, `delete_all_buckets`, `delete_object`, `delete_all_objects_in_bucket`, `move_all_objects`, `copy_all_objects`

---

## Project Structure
```
aws-ETL-agent/
├── main.py                    # Entry point — conversational loop + startup checks
├── test-agent.py              # Testing scripts
├── agent/
│   ├── s3_agent.py            # Core agent loop, tool dispatch, confirmation logic
│   ├── models.py              # Data models
│   └── confirmation.py        # Human-in-the-loop confirmation handler
├── config/
│   ├── aws_session.py         # AWS session setup (key / profile / session token)
│   └── settings.py            # LLM and app configuration
└── tools/
    ├── tool_registry.py       # Tool definitions and schemas exposed to LLM
    ├── s3_tools.py            # S3 CRUD — buckets and objects
    ├── glue_tools.py          # AWS Glue job management
    ├── schema_tools.py        # Schema validation
    ├── schema_inference.py    # Auto schema detection from data
    ├── etl_orchestrator.py    # Pipeline orchestration
    └── upload_local_folder.py # Bulk local folder → S3 upload
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10 |
| LLM (default) | Ollama — Llama 3.2 (local) |
| LLM (swappable) | OpenAI / AWS Bedrock / Groq / any OpenAI-compatible API |
| Cloud | AWS S3, AWS Glue |
| Auth | boto3 — IAM keys, profiles, or session tokens |

---

## Getting Started
```bash
# Clone
git clone https://github.com/Ayansyd/aws-ETL-agent.git
cd aws-ETL-agent

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
```

Add to `.env`:
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
```

**With Ollama (default):**
```bash
ollama serve
ollama pull llama3.2:latest
python main.py
```

**Swap to a different LLM:** update the model config in `config/settings.py` — the tool-calling interface is the same regardless of backend.

---

## Example Usage
```
💬 list my buckets
💬 create buckets called dev-raw dev-processed dev-output
💬 upload ./data folder to s3://dev-raw/
💬 copy all objects from dev-raw to dev-processed
💬 infer schema from dev-processed/dataset.csv
💬 delete all objects in dev-raw

⚠️  SAFETY CHECK — dry-run result: would delete 142 objects
   Type: CONFIRM delete_all_objects_in_bucket {"bucket": "dev-raw"}

💬 CONFIRM delete_all_objects_in_bucket {"bucket": "dev-raw"}
✓ Deleted 142 objects (bucket kept)
```

---

## Status

✅ S3 agent — complete and working
✅ Safety layer (dry-run + confirmation) — complete
🔄 Glue and schema tooling — in progress
🔄 Multi-LLM config — in progress

---

## Author

**Mohammed Ayan Syed**

---

*Never commit your `.env` file — AWS credentials must stay local.*
