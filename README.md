# 🤖 AWS ETL Agent

A conversational AI agent that orchestrates ETL pipelines on AWS through natural language. Tell it what you want to do — it figures out the steps, calls the right AWS tools, and confirms before executing.

---

## Overview

Instead of writing ETL scripts manually, you interact with the agent in plain English. It handles S3 operations, schema inference, Glue jobs, and data uploads — all through a tool-calling loop with human confirmation built in.
```
You: "upload my local dataset to S3 and infer the schema"
         │
         ▼
┌─────────────────────┐
│     S3 Agent        │  ◄── Bedrock / LLM backbone
│  (Conversation      │
│     Loop)           │
└────────┬────────────┘
         │  selects tools
         ▼
┌─────────────────────────────────────────────┐
│                Tool Registry                │
│                                             │
│  s3_tools   │  glue_tools  │  schema_tools  │
│  upload     │  run job     │  infer schema  │
│  list       │  create job  │  validate      │
│  download   │              │                │
└─────────────────────────────────────────────┘
         │
         ▼  confirmation prompt
┌─────────────────────┐
│   AWS Execution     │
│   S3  │  Glue       │
└─────────────────────┘
```

---

## Features

- **Natural language interface** — no need to write boto3 scripts
- **S3 operations** — upload, list, download, manage buckets
- **Schema inference** — automatically detects data types and structure
- **Glue integration** — create and run ETL jobs
- **Confirmation step** — reviews actions before executing against AWS
- **Conversation history** — maintains context across commands

---

## Project Structure
```
aws-ETL-agent/
├── main.py                    # Entry point — conversational loop
├── test-agent.py              # Testing scripts
├── agent/
│   ├── s3_agent.py            # Core agent logic & tool-calling loop
│   ├── models.py              # Data models
│   └── confirmation.py        # Human-in-the-loop confirmation
├── config/
│   ├── aws_session.py         # AWS session & credentials setup
│   └── settings.py            # Configuration
└── tools/
    ├── tool_registry.py       # Tool definitions exposed to the agent
    ├── s3_tools.py            # S3 operations
    ├── glue_tools.py          # AWS Glue job management
    ├── schema_tools.py        # Schema validation
    ├── schema_inference.py    # Auto schema detection
    ├── etl_orchestrator.py    # Pipeline orchestration
    └── upload_local_folder.py # Bulk local folder upload
```

---

## Tech Stack

- **Language** — Python 3.10
- **AI backbone** — AWS Bedrock (Claude)
- **Cloud** — AWS S3, AWS Glue
- **Auth** — boto3 / IAM

---

## Getting Started
```bash
# Clone
git clone https://github.com/Ayansyd/aws-ETL-agent.git
cd aws-ETL-agent

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
cp .env.example .env
# Add your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Run
python main.py
```

**Example usage:**
```
💬 list all buckets in my account
💬 upload ./data folder to s3://my-bucket/raw/
💬 infer schema from s3://my-bucket/raw/dataset.csv
💬 run glue job on that dataset
```

---

## Status

✅ Phase 1 complete — S3 agent working
🔄 Actively improving — Glue and schema tooling in progress

---

## Author

**Mohammed Ayan Syed**

---

*AWS credentials are required. Never commit your `.env` file.*
