import requests
import json
from tools.tool_registry import tool_schemas

SYSTEM_PROMPT = """Your S3 agent prompt (same as before)"""

def call_ollama(messages):
    payload = {
        "model": "llama3.2:latest",
        "messages": messages,
        "tools": tool_schemas,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 256,
            "top_p": 0.9,
            "repeat_penalty": 1.1
        }
    }

    try:
        resp = requests.post(
            "http://localhost:11434/api/chat",
            json=payload,
            timeout=120
        )
        resp.raise_for_status()
        return resp.json().get("message", {})
    except Exception as e:
        print(f"❌ Ollama Error: {e}")
        return None
