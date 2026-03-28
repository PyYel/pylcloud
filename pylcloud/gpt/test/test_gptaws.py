import os, sys
import pprint


from dotenv import load_dotenv

MODULE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(MODULE_DIR_PATH))

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(MODULE_DIR_PATH)), ".env"))

from gpt import GPTAWS

api = GPTAWS(
    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID"),
    AWS_ACCESS_KEY_SECRET=os.getenv("AWS_ACCESS_KEY_SECRET"),
    AWS_REGION_NAME=os.getenv("AWS_REGION_NAME", "eu-west-1"),
)

# EMBEDDING
print("\n==================================\nEMBEDDING")
reponse = api.return_embedding(model_name="titan-text-embeddings", prompt="test", dimensions=256)
pprint.pprint(reponse)


# SIMPLE GENERATION CALL
print("\n==================================\nRETURN")
reponse = api.return_generation(model_name="nova-lite", user_prompt="test")
pprint.pprint(reponse)


# STREAMING GENERATION CALL
print("\n==================================\nSTREAMING")
for chunk in api.yield_generation(model_name="nova-lite", user_prompt="test"):
    print(chunk)
    reponse = chunk
pprint.pprint(reponse)
print(api.compute_costs(model_name="nova-lite", usage=reponse["usage"]))


# AGENT LOOP: dummy RAG example
print("\n==================================\nAGENT")
# Note: for MCP, this part should be a ``MCP_SERVER_HOST/tools`` route query
TOOLS_SPEC = [
    {
        "toolSpec": {
            "name": "search_knowledge_base",
            "description": "Search the document knowledge base for relevant chunks given a query.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "The search query."},
                        "top_k": {"type": "integer", "description": "Number of results to return.", "default": 3},
                    },
                    "required": ["query"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "get_document_metadata",
            "description": "Retrieve metadata (title, author, date) for a specific document by its ID.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "document_id": {"type": "string", "description": "The document ID."},
                    },
                    "required": ["document_id"],
                }
            },
        }
    },
]

# Dummy data simulating a vector store + metadata DB
FAKE_CHUNKS = {
    "solar panels": [
        {"doc_id": "doc_001", "text": "Monocrystalline solar panels offer efficiency rates of 20-23%."},
        {"doc_id": "doc_002", "text": "Polycrystalline panels are cheaper but average 15-17% efficiency."},
    ],
    "installation": [
        {"doc_id": "doc_001", "text": "South-facing roofs with 30° tilt maximize annual yield."},
    ],
}

FAKE_METADATA = {
    "doc_001": {"title": "Solar Panel Guide 2024", "author": "John Doe", "date": "2024-03-01"},
    "doc_002": {"title": "Budget Solar Options", "author": "Jane Smith", "date": "2024-01-15"},
}

def dummy_tool_handler(name: str, inputs: dict) -> str:
    """
    The callable that executes the fucntions/requests if the agent decides to use a tool
    """
    if name == "search_knowledge_base":
        query = inputs["query"].lower()
        # Match any fake chunk whose key appears in the query
        results = next(
            (chunks for key, chunks in FAKE_CHUNKS.items() if key in query), []
        )
        if not results:
            return "No documents found."
        return "".join(f"[{r['doc_id']}] {r['text']}" for r in results)

    elif name == "get_document_metadata":
        doc_id = inputs["document_id"]
        meta = FAKE_METADATA.get(doc_id)
        return str(meta) if meta else f"No metadata found for document '{doc_id}'."

    return f"Unknown tool: {name}"

model_name = "nova-micro"
result, details = api.return_agent(
    model_name=model_name,
    user_prompt="What are the most efficient solar panels, and who wrote the document that mentions them?",
    tools=TOOLS_SPEC,
    tool_handler=dummy_tool_handler,
    system_prompt="You are a helpful assistant with access to a knowledge base. Always search before answering.",
)

pprint.pprint(result)

import json
with open(os.path.join(os.path.dirname(__file__), "agent.json"), "w") as f:
    json.dump(details['messages'], f, indent=4)

print(f"Iterations: {details['iterations']}")
print(f"Usage: {result['usage']}")
print(f"Cost: {api.compute_costs(model_name=model_name, usage=result['usage'])}")