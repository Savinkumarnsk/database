from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import google.generativeai as genai

# Configure your Gemini API key
genai.configure(api_key="⬅️ Use your actual Gemini API key")

app = FastAPI()

# CORS for frontend (e.g., Vite app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Function to create SQLAlchemy DB engine
def create_dynamic_engine(conn):
    try:
        db_url = (
            f"mysql+pymysql://{conn['user']}:{conn['password']}"
            f"@{conn['host']}:{conn['port']}/{conn['database']}"
        )
        return create_engine(db_url)
    except Exception as e:
        raise ValueError(f"Invalid connection: {e}")

# Function to interact with Gemini API
def chat_with_gemini(system_content, user_prompt):
    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        convo = model.start_chat(history=[
            {"role": "user", "parts": [system_content]},
            {"role": "model", "parts": ["Okay."]},
            {"role": "user", "parts": [user_prompt]}
        ])
        response = convo.send_message(user_prompt)
        return response.text
    except Exception as e:
        raise RuntimeError(f"Gemini generation error: {e}")

@app.post("/query")
async def query_db(request: Request):
    body = await request.json()
    prompt = body.get("prompt", "")
    conn_info = body.get("connection", {})

    if not prompt or not conn_info:
        return {"error": "Prompt or DB connection details missing."}

    try:
        # Step 1: Create DB engine
        engine = create_dynamic_engine(conn_info)

        # Step 2: Ask Gemini for table names
        table_names_text = chat_with_gemini(
            "Extract only the table names used in the following prompt. Return a comma-separated list. No explanation.",
            prompt
        )
        tables = [t.strip() for t in table_names_text.split(",") if t.strip()]
        if not tables:
            return {"error": "Could not extract valid table names from prompt."}

        # Step 3: Fetch table schema
        schema_parts = []
        with engine.begin() as conn:
            for table in tables:
                try:
                    rows = conn.execute(text(f"DESCRIBE {table}")).fetchall()
                    columns = [f"{row[0]} {row[1]}" for row in rows]
                    schema_parts.append(f"{table}({', '.join(columns)})")
                except Exception as e:
                    return {"error": f"Schema error for '{table}': {e}"}

        schema = "\n".join(schema_parts)

        # Step 4: Ask Gemini to generate SQL query
        system_prompt = f"""You are a MySQL expert.
Use the schema below to write a valid SQL query for the user's prompt.
Make sure the query works on older MySQL versions.
Avoid using LIMIT in subqueries with IN or ANY. Prefer JOINs instead.
Return only the SQL query, with no explanation.

Schema:
{schema}
"""

        sql = chat_with_gemini(system_prompt, prompt)

        # Step 5: Clean up SQL formatting
        if sql.startswith("```"):
            sql = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("```"))
        sql = sql.strip()

        # Step 6: Execute SQL query
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            if sql.lower().startswith("select"):
                rows = [dict(row._mapping) for row in result]
                return {"sql": sql, "data": rows}
            else:
                return {"sql": sql, "message": f"{result.rowcount} rows affected."}

    except Exception as e:
        return {"error": f"Server error: {e}"}
