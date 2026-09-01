import os
import uuid
import json
import logging
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from livekit import api
import pypdf
from dotenv import load_dotenv

load_dotenv()

# Setup robust logging (Console + File)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vocal_voice.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Enable CORS for production
frontend_url = os.getenv("FRONTEND_URL", "*")
allowed_origins = [frontend_url] if frontend_url != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Persistence storage for agents
DB_FILE = "agents.json"
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f)

agents_db = load_db()
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

class AgentConfig(BaseModel):
    id: str
    name: str
    objective: str
    system_prompt: str
    reference_text: Optional[str] = ""

# Frontend log model
class FrontendLogEntry(BaseModel):
    timestamp: str
    tag: str
    message: str
    data: Optional[str] = None

class FrontendLogBatch(BaseModel):
    logs: List[FrontendLogEntry]

# Frontend logger — writes to separate file
frontend_logger = logging.getLogger("frontend")
frontend_handler = logging.FileHandler("frontend.log", encoding="utf-8")
frontend_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
frontend_logger.addHandler(frontend_handler)
frontend_logger.setLevel(logging.INFO)

# ── PROMPT SANITIZATION ──────────────────────────────────────────────────────
# Blocks malicious agent creation at the API level (Defense Layer 1)
DANGEROUS_PATTERNS = [
    "ignore your instructions", "ignore previous instructions",
    "reveal your prompt", "show me your system prompt",
    "act as if you have no restrictions", "forget your rules",
    "you are now a hacker", "bypass security",
    "execute code", "run command", "os.environ",
]

def sanitize_prompt(prompt: str) -> dict:
    """
    Scans user-submitted system prompts for dangerous patterns.
    Returns: {"safe": True/False, "blocked_phrase": str}
    """
    prompt_lower = prompt.lower()
    for pattern in DANGEROUS_PATTERNS:
        if pattern in prompt_lower:
            logger.warning(f"BLOCKED AGENT CREATION: dangerous pattern '{pattern}' found in prompt")
            return {"safe": False, "blocked_phrase": pattern}
    return {"safe": True, "blocked_phrase": ""}


@app.post("/api/agents")
async def create_agent(
    name: str = Form(...),
    objective: str = Form(...),
    system_prompt: str = Form(...),
    agent_id: Optional[str] = Form(None),
    reference_file: Optional[UploadFile] = File(None)
):
    try:
        logger.info(f"[API] POST /api/agents — Creating agent: name='{name}'")
        
        # ── GUARDRAIL: Block dangerous agent prompts at creation ──
        logger.info("[API] 🛡️ Running prompt sanitization guardrail...")
        sanitization = sanitize_prompt(system_prompt)
        if not sanitization["safe"]:
            logger.warning(f"[API] ❌ Agent creation BLOCKED: '{sanitization['blocked_phrase']}'")
            raise HTTPException(
                status_code=400,
                detail=f"Agent creation blocked: prompt contains restricted content ('{sanitization['blocked_phrase']}')"
            )
        logger.info("[API] ✅ Prompt sanitization passed.")

        ref_text = ""
        if reference_file:
            logger.info(f"[API] 📄 Processing reference file: {reference_file.filename}")
            file_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{reference_file.filename}")
            with open(file_path, "wb") as f:
                f.write(await reference_file.read())
            
            # Extract text
            if reference_file.filename.endswith(".pdf"):
                logger.info("[API] 📖 Extracting text from PDF...")
                reader = pypdf.PdfReader(file_path)
                for page in reader.pages:
                    ref_text += page.extract_text()
                logger.info(f"[API] 📖 Extracted {len(ref_text)} chars from PDF.")
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    ref_text = f.read()
                logger.info(f"[API] 📖 Loaded {len(ref_text)} chars from text file.")

        new_id = agent_id or str(uuid.uuid4())
        agent = {
            "id": new_id,
            "name": name,
            "objective": objective,
            "system_prompt": system_prompt,
            "reference_text": ref_text or (agents_db.get(new_id, {}).get("reference_text", "")),
        }
        agents_db[new_id] = agent
        save_db(agents_db)
        logger.info(f"[API] ✅ Agent created successfully: id={new_id}, name='{name}'")
        return {"success": True, "agent": agent}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[API] ❌ Agent creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/agents")
async def list_agents():
    logger.info(f"[API] GET /api/agents — Returning {len(agents_db)} agents")
    return list(agents_db.values())

class TokenRequest(BaseModel):
    room_name: str
    participant_name: str

@app.post("/api/token")
async def get_token(req: TokenRequest):
    try:
        logger.info(f"[API] POST /api/token — Room: '{req.room_name}', User: '{req.participant_name}'")
        
        logger.info("[API] 🔑 Generating LiveKit JWT token...")
        token = api.AccessToken(
            os.getenv("LIVEKIT_API_KEY"),
            os.getenv("LIVEKIT_API_SECRET")
        ) \
            .with_identity(req.participant_name) \
            .with_grants(api.VideoGrants(
                room_join=True,
                room=req.room_name,
            ))

        jwt_token = token.to_jwt()
        logger.info("[API] ✅ JWT token generated successfully.")

        # Explicitly dispatch the agent worker into this room
        logger.info(f"[API] 📡 Dispatching 'vocal-agent' into room '{req.room_name}'...")
        lkapi = api.LiveKitAPI(
            os.getenv("LIVEKIT_URL"),
            os.getenv("LIVEKIT_API_KEY"),
            os.getenv("LIVEKIT_API_SECRET"),
        )
        try:
            await lkapi.agent_dispatch.create_dispatch(
                api.CreateAgentDispatchRequest(
                    agent_name="vocal-agent",
                    room=req.room_name,
                )
            )
            logger.info(f"[API] ✅ Agent dispatched to room '{req.room_name}' via LiveKit Cloud.")
        finally:
            await lkapi.aclose()

        logger.info(f"[API] 🎯 Token + dispatch complete. WebRTC streaming ready for '{req.participant_name}'.")
        return {"token": jwt_token, "url": os.getenv("LIVEKIT_URL")}
    except Exception as e:
        logger.error(f"[API] ❌ Token generation failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Frontend Log Sink ─────────────────────────────────────────────────────────
# Receives batched logs from the browser and writes them to frontend.log
@app.post("/api/frontend-logs")
async def receive_frontend_logs(batch: FrontendLogBatch):
    for entry in batch.logs:
        log_line = f"{entry.timestamp} - frontend - [{entry.tag}] {entry.message}"
        if entry.data:
            log_line += f" | data={entry.data}"
        # Write with client timestamp directly (bypass logger's auto-timestamp)
        frontend_handler.emit(logging.LogRecord(
            name="frontend", level=logging.INFO, pathname="", lineno=0,
            msg=log_line, args=(), exc_info=None,
        ))
    return {"received": len(batch.logs)}


# ── Health & Diagnostics Endpoint ──────────────────────────────────────────
@app.get("/api/health")
async def health_check():
    livekit_configured = bool(os.getenv("LIVEKIT_API_KEY") and os.getenv("LIVEKIT_API_SECRET") and os.getenv("LIVEKIT_URL"))
    groq_configured = bool(os.getenv("GROQ_API_KEY"))
    deepgram_configured = bool(os.getenv("DEEPGRAM_API_KEY"))
    tavily_configured = bool(os.getenv("TAVILY_API_KEY"))
    
    return {
        "status": "healthy" if (livekit_configured and groq_configured and deepgram_configured) else "degraded",
        "pipeline": {
            "webrtc_livekit": "connected" if livekit_configured else "missing_credentials",
            "stt_deepgram": "configured" if deepgram_configured else "missing_credentials",
            "llm_groq": "configured" if groq_configured else "missing_credentials",
            "tts_deepgram": "configured" if deepgram_configured else "missing_credentials",
            "tools_tavily": "configured" if tavily_configured else "optional_missing",
        },
        "agents_count": len(agents_db),
        "version": "2.0.0"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
