# Vocal Voice 🎙️ — Real-Time Conversational Voice AI Agent

A production-grade, ultra-low latency conversational voice AI platform built with a fully streaming WebRTC pipeline. Users can create custom AI agents with unique personas, engage in natural voice conversations, and experience seamless barge-in interruptions — all in real time.

**Built by:** Harshal Zarikar

---

## 🏗️ System Architecture & Protocols

```
┌──────────────┐     WebRTC (UDP)      ┌──────────────────────────────────────────────┐
│              │  ◄──────────────────►  │              LiveKit Server                  │
│   Browser    │   48kHz Opus Audio     │                                              │
│  (React +    │                        │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
│   LiveKit)   │   20ms audio chunks    │  │   STT   │→ │   LLM   │→ │    TTS      │  │
│              │  ◄──────────────────►  │  │Deepgram │  │  Groq   │  │  Deepgram   │  │
│              │   24kHz PCM Audio      │  │ nova-3  │  │gpt-oss  │  │  aura-2     │  │
│              │                        │  │ (16kHz) │  │ (20B)   │  │  (24kHz)    │  │
│              │  ◄── Data Channel ───► │  └─────────┘  └─────────┘  └─────────────┘  │
│              │   Turn Telemetry &     │  ┌────────────────────────────────────────┐  │
│              │   Ghost-Audio Filter   │  │ Generation Manager & Monotonic Seq     │  │
└──────────────┘                        │  └────────────────────────────────────────┘  │
                                        └──────────────────────────────────────────────┘
```

### End-to-End Streaming Pipeline

The entire pipeline operates in **parallel streams**, not sequential waterfall:

1. **User Speaks** → Browser captures audio at **48 kHz** via WebRTC.
2. **STT (Speech-to-Text)** → Streams 20ms chunks to Deepgram Nova-3 with `interim_results=True` and `no_delay=True`.
3. **Guardrails** → Real-time keyword filter + prompt injection detection runs in <1ms before LLM ingestion.
4. **LLM (Language Model)** → Groq streams tokens with `preemptive_generation=True` for immediate TTFT.
5. **TTS (Text-to-Speech)** → Deepgram Aura-2 synthesizes speech at **24 kHz** and streams chunks back to the browser.
6. **Telemetry & Control Channel** → Data Channel publishes real-time millisecond timestamps (`stt_ms`, `llm_ttft_ms`, `tts_ttfb_ms`, `e2e_ms`) and `generation_cancelled` signals.

---

## ⚡ Robustness & Assessment Features

| Feature | Implementation | Benefit |
|---|---|---|
| **Per-Turn Latency Telemetry** | High-precision timers published per-turn over Data Channel | UI displays live breakdown: STT, LLM TTFT, TTS TTFB, and E2E Turnaround |
| **Ghost-Audio Prevention** | Atomic `generation_id` per response + barge-in invalidation | Discards stale in-flight audio packets when user interrupts |
| **Turn Retention** | Sliding window `chat_ctx.truncate(max_items=10)` | Retains recent 5-10 turns without hitting token limits |
| **Error Recovery** | Real-time health pills + actionable recovery alert banners | Clear recovery states for Mic, WebRTC, STT, LLM, and TTS |
| **Backpressure & Timeout Guard** | 4.0s tool timeouts + 2.5s slow generation warning banner | Prevents system lockup during API jitter |
| **Packet Dedup & Ordering** | Monotonic packet sequence counter (`seq`) | Rejects out-of-order and duplicate audio/control frames |
| **Interim vs. Final Transcripts** | Shimmering interim bubble $\to$ solid committed bubble | Visual distinction between partial recognition and committed speech |
| **Interactive Architecture Spec** | In-app modal with latency waterfall and stack specs | Complete architectural clarity (see [ARCHITECTURE.md](ARCHITECTURE.md)) |

---

## ⚡ Latency Optimization

| Optimization | Configuration | Impact |
|---|---|---|
| VAD Endpointing | `min_delay=0.1s` (100ms) | Agent responds instantly after user stops speaking |
| STT Endpointing | `endpointing_ms=25` | Minimum silence detection threshold |
| Preemptive Generation | `enabled=True` | LLM starts on interim transcripts before STT finalizes |
| Barge-In | `min_words=1`, `min_duration=0.2s` | Single-word interruptions like "Stop" halt the agent immediately |
| False Interruption Timeout | `0.5s` | Background noise (coughs) don't permanently stop the agent |
| LLM Fallback | `FallbackAdapter` | Auto-routes to backup model on rate limits or errors |

### Measured Latency (Streaming via WebRTC)

| Stage | Latency |
|---|---|
| VAD Endpointing | ~100 ms |
| Guardrails | <1 ms |
| LLM TTFT (Groq) | ~200-400 ms |
| TTS TTFB (Deepgram) | ~80-150 ms |
| **Perceived E2E** | **~1.2 seconds** |

Latency is measured using `time.perf_counter()` on the backend and `performance.now()` on the frontend, with millisecond-precision logging at every pipeline stage.

---

## 🛡️ Security — Two-Layer Guardrail System

### Layer 1: API-Level Prompt Sanitization (`main.py`)
When users create agents with custom system prompts, the API scans for dangerous patterns (prompt injection, code execution attempts) and **blocks agent creation** before any malicious prompt is saved.

### Layer 2: Runtime Input Guardrails (`agent_worker.py`)
During live conversations, every user utterance is intercepted **between STT and LLM**:
- **Blocked Topic Filter** — Keywords related to illegal activities, hacking, weapons
- **Prompt Injection Detection** — Phrases like "ignore your instructions", "forget your rules"
- **Prompt Sandwich Architecture** — User's custom instructions are always injected *below* hardcoded safety rules, preventing override

---

## 🧠 AI Models

| Component | Model | Provider | Why |
|---|---|---|---|
| **STT** | `nova-3` | Deepgram | Fastest streaming transcription, 1.5% WER on LibriSpeech |
| **LLM (Primary)** | `gpt-oss-20b` | Groq | Ultra-fast inference (~200ms TTFT) |
| **LLM (Fallback)** | `gpt-oss-120b` | Groq | Higher quality reasoning, auto-activated on rate limits |
| **TTS** | `aura-2-andromeda-en` | Deepgram | Fastest streaming voice synthesis |
| **Web Search Tool** | Tavily API | Tavily | Real-time information retrieval (weather, news, scores) |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | React 18 + TypeScript + Vite |
| **UI Components** | `@livekit/components-react` for WebRTC voice session |
| **Backend API** | FastAPI (Python) with Uvicorn |
| **Voice Pipeline** | LiveKit Agents SDK (WebRTC over UDP) |
| **STT Plugin** | `livekit-plugins-deepgram` |
| **LLM Plugin** | `livekit-plugins-openai` (OpenAI-compatible, pointing to Groq) |
| **Deployment** | Render (render.yaml included) |

---

## 🚀 Getting Started

### Prerequisites

- Node.js v18+
- Python 3.10+
- API Keys for: [LiveKit Cloud](https://cloud.livekit.io), [Deepgram](https://console.deepgram.com), [Groq](https://console.groq.com), [Tavily](https://tavily.com) (optional, for web search)

### 1. Clone the Repository

```bash
git clone https://github.com/Harshalzarikar/vocal-voice.git
cd vocal-voice
```

### 2. Frontend Setup

```bash
yarn install
```

### 3. Backend Setup

```bash
cd backend
python -m venv venv

# Windows:
.\venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 4. Environment Variables

Create `backend/.env`:

```env
# LiveKit (from cloud.livekit.io)
LIVEKIT_URL=wss://your-project.livekit.cloud
LIVEKIT_API_KEY=your_livekit_api_key
LIVEKIT_API_SECRET=your_livekit_api_secret

# Deepgram (from console.deepgram.com)
DEEPGRAM_API_KEY=your_deepgram_api_key

# Groq (from console.groq.com)
GROQ_API_KEY=your_groq_api_key

# Tavily - Optional (from tavily.com)
TAVILY_API_KEY=your_tavily_api_key
```

---

## 🏃 Running the Application

You need **three terminals** running simultaneously:

### Terminal 1 — Frontend (React)
```bash
# From project root
yarn dev
# → Opens at http://localhost:5173
```

### Terminal 2 — Backend API (FastAPI)
```bash
# From backend/ with venv activated
python main.py
# → API server at http://localhost:5000
```

### Terminal 3 — Voice Agent Worker (LiveKit)
```bash
# From backend/ with venv activated
python agent_worker.py start
# → Connects to LiveKit Cloud, waits for WebRTC sessions
```

---

## 🎙️ Usage

1. Open `http://localhost:5173` in your browser
2. Click **"Create Agent"** — define a name, objective, and custom system prompt
3. Optionally upload a PDF/TXT reference document for the agent's knowledge
4. Click **"Start Voice Session"** on the agent card
5. Allow microphone access — the agent will greet you
6. Speak naturally — watch the real-time transcript and voice visualizer
7. Interrupt the agent at any time by speaking over it (barge-in)

---

## 📁 Project Structure

```
vocal-voice/
├── src/                    # React frontend
│   ├── App.tsx             # Main application (agent CRUD, voice session, UI)
│   ├── App.css             # Component styles
│   ├── index.css           # Design system tokens
│   └── main.tsx            # Entry point
├── backend/
│   ├── agent_worker.py     # LiveKit voice agent (STT → Guardrails → LLM → TTS)
│   ├── main.py             # FastAPI server (agent CRUD, token generation)
│   ├── requirements.txt    # Python dependencies
│   └── start.sh            # Production startup script
├── render.yaml             # Render deployment configuration
├── package.json            # Node.js dependencies
└── README.md
```

---

## 📊 Observability & Logging

The system provides full-stack observability with millisecond-precision telemetry:

**Backend** (`vocal_voice.log`):
```
[PIPELINE: 1] 🎤 User started speaking (Turn #1)
[PIPELINE: 3] 📝 STT Final Transcript: 'hello'
[PIPELINE: 3]   └─ ⏱️ STT Endpointing Latency: 25.1ms
[PIPELINE: 4] ✅ Guardrails passed (⏱️ 0.2ms).
[PIPELINE: 6] 🔊 AI Audio Started Playing to User!
[PIPELINE: 6]   └─ ⏱️ E2E AI Response Latency (LLM + TTS TTFB): 650.4ms
[PIPELINE: 7] ✅ Turn #1 complete. Full cycle: Mic → STT → Guardrails → LLM → TTS → Speakers
```

**Frontend** (Browser DevTools Console):
```
[SESSION]  ✅ Received LiveKit token (⏱️ API Latency: 112.5ms)
[WEBRTC]   ✅ WebRTC connection established! (⏱️ Connection Latency: 340.1ms)
[STREAM]   User Mic → WebRTC → Deepgram STT → Guardrails → Groq LLM → Deepgram TTS → Speakers
```

---

## 📝 License

This project is open source and free to use.