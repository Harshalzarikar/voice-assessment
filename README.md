# Vocal-Voice: Real-Time AI Voice Assistant

This project is a high-performance, real-time voice AI assistant built for ultra-low latency conversational interactions. It utilizes WebRTC for continuous audio streaming, Deepgram for streaming STT/TTS, and Groq for blazing-fast LLM token generation.

## Features Implemented
- **Dynamic Agent Creation:** Go beyond hardcoded prompts! Users can configure new agents in the UI with custom names, objectives, system prompts, and upload PDF/TXT knowledge reference files.
- **WebRTC Streaming:** Full-duplex, low-latency audio via LiveKit, entirely avoiding the overhead of raw WebSocket audio processing.
- **Progressive UI:** Real-time interim STT transcripts and token-by-token LLM streaming.
- **Latency Telemetry:** True timestamp-based measurement of STT, LLM TTFT (Time to First Token), and TTS TTFB (Time to First Byte).
- **Ghost-Audio Prevention (Barge-In):** Immutable generation IDs and immediate cancellation broadcasts ensure that when a user interrupts the AI, stale TTS audio buffers are instantly discarded.
- **Slow-TTS Backpressure:** Custom 2.5s TTS timeout guards against pipeline stalling, gracefully degrading to a text-only UI warning if the TTS provider hangs.
- **Monotonic Sequence Tracking:** Drops duplicate/stale out-of-order WebRTC data packets.
- **Context Sliding Window:** Dynamically truncates the conversational history buffer to the most recent 5-10 turns to prevent LLM context overflow while preserving the immutable system prompt.
- **Robust Error Recovery:** Explicit UI state handling for Microphone permission denials, WebSocket disconnects, and API failures.

## Architecture & Technology
Please see [`ARCHITECTURE.md`](./ARCHITECTURE.md) for a detailed architecture diagram, pipeline latency waterfalls, and an explanation of the technology choices (e.g., why WebRTC was chosen over raw WebSockets, and how session state is managed).

---

## 🚀 How to Run Locally

### 1. Environment Setup
Create a `.env` file in the root directory by copying the provided example:
```bash
cp .env.example .env
```
Fill in the `.env` file with your actual API keys (LiveKit, Groq, Deepgram, Tavily). **Do not hardcode or commit these keys.**

### 2. Start the Frontend
The frontend is built with React/Vite.
```bash
# Install dependencies
yarn install

# Start the Vite development server
yarn dev
```

### 3. Start the Backend API & Agent Worker
The backend requires Python 3.10+. Open a **second terminal**.
```bash
# Navigate to the backend directory
cd backend

# Create and activate a virtual environment
python -m venv venv
# On Windows: .\venv\Scripts\activate
# On Mac/Linux: source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the FastAPI server (handles tokens & dispatch)
python main.py
```

### 4. Start the Voice Agent
Open a **third terminal** (ensure your virtual environment is still activated).
```bash
cd backend
python agent_worker.py start
```

### 5. Open the Application
Navigate to `http://localhost:5173` in your browser. Click **Connect** and begin speaking!

---

## Testing Logical Challenges (Reviewer Guide)
To prove the resilience of the pipeline against edge cases, you can speak the following secret trigger phrases into the microphone during a session:
- Say **"Simulate delay"**: The backend will intentionally stall the TTS pipeline for 3 seconds. Watch the yellow backpressure warning banner drop down and the system gracefully recover without freezing.
- Say **"Simulate duplicate"**: The backend will intentionally broadcast a duplicated WebRTC packet with an identical sequence number. Check your browser's Developer Console to verify the UI explicitly drops the duplicate packet `(seq X <= X)`.