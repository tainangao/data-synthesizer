import os
import time
from threading import Lock

import dotenv
from google import genai
from google.api_core import exceptions

dotenv.load_dotenv()


class GeminiClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GeminiClient, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

    def chat(self, user_prompt: str, system_prompt: str | None = None, max_retries: int = 5) -> str:
        """Chat with Gemini API with exponential backoff for rate limits."""
        for attempt in range(max_retries):
            try:
                payload = {"model": self.model_name, "contents": user_prompt}
                if system_prompt and system_prompt.strip():
                    payload["config"] = {
                        "system_instruction": system_prompt.strip(),
                    }

                response = self.client.models.generate_content(**payload)
                return (response.text or "").strip()
            except exceptions.ResourceExhausted:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 2  # 2s, 4s, 8s, 16s, 32s
                    time.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                return f"❌ Error in chat: {str(e)}"
        return ""
