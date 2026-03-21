import os
from threading import Lock

import dotenv
from google import genai

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

    def chat(self, user_prompt: str, system_prompt: str | None = None) -> str:
        try:
            payload = {"model": self.model_name, "contents": user_prompt}
            if system_prompt and system_prompt.strip():
                payload["config"] = {
                    "system_instruction": system_prompt.strip(),
                }

            response = self.client.models.generate_content(**payload)
            return (response.text or "").strip()
        except Exception as e:
            return f"❌ Error in chat: {str(e)}"
