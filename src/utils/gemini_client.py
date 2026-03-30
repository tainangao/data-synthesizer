import os
import time
import json
from threading import Lock

import dotenv
import requests
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
        self.base_url = "https://api.gptoai.top"
        self.api_key = os.getenv("OPENAI_API_KEY")

    def chat(self, user_prompt: str, system_prompt: str | None = None, max_retries: int = 5, use_post: bool = True) -> str:
        """Chat with Gemini API with exponential backoff for rate limits.

        Args:
            user_prompt: User message
            system_prompt: Optional system instruction
            max_retries: Max retry attempts for rate limits
            use_post: If True, use POST request; if False, use genai client
        """
        if use_post:
            return self._chat_post(user_prompt, system_prompt, max_retries)
        else:
            return self._chat_genai(user_prompt, system_prompt, max_retries)

    def _chat_post(self, user_prompt: str, system_prompt: str | None, max_retries: int) -> str:
        """Chat using POST request."""
        for attempt in range(max_retries):
            try:
                messages = []
                if system_prompt and system_prompt.strip():
                    messages.append({"role": "system", "content": system_prompt.strip()})
                messages.append({"role": "user", "content": user_prompt})

                payload = json.dumps({
                    "model": self.model_name,
                    "messages": messages
                })

                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                }

                response = requests.post(
                    f"{self.base_url}/v1/chat/completions",
                    headers=headers,
                    data=payload,
                    timeout=60
                )

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception("Rate limit exceeded")

                response.raise_for_status()
                result = response.json()
                return result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 2
                    time.sleep(wait_time)
                else:
                    return f"❌ Error in chat: {str(e)}"
        return ""

    def _chat_genai(self, user_prompt: str, system_prompt: str | None, max_retries: int) -> str:
        """Chat using genai client."""
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
                    wait_time = (2 ** attempt) * 2
                    time.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                return f"❌ Error in chat: {str(e)}"
        return ""
