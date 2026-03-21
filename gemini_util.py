import dotenv
import os
from google import genai
from threading import Lock

dotenv.load_dotenv()


class GeminiClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Ensure only one instance (singleton)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GeminiClient, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize Gemini client."""
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        self.embedding_model_name = os.getenv("EMBEDDING_MODEL")

    def chat(self, user_prompt: str, system_prompt: str | None = None) -> str:
        """Generate a chat response."""
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


if __name__ == "__main__":
    # Example usage
    client = GeminiClient()

    user_prompt = "Hello, world!"
    sys_prompt = "You are a humorous assistant, and you always respond with a joke."

    response = client.chat(user_prompt, sys_prompt)
    print("Chat response:", response)
