from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google import genai

DEFAULT_GEMINI_MODEL = "gemini-2.5-pro"


def create_gemini_client(
    api_key: str | None = None,
) -> genai.Client:
    """Create a Gemini client using google-genai."""
    resolved_api_key = (
        api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    )
    if not resolved_api_key:
        raise ValueError(
            "Gemini API key not found. Set GEMINI_API_KEY or GOOGLE_API_KEY in your environment."
        )

    return genai.Client(api_key=resolved_api_key)


def call_gemini(
    user_prompt: str,
    *,
    system_prompt: str | None = None,
    model: str = DEFAULT_GEMINI_MODEL,
    temperature: float = 0.1,
    max_output_tokens: int | None = None,
    api_key: str | None = None,
) -> str:
    """Send a prompt to Gemini and return plain text output."""
    cleaned_prompt = user_prompt.strip()
    if not cleaned_prompt:
        raise ValueError("user_prompt must not be empty.")

    client = create_gemini_client(api_key=api_key)

    config: dict[str, object] = {"temperature": temperature}
    if system_prompt and system_prompt.strip():
        config["system_instruction"] = system_prompt.strip()
    if max_output_tokens is not None:
        config["max_output_tokens"] = max_output_tokens

    response = client.models.generate_content(
        model=model,
        contents=cleaned_prompt,
        config=config,
    )

    if response.text:
        return response.text.strip()

    fallback_chunks: list[str] = []
    candidates = response.candidates or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        if not content:
            continue

        parts = getattr(content, "parts", None) or []
        for part in parts:
            text = getattr(part, "text", None)
            if text:
                fallback_chunks.append(text)

    return "\n".join(chunk.strip() for chunk in fallback_chunks if chunk.strip())


def _read_optional_file(file_path: str | None) -> str | None:
    if not file_path:
        return None
    return Path(file_path).read_text(encoding="utf-8").strip()


def _resolve_user_prompt(args: argparse.Namespace) -> str:
    file_prompt = _read_optional_file(args.prompt_file)
    if args.prompt and args.prompt.strip():
        return args.prompt.strip()
    if file_prompt:
        return file_prompt

    if not sys.stdin.isatty():
        stdin_prompt = sys.stdin.read().strip()
        if stdin_prompt:
            return stdin_prompt

    raise ValueError("Provide --prompt, --prompt-file, or pipe a prompt via stdin.")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Call Gemini with a user prompt and optional system prompt."
    )
    parser.add_argument("--prompt", help="User prompt text.")
    parser.add_argument(
        "--prompt-file", help="Path to a file containing the user prompt."
    )
    parser.add_argument("--system", help="Optional system prompt text.")
    parser.add_argument(
        "--system-file", help="Path to a file containing system prompt."
    )
    parser.add_argument(
        "--model", default=DEFAULT_GEMINI_MODEL, help="Gemini model name."
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Sampling temperature.",
    )
    parser.add_argument(
        "--max-output-tokens",
        type=int,
        help="Optional max output tokens.",
    )
    return parser


def main() -> None:
    load_dotenv()
    parser = _build_arg_parser()
    args = parser.parse_args()

    try:
        user_prompt = _resolve_user_prompt(args)
        system_prompt = args.system or _read_optional_file(args.system_file)

        response_text = call_gemini(
            user_prompt,
            system_prompt=system_prompt,
            model=args.model,
            temperature=args.temperature,
            max_output_tokens=args.max_output_tokens,
        )
    except ValueError as exc:
        parser.error(str(exc))
        return

    print(response_text)


if __name__ == "__main__":
    main()
