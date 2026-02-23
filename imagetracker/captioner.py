from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

import requests


@dataclass
class CaptionResult:
    short_description: str
    model: str


class Captioner(Protocol):
    def generate_caption(self, image_bytes: bytes) -> Optional[CaptionResult]:
        ...


def _extract_text(response_json: Dict[str, Any]) -> str:
    output_text = response_json.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    outputs = response_json.get("output", [])
    for output in outputs:
        for content in output.get("content", []):
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


def _normalize_caption(text: str, max_words: int) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return ""

    sentence_split = re.split(r"(?<=[.!?])\s+", cleaned)
    first_sentence = sentence_split[0].strip()
    if not first_sentence:
        first_sentence = cleaned

    words = first_sentence.split()
    clipped = " ".join(words[:max_words])

    if clipped and clipped[-1] not in ".!?":
        clipped += "."

    return clipped


class OpenAIVisionCaptioner:
    def __init__(
        self,
        api_key: str,
        model: str,
        max_words: int,
        session: Optional[requests.Session] = None,
    ):
        self._api_key = api_key
        self._model = model
        self._max_words = max_words
        self._session = session or requests.Session()

    @property
    def model(self) -> str:
        return self._model

    def generate_caption(self, image_bytes: bytes) -> Optional[CaptionResult]:
        encoded = base64.b64encode(image_bytes).decode("ascii")
        instruction = (
            "Describe this image in exactly one sentence under "
            f"{self._max_words} words. Do not identify people, addresses, "
            "or sensitive attributes. If it is a screenshot or document, "
            "describe the content at a high level."
        )

        payload = {
            "model": self._model,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": instruction},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/jpeg;base64,{encoded}",
                        },
                    ],
                }
            ],
            "max_output_tokens": 100,
        }

        response = self._session.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Caption generation failed ({response.status_code}): {response.text}")

        text = _extract_text(response.json())
        normalized = _normalize_caption(text, self._max_words)
        if not normalized:
            return None

        return CaptionResult(short_description=normalized, model=self._model)
