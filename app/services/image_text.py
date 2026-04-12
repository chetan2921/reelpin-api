import base64
import json
import logging
from pathlib import Path

from groq import Groq

from app.config import get_settings

logger = logging.getLogger(__name__)

_OCR_PROMPT = """
You are reading Instagram post or carousel images.
Extract all clearly visible text from each image and add one short sentence
about what the slide is communicating.

Return only valid JSON in this shape:
{
  "slides": [
    {
      "index": 1,
      "text": "all readable text from this slide",
      "summary": "one-sentence summary of the slide"
    }
  ]
}

Rules:
- Preserve meaningful headings, bullets, numbered points, prices, and calls to action.
- Do not invent text that is not visible.
- If a slide has little or no readable text, leave "text" empty and rely on "summary".
- Keep slide order the same as the input order.
"""


def _get_groq_client() -> Groq:
    settings = get_settings()
    return Groq(api_key=settings.GROQ_API_KEY)


def extract_text_from_images(image_paths: list[str]) -> str:
    if not image_paths:
        return ""

    settings = get_settings()
    client = _get_groq_client()
    parts: list[str] = []

    for chunk_start in range(0, len(image_paths), 5):
        chunk = image_paths[chunk_start:chunk_start + 5]
        logger.info(
            "Extracting visible text from %s image slide(s) with Groq vision",
            len(chunk),
        )

        user_content = [{"type": "text", "text": _OCR_PROMPT}]
        for index, image_path in enumerate(chunk, start=chunk_start + 1):
            encoded = base64.b64encode(Path(image_path).read_bytes()).decode("ascii")
            user_content.append(
                {
                    "type": "text",
                    "text": f"Slide {index}",
                }
            )
            user_content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{encoded}",
                    },
                }
            )

        response = client.chat.completions.create(
            model=settings.VISION_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise OCR and slide summarization assistant. Return JSON only.",
                },
                {
                    "role": "user",
                    "content": user_content,
                },
            ],
            temperature=0.1,
            max_tokens=1800,
            response_format={"type": "json_object"},
        )

        raw_response = response.choices[0].message.content or "{}"
        payload = json.loads(raw_response)
        slides = payload.get("slides", [])
        if not isinstance(slides, list):
            slides = []

        for slide in slides:
            if not isinstance(slide, dict):
                continue

            slide_index = slide.get("index")
            text = str(slide.get("text", "")).strip()
            summary = str(slide.get("summary", "")).strip()

            block: list[str] = []
            if slide_index:
                block.append(f"Slide {slide_index}")
            if text:
                block.append(f"Visible text: {text}")
            if summary:
                block.append(f"Summary: {summary}")

            if block:
                parts.append("\n".join(block))

    combined = "\n\n".join(part for part in parts if part.strip())
    logger.info("Image text extraction complete: %s chars", len(combined))
    return combined
