import logging
from groq import Groq
from app.config import get_settings

logger = logging.getLogger(__name__)


def get_groq_client() -> Groq:
    """Get an authenticated Groq client."""
    settings = get_settings()
    return Groq(api_key=settings.GROQ_API_KEY)


def transcribe_audio(video_path: str) -> dict:
    """
    Transcribe audio from a video file using Groq Whisper.

    Args:
        video_path: Path to the video file

    Returns:
        dict with 'text' (full transcript) and 'segments' (timestamped segments)
    """
    settings = get_settings()
    client = get_groq_client()

    try:
        logger.info(f"Transcribing audio from: {video_path}")

        with open(video_path, "rb") as audio_file:
            transcription = client.audio.transcriptions.create(
                file=audio_file,
                model=settings.WHISPER_MODEL,
                response_format="verbose_json",
                language="en",
            )

        result = {
            "text": transcription.text,
            "segments": [],
        }

        # Extract segments if available
        if hasattr(transcription, "segments") and transcription.segments:
            result["segments"] = [
                {
                    "start": seg.start if hasattr(seg, "start") else getattr(seg, "start", 0),
                    "end": seg.end if hasattr(seg, "end") else getattr(seg, "end", 0),
                    "text": seg.text if hasattr(seg, "text") else getattr(seg, "text", ""),
                }
                for seg in transcription.segments
            ]

        logger.info(
            f"Transcription complete: {len(result['text'])} chars, "
            f"{len(result['segments'])} segments"
        )
        return result

    except Exception as e:
        logger.error(f"Transcription error: {e}")
        raise Exception(f"Failed to transcribe audio: {str(e)}")
