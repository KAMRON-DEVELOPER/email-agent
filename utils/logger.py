import sys
from pathlib import Path

from loguru import logger

from settings.config import get_settings

settings = get_settings()


def custom_log_sink(message):
    """Custom Loguru Sink with clickable file paths."""
    reset = "\033[0m"
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    blue = "\033[34m"
    magenta = "\033[35m"
    cyan = "\033[36m"
    white = "\033[37m"

    log_levels = {
        "TRACE": {"emoji": "🔍", "color": cyan},
        "DEBUG": {"emoji": "🐛", "color": blue},
        "INFO": {"emoji": "💡", "color": green},
        "WARNING": {"emoji": "🚨", "color": yellow},
        "ERROR": {"emoji": "🌋", "color": red},
        "CRITICAL": {"emoji": "👾", "color": magenta},
    }

    record = message.record
    msg = record["message"]

    level = record["level"].name
    color = log_levels.get(level, {}).get("color", white)
    emoji = log_levels.get(level, {}).get("emoji", "📌")

    full_path = Path(record["file"].path)
    relative_path = full_path.relative_to(Path(__file__).parent.parent.parent)
    line = record["line"]

    # Format for clickability: file.py:123
    clickable_path = f"{relative_path}:{line}"

    sys.stdout.write(f"{color}{clickable_path}    {emoji} {msg}{reset}\n")


logger.remove()
logger.add(custom_log_sink, level="TRACE" if settings.DEBUG else "WARNING")
