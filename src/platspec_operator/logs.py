"""Logging setup for the Platspec Operator."""

import sys
from pathlib import Path
from typing import Any, Optional

from loguru import logger


_TEXT_FMT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{name}:{function}:{line} | "
    "{message}"
)


def _dev_fmt(record: Any) -> str:
    """Dev format — includes resource context (e.g. Platform/ns/name) when bound."""
    resource = record["extra"].get("resource", "")
    res_part = f" | <magenta>{resource}</magenta>" if resource else ""
    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
        + res_part
        + " | <level>{message}</level>\n"
    )


def setup_logging(
    level: str = "INFO",
    dev: bool = False,
    log_format: str = "json",
    log_file: Optional[Path] = None,
) -> None:
    logger.remove()

    if dev:
        # Dev mode: colored text with backtrace and diagnose, regardless of log_format.
        logger.add(
            sys.stderr,
            format=_dev_fmt,
            level=level,
            colorize=True,
            enqueue=True,
            backtrace=True,
            diagnose=True,
        )
    elif log_format == "text":
        logger.add(
            sys.stdout,
            format=_TEXT_FMT,
            level=level,
            colorize=False,
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )
    else:
        # Default: structured JSON output.
        def serialize(record):  # type: ignore[no-untyped-def]
            subset = {
                "timestamp": record["time"].isoformat(),
                "level": record["level"].name,
                "logger": record["name"],
                "function": record["function"],
                "line": record["line"],
                "message": record["message"],
            }
            if record.get("extra"):
                subset.update(record["extra"])
            if record["exception"]:
                subset["exception"] = {
                    "type": record["exception"].type.__name__,
                    "value": str(record["exception"].value),
                }
            return subset

        logger.add(
            sys.stdout,
            format="{message}",
            level=level,
            serialize=serialize,
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )

    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            p,
            format="{message}",
            level=level,
            rotation="1 day",
            retention="30 days",
            enqueue=True,
            serialize=True,
        )

    _configure_kopf_logging(level, dev)
    _configure_k8s_logging(level)
    effective_format = "dev" if dev else log_format
    logger.info(f"Logging configured — level={level} format={effective_format}")


def _configure_kopf_logging(level: str, dev: bool) -> None:
    import logging

    import kopf

    kopf.configure(verbose=dev, log_format=kopf.LogFormat.PLAIN)

    class _Intercept(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                lvl = logger.level(record.levelname).name
            except ValueError:
                lvl = str(record.levelno)
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back  # type: ignore[assignment]
                depth += 1
            logger.opt(depth=depth, exception=record.exc_info).log(
                lvl, record.getMessage()
            )

    for name in ["kopf", "kopf.objects", "kopf.reactor", "kopf.timers"]:
        lg = logging.getLogger(name)
        lg.handlers = [_Intercept()]
        lg.propagate = False


def _configure_k8s_logging(level: str) -> None:
    import logging

    import urllib3

    if level != "DEBUG":
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    for name in ["kubernetes", "kubernetes.client", "pykube"]:
        lg = logging.getLogger(name)
        lg.setLevel(logging.DEBUG if level == "DEBUG" else logging.WARNING)
