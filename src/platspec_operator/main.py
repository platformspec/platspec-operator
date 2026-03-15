"""Entry point for the Platspec Operator."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
import kopf
from loguru import logger

from .config import Config
from .logs import setup_logging


def register_handlers() -> None:
    from .handlers import binding, infrastructure, platform, registry, startup, status  # noqa: F401

    logger.info("Registered all kopf handlers")


@click.command()
@click.option("--namespace", "-n", default=None, help="Namespace to watch (default: all)")
@click.option(
    "--config-file",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "--log-level",
    "-l",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
@click.option(
    "--log-format",
    default="json",
    type=click.Choice(["json", "text"]),
    help="Log output format: json (default) or text.",
)
@click.option("--dev", is_flag=True, help="Development mode")
@click.option("--dry-run", is_flag=True, help="Dry-run mode (no changes applied)")
@click.version_option()
def main(
    namespace: Optional[str],
    config_file: Optional[Path],
    log_level: str,
    log_format: str,
    dev: bool,
    dry_run: bool,
) -> None:
    """Start the Platspec Kubernetes Operator."""
    setup_logging(level=log_level, dev=dev, log_format=log_format)

    config = Config.load(config_file)
    logger.info(f"Loaded configuration: {config}")

    if dry_run:
        logger.info("Dry-run mode enabled — no changes will be applied")

    register_handlers()

    kopf.configure(
        verbose=dev,
        log_format=kopf.LogFormat.PLAIN if dev else kopf.LogFormat.JSON,
    )

    logger.info("Starting Platspec Operator")
    logger.info(f"Watching namespace: {namespace or 'all namespaces'}")

    try:
        asyncio.run(
            kopf.operator(
                namespace=namespace,
                clusterwide=namespace is None,
                priority=100,
            )
        )
    except KeyboardInterrupt:
        logger.info("Operator stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Operator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
