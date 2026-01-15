import importlib.util
from pathlib import Path

import typer
from loguru import logger

from settings import Organization, Settings, validate_settings

app = typer.Typer(help="Dialog CLI")


@app.callback()
def main():
    """Global pre-run hook."""
    settings = Settings()
    validate_settings(settings)


@app.command()
def version():
    """Show the version of the CLI."""
    logger.info("Dialog CLI version 1.0.0")


@app.command()
def integrate(organization: Organization):  # type: ignore[valid-type]
    """Sync data for a specific organization to Dialog API."""
    main_file = Path("integrations") / organization.name / "main.py"

    try:
        if not main_file.exists():
            raise FileNotFoundError(main_file)

        spec = importlib.util.spec_from_file_location(
            f"integrations.{organization}.main", main_file
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {main_file}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "main"):
            raise AttributeError("main() not found")

        logger.info(f"Running integration for: {organization}")
        module.main()

    except FileNotFoundError:
        logger.error(f"No integration found for network: {organization}")
        logger.info(f"Expected file: {main_file}")
        raise typer.Exit(code=1)

    except Exception as e:
        logger.exception(f"Integration failed for {organization}: {e}")
        raise typer.Exit(code=1)


@app.command()
def publish_measures(organization: Organization):  # type: ignore[valid-type]
    """Publish all measures"""
    logger.info(f"Publishing measures for network: {organization}")
