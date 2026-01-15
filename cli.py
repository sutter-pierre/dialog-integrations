import importlib.util
from pathlib import Path

import typer
from loguru import logger

app = typer.Typer(help="Dialog CLI")


@app.command()
def version():
    """Show the version of the CLI."""
    logger.info("Dialog CLI version 1.0.0")


@app.command()
def integrate(network: str):
    """Sync data for a specific network to Dialog API."""

    integration_dir = Path("integrations") / network
    main_file = integration_dir / "main.py"

    try:
        if not main_file.exists():
            raise FileNotFoundError(main_file)

        spec = importlib.util.spec_from_file_location(f"integrations.{network}.main", main_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {main_file}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "main"):
            raise AttributeError("main() not found")

        logger.info(f"Running integration for: {network}")
        module.main()

    except FileNotFoundError:
        logger.error(f"No integration found for network: {network}")
        logger.info(f"Expected file: {main_file}")
        raise typer.Exit(code=1)

    except Exception as e:
        logger.exception(f"Integration failed for {network}: {e}")
        raise typer.Exit(code=1)
