import importlib.util
from pathlib import Path

import typer
from loguru import logger

from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as get_identifiers,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from integrations.shared import get_client
from settings import Organization, OrganizationSettings, Settings

app = typer.Typer(help="Dialog CLI")


@app.callback()
def main():
    """Global pre-run hook."""
    settings = Settings()
    OrganizationSettings.validate_all_organization_settings(settings)


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
def publish(organization: Organization):  # type: ignore[valid-type]
    """Publish all measures"""
    settings = OrganizationSettings.from_organization(organization.value)
    client = get_client(settings)

    logger.info(f"Publishing measures for organization: {organization}")

    # Get the organization identifiers
    resp = get_identifiers(client=client)

    if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
        logger.error("Failed to fetch identifiers")
        raise typer.Exit(code=1)

    identifiers: list[str] = list(resp.parsed.identifiers)  # type: ignore
    logger.info(f"Found {len(identifiers)} identifier(s) to publish")

    # Publish each identifier
    has_error = False
    for identifier in identifiers:
        logger.info(f"Publishing identifier: {identifier}")
        publish_resp = publish_regulation(identifier=identifier, client=client)

        if publish_resp.status_code == 200:
            logger.success(f"Successfully published: {identifier}")
        else:
            logger.error(f"Failed to publish {identifier}: {publish_resp.status_code}")
            has_error = True

    if has_error:
        logger.error("Some measures failed to publish")
        raise typer.Exit(code=1)

    logger.success("Finished publishing all measures")
