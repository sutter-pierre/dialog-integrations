import typer
from loguru import logger

from integrations.shared import DialogIntegration
from settings import Organization

app = typer.Typer(help="Dialog CLI")


@app.command()
def integrate(organization: Organization):  # type: ignore[valid-type]
    """Sync data for a specific organization to Dialog API."""
    dialog_integration = DialogIntegration.from_organization(organization.name)
    logger.info(f"Integrating measures for organization: {organization.name}")
    dialog_integration.integrate()


@app.command()
def publish(organization: Organization):  # type: ignore[valid-type]
    """Publish all measures"""
    dialog_integration = DialogIntegration.from_organization(organization.name)
    logger.info(f"Publishing measures for organization: {organization.name}")
    dialog_integration.publish()
