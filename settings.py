from enum import Enum
from pathlib import Path

from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict

organization_names = [
    p.name
    for p in Path("integrations").iterdir()
    if p.is_dir() and p.name != "shared" and not p.name.startswith("__")
]
Organization: Enum = Enum(
    "Organization",
    {name: name for name in organization_names},
    type=str,
)


def complete_organization(ctx, param, incomplete):
    return organization_names


env_prefix = "DIALOG_"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="DIALOG_",
        case_sensitive=False,
        extra="allow",
    )

    base_url: str | None = None


class OrganizationSettings:
    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    def __init__(self, settings: Settings, organization: str):
        self.network = organization.lower()
        self.base_url = settings.base_url

        self.client_id = self._get(settings, "client_id")
        self.client_secret = self._get(settings, "client_secret")

    def _get(self, settings: Settings, key: str) -> str | None:
        attr = f"dialog_{self.network}_{key}"
        value = getattr(settings, attr, None)

        if value is None:
            logger.warning(
                f"[settings] Missing env var: DIALOG_{self.network.upper()}_{key.upper()}"
            )
        return value


def validate_settings(settings: Settings) -> None:
    for organization in organization_names:
        OrganizationSettings(settings, organization)
