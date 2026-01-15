from enum import Enum
from pathlib import Path

from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict

Organization = Enum(
    "Organization",
    {
        name: name
        for name in [
            p.name
            for p in Path("integrations").iterdir()
            if p.is_dir() and p.name != "shared" and not p.name.startswith("__")
        ]
    },
    type=str,
)


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
        self.organization = organization.lower()
        self.base_url = settings.base_url

        self.client_id = getattr(settings, f"dialog_{organization}_client_id", None)
        self.client_secret = getattr(settings, f"dialog_{organization}_client_secret", None)

    def validate(self) -> bool:
        """
        Validate that all public attributes are set.
        """
        valid = True
        for name, value in vars(self).items():
            if value is None:
                logger.warning(f"[settings] Missing value for {self.organization}:{name}")
                valid = False

        return valid

    @classmethod
    def from_organization(cls, organization: str) -> "OrganizationSettings":
        settings = Settings()
        organization_settings = OrganizationSettings(settings, organization)
        if not organization_settings.validate():
            raise ValueError(f"Invalid settings for organization: {organization}")
        return organization_settings

    @classmethod
    def validate_all_organization_settings(cls, settings: Settings) -> None:
        for organization in Organization:  # type: ignore
            OrganizationSettings(settings, organization.name).validate()
