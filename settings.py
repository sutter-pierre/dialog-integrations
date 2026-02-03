from enum import Enum
from pathlib import Path
from typing import Any

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
        env_prefix="DIALOG_",
        case_sensitive=False,
        extra="allow",
    )

    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    def __init__(self, organization: str, env: str = "dev", **data: Any):
        # Determine which .env file to load: .env.{organization}.{env}
        env_file = Path(f".env.{organization}.{env}")

        # Update model_config with the env_file if it exists
        if env_file.exists():
            logger.info(f"Loading environment variables from {env_file}")
            self.model_config["env_file"] = str(env_file)
        else:
            logger.warning(f"Environment file not found: {env_file}")
            logger.warning("Using environment variables from CI/CD.")

        super().__init__(**data)


class OrganizationSettings:
    organization: str
    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    def __init__(self, settings: Settings, organization: str):
        self.organization = organization
        self.base_url = settings.base_url
        self.client_id = settings.client_id
        self.client_secret = settings.client_secret

        missing_values = [
            name for (name, value) in vars(self).items() if value is None and name != "organization"
        ]
        if missing_values:
            raise Exception(f"Invalid settings for {organization}: {missing_values}")

    @classmethod
    def from_env(cls, organization: str, env: str = "dev") -> "OrganizationSettings":
        """Create OrganizationSettings from organization and environment."""
        settings = Settings(organization=organization, env=env)
        return cls(settings, organization)
