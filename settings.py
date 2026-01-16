from enum import Enum
from pathlib import Path

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
        self.base_url = settings.base_url
        self.client_id = getattr(settings, f"dialog_{organization}_client_id", None)
        self.client_secret = getattr(settings, f"dialog_{organization}_client_secret", None)

        missing_values = [name for (name, value) in vars(self).items() if value is None]
        if missing_values:
            raise Exception(f"Invalid settings: {missing_values}")
