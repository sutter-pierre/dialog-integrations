import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as get_identifiers,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from settings import OrganizationSettings, Settings


class DialogIntegration:
    client: Client
    draft_status: bool = False
    organization_settings: OrganizationSettings
    organization: str

    def __init__(self, organization: str):  # type: ignore
        self.organization = organization
        self.organization_settings = OrganizationSettings(Settings(), organization)
        self.client = Client(
            base_url=self.organization_settings.base_url,  # type: ignore
            headers={
                "X-Client-Id": self.organization_settings.client_id,
                "X-Client-Secret": self.organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

    def fetch_data(self) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement fetch_data method")

    def publish(self):
        logger.info(f"Publishing measures for organization: {self.organization}")

        # Get the organization identifiers
        resp = get_identifiers(client=self.client)

        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")

        identifiers: list[str] = list(resp.parsed.identifiers)  # type: ignore
        logger.info(f"Found {len(identifiers)} identifier(s) to publish")

        # Publish each identifier
        count_error = 0
        for index, identifier in enumerate(identifiers):
            publish_resp = publish_regulation(identifier=identifier, client=self.client)

            if publish_resp.status_code == 200:
                logger.success(
                    f"Measure {index}/{len(identifiers)} successfully published: {identifier}"
                )
            else:
                logger.error(f"Measure {index}/{len(identifiers)} failed to publish: {identifier}")
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
            logger.success(
                f"Finished publishing {len(identifiers) - count_error} measures successfully"
            )
        else:
            logger.success("Finished publishing all measures")
