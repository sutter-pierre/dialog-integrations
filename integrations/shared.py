from api.dia_log_client import Client
from settings import OrganizationSettings


def get_client(organization_settings: OrganizationSettings) -> Client:
    return Client(
        base_url=organization_settings.base_url,  # type: ignore
        headers={
            "X-Client-Id": organization_settings.client_id,
            "X-Client-Secret": organization_settings.client_secret,
            "Accept": "application/json",
        },  # type: ignore
    )
