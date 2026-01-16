import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import polars as pl
import requests
from loguru import logger

from integrations.shared import DialogIntegration

URL = "https://www.data.gouv.fr/api/1/datasets/r/3ca7bd06-6489-45a2-aee9-efc6966121b2"
FILENAME = "DEP_ARR_CIRC_STAT_L_V.shp"


class Integration(DialogIntegration):
    draft = False

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading and reading shapefile data from {URL}")
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "data.zip"

            # download
            r = requests.get(URL)
            r.raise_for_status()
            zip_path.write_bytes(r.content)
            logger.info(f"Downloaded zip file to {zip_path}")

            # unzip
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)

            # find .shp
            shp_path = next(Path(tmpdir).rglob("*.shp"))
            shp_path = Path(tmpdir) / FILENAME

            # read
            gdf = gpd.read_file(shp_path)

        # geometry -> WKT pour Polars
        gdf["geometry"] = gdf.geometry.to_wkt()
        logger.info(f"Read {len(gdf)} records from shapefile {FILENAME}")
        return pl.from_pandas(gdf)

#     sync_detailed as get_identifiers,
# )
# from api.dia_log_client.api.private.post_api_regulations_add import (
#     sync_detailed as add_regulation,
# )
# from api.dia_log_client.models import (
#     PostApiRegulationsAddBody,
#     PostApiRegulationsAddBodyCategory,
#     PostApiRegulationsAddBodySubject,
#     RegulationOrderRecordStatusEnum,
#     SaveMeasureDTO,
# )
# from settings import Settings

# from .config import DESCRIPTION_CONFIG, create_measure
# from .schema import BREST_SCHEMA


# def main():
#     """Main entry point for Brest integration."""
#     # Load settings from environment
#     settings = Settings()  # type: ignore[call-arg]

#     # Config API DiaLog
#     client = Client(
#         base_url=settings.base_url,
#         headers={
#             "X-Client-Id": settings.client_id,
#             "X-Client-Secret": settings.client_secret,
#             "Accept": "application/json",
#         },
#     )

#     suffix = "0"
#     status = RegulationOrderRecordStatusEnum.DRAFT

#     # Get the organization identifiers
#     resp = get_identifiers(client=client)
#     if resp.parsed and resp.parsed:
#         # type: ignore
#         identifiers = [
#             f"{identifier}-{suffix}"
#             for identifier in resp.parsed.identifiers  # type: ignore
#         ]
#     else:
#         identifiers = []

#     # Load data
#     def cast(name):
#         return (
#             pl.when(pl.col(name).str.to_uppercase() == "OUI")
#             .then(True)
#             .when(pl.col(name).str.to_uppercase() == "NON")
#             .then(False)
#             .cast(pl.Boolean)
#             .alias(name)
#         )

#     # Get data file path relative to this file
#     from pathlib import Path

#     integration_dir = Path(__file__).parent
#     data_file = integration_dir / "data" / "brest.parquet"

#     raw_data = pl.read_parquet(data_file)

#     df = (
#         raw_data.with_columns([cast("CYCLO"), cast("VELO")])
#         .with_columns([pl.col(col).cast(dtype) for col, dtype in BREST_SCHEMA.items()])
#         .select(list(BREST_SCHEMA.keys()))
#         .filter(pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys()))
#         .filter(~(pl.col("NOARR") + f"-{suffix}-{suffix}").is_in(identifiers))
#         .filter(~(pl.col("DESCRIPTIF").eq("Sens interdit / Sens unique") & pl.col("SENS").eq(1)))
#     )

#     logger.info(f"Nombre de lignes initiales : {raw_data.height}")
#     logger.info(f"Nombre de lignes gardées : {df.height}")
#     logger.info(f"Intégration de {df['NOARR'].n_unique()} arrêtés uniques.")

#     ## Process and send data
#     num_failures = 0
#     for group_key, group_df in df.sort("NOARR").group_by("NOARR"):
#         logger.info(f"Traitement de l'arrêté {group_key[0]} avec {group_df.height} mesures.")
#         regulation_id = group_key[0]

#         # Measures creation
#         try:
#             measures: list[SaveMeasureDTO] = [
#                 create_measure(measure)  # type: ignore
#                 for measure in group_df.iter_rows(named=True)
#             ]
#         except Exception as e:
#             logger.error(f"Erreur lors de la création de l'arrêté {group_key[0]}")
#             logger.error(e)
#             num_failures += 1
#             continue

#         # API payload creation
#         first_row = group_df.row(0, named=True)
#         title = f"{first_row['DESCRIPTIF']} – {first_row['LIBRU']}"
#         payload = PostApiRegulationsAddBody(
#             identifier=f"{regulation_id}-{suffix}",
#             category=PostApiRegulationsAddBodyCategory.PERMANENTREGULATION,
#             status=status,  # type: ignore
#             subject=PostApiRegulationsAddBodySubject.OTHER,
#             title=title,
#             other_category_text="Circulation",
#             measures=measures,  # type: ignore
#         )

#         # API call
#         resp = add_regulation(client=client, body=payload)
#         if resp.status_code != HTTPStatus.CREATED:
#             logger.error(
#                 f"Échec de la création de l'arrêté {regulation_id} : "
#                 f"{resp.status_code} {resp.content}"
#             )
#             breakpoint()
#             num_failures += 1
#         else:
#             logger.info(f"Arrêté {regulation_id} créé avec succès.")

#     total_regulations = df["NOARR"].n_unique()
#     logger.info(
#         f"Intégration terminée avec {num_failures} échecs sur "
#         f"{total_regulations} arrêtés à intégrer"
#     )
