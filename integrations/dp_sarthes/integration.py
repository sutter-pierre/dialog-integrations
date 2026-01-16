import polars as pl

from integrations.shared import DialogIntegration


class Integration(DialogIntegration):
    draft = True

    def fetch_raw_data(self) -> pl.DataFrame:
        return pl.DataFrame()
