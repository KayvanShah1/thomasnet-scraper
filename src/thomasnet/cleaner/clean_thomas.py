import traceback

import numpy as np
import pandas as pd
import warnings

warnings.simplefilter(action="ignore")


class CleanThomas:
    def __init__(self, **kwargs):
        self.cleaned_df = None
        self.raw_df = None
        self.metadata_df = None
        self.config = kwargs.get("config")

    def load_data(self, raw_data_path: str):
        try:
            raw_df = pd.read_csv(raw_data_path)
            return raw_df
        except FileNotFoundError as e:
            print(e)

    def save_data(self, clean_data_path: str):
        try:
            self.cleaned_df.to_csv(clean_data_path, index=False)
            print(f"Successfully saved data to {clean_data_path}")
        except Exception as e:
            print(e)

    def clean_data(self, df: pd.core.frame.DataFrame):
        df["keyword"] = self.config["keyword"]
        return df

    def run(self):
        try:
            self.raw_df = self.load_data(
                raw_data_path=self.config["paths"]["master_data_path"]
            )
            self.cleaned_df = self.clean_data(self.raw_df)
        except Exception as e:
            print(
                f"Error encountered while running the cleaner", traceback.format_exc()
            )
        finally:
            self.save_data(clean_data_path=self.config["paths"]["cleaned_data_path"])


if __name__ == "__main__":
    config = {
        "keyword": "hydraulic cylinders",
        "heading": 21650809,
        "paths": {
            "saving_path": "data/hydraulic_cylinders/hc_suppliers_metadata.csv",
            "reference_url_path": "data/hydraulic_cylinders/hc_suppliers_urls.csv",
            "success_url_path": "data/hydraulic_cylinders/success_url.csv",
            "failed_url_path": "data/hydraulic_cylinders/failed_url.csv",
            "master_data_path": "data/hydraulic_cylinders/hc_master_data.csv",
            "cleaned_data_path": "data/hydraulic_cylinders/hc_clean_data.csv",
        },
    }
    cleaner = CleanThomas(config=config)
    cleaner.run()
