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

    def load_data(self):
        pass

    def save_data(self):
        pass

    def clean_data(self):
        pass

    def run(self):
        try:
            self.load_data()
            self.clean_data()
        except Exception as e:
            print(
                f"Error encountered while running the cleaner", traceback.format_exc()
            )
        finally:
            self.save_data()


if __name__ == "__main__":
    config = {
        "keyword": "hydraulic cylinders",
        "heading": 21650809,
        "paths": {
            "saving_path": "data/hydraulic_cylinders/hydraulic_cylinders_suppliers_metadata.csv",
            "reference_url_path": "data/hydraulic_cylinders/hydraulic_cylinders_suppliers_urls.csv",
            "success_url_path": "data/hydraulic_cylinders/success_url.csv",
            "failed_url_path": "data/hydraulic_cylinders/failed_url.csv",
            "master_data_path": "data/hydraulic_cylinders/hydraulic_cylinders_master_data.csv",
            "cleaned_data_path": "data/hydraulic_cylinders/hydraulic_cylinders_clean_data.csv",
        },
    }
    cleaner = CleanThomas(config=config)
    cleaner.run()
