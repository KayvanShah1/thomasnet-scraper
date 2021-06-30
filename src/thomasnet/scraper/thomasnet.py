import time
import os
import traceback
from tqdm import tqdm

import requests
from bs4 import BeautifulSoup

import pandas as pd
import warnings

warnings.simplefilter(action="ignore")


class ThomasnetScraper:
    def __init__(self, **kwargs):
        self.config = kwargs.get("config")
        self.data_columns = ["company_id", "url"]

        self.ref_urls_df = pd.DataFrame(columns=self.data_columns)
        self.success_urls = pd.DataFrame(columns=self.data_columns)
        self.failed_urls = pd.DataFrame(columns=self.data_columns)

        self.collected_data = []

    def get_scraping_list_df(self):
        scraping_list = list(
            set([tuple(row) for row in self.ref_urls_df.values.tolist()])
            .difference(set([tuple(row) for row in self.failed_urls.values.tolist()]))
            .difference(set([tuple(row) for row in self.success_urls.values.tolist()]))
        )
        return pd.DataFrame(scraping_list, columns=self.data_columns)

    def add_to_collected_data(self, page_data):
        self.collected_data.append(page_data)

    def add_url_to_success_list(self, company_id, url: str):
        self.success_urls = self.success_urls.append(
            {"company_id": company_id, "url": url}, ignore_index=True
        )

    def add_url_to_failed_list(self, company_id, url: str):
        self.failed_urls = self.failed_urls.append(
            {"company_id": company_id, "url": url}, ignore_index=True
        )

    def get_response(self, url: str):
        passed = False
        retry = 0
        while not passed:
            try:
                page = BeautifulSoup(requests.get(url).text, "lxml")
                passed = True
                return page
            except Exception as e:
                retry = retry + 1
                print(f"Retrying {retry}/5 times...")
                if retry == 5:
                    break
                else:
                    time.sleep(5)
                    pass

    def extract_data(self):
        pass

    def load_data(
        self,
        reference_urls_path: str = "",
        master_data_path: str = "",
        success_urls_path: str = "",
        failed_urls_path: str = "",
    ):
        try:
            self.master_df = pd.read_csv(master_data_path)
        except FileNotFoundError as e:
            print("Master file not found:\n", e)

        try:
            self.ref_urls_df = pd.read_csv(reference_urls_path)
            print(f"Total urls: {self.ref_urls_df.shape[0]}")
        except FileNotFoundError as e:
            print("Reference URLs file not found:\n", e)

        try:
            self.success_urls = pd.read_csv(success_urls_path)
            print(f"Total successful urls: {self.success_urls.shape[0]}")
        except FileNotFoundError as e:
            print("Success URLs file not found:\n", e)

        try:
            self.failed_urls = pd.read_csv(failed_urls_path)
            print(f"Total failed urls: {self.failed_urls.shape[0]}")
        except FileNotFoundError as e:
            print("Failed URLs file not found:\n", e)

        self.scraping_list_df = self.get_scraping_list_df()
        print(f"Total urls to be tried: {self.scraping_list_df.shape[0]}")

    def save_data(
        self,
        master_data_path: str = "",
        success_urls_path: str = "",
        failed_urls_path: str = "",
    ):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    config = {
        "keyword": "hydraulic cylinders",
        "heading": 21650809,
        "paths": {
            "saving_path": "data/hydraulic_cylinders/hydraulic_cylinders_suppliers_metadata.csv",
            "reference_url_path": "data/hydraulic_cylinders/hydraulic_cylinders_suppliers_urls.csv",
            "sucess_url_path": "data/hydraulic_cylinders/sucess_url.csv",
            "failed_url_path": "data/hydraulic_cylinders/failed_url.csv",
            "master_data_path": "data/hydraulic_cylinders/hydraulic_cylinders_master_data.csv",
            "cleaned_data_path": "data/hydraulic_cylinders/hydraulic_cylinders_clean_data.csv",
        },
    }
    scraper = ThomasnetScraper(config=config)
    scraper.run()
