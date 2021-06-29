import os, re, time
import traceback

import requests
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore')

from multiprocessing import Pool


class ThomasnetMetaDataScraper:
    def __init__(self, **kwargs):
        self.config = kwargs.get('config')
        self.collected_data = []
        self.list_of_payloads = []
        self.BASE_URL = "https://www.thomasnet.com/nsearch.html"

    def generate_payload(self):
        pass

    @staticmethod
    def extract_data(payload):
        pass

    def save_data(self):
        try:
            pool = Pool(processes=25)
            final_result = pool.map(self.extract_data, self.list_of_payloads)
            for result in final_result:
                if result["success"]:
                    self.collected_data.extend(result["page_data"])
        except Exception as e:
            print(f"Error occurred. Closing scraping process.\n{str(e)}", traceback.print_exc())
        finally:
            self.save_data()

    def run(self):
        pass


if __name__ =='__main__':
    scraper = ThomasnetMetaDataScraper()