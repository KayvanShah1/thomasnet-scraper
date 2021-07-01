import time
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
        self.master_df = None

        self.collected_data = []

    def get_scraping_list_df(self):
        scraping_list = list(
            set([tuple(row) for row in self.ref_urls_df.values.tolist()])
            .difference(set([tuple(row) for row in self.failed_urls.values.tolist()]))
            .difference(set([tuple(row) for row in self.success_urls.values.tolist()]))
        )
        return pd.DataFrame(scraping_list, columns=self.data_columns)

    def add_to_collected_data(self, page_data):
        if type(page_data) == dict:
            self.collected_data.append(page_data)
        elif type(page_data) == list:
            self.collected_data.extend(page_data)

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
                    print(f"Error requesting URL:{url}")
                    break
                else:
                    time.sleep(5)
                    pass

    def extract_data(self, row):
        page_data = {
            "company_id": row[0],
            "company_name": "",
            "company_type": "",
            "company_url": "",
            "location": "",
            "telephone": "",
            "product_description": "",
            "about_company": "",
            "product_types": "",
            "all_product_services": "",
            "link_prod_services": "",
            "brands": "",
            "link_all_brands": "",
            "primary_company_type": "",
            "additional_activities": "",
            "key_personnel": "",
            "social_media": "",
            "other_locations": "",
            "annual_revenue": "",
            "year_founded": "",
            "num_employees": "",
            "url": row[1],
        }
        try:
            page = requests.get(row[1]).text
            soup = BeautifulSoup(page, "lxml")
            gen_info = soup.find("div", {"class": "copro_naft"})
            try:
                page_data["company_name"] = (
                    gen_info.find("div", {"class": "codetail"})
                    .find("h1")
                    .find("a")
                    .text.strip()
                )
            except Exception as e:
                pass
            try:
                page_data["company_url"] = (
                    gen_info.find("div", {"class": "codetail"})
                    .find("h1")
                    .find("a")
                    .get("href")
                )
            except Exception as e:
                pass
            try:
                page_data["company_type"] = (
                    gen_info.find("div", {"class": "codetail"})
                    .find("p")
                    .findAll("span")[2]
                    .text.strip()
                )
            except Exception as e:
                pass
            try:
                page_data["location"] = (
                    gen_info.find("p", {"class": "addrline"}).text.split("|")[0].strip()
                )
            except Exception as e:
                pass
            try:
                page_data["telephone"] = gen_info.find(
                    "a", {"data-conversion_action": "Call"}
                ).get("href")
            except Exception as e:
                pass

            ################# Business Description ####################
            business_desc = soup.find("div", {"id": "copro_description"})
            try:
                page_data["product_description"] = business_desc.find(
                    "div", {"id": "copro_pdm"}
                ).text.strip()
            except Exception as e:
                pass
            try:
                page_data["about_company"] = business_desc.find(
                    "div", {"id": "copro_about"}
                ).text.strip()
            except Exception as e:
                pass

            ################# Products and Services/Brands ####################
            prod_serv = soup.find("div", {"id": "copro_prodserv"})
            try:
                prod_cats = prod_serv.find("div", {"id": "copro_prodserv_cats"})
                prod_types = prod_cats.findAll("div", {"class": "prodserv_group"})[0]
                page_data["product_types"] = [
                    i.text.strip() for i in prod_types.findAll("li")
                ]
                all_prodserv = prod_cats.findAll("div", {"class": "prodserv_group"})[1]
                page_data["all_product_services"] = [
                    i.text.strip() for i in all_prodserv.findAll("li")
                ]
                page_data[
                    "link_prod_services"
                ] = "https://www.thomasnet.com" + all_prodserv.find("a").get("href")
            except Exception as e:
                pass

            try:
                prod_brands = prod_serv.find("div", {"id": "copro_prodserv_brands"})
                brands = prod_brands.find("div", {"class": "prodserv_group"})
                page_data["brands"] = [i.text.strip() for i in brands.findAll("li")]
                page_data[
                    "link_all_brands"
                ] = "https://www.thomasnet.com" + brands.find("a").get("href")
            except Exception as e:
                pass

            ################# Business Details ####################
            bus_det = soup.find("div", {"id": "copro_bizdetails"})
            col1 = bus_det.find("div", {"class": "bdcol1"})
            col1_ = col1.findAll("div", {"class": "bizdetail"})
            for div in col1_:
                label = div.find("div", {"class": "label"}).text.strip()
                if label == "Primary Company Type:":
                    page_data["primary_company_type"] = div.find("li").text.strip()
                if label == "Additional Activities:":
                    page_data["additional_activities"] = div.find("li").text.strip()
                if label == "Key Personnel:":
                    page_data["key_personnel"] = [
                        i.text.strip() for i in div.findAll("li")
                    ]
                if label == "Locations:":
                    page_data[
                        "other_locations"
                    ] = "https://www.thomasnet.com" + div.find("a").get("href")
                if label == "Social:":
                    page_data["social_media"] = [
                        i.get("href") for i in div.findAll("a")
                    ]

            col2 = bus_det.find("div", {"class": "bdcol2"})
            col22_ = col2.findAll("div", {"class": "bizdetail"})
            for div in col22_:
                if div.find("div", {"class": "label"}):
                    label = div.find("div", {"class": "label"}).text.strip()
                    if label == "Annual Sales:":
                        page_data["annual_revenue"] = div.find("li").text.strip()
                    if label == "No of Employees:":
                        page_data["num_employees"] = div.find("li").text.strip()
                    if label == "Year Founded:":
                        page_data["year_founded"] = div.find("li").text.strip()

            ################# Consolidation of Data ####################
            self.add_to_collected_data(page_data)
            self.add_url_to_success_list(row[0], row[1])
        except Exception as e:
            self.add_url_to_failed_list(row[0], row[1])
            print(
                f"Error scraping page {row[1]}\n{e}",
            )

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
            pass

        try:
            self.failed_urls = pd.read_csv(failed_urls_path)
            print(f"Total failed urls: {self.failed_urls.shape[0]}")
        except FileNotFoundError as e:
            print("Failed URLs file not found:\n", e)
            pass

        self.scraping_list_df = self.get_scraping_list_df()
        print(f"Total urls to be tried: {self.scraping_list_df.shape[0]}")

    def save_data(
        self,
        master_data_path: str = "",
        success_urls_path: str = "",
        failed_urls_path: str = "",
    ):
        try:
            if self.master_df == None:
                self.master_df = pd.DataFrame(self.collected_data)
            else:
                self.master_df = self.master_df.append(
                    pd.DataFrame(self.collected_data), ignore_index=True
                )
            self.master_df.drop_duplicates(
                subset=self.data_columns[0], keep="last", inplace=True
            )
            self.master_df.to_csv(master_data_path, index=False)
        except Exception as e:
            print(f"Error saving data: {e}")

        try:
            self.success_urls.to_csv(success_urls_path, index=False)
            self.failed_urls.to_csv(failed_urls_path, index=False)
        except Exception as e:
            print(f"Error logging URLs: {e}")

    def run(self):
        try:
            self.load_data(
                reference_urls_path=self.config["paths"]["reference_url_path"],
                master_data_path=self.config["paths"]["master_data_path"],
                success_urls_path=self.config["paths"]["success_url_path"],
                failed_urls_path=self.config["paths"]["failed_url_path"],
            )

            for _, row in tqdm(
                self.scraping_list_df.iterrows(), total=self.scraping_list_df.shape[0]
            ):
                self.extract_data(row)

        except Exception as e:
            print(
                traceback.print_exc(),
                f"\nError occurred. Killing the scraping process:\n{e}",
            )
        finally:
            self.save_data(
                master_data_path=self.config["paths"]["master_data_path"],
                success_urls_path=self.config["paths"]["success_url_path"],
                failed_urls_path=self.config["paths"]["failed_url_path"],
            )


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
    scraper = ThomasnetScraper(config=config)
    scraper.run()
