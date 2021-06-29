import os, re, time
import traceback

import requests
from bs4 import BeautifulSoup

import math
import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore')

from multiprocessing import Pool


class ThomasnetMetaDataScraper:
    def __init__(self, **kwargs):
        self.config = kwargs.get('config')
        self.BASE_URL = "https://www.thomasnet.com/nsearch.html"
        self.base_payload = {
            "cov": "NA",
            "heading": self.config['heading'],
            "searchsource": "suppliers",
            "searchterm": self.config['heading'],
            "what": self.config['heading'],
            "pg": 1
        }
        self.collected_data = []


    def find_num_pages(self,payload):
        page = requests.get(self.BASE_URL, params=payload)
        soup = BeautifulSoup(page.text,"lxml")

        total_suppliers = soup.find('p',class_="supplier-search-results__subheader").findAll('b')[-1].text
        print(f"{total_suppliers} suppliers found")

        n_suppliers = len(soup.findAll('div',class_="supplier-search-results__card"))
        print(f"{n_suppliers} found on this page")

        number_of_pages = math.ceil(float(total_suppliers)/float(n_suppliers))
        print(f"Total Pages: {number_of_pages}")


    def generate_payload(self, page_num: int, keyword: str):
        payloads = []
        for i in range(page_num):
            payload = {
                "cov": "NA",
                "heading": self.config['heading'],
                "searchsource": "suppliers",
                "searchterm": keyword,
                "what": keyword,
                "pg": i+1
            }
            payloads.append(payload)
        return payloads


    @staticmethod
    def extract_data(payload):
        BASE_URL = "https://www.thomasnet.com/nsearch.html"
        def get_html(params):
            passed = False
            retry = 0
            while not passed:
                try:
                    page = BeautifulSoup(requests.get(BASE_URL,params=params).text,'lxml')
                    passed = True
                    return page
                except Exception as e:
                    retry = retry + 1
                    print(f'Retrying {retry}/5 times...')
                    if retry==5:
                        break
                    else:
                        time.sleep(5)
                        pass
        try:
            soup = get_html(payload)
            suppliers_list = []
            suppliers = soup.findAll('div',class_="supplier-search-results__card")
            for sup in suppliers:
                card_data = {
                    "company_id":"",
                    "company_name":"",
                    "company_type":"",
                    "annual_revenue":"",
                    "year_founded":"",
                    "num_employees":"",
                    "location":"",
                    "company_url":"",
                    "brands":"",
                    "description":"",
                    "url":"",
                    "telephone":"",
                    "searchterm": payload['searchterm']
                }
                result = {"page_data": [card_data], "success": False}
                try:
                    header = sup.find('header',class_='profile-card__header')
                    card_data['company_id'] = eval(sup.get('data-impression-tracking'))['company_id']
                    card_data['company_name'] = header.find('h2',class_='profile-card__title').text.strip()
                    card_data['url'] = 'https://www.thomasnet.com'+header.find('h2',class_='profile-card__title').find('a').get('href')
                    card_data['telephone'] = sup.find('a',{'data-conversion_action':'Call'}).get('href')

                    sup_data = sup.find('div',class_='profile-card__supplier-data')
                    card_data['location'] = sup_data.find('span',class_='profile-card__location').text.replace('ico-map','').strip()
                    card_data['company_type'] = sup_data.find('span',{'data-content':'Company Type'}).text.strip()
                    try:
                        card_data['annual_revenue'] = sup_data.find('span',{'data-content':'Annual Revenue'}).text.strip()
                    except:
                        pass
                    try:
                        card_data['num_employees'] = sup_data.find('span',{'data-content':'Number of Employees'}).text.strip()
                    except:
                        pass
                    try:
                        card_data['year_founded'] = sup_data.find('span',{'data-content':'Year Founded'}).text.strip()
                    except:
                        pass

                    content = sup.find('div',class_='profile-card__content')
                    card_data['description'] = content.findAll('p')[0].text.strip()
                    card_data['company_url'] = content.find('p',{'class':'profile-card_web-link-wrap'}).find('a').get('href')
                    try:
                        card_data['brands'] = content.find('p',{'class':'profile-card__brands__body'}).text.strip()
                    except:
                        pass
                    suppliers_list.append(card_data)
                except Exception as e:
                    print("Error encountered while extraction of data\n", traceback.print_exc())
                    pass

            result = {"page_data": suppliers_list, "success": True}
        except Exception as e:
            print(f"Error encountered scraping page {payload['pg']}:\n{e}")
        finally:
            return result


    def save_data(self):
        self.metadata = pd.DataFrame(self.collected_data)


    def run(self):
        num_pages = self.find_num_pages(self.base_payload)
        list_of_payloads = self.generate_payload(self.config['keyword'],num_pages)
        try:
            pool = Pool(processes=15)
            final_result = pool.map(self.extract_data, list_of_payloads)
            for result in final_result:
                if result["success"]:
                    self.collected_data.extend(result["page_data"])
        except Exception as e:
            print(f"Error occurred. Closing scraping process.\n{str(e)}", traceback.print_exc())
        finally:
            self.save_data()


if __name__ =='__main__':
    config = {
        "keyword": "hydraulic cylinders",
        "heading": 21650809,
        "saving_path": "data/hydraulic_cylinders/metadata.csv"
    }
    scraper = ThomasnetMetaDataScraper(
        config=config
    )
    scraper.run()