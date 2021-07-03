import argparse
import sys
import os

sys.path.append(os.path.join(os.getcwd(), "src", "thomasnet"))

try:
    from thomasnet.metadata_scraper.metascraper import ThomasnetMetaDataScraper
    from thomasnet.metadata_scraper.fastmetascraper import ThomasnetFastMetaDataScraper
    from thomasnet.scraper.thomasnet import ThomasnetScraper
    from thomasnet.scraper.fastthomasnet import FastThomasnetScraper
    from thomasnet.cleaner.clean_thomas import CleanThomas
except ImportError as e:
    print(e)


def create_path_config(keyword: str, heading: int):
    keyword = keyword.lower()
    headdir = keyword.replace(" ", "_")
    abbr = "".join([i[0] for i in keyword.split("_")])
    config = {
        "keyword": keyword,
        "heading": heading,
        "paths": {
            "saving_path": f"data/{headdir}/{abbr}_suppliers_metadata.csv",
            "reference_url_path": f"data/{headdir}/{abbr}_suppliers_urls.csv",
            "success_url_path": f"data/{headdir}/success_url.csv",
            "failed_url_path": f"data/{headdir}/failed_url.csv",
            "master_data_path": f"data/{headdir}/{abbr}_master_data.csv",
            "cleaned_data_path": f"data/{headdir}/{abbr}_clean_data.csv",
        },
    }
    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Thomasnet Data Scraper",
        description="Scrape Suppliers Data from Thomasnet website",
    )
    parser.add_argument("--keyword", help="Product Name", type=str, required=True)
    parser.add_argument(
        "--heading", help="Heading for product from website", type=int, required=True
    )
    args = parser.parse_args()

    print(args.keyword, args.heading)
