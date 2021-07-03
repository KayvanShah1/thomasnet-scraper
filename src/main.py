import argparse
import sys
import os

# sys.path.append(os.path.join(os.getcwd(), "src", "thomasnet"))
sys.path.append("../src/thomasnet")

try:
    from thomasnet.metadata_scraper.metascraper import ThomasnetMetaDataScraper
    from thomasnet.metadata_scraper.fastmetascraper import ThomasnetFastMetaDataScraper
    from thomasnet.scraper.thomasnet import ThomasnetScraper
    from thomasnet.scraper.fastthomasnet import FastThomasnetScraper
    from thomasnet.cleaner.clean_thomas import CleanThomas
except ImportError as e:
    print(e)


class ThomasConfig:
    def __init__(self, **kwargs):
        self.heading: int = kwargs.get("heading")
        self.keyword: str = kwargs.get("keyword").lower()
        self.headdir = self.keyword.replace(" ", "_")
        self.abbr = "".join([i[0] for i in self.headdir.split("_")])

    def __json__(self):
        config = {
            "keyword": self.keyword,
            "heading": self.heading,
            "paths": {
                "saving_path": f"data/{self.headdir}/{self.abbr}_suppliers_metadata.csv",
                "reference_url_path": f"data/{self.headdir}/{self.abbr}_suppliers_urls.csv",
                "success_url_path": f"data/{self.headdir}/success_url.csv",
                "failed_url_path": f"data/{self.headdir}/failed_url.csv",
                "master_data_path": f"data/{self.headdir}/{self.abbr}_master_data.csv",
                "cleaned_data_path": f"data/{self.headdir}/{self.abbr}_clean_data.csv",
            },
        }
        return config


class Thomas(ThomasConfig):
    def __init__(self, **kwargs):
        super().__init__(keyword=kwargs.get("keyword"), heading=kwargs.get("heading"))
        self.config = self.__json__()

    def run(self):
        ThomasnetMetaDataScraper(config=self.config)
        ThomasnetScraper(config=self.config)
        CleanThomas(config=self.config)


class FastThomas(ThomasConfig):
    def __init__(self, **kwargs):
        super().__init__(keyword=kwargs.get("keyword"), heading=kwargs.get("heading"))
        self.config = self.__json__()

    def run(self):
        ThomasnetFastMetaDataScraper(config=self.config)
        FastThomasnetScraper(config=self.config)
        CleanThomas(config=self.config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Thomasnet Data Scraper",
        description="Scrape Suppliers Data from Thomasnet website",
    )
    parser.add_argument(
        "-k", "--keyword", help="Product Name to search", type=str, required=True
    )
    parser.add_argument(
        "-hd",
        "--heading",
        help="Heading for the product from website",
        type=int,
        required=True,
    )
    parser.add_argument(
        "-f",
        "--fast",
        action="store_true",
        help="Fast Scraping",
    )
    args = parser.parse_args()
    if args.fast:
        scraper = FastThomas(keyword=args.keyword, heading=args.heading)
    else:
        scraper = Thomas(keyword=args.keyword, heading=args.heading)
    scraper.run()
