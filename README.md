# Thomasnet Hardware Suppliers Data Scraper

## Clone the repository
```bash
git clone https://github.com/KayvanShah1/thomasnet-scraper.git
```
Enter into the repository's root directory
```bash
cd thomasnet-scraper
```
Create a virtual environment and install the dependencies
```bash
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Visit the source
<div align='center'>
  <img src='https://cdn40.thomasnet.com/img40/og-thomas-for-industry.jpg'></img>
</div>

 - [Link to the source](https://www.thomasnet.com/)
 - Find the heading for the product (to be passed as an argument below to run the script)
    - Locate the heading in the URL after searching for the interested product
    ```
    https://www.thomasnet.com/nsearch.html?cov=NA&heading=21650809&searchsource=suppliers&searchterm=Hydraulic+Cylinders&searchx=true&what=Hydraulic+Cylinders&which=prod
    ```
    - Here the heading is `21650809`

## Getting started
### Help
```
/thomasnet-scraper> py src/main.py -h                             
usage: Thomasnet Data Scraper [-h] -k KEYWORD -hd HEADING [-f]

Scrape Suppliers Data from Thomas website

optional arguments:
  -h, --help            show this help message and exit
  -k KEYWORD, --keyword KEYWORD
                        Product Name to search
  -hd HEADING, --heading HEADING
                        Heading for the product from website
  -f, --fast            Fast Scraping
```
### Example
```bash
py src/main.py -k "hydraulic cylinder" -h 21650809
```

### Note
- Find the data exported in *CSV files* in **data** folder

### Any problems or bugs to be reported
- Create an issue [here](https://github.com/KayvanShah1/thomasnet-scraper/issues)

## Documentation
- For more details about the scraping process read the [WIKI documentation](https://github.com/KayvanShah1/thomasnet-scraper/wiki)
