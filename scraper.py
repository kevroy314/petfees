import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm.autonotebook import tqdm
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

hrefs_cache_file = "hrefs.csv"

try:
    logging.info(f"Attempting to load cache file {hrefs_cache_file}")
    hrefs_df = pd.read_csv(hrefs_cache_file)
except:
    logging.info(f"Failed to load cache file")
    base_url = "https://www.apartments.com/houston-tx/pet-friendly/"
    logging.info(f"Requesting URL Base {base_url}")
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',}
    r = requests.get(base_url, headers=headers)
    c = r.content

    logging.info("Parsing URL Base")
    soup = BeautifulSoup(c, "html.parser")

    logging.info("Extracting pages")
    n_pages = int(soup.find("span",{"class", "pageRange"}).text.split()[-1])
    pages = list(range(n_pages+1))[2:]
    logging.info(f"Found {len(pages)} pages")
    links = soup.find_all("a", {"class": "property-link"})
    hrefs = list(set([l['href'] for l in links]))
    all_hrefs = hrefs

    for page_number in tqdm(pages):
        r = requests.get(base_url+str(page_number)+"/", headers=headers, timeout=1)
        c = r.content
        soup = BeautifulSoup(c,"html.parser")
        links = soup.find_all("a", {"class": "property-link"})
        hrefs = list(set([l['href'] for l in links]))
        all_hrefs.extend(hrefs)
        
    hrefs_df = pd.DataFrame(list(set(all_hrefs)))
    hrefs_df.columns = ["href"]
    if hrefs_cache_file:
        hrefs_df.to_csv(hrefs_cache_file)
    
logging.info(f"Found {len(hrefs_df)} pages")
print(hrefs_df.columns)
pages = []
for url in tqdm(list(hrefs_df['href'])):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',}
    r = requests.get(url, headers=headers, timeout=5)
    c = r.content
    try:
        outfile = "./data/"+url.replace('/', '.').replace(':', '.')
        logging.info(f"Writing to {outfile}")
        with open(outfile, "wb") as fp:
            fp.write(c)
    except:
        pass