import os
import time
import logging
import pickle as pkl
from multiprocessing import Pool

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


def extract_fields(html):
	soup = BeautifulSoup(html, "html.parser")
	name = soup.find("h1", {"class": "propertyName"}).text.strip()
	address = soup.find("div", {"class": "propertyAddress"}).text.strip().replace('\r\n', '').replace('\n\n', '').replace('  ', '').replace('\n', ' ')
	ranges = soup.find_all("span", {"class": "rentRollup"})
	parsed_ranges = {}
	for r in ranges:
		label = r.text.split("$")[0].strip().split('\n')[0].strip().split('\t')[0]
		try:
			low = int(r.text.split("$")[1].split(' ')[0].strip().replace(',', ''))
		except:
			low = np.nan
		try:
			high = int(r.text.split("$")[1].split(' ')[2].strip().replace(',', ''))
		except:
			high = low
		parsed_ranges[label] = (low, high)
	try:
		rating = float(soup.find("p", {"class": "reviewDetails"}).text.split(' ')[0])
		reviews = int(soup.find("a", {"class": "reviewLink"}).text.strip().split()[2])
	except:
		rating = np.nan
		reviews = 0
	
	fees = soup.find("section", {"id", "feesSection"})
	recurring_fees_parsed = {}
	onetime_fees_parsed = {}
	try:
		recurring = fees.find("div", {"class", "monthlyFees"})
		recurring_fees = recurring.find_all("div", {"class", "descriptionWrapper"})
		
		for fee in recurring_fees:
			label, r = [s.text for s in fee.find_all("span")]
			if ' - ' in r:
				r = (float(r.replace('$', '').split(' ')[0]), float(r.replace('$', '').split(' ')[2]))
			else:
				r = float(r.replace('$', ''))
				r = (r, r)
			recurring_fees_parsed[label] = r
	except:
		pass
	try:
		onetime = fees.find("div", {"class", "oneTimeFees"})
		onetime_fees = onetime.find_all("div", {"class", "descriptionWrapper"})	
		for fee in onetime_fees:
			label, r = [s.text for s in fee.find_all("span")]
			if ' - ' in r:
				r = (float(r.replace('$', '').split(' ')[0]), float(r.replace('$', '').split(' ')[2]))
			else:
				r = float(r.replace('$', ''))
				r = (r, r)
			onetime_fees_parsed[label] = r
	except:
		pass
	df = pd.read_html(html)
	sqfts = []
	try:
		for i in range(5):
			sqft = np.mean(df[0][df[0]['Beds'].str.contains(str(i+1))]['Sq Ft'].apply(lambda s: int(s.replace(',', '').split(' ')[0])))
			sqfts.append(sqft)
	except:
		sqfts = [np.nan for i in range(5)]

	return {"name": name,
			"address": address,
			"rating": rating,
			"reviews": reviews,
			"price_ranges": parsed_ranges,
			"recurring_fees": recurring_fees_parsed,
			"onetime_fees": onetime_fees_parsed,
			"sqfts": sqfts}


def crawl(base_url, hrefs_cache_file=None, data_dir='./data/', output_filename=None, query_timeout=10):
	try:
		os.makedirs(data_dir)
	except:
		pass
	try:
		logging.info(f"Attempting to load cache file {hrefs_cache_file}")
		hrefs_df = pd.read_csv(hrefs_cache_file)
	except:
		logging.info(f"Failed to load cache file")
		logging.info(f"Requesting URL Base {base_url}")
		headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',}
		r = requests.get(base_url, headers=headers, timeout=query_timeout)
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
			r = requests.get(base_url+str(page_number)+"/", headers=headers, timeout=query_timeout)
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
	logging.info(f"Saving page cache to {data_dir}")
	pages = []
	for url in tqdm(list(hrefs_df['href'])):
		headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',}
		r = requests.get(url, headers=headers, timeout=query_timeout)
		c = r.content
		try:
			outfile = data_dir+url.replace('/', '.').replace(':', '.')
			logging.info(f"Writing to {outfile}")
			with open(outfile, "wb") as fp:
				fp.write(c)
		except:
			pass
	
	files = os.listdir(data_dir)
	logging.info(f"Beginning read of page cache ({len(files)} files)")
	pages = []
	urls = []
	for file in tqdm(files):
		with open(os.path.join(data_dir, file), "rb") as fp:
			pages.append(fp.read())
		urls.append(file.replace('.', '/').replace('http/', 'http:'))
	df = pd.DataFrame(list(zip(urls, pages)))
	df.columns = ["urls", "pages"]

	logging.info("Parsing page cache")
	with Pool(8) as p:
		rows = list(tqdm(p.imap(extract_fields, list(df['pages'])), total=len(df)))

	logging.info("Done parsing, constructing dataframe")
	df = pd.DataFrame(rows)

	logging.info("Applying transformations to dataframe to finalize cleaned data set")
	sqfts = df['sqfts'].apply(pd.Series)
	sqfts.columns = [str(int(s)+1) + " BR SQFT" for s in sqfts.columns]
	df = pd.concat([df, sqfts], axis=1).drop(columns='sqfts')
	recurring_fees = df['recurring_fees'].apply(pd.Series)
	recurring_fees.columns = [s + " Recurring" for s in recurring_fees.columns]
	df = pd.concat([df, recurring_fees], axis=1).drop(columns="recurring_fees")
	onetime_fees = df['onetime_fees'].apply(pd.Series)
	onetime_fees.columns = [s + " OneTime" for s in onetime_fees.columns]
	df = pd.concat([df, onetime_fees], axis=1).drop(columns="onetime_fees")
	if output_filename:
		logging.info("Saving")
		df.to_csv(output_filename)
	logging.info("Done crawling base url = {base_url}")
	return df


if __name__ == "__main__":
	skip_complete = True
	top_n = 20
	state = "tx"
	state_population_wikipedia = "https://en.wikipedia.org/wiki/List_of_cities_in_Texas_by_population"
	state_population_wikipedia_table_number = 1
	df = pd.read_html(state_population_wikipedia)
	places = [s.lower().replace(" ", "-") for s in list(df[state_population_wikipedia_table_number]['Place name'][:top_n])]
	root_urls = [f"https://apartments.com/{place}-{state}/pet-friendly/" for place in places]
	dfs = []
	for place, url in zip(places, root_urls):
		if skip_complete and os.path.exists(f"{place}.csv"):
			logging.info(f"Skipping crawl for {place} at url {url} as output file already exists")
			continue
		logging.info(f"Starting crawl for {place} at url {url}")
		dfs.append(crawl(url, output_filename=f"{place}.csv", data_dir=f"./{place}/"))
		logging.info(f"Finished crawling {place} at url {url}")