from bs4 import BeautifulSoup
import pandas as pd
import pickle as pkl
import numpy as np
from tqdm.autonotebook import tqdm
from multiprocessing import Pool
import time

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
			"sqfts": sqfts
		}

if __name__ == '__main__':
	with open('data.pkl', "rb") as fp:
		df = pkl.load(fp)

	with Pool(8) as p:
		rows = list(tqdm(p.imap(extract_fields, list(df['pages'])), total=len(df)))

	with open("tmp.pkl", "wb") as fp:
		pkl.dump(rows, fp)
	output_df = pd.DataFrame(list(zip(rows)))
	output_df.columns = []
	output_df.to_csv("data.csv")