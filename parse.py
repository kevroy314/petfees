import os
import pandas as pd
import pickle as pkl
from tqdm.autonotebook import tqdm

data_dir = './data'
files = os.listdir(data_dir)
pages = []
urls = []
for file in tqdm(files):
	with open(os.path.join(data_dir, file), "rb") as fp:
		pages.append(fp.read())
	urls.append(file.replace('.', '/').replace('http/', 'http:'))
df = pd.DataFrame(list(zip(urls, pages)))
df.columns = ["urls", "pages"]

with open("data.pkl", "wb") as fp:
	pkl.dump(df, fp)