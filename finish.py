import pickle as pkl
import pandas as pd

with open("tmp.pkl", "rb") as fp:
  tmp = pkl.load(fp)
  df = pd.DataFrame(tmp)

sqfts = df['sqfts'].apply(pd.Series)
sqfts.columns = [str(int(s)+1) + " BR SQFT" for s in sqfts.columns]
df = pd.concat([df, sqfts], axis=1).drop(columns='sqfts')
recurring_fees = df['recurring_fees'].apply(pd.Series)
recurring_fees.columns = [s + " Recurring" for s in recurring_fees.columns]
df = pd.concat([df, recurring_fees], axis=1).drop(columns="recurring_fees")
onetime_fees = df['onetime_fees'].apply(pd.Series)
onetime_fees.columns = [s + " OneTime" for s in recurring_fees.columns]
df = pd.concat([df, onetime_fees], axis=1).drop(columns="onetime_fees")
df.to_csv("data.csv")