import pandas as pd

df = pd.read_excel("data/Online Retail.xlsx", dtype={"InvoiceNo": str, "StockCode": str})
df["CustomerID"] = df["CustomerID"].astype("Int64")   # whole numbers, avoids '17850.0'
df.to_csv("data/online_retail.csv", index=False)
print("rows written:", len(df))