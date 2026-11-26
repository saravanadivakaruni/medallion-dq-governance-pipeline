import pandas as pd

df = pd.read_excel("data/Online Retail.xlsx", dtype={"InvoiceNo": str, "StockCode": str})
ref = set(pd.read_csv("governance/ref_country.csv")["country_name"])

aliases = {"EIRE": "Ireland", "USA": "United States", "RSA": "South Africa"}
countries = df["Country"].dropna().replace(aliases)

not_in_ref = sorted(set(countries) - ref)
unused_in_ref = sorted(ref - set(countries))

print("In data but NOT in reference list:", not_in_ref)
print("In reference list but not in data:", unused_in_ref)
print("Rows that would fail the reference check:", (~countries.isin(ref)).sum())