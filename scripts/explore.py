import pandas as pd

# ---------- 1. LOAD ----------
# dtype=str keeps InvoiceNo and StockCode as text (some contain letters, e.g. C536379, 85123A)
df = pd.read_excel("data/Online Retail.xlsx", dtype={"InvoiceNo": str, "StockCode": str})

print("=== 1. SHAPE (rows, columns) ===")
print(df.shape)
print(df.dtypes)

# ---------- 2. MISSING VALUES ----------
print("\n=== 2. NULLS PER COLUMN ===")
nulls = df.isna().sum()
print(pd.DataFrame({"nulls": nulls, "pct_of_rows": (nulls / len(df) * 100).round(2)}))

# ---------- 3. EXACT DUPLICATES ----------
print("\n=== 3. EXACT DUPLICATE ROWS ===")
print(df.duplicated().sum())

# ---------- 4. QUANTITY ----------
is_cancel = df["InvoiceNo"].str.startswith("C", na=False)   # invoice numbers starting with C = cancellations
print("\n=== 4. QUANTITY ===")
print("rows with Quantity <= 0:", (df["Quantity"] <= 0).sum())
print("  of which cancellations (C invoices):", (is_cancel & (df["Quantity"] <= 0)).sum())
print("negative Quantity but NOT a cancellation:", ((df["Quantity"] < 0) & ~is_cancel).sum())
print("cancellation with positive Quantity:", (is_cancel & (df["Quantity"] > 0)).sum())

# ---------- 5. UNIT PRICE ----------
print("\n=== 5. UNIT PRICE ===")
print("UnitPrice == 0:", (df["UnitPrice"] == 0).sum())
print("UnitPrice < 0 :", (df["UnitPrice"] < 0).sum())
print(df[df["UnitPrice"] <= 0][["InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", "CustomerID"]].head(8))

# ---------- 6. COUNTRY ----------
print("\n=== 6. DISTINCT COUNTRIES ===")
print(sorted(df["Country"].dropna().unique()))

# ---------- 7. STOCK CODES ----------
pattern = r"^[0-9]{5}[A-Za-z]{0,2}$"        # 5 digits + up to 2 letters = a normal product code
odd = df[~df["StockCode"].str.match(pattern, na=False)]
print("\n=== 7. STOCK CODES NOT MATCHING THE PRODUCT PATTERN ===")
print("rows:", len(odd))
print(odd["StockCode"].value_counts().head(15))
print("lower-case letters in StockCode:", df["StockCode"].str.contains(r"[a-z]", na=False).sum())

# ---------- 8. DATES ----------
print("\n=== 8. INVOICE DATE RANGE ===")
print(df["InvoiceDate"].min(), "to", df["InvoiceDate"].max())

# ---------- 9. SAME KEY, NOT EXACT DUPLICATE ----------
no_exact = df.drop_duplicates()
same_key = no_exact.duplicated(subset=["InvoiceNo", "StockCode"], keep=False).sum()
print("\n=== 9. ROWS SHARING (InvoiceNo, StockCode) AFTER REMOVING EXACT DUPLICATES ===")
print(same_key)

# ---------- 10. TEXT TIDINESS ----------
desc = df["Description"].dropna()
print("\n=== 10. DESCRIPTIONS WITH EXTRA SPACES AT START/END ===")
print((desc != desc.str.strip()).sum())

# ---------- 11. EXAMPLES OF STRAY SPACES ----------
bad = desc[desc != desc.str.strip()]
print("\n=== 11. EXAMPLES (repr shows hidden spaces) ===")
print(bad.head(5).map(repr).tolist())