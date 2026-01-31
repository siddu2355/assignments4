import pandas as pd
import re

df = pd.read_excel("cbic_hsn_gst_rates.xlsx")

mask = df.iloc[:, 0].astype(str).apply(lambda x: bool(re.fullmatch(r'\d+', x)))

valid_rows = df[mask]
invalid_rows = df[~mask]

valid_rows.to_excel("valid_rows.xlsx", index=False)
invalid_rows.to_excel("invalid_rows.xlsx", index=False)

print("✅ Done! Two files created: valid_rows.xlsx & invalid_rows.xlsx")
