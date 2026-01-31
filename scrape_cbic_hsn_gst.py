import pandas as pd

URL = "https://cbic-gst.gov.in/gst-goods-services-rates.html"
OUTPUT_FILE = "cbic_hsn_gst_rates.xlsx"

def scrape_cbic_hsn_to_gst(url=URL, out_file=OUTPUT_FILE):
    print(f"Fetching tables from {url} ...")
    tables = pd.read_html(url)  # returns list of DataFrames
    print(f"Found {len(tables)} tables.")

    extracted = []

    for i, df in enumerate(tables):
        cols = [c.strip().lower() for c in df.columns.astype(str)]
        # look for required columns
        if any("chapter" in c for c in cols) and any("igst" in c for c in cols):
            print(f"Using table {i} with columns: {df.columns.tolist()}")
            sub = df[[
                next(c for c in df.columns if "chapter" in c.lower()),
                next(c for c in df.columns if "igst" in c.lower())
            ]]
            sub.columns = ["HSN", "IGST"]
            extracted.append(sub)

    if not extracted:
        raise RuntimeError("No matching tables with HSN and IGST columns were found.")

    final_df = pd.concat(extracted, ignore_index=True)
    final_df.to_excel(out_file, index=False)
    print(f"Saved {len(final_df)} rows to {out_file}")

if __name__ == "__main__":
    scrape_cbic_hsn_to_gst()
