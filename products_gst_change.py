import re
import pymongo
import pandas as pd

clients = {
    "prod": pymongo.MongoClient(
        "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
    ),
    "test": pymongo.MongoClient(
        "mongodb+srv://careeco_system:IojrVNu8P2TiGsJd@cluster0.uoqzz4p.mongodb.net/"
    ),
    "local": pymongo.MongoClient("mongodb://localhost:27017"),
}

client = clients["prod"]
db = client["main"]
product_details_300923 = db["product_details_300923"]
inventory_itemized_details = db["inventory_itemized_details"]

GST_CHANGES = {
    "30": [["5", "0"], ["12", "5"]],   #12 to nil we dont have those salts in our db at all so removed , ["12", "0"]
    "3006": [["12", "5"]],
    "3822": [["12", "5"]],
    "4015": [["12", "5"]],
    "90": [["12", "5"]],
    "9004": [["12", "5"]],
    "9018": [["12", "5"]],
    "9019": [["12", "5"]],
    "9020": [["12", "5"]],
    "9022": [["12", "5"]],
    "9804": [["12", "5"], ["28", "18"]],
    "9025": [["18", "5"]],
    "9027": [["18", "5"]],
    "28": [["12", "5"]],
    "280120": [["12", "5"]],
    "28044010": [["12", "5"]],
    "2847": [["12", "5"]],
    "3001": [["12", "5"]],
    "3002": [["12", "5"]],
    "3003": [["12", "5"]],
    "3004": [["12", "5"]],
    "3005": [["12", "5"]],
    "36050010": [["12", "5"]],
    "3701": [["12", "5"]],
    "3705": [["12", "5"]],
    "3706": [["12", "5"]],
    "3808": [["12", "5"]],
    "3818": [["12", "5"]],
    "3826": [["12", "18"]],
    "3926": [["12", "5"]],
    "4007": [["12", "5"]],
    "4011": [["18", "5"], ["28", "18"]],
    "40117000": [["18", "5"]],
    "40139049": [["18", "5"]],
    "4014": [["12", "5"]],
    "4016": [["5", "0"], ["12", "5"]],
    "4107": [["12", "5"]],
    "4112": [["12", "5"]],
    "4113": [["12", "5"]],
    "4114": [["12", "5"]],
    "4115": [["12", "5"]],
    "420222": [["12", "5"]],
    "4202": [["12", "5"]],
    "29": [["12", "5"]],
    "42023110": [["12", "5"]],
    "96190030": [["12", "5"]],
    "96190040": [["12", "5"]],
    "96190090": [["12", "5"]],
    "9701": [["12", "5"]],
    "9702": [["12", "5"]],
    "9703": [["12", "5"]],
    "9705": [["12", "5"]],
    "9706": [["12", "5"]],
}

def normalize_gst(raw):
    if raw is None:
        return "0"
    s = str(raw).strip()
    if s == "":
        return "0"
    if "." in s:
        try:
            f = float(s)
            return str(int(f))
        except ValueError:
            pass
    m = re.search(r'\d+', s)
    if m:
        return (str(int(m.group(0))))
    return "0"

def find_replacement(original_hsn, original_gst):
    cur = original_hsn
    while len(cur) >= 2:
        if cur in GST_CHANGES:
            pairs = GST_CHANGES[cur]
            for p in pairs:
                old = p[0]
                new = p[1]
                if old == original_gst:
                    return new, cur
        cur = cur[:-2]
    return None, None

not_found_rows = []
updated = 0
processed = 0

product_docs = list(product_details_300923.find({}))

for doc in product_docs:
    processed += 1
    normalized_gst = normalize_gst(doc.get("gst"))
    hsn_raw = doc.get("hsn", "")
    hsn_trimmed = ""
    if hsn_raw:
        hsn_trimmed = hsn_raw.strip()

    replacement, matched_hsn = find_replacement(hsn_trimmed, normalized_gst)
    if replacement:
        if replacement != normalized_gst:
            # product_details_300923.update_one({"_id": doc["_id"]}, {"$set": {"gst": replacement}})
            # inventory_itemized_details.update_many({"skuID": doc["skuID"]}, {"$set": {"gst": replacement}})

            updated += 1
            print(f"Updated sku={doc.get('skuID')} hsn={hsn_trimmed} (matched {matched_hsn}): {normalized_gst} -> {replacement}")
        else:
            print(f"No change needed sku={doc.get('skuID')} hsn={hsn_trimmed}: already {normalized_gst}")
    else:
        not_found_rows.append({
            "skuID": doc.get("skuID", ""),
            "hsn": hsn_trimmed,
            "gst": normalized_gst
        })

# export not-found to excel
if not_found_rows:
    df = pd.DataFrame(not_found_rows, columns=["skuID", "hsn", "gst"])
    out_file = "gst_not_found_export_main.xlsx"
    df.to_excel(out_file, index=False)
    print(f"Exported {len(not_found_rows)} not-found rows to {out_file}")

print(f"Processed {processed} docs, updated {updated} documents.")
