import re
import pymongo
import pandas as pd
from datetime import datetime

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
db_aux = client["aux"]
product_details_300923 = db["product_details_300923"]
inventory_itemized_details = db["inventory_itemized_details"]
inventory_itemized_details_aux = db_aux["inventory_itemized_details"]

def log_action(sku_id, action, log_file):
    """Append log entry to log file"""
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {action:<50} | {sku_id}\n")

def normalize_gst(raw):
    if raw is None:
        return ""
    s = str(raw).strip()
    if s == "":
        return ""
    if "." in s:
        try:
            f = float(s)
            return str(int(f))
        except ValueError:
            pass
    m = re.search(r'\d+', s)
    if m:
        return str(int(m.group(0)))
    return ""

def normalize_hsn(hsn):
    if hsn == "":
        return ""
    if len(hsn) % 2 != 0 or len(hsn) > 8:
        return ""
    if not re.fullmatch(r"\d+", hsn):
        return ""
    return hsn

updated = 0
processed = 0
updated_rows =[]

product_docs = list(product_details_300923.find({}, {"_id":1, "skuID": 1, "gst": 1, "hsn": 1}))

invalid_hsns = [
    "200002152",
    "N/A",
    "300490111",
    "390025099",
    "210690099",
    "testhsn1",
    "hsntest1",
    "testhsn",
    "null",
    "0",
    "6544653214",
    "General",

    "34r5990",
    "4KO0245",
    "REL24009",
    "PNT23027",
    "6.57465E+11",
    "BSSD661",
    "AM-7665",
    "1901 10 90",
    "9619 00 10",
    "3401 11 90",
    "3305 10 90",
    "3305.90.11",
    "1513.11.00",
    "1905 32 11",
    "3004 90.11",
    "3304 90 11",
    "3305 90 30",
    "000000",
    "0000"
]

bad_hsn_count = 0
bad_gst_count = 0

for doc in product_docs:
    processed += 1
    original_gst = doc.get("gst", "")
    normalized_gst = normalize_gst(doc.get("gst"))
    
    if normalized_gst not in ["0", "5" , "12", "18", "28"]:
        normalized_gst = ""

    product_hsn = doc.get("hsn", "")
    if product_hsn is None:
        product_hsn = ""
    else:
        product_hsn = product_hsn.strip().replace(" ", "").replace(".", "")
    
    normalized_hsn = normalize_hsn(product_hsn)
    
    if normalized_hsn in invalid_hsns:
        normalized_hsn = ""
    
    if False and ((product_hsn and not normalized_hsn) or normalized_hsn in invalid_hsns):
        pass
        # bad_hsn_count += 1
        # product_details_300923.update_one(
        #     {"_id": doc["_id"]},
        #     {"$set": {
        #         "gst": "",
        #         "hsn": "",
        #         "updatedTime": datetime.now(),
        #         "updatedByPos":"gst_migration",
        #         "updatedBy":"siddu_gst_migration",
        #     }})
        # log_action(doc["skuID"], f"bad hsn:{normalized_hsn} resetting product gst & hsn", "Bad_data_resetter_log.txt")
        # print(doc["skuID"], f"bad hsn:{normalized_hsn} resetting product gst & hsn")

        # inventory_itemized_details.update_many(
        #     {"skuID": doc["skuID"]}, 
        #     {"$set": {
        #         "gst": "",
        #         "updatedTime": datetime.now(),
        #         "updatedByPos":"gst_migration",
        #         "updatedBy":"siddu_gst_migration",
        #     }})
        
        # log_action(doc["skuID"], f"bad hsn:{normalized_hsn} resetting gst in inventory_main", "Bad_data_resetter_log.txt")
        # print(doc["skuID"], f"bad hsn:{normalized_hsn} resetting gst in inventory_main")
        
        # inventory_itemized_details_aux.update_many(
        #     {"skuID": doc["skuID"]},
        #     {"$set": {
        #         "gst": "",
        #         "updatedTime": datetime.now(),
        #         "updatedByPos":"gst_migration",
        #         "updatedBy":"siddu_gst_migration",
        #     }})
        # log_action(doc["skuID"], f"bad hsn::{normalized_hsn} resetting gst in inventory_aux", "Bad_data_resetter_log.txt")
        # print(doc["skuID"], f"bad hsn::{normalized_hsn} resetting gst in inventory_aux")
    else:
        bad_gst_count += 1
        if original_gst != normalized_gst or product_hsn != normalized_hsn:
            product_details_300923.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {
                    "gst": normalized_gst,
                    "hsn": normalized_hsn,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration",
                    "updatedBy":"siddu_gst_migration",
                }})

            log_action(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {normalized_gst}", "Bad_data_resetter_log.txt")
            print(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {normalized_gst}")
            inventory_itemized_details.update_many(
                {"skuID": doc["skuID"]},
                {"$set": {
                    "gst": normalized_gst,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration",
                    "updatedBy":"siddu_gst_migration",
                }})

            log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_main", "Bad_data_resetter_log.txt")
            print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_main")

            inventory_itemized_details_aux.update_many(
                {"skuID": doc["skuID"]}, 
                {"$set": {
                    "gst": normalized_gst,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration",
                    "updatedBy":"siddu_gst_migration",
                }})

            log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_aux", "Bad_data_resetter_log.txt")
            print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_aux")

            updated_rows.append({
                "skuID": doc.get("skuID", ""),
                "original_gst": original_gst,
                "normalized_gst": normalized_gst,
            })
            updated += 1
   

if updated_rows:
    df = pd.DataFrame(updated_rows, columns=["skuID", "original_gst", "normalized_gst"])
    out_file = "normalize_gst_rates.xlsx"
    df.to_excel(out_file, index=False)
    print(f"Exported {len(updated_rows)} updated rows to {out_file}")

print(f"Processed {processed} docs, updated {updated} documents.")