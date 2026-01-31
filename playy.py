# import re
# import pymongo
# import pandas as pd

# # ------------------ DB Connections ------------------
# clients = {
#     "prod": pymongo.MongoClient(
#         "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
#     ),
#     "test": pymongo.MongoClient(
#         "mongodb+srv://careeco_system:IojrVNu8P2TiGsJd@cluster0.uoqzz4p.mongodb.net/"
#     ),
#     "local": pymongo.MongoClient("mongodb://localhost:27017"),
# }

# client = clients["prod"]
# db = client["main"]
# product_details_300923 = db["product_details_300923"]
# inventory_itemized_details = db["inventory_itemized_details"]

# # ------------------ Helpers ------------------
# def normalize_gst(raw):
#     if raw is None:
#         return "0"
#     s = str(raw).strip()
#     if s == "":
#         return "0"
#     if "." in s:
#         try:
#             f = float(s)
#             return str(int(f))
#         except ValueError:
#             pass
#     m = re.search(r"\d+", s)
#     if m:
#         return m.group(0)
#     return "0"


# # ------------------ Step 1: Read Golden Excel ------------------
# golden_file = "valid_rows.xlsx"  # <--- put your file here
# df = pd.read_excel(golden_file, dtype=str)

# # assume HSN is col A, IGST is col C
# hsn_col = df.columns[0]
# gst_col = df.columns[2]

# gst_map = {}
# for _, row in df.iterrows():
#     hsn = row.get(hsn_col, "").strip().replace(" ", '')
#     gst = normalize_gst(row.get(gst_col, ""))
#     if not hsn:
#         continue
#     if hsn not in gst_map:  # ignore duplicates
#         gst_map[hsn] = gst

# file = open("hsn_gst_golden_map.txt", "w")
# file.write(gst_map)
# file.close()
# print(f"Loaded {len(gst_map)} HSN→GST mappings from golden file.")

# # ------------------ Step 2: Compare with DB ------------------
# results = []
# processed = 0
# updated = 0

# product_docs = list(product_details_300923.find({}, {"skuID": 1, "hsn": 1, "gst": 1}))

# def find_golden_gst(hsn):
#     """Try exact match, else reduce HSN by 2 digits at a time."""
#     cur = hsn
#     while len(cur) >= 2:
#         if cur in gst_map:
#             return gst_map[cur], cur
#         cur = cur[:-2]
#     return None, None

# for doc in product_docs:
#     processed += 1
#     sku = doc.get("skuID", "")
#     product_hsn_raw = doc.get("hsn", "")
#     product_hsn = ""
#     if product_hsn_raw:
#         product_hsn = product_hsn_raw.strip()
#     product_gst = normalize_gst(doc.get("gst"))

#     golden_gst, matched_hsn = find_golden_gst(product_hsn)
#     inventory_exists = False
#     update_needed = False

#     if golden_gst and golden_gst != product_gst:
#         update_needed = True
#         inv_count = inventory_itemized_details.count_documents({"skuID": sku})
#         inventory_exists = inv_count > 0
#         updated += 1

#     results.append({
#         "skuID": sku,
#         "product_hsn": product_hsn,
#         "product_gst": product_gst,
#         "golden_gst": golden_gst or "",
#         "matched_hsn": matched_hsn or "",
#         "update_needed": update_needed,
#         "inventory_exists": inventory_exists,
#     })

# # ------------------ Step 3: Export ------------------
# out_file = "gst_check_output.xlsx"
# pd.DataFrame(results).to_excel(out_file, index=False)

# print(f"Processed {processed} products, {updated} need update.")
# print(f"Exported results to {out_file}")


# --------------------script for normalizing gst for categorized ------

# import re
# import pymongo
# import pandas as pd
# from datetime import datetime

# clients = {
#     "prod": pymongo.MongoClient(
#         "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
#     ),
#     "test": pymongo.MongoClient(
#         "mongodb+srv://careeco_system:IojrVNu8P2TiGsJd@cluster0.uoqzz4p.mongodb.net/"
#     ),
#     "local": pymongo.MongoClient("mongodb://localhost:27017"),
# }

# client = clients["prod"]
# db = client["main"]
# db_aux = client["aux"]
# product_details_300923 = db["product_details_categorized"]
# inventory_itemized_details = db["inventory_itemized_details"]
# inventory_itemized_details_aux = db_aux["inventory_itemized_details"]

# def log_action(sku_id, action, log_file):
#     """Append log entry to log file"""
#     with open(log_file, "a", encoding="utf-8") as f:
#         f.write(f"{datetime.now().isoformat()} | {action:<50} | {sku_id}\n")

# def normalize_gst(raw):
#     if raw is None:
#         return ""
#     s = str(raw).strip()
#     if s == "":
#         return ""
#     if "." in s:
#         try:
#             f = float(s)
#             return str(int(f))
#         except ValueError:
#             pass
#     m = re.search(r'\d+', s)
#     if m:
#         return str(int(m.group(0)))
#     return ""

# def normalize_hsn(hsn):
#     if hsn == "":
#         return ""
#     if len(hsn) % 2 != 0 and len(hsn) > 8:
#         return ""
#     if not re.fullmatch(r"\d+", hsn):
#         return ""
#     return hsn

# updated = 0
# processed = 0
# updated_rows =[]

# product_docs = list(product_details_300923.find({}, {"_id":1, "skuID": 1, "gst": 1, "hsn": 1}))

# invalid_hsns = [
#     "200002152",
#     "N/A",
#     "300490111",
#     "390025099",
#     "210690099",
#     "testhsn1",
#     "hsntest1",
#     "testhsn",
#     "null",
#     "0",
#     "6544653214",
#     "General",

#     "34r5990",
#     "4KO0245",
#     "REL24009",
#     "PNT23027",
#     "6.57465E+11",
#     "BSSD661",
#     "AM-7665",
#     "1901 10 90",
#     "9619 00 10",
#     "3401 11 90",
#     "3305 10 90",
#     "3305.90.11",
#     "1513.11.00",
#     "1905 32 11",
#     "3004 90.11",
#     "3304 90 11",
#     "3305 90 30",
#     "000000",
#     "0000"
# ]

# bad_hsn_count = 0
# bad_gst_count = 0

# for doc in product_docs:
#     processed += 1
#     original_gst = doc.get("gst", "")
#     normalized_gst = normalize_gst(doc.get("gst"))
    
#     if normalized_gst not in ["0", "5" , "12", "18", "28"]:
#         normalized_gst = ""

#     product_hsn = doc.get("hsn", "")
#     normalized_hsn = product_hsn
#     if product_hsn is None:
#         product_hsn = ""
#         normalized_hsn = ""
#     else:
#         normalized_hsn = str(product_hsn).strip().replace(" ", "").replace(".", "")

#     normalized_hsn = normalize_hsn(normalized_hsn)
    
#     if normalized_hsn in invalid_hsns:
#         normalized_hsn = ""
    
#     if False and ((product_hsn and not normalized_hsn) or normalized_hsn in invalid_hsns):
#         pass
#         # bad_hsn_count += 1
#         # product_details_300923.update_one(
#         #     {"_id": doc["_id"]},
#         #     {"$set": {
#         #         "gst": "",
#         #         "hsn": "",
#         #         "updatedTime": datetime.now(),
#         #         "updatedByPos":"gst_migration",
#         #         "updatedBy":"siddu_gst_migration",
#         #     }})
#         # log_action(doc["skuID"], f"bad hsn:{normalized_hsn} resetting product gst & hsn", "Bad_data_resetter_log.txt")
#         # print(doc["skuID"], f"bad hsn:{normalized_hsn} resetting product gst & hsn")

#         # inventory_itemized_details.update_many(
#         #     {"skuID": doc["skuID"]}, 
#         #     {"$set": {
#         #         "gst": "",
#         #         "updatedTime": datetime.now(),
#         #         "updatedByPos":"gst_migration",
#         #         "updatedBy":"siddu_gst_migration",
#         #     }})
        
#         # log_action(doc["skuID"], f"bad hsn:{normalized_hsn} resetting gst in inventory_main", "Bad_data_resetter_log.txt")
#         # print(doc["skuID"], f"bad hsn:{normalized_hsn} resetting gst in inventory_main")
        
#         # inventory_itemized_details_aux.update_many(
#         #     {"skuID": doc["skuID"]},
#         #     {"$set": {
#         #         "gst": "",
#         #         "updatedTime": datetime.now(),
#         #         "updatedByPos":"gst_migration",
#         #         "updatedBy":"siddu_gst_migration",
#         #     }})
#         # log_action(doc["skuID"], f"bad hsn::{normalized_hsn} resetting gst in inventory_aux", "Bad_data_resetter_log.txt")
#         # print(doc["skuID"], f"bad hsn::{normalized_hsn} resetting gst in inventory_aux")
#     else:
#         bad_gst_count += 1
#         if original_gst != normalized_gst or product_hsn != normalized_hsn:
#             product_details_300923.update_one(
#                 {"_id": doc["_id"]}, 
#                 {"$set": {
#                     "gst": normalized_gst,
#                     "hsn": normalized_hsn,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }})

#             log_action(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {normalized_gst}", "Bad_data_resetter_log.txt")
#             print(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {normalized_gst}")
#             inventory_itemized_details.update_many(
#                 {"skuID": doc["skuID"]},
#                 {"$set": {
#                     "gst": normalized_gst,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }})

#             log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_main", "Bad_data_resetter_log.txt")
#             print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_main")

#             inventory_itemized_details_aux.update_many(
#                 {"skuID": doc["skuID"]}, 
#                 {"$set": {
#                     "gst": normalized_gst,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }})

#             log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_aux", "Bad_data_resetter_log.txt")
#             print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {normalized_gst} inventory_aux")

#             updated_rows.append({
#                 "skuID": doc.get("skuID", ""),
#                 "original_gst": original_gst,
#                 "normalized_gst": normalized_gst,
#             })
#             updated += 1
   

# if updated_rows:
#     df = pd.DataFrame(updated_rows, columns=["skuID", "original_gst", "normalized_gst"])
#     out_file = "normalize_gst_rates.xlsx"
#     df.to_excel(out_file, index=False)
#     print(f"Exported {len(updated_rows)} updated rows to {out_file}")

# print(f"Processed {processed} docs, updated {updated} documents.")


# ---------------------script to replace th gst data with the data acc to cbit data -------------------------
# from datetime import datetime
# import re
# import pymongo
# import pandas as pd

# # ------------------ DB Connections ------------------
# clients = {
#     "prod": pymongo.MongoClient(
#         "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
#     ),
#     "test": pymongo.MongoClient(
#         "mongodb+srv://careeco_system:IojrVNu8P2TiGsJd@cluster0.uoqzz4p.mongodb.net/"
#     ),
#     "local": pymongo.MongoClient("mongodb://localhost:27017"),
# }

# client = clients["prod"]
# db = client["main"]
# db_aux = client["aux"]
# product_details_300923 = db["gst_changing_products_categorized"]
# inventory_itemized_details = db["gst_changing_inventory"]
# inventory_itemized_details_aux = db_aux["gst_changing_inventory"]
# elixire_store_details = db["elixire_store_details"]

# def normalize_gst(raw):
#     if raw is None:
#         return ""
#     s = str(raw).strip()
#     if s == "":
#         return ""
#     if "." in s:
#         try:
#             f = float(s)
#             return str(int(f))
#         except ValueError:
#             pass
#     m = re.search(r"\d+", s)
#     if m:
#         return (str(int(m.group(0))))
#     return ""


# golden_file = "valid_rows.xlsx"
# df = pd.read_excel(golden_file, dtype=str)

# def find_golden_gst(hsn):
#     """Try exact match, else reduce HSN by 2 digits at a time."""
#     while len(hsn) >= 6:
#         if hsn in gst_map:
#             return gst_map[hsn], hsn
#         hsn = hsn[:-2]
#     return None, None


# def log_action(sku_id, action, log_file):
#     """Append log entry to log file"""
#     with open(log_file, "a", encoding="utf-8") as f:
#         f.write(f"{datetime.now().isoformat()} | {action:<50} | {sku_id}\n")

# hsn_col = df.columns[0]
# gst_col = df.columns[2]

# gst_map = {}

# for _, row in df.iterrows():
#     hsn = row.get(hsn_col, "").strip().replace(" ", '')
#     gst = normalize_gst(row.get(gst_col, ""))
#     if not hsn:
#         continue
#     if hsn not in gst_map:
#         gst_map[hsn] = gst

# results = []
# processed = 0
# updated = 0

# product_docs = list(product_details_300923.find({
#     "$and": [
#         {"hsn": {"$exists": True}},
#         {"hsn": {"$ne": ""}},
#     ]
# }, {"_id": 1, "skuID": 1, "hsn": 1, "gst": 1}))

# live_store_docs = list(elixire_store_details.find({"live": True}, {"_id":0, "storeID": 1}))

# live_store_ids = [doc.get("storeID") for doc in live_store_docs]

# for doc in product_docs:
#     processed += 1
#     skuID = doc.get("skuID", "")
#     product_hsn_raw = doc.get("hsn", "")

#     product_hsn = ""
#     if product_hsn_raw:
#         product_hsn = product_hsn_raw.strip()

#     original_gst = normalize_gst(doc.get("gst"))
#     golden_gst, matched_hsn = find_golden_gst(product_hsn)

#     if golden_gst and original_gst != golden_gst:
#         product_details_300923.update_one(
#             {"_id": doc["_id"]},
#             {"$set": {
#                     "gst": golden_gst,
#                     "incorrectGst": original_gst,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }}
#         )
#         log_action(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {golden_gst}", "Bad_data_resetter_log.txt")
#         print(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {golden_gst}")
            
#         inventory_itemized_details.update_many(
#             {
#                 "skuID": skuID, 
#                 "storeID": {"$in": live_store_ids}
#             },
#             {
#                 "$set": {
#                     "gst": golden_gst, 
#                     "incorrectGst": original_gst,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }}
#         )
#         log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {golden_gst} inventory_main", "Bad_data_resetter_log.txt")
#         print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {golden_gst} inventory_main")
        
#         inventory_itemized_details_aux.update_many(
#             {
#                 "skuID": skuID, 
#                 "storeID": {"$in": live_store_ids}
#             },
#             {"$set": {
#                     "gst": golden_gst, 
#                     "incorrectGst": original_gst,
#                     "updatedTime": datetime.now(),
#                     "updatedByPos":"gst_migration",
#                     "updatedBy":"siddu_gst_migration",
#                 }}
#         )
        
#         log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {golden_gst} inventory_aux", "Bad_data_resetter_log.txt")
#         print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {golden_gst} inventory_aux")
#         updated += 1
        
# print(f"Processed {processed} docs, updated {updated} documents.")


import re
import pymongo
import pandas as pd

# ------------------ DB Connections ------------------
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
db_aux = client["main"]

product_details_300923 = db["product_details_categorized"]
inventory_itemized_details = db["inventory_itemized_details"]
inventory_itemized_details_aux = db_aux["inventory_itemized_details"]
elixire_store_details = db["elixire_store_details"]

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
    m = re.search(r"\d+", s)
    if m:
        return (str(int(m.group(0))))
    return "0"

results = []
processed = 0
updated = 0

product_docs = list(product_details_300923.find({"gst": "12"}, {"skuID": 1, "hsn": 1, "gst": 1}))
live_store_docs = list(elixire_store_details.find({"live": True}, {"_id":0, "storeID": 1}))

live_store_ids = [doc.get("storeID") for doc in live_store_docs]
print(len(product_docs))
for doc in product_docs:
    processed += 1
    print(processed)
    sku = doc.get("skuID", "")
    product_hsn_raw = doc.get("hsn", "")
    product_hsn = ""
    if product_hsn_raw:
        product_hsn = product_hsn_raw.strip()
    product_gst = normalize_gst(doc.get("gst"))

    inventory_exists_main = False
    inventory_exists_aux = False

    inv_count_main = inventory_itemized_details.count_documents({"skuID": sku, "storeID": {"$in": live_store_ids}})
    inv_count_aux = inventory_itemized_details_aux.count_documents({"skuID": sku, "storeID": {"$in": live_store_ids}})
    
    sellable_inv_count_main = inventory_itemized_details.count_documents({
        "skuID": sku, 
        "inventoryStatus": {"$in": [1, 3, 5]},
        "$or": [
            {
                "totalInventoryInHand":{"$gt": 0}
            },
            {
                "$and": [
                    {"looseInventoryDetails.sellableInventory": {"$exists": True}},
                    {"looseInventoryDetails.sellableInventory": {"$gt": 0}},
                ],
            }
        ],
        "storeID": {"$in": live_store_ids}
    })
    
    sellable_inv_count_aux = inventory_itemized_details_aux.count_documents({
        "skuID": sku, 
        "inventoryStatus": {"$in": [1, 3, 5]},
        "$or": [
            {
                "totalInventoryInHand":{"$gt": 0}
            },
            {
                "$and": [
                    {"looseInventoryDetails.sellableInventory": {"$exists": True}},
                    {"looseInventoryDetails.sellableInventory": {"$gt": 0}},
                ],
            }
        ],
        "storeID": {"$in": live_store_ids}
    })

    inventory_exists_main = inv_count_main > 0
    inventory_exists_aux = inv_count_aux > 0
    
    updated += 1

    results.append({
        "skuID": sku,
        "product_hsn": product_hsn,
        "product_gst": product_gst,
        
        "inventory_main_exists": inventory_exists_main,
        "sellable_inventory_exists_main": sellable_inv_count_main,
        
        "inventory_aux_exists": inventory_exists_aux,
        "sellable_inventory_exists_aux": sellable_inv_count_aux
    })

# ------------------ Step 3: Export ------------------
out_file = "gst_check_output_main9.xlsx"
pd.DataFrame(results).to_excel(out_file, index=False)

print(f"Processed {processed} products, {updated} need update.")
print(f"Exported results to {out_file}")
