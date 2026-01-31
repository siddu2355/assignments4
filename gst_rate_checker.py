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

product_details_300923 = db["product_details_300923"]
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
out_file = "gst_check_output_main2.xlsx"
pd.DataFrame(results).to_excel(out_file, index=False)

print(f"Processed {processed} products, {updated} need update.")
print(f"Exported results to {out_file}")
