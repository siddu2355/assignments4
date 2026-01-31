import re
import pymongo
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
elixire_store_details = db["elixire_store_details"]

GST_CHANGES = {
    # "30": [["5", "0"], ["12", "5"]],   #12 to nil we dont have those salts in our db at all so removed , ["12", "0"]
    "3006": "5",
    "3822": "5",
    "4015": "5",
    "90": "5",
    "9004": "5",
    "9018": "5",
    "9019": "5",
    "9020": "5",
    "9022": "5",
    # "9804": [["12", "5"], ["28", "18"]],
    "9025": "5",
    "9027": "5",
    "28": "5",
    "280120": "5",
    "28044010": "5",
    "2847": "5",
    "3001": "5",
    "3002": "5",
    "3003": "5",
    "3004": "5",
    "3005": "5",
    "36050010": "5",
    "3701": "5",
    "3705": "5",
    "3706": "5",
    "3808": "5",
    "3818": "5",
    "3826": "18",
    "3926": "5",
    "4007": "5",
    # "4011": [["18", "5"], ["28", "18"]],
    "40117000": "5",
    "40139049": "5",
    "4014": "5",
    # "4016": [["5", "0"], ["12", "5"]],
    "4107": "5",
    "4112": "5",
    "4113": "5",
    "4114": "5",
    "4115": "5",
    "420222": "5",
    "4202": "5",
    "29": "5",
    "42023110": "5",
    "96190030": "5",
    "96190040": "5",
    "96190090": "5",
    "9701": "5",
    "9702": "5",
    "9703": "5",
    "9705": "5",
    "9706": "5",
    "17049020": "5",
    "20089700": "5",
    "20091200": "5",
    "20098990": "5",
    "210690": "5",
    "21069091": "5",
    "33061010": "5",
    "56012110": "5",
    "96159000": "5",
}

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
        return (str(int(m.group(0))))
    return ""

def find_replacement(original_hsn):
    cur = original_hsn
    while len(cur) >= 2:
        if cur in GST_CHANGES:
            return GST_CHANGES[cur]
        cur = cur[:-2]
    return None

updated = 0
processed = 0

product_bulk_updates = []
inventory_main_bulk_updates = []
inventory_aux_bulk_updates = []

product_docs = list(product_details_300923.find({
    "$and": [
        {"hsn": {"$exists": True}},
        {"hsn": {"$ne": ""}},
    ]
}, {"_id": 1, "skuID": 1, "hsn": 1, "gst": 1}))

live_store_docs = list(elixire_store_details.find({"live": True}, {"_id":0, "storeID": 1}))

live_store_ids = [doc.get("storeID") for doc in live_store_docs]

for doc in product_docs:
    processed += 1
    original_gst = normalize_gst(doc.get("gst"))
    
    skuID = doc["skuID"]

    hsn_raw = doc.get("hsn", "")
    hsn_trimmed = ""
    if hsn_raw:
        hsn_trimmed = hsn_raw.strip()

    replaced_gst = find_replacement(hsn_trimmed)

    if replaced_gst and replaced_gst != original_gst: 
        product_bulk_updates.append(
            pymongo.UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "gst": replaced_gst,
                    "oldGst": original_gst,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration_3",
                    "updatedBy":"siddu_gst_migration_3",
                }}
            )
        )

        log_action(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {replaced_gst}", "Bad_data_resetter_log.txt")
        print(doc["skuID"], f"bad gst:{original_gst} resetting product gst to {replaced_gst} for hsn: {hsn_trimmed}")

        inventory_main_bulk_updates.append(
            pymongo.UpdateMany(
                {"skuID": skuID, "storeID": {"$in": live_store_ids}},
                {"$set": {
                    "gst": replaced_gst,
                    "oldGst": original_gst,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration_3",
                    "updatedBy":"siddu_gst_migration_3",
                }},
                upsert=False
            )
        )

        log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {replaced_gst} inventory_main", "Bad_data_resetter_log.txt")
        print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {replaced_gst} inventory_main for hsn: {hsn_trimmed}")
        
        inventory_aux_bulk_updates.append(
            pymongo.UpdateMany(
                {"skuID": skuID, "storeID": {"$in": live_store_ids}},
                {"$set": {
                    "gst": replaced_gst,
                    "oldGst": original_gst,
                    "updatedTime": datetime.now(),
                    "updatedByPos":"gst_migration_3",
                    "updatedBy":"siddu_gst_migration_3",
                }},
                upsert=False
            )
        )

        log_action(doc["skuID"], f"bad gst:{original_gst} resetting gst to {replaced_gst} inventory_aux", "Bad_data_resetter_log.txt")
        print(doc["skuID"], f"bad gst:{original_gst} resetting gst to {replaced_gst} inventory_aux for hsn: {hsn_trimmed}")
        updated += 1

if product_bulk_updates:
    product_details_300923.bulk_write(product_bulk_updates)

if inventory_main_bulk_updates:
    inventory_itemized_details.bulk_write(inventory_main_bulk_updates)

if inventory_aux_bulk_updates:
    inventory_itemized_details_aux.bulk_write(inventory_aux_bulk_updates)

print(f"Processed {processed} docs, updated {updated} documents.")
