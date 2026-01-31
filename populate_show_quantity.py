from datetime import datetime
import pymongo

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

purchase_sku_matching_result = db["purchase_sku_matching_result"]
b2b_product_supplier_link_details = db["b2b_product_supplier_link_details"]

supplierID = "SUP212602"

skuIDs = list(set([d["skuID"] for d in purchase_sku_matching_result.find({"storeID": supplierID}, {"skuID": 1})]))

bulk_writes = []

for skuID in skuIDs:
    bulk_writes.append(
        pymongo.UpdateOne(
            {"skuID": skuID, "supplierID": supplierID},
            {
                "$set": {
                    "skuID": skuID, 
                    "supplierID": supplierID,
                    "showQuantity": True,
                    "updatedTime": datetime.now(),
                    "updatedBy": "Siddu_Manual",
                    "updatedByPos": "Siddu_Manual"
                },
                "$setOnInsert": {
                    "createdTime": datetime.now(), 
                    "createdBy": "Siddu_Manual", 
                    "createdByPos": "Siddu_Manual"
                },
            },
            upsert=True
        )
    )
    
if bulk_writes:
    result = b2b_product_supplier_link_details.bulk_write(bulk_writes)
    print("Matched:", result.matched_count, "Modified:", result.modified_count, "Upserted:", len(result.upserted_ids))  