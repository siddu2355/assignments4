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

client = clients["test"]

db = client["main"]
db_con = client["controls"]

elixire_supplier_retailer_mapping = db["elixire_supplier_retailer_mapping"]
elixire_store_details = db["elixire_store_details"]
elixire_entity_details = db["elixire_entity_details"]
elixire_user = db["elixire_user"]
elixire_roles_store_assignments = db_con["elixire_roles_store_assignments"]
elixire_license_setups = db_con["elixire_license_setups"]
elixire_entity_wallet_running_balance = db["elixire_entity_wallet_running_balance"]
elixire_document_deletion_audit = db["elixire_document_deletion_audit"]
b2b_beat_details = db["b2b_beat_details"]

supplierID = "SUP281862339"

mappings = list(elixire_supplier_retailer_mapping.find({"supplierID": supplierID}))
storeIDs = [m["storeID"] for m in mappings]

store_records = list(elixire_store_details.find({"storeID": {"$in": storeIDs}, "live":{"$ne":True}, "location": {"$exists": False}}))
entityIDs = [s.get("entityID") for s in store_records]

print("StoreIDs:", len(storeIDs))
print("EntityIDs:", len(entityIDs))


def audit_deletions(docs, collection_name, store_field="storeID", entity_field="entityID", db_name="main"):
    audit_docs = []
    for doc in docs:
        audit_docs.append({
            "deleted_id": doc["_id"],
            "db_name": "main",
            "collection_name": collection_name,
            "storeID": doc.get(store_field),
            "entityID": doc.get(entity_field),
            "createdTime": datetime.now(),
            "updatedTime": datetime.now(),
        })
    if audit_docs:
        elixire_document_deletion_audit.insert_many(audit_docs)


# === Supplier Retailer Mapping
docs1 = list(elixire_supplier_retailer_mapping.find({"supplierID": supplierID}))
audit_deletions(docs1, "elixire_supplier_retailer_mapping", "storeID", "entityID")
deleted1 = elixire_supplier_retailer_mapping.delete_many({"supplierID": supplierID})
print("Deleted Mappings:", deleted1.deleted_count)

# === Store Details
docs2 = list(elixire_store_details.find({"storeID": {"$in": storeIDs}, "live": {"$ne": True}, "location": {"$exists": False}}))
audit_deletions(docs2, "elixire_store_details", "storeID", "entityID")
deleted2 = elixire_store_details.delete_many({"storeID": {"$in": storeIDs}})
print("Deleted Stores:", deleted2.deleted_count)

# === Entity Details
docs3 = list(elixire_entity_details.find({"entityID": {"$in": entityIDs}}))
audit_deletions(docs3, "elixire_entity_details", "storeID", "entityID")
deleted3 = elixire_entity_details.delete_many({"entityID": {"$in": entityIDs}})
print("Deleted Entities:", deleted3.deleted_count)

# === Users
docs4 = list(elixire_user.find({
    "storeID": {
        "$elemMatch": {
            "storeID": {"$in": storeIDs},
            "entityID": {"$in": entityIDs}
        }
    }
}))
audit_deletions(docs4, "elixire_user", "storeID", "entityID")
deleted4 = elixire_user.delete_many({
    "storeID": {
        "$elemMatch": {
            "storeID": {"$in": storeIDs},
            "entityID": {"$in": entityIDs}
        }
    }
})
print("Deleted Users:", deleted4.deleted_count)

# === Roles Store Assignments
docs5 = list(elixire_roles_store_assignments.find({"storeID": {"$in": storeIDs}}))
audit_deletions(docs5, "elixire_roles_store_assignments", "storeID", "entityID", "controls")
deleted5 = elixire_roles_store_assignments.delete_many({"storeID": {"$in": storeIDs}})
print("Deleted Role Assignments:", deleted5.deleted_count)

# === License Setups
docs6 = list(elixire_license_setups.find({"entityID": {"$in": entityIDs}}))
audit_deletions(docs6, "elixire_license_setups", "storeID", "entityID", "controls")
deleted6 = elixire_license_setups.delete_many({"entityID": {"$in": entityIDs}})
print("Deleted License Setups:", deleted6.deleted_count)

# === Wallet Balances
docs7 = list(elixire_entity_wallet_running_balance.find({"entityID": {"$in": entityIDs}}))
audit_deletions(docs7, "elixire_entity_wallet_running_balance", "storeID", "entityID")
deleted7 = elixire_entity_wallet_running_balance.delete_many({"entityID": {"$in": entityIDs}})
print("Deleted Wallet Balances:", deleted7.deleted_count)
