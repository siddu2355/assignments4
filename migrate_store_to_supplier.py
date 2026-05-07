import pandas as pd
import uuid
from datetime import datetime, timezone
import pymongo
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
DB_URI = "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
FILENAME = "VP upload file.xlsx"
CREATED_BY = "vp_store_to_supplier_script"
CREATED_BY_POS = "VP_STORE_TO_SUPPLIER_MIGRATION"

# Dry Run Flag
DRY_RUN = True # Default to True for safety

client = pymongo.MongoClient(DB_URI)
db_main = client["main"]
db_aux = client["aux"]
db_archive = client["archive"]

store_details_col = db_main["elixire_store_details"]
supplier_details_col = db_main["supplier_details"]
audit_col = db_main["elixire_document_deletion_audit"]
mapping_col = db_main["elixire_supplier_retailer_mapping"]
archived_store_col = db_archive["archived_elixire_store_details"]

# Sales and Payment collections in both DBs
SALES_COLS = {
    "main": db_main["b2b_sales_details"],
    "aux": db_aux["b2b_sales_details"]
}
PAYMENT_COLS = {
    "main": db_main["b2b_sales_payment_transaction_log"],
    "aux": db_aux["b2b_sales_payment_transaction_log"]
}

def normalize_val(val):
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()

def create_address(row):
    # Try to use fields from row or store doc fallback
    parts = []
    for k in ["Shop No", "Street", "Locality", "area", "city", "state"]:
        v = row.get(k, "")
        if v and str(v).strip() != "nan":
            parts.append(str(v).strip())
    return ", ".join(parts)

def parse_excel_row(row):
    return {
        "storeName": normalize_val(row.get("storeName")),
        "GST": normalize_val(row.get("GST")),
        "area": normalize_val(row.get("area")),
        "city": normalize_val(row.get("city")),
        "state": normalize_val(row.get("state")),
        "pin": normalize_val(row.get("pin"))
    }

def match_store(parsed_row):
    # Find stores matching the Excel criteria
    query = {
        k: v for k, v in {
            "storeName": parsed_row.get("storeName", ""),
            "GST": parsed_row.get("GST", ""),
            "area": parsed_row.get("area", ""),
            "city": parsed_row.get("city", ""),
            "state": parsed_row.get("state", ""),
            "pin": parsed_row.get("pin", "")
        }.items() if v
    }
    
    # We might find multiple stores if only broad criteria is provided
    # The user said: "print the storeID's of the records of the items u match from excel against db"
    stores = list(store_details_col.find(query))
    return stores

def migrate_store_to_supplier(store_doc, session=None):
    store_id = store_doc["storeID"]
    entity_id = store_doc.get("entityID", "")
    
    # 1. Archive
    if not DRY_RUN:
        archived_store_col.insert_one(store_doc, session=session)
        logger.info(f"Archived store {store_id}")
    else:
        logger.info(f"[DRY RUN] Would archive store {store_id}")

    # 2. Deletion Audit
    audit_record = {
        "deleted_id": store_doc["_id"],
        "db_name": "main",
        "collection_name": "elixire_store_details",
        "storeID": store_id,
        "createdTime": datetime.now(timezone.utc),
        "updatedTime": datetime.now(timezone.utc),
        "createSource": "migration_incorrect_store_record"
    }
    if not DRY_RUN:
        audit_col.insert_one(audit_record, session=session)
        logger.info(f"Created audit record for {store_id}")
    else:
        logger.info(f"[DRY RUN] Would create audit record for {store_id}")

    # 3. Create Supplier
    supplier_id = f"SUP{uuid.uuid4().hex[:12].upper()}"
    new_supplier = {
        "supplierID": supplier_id,
        "entityID": entity_id,
        "entityName": store_doc.get("storeName", ""),
        "entityType": store_doc.get("storeType", "Distributor"),
        "address": store_doc.get("address", ""),
        "addressPinCode": store_doc.get("pin", ""),
        "adminContactNo": store_doc.get("storeContact", ""),
        "adminEmail": store_doc.get("email", ""),
        "city": store_doc.get("city", ""),
        "gst": store_doc.get("GST", ""),
        "supplierCategory": [],
        "supplierType": "Manufacturer",
        "config": {
            "negativeInventory": True,
            "cashBilling": True
        },
        "createdBy": CREATED_BY,
        "createdByPos": CREATED_BY_POS,
        "createdTime": datetime.now(timezone.utc),
        "updatedBy": CREATED_BY,
        "updatedByPos": CREATED_BY_POS,
        "updatedTime": datetime.now(timezone.utc)
    }
    if not DRY_RUN:
        supplier_details_col.insert_one(new_supplier, session=session)
        logger.info(f"Created supplier {supplier_id} from {store_id}")
    else:
        logger.info(f"[DRY RUN] Would create supplier {supplier_id} from {store_id}")

    # 4. Update Mappings
    if not DRY_RUN:
        res = mapping_col.update_many(
            {"storeID": store_id},
            {
                "$set": {
                    "clientSupplierID": supplier_id,
                    "updatedTime": datetime.now(timezone.utc),
                    "updatedBy": CREATED_BY
                },
                "$unset": {"storeID": ""}
            },
            session=session
        )
        logger.info(f"Updated mapping: {res.modified_count} records")
    else:
        logger.info(f"[DRY RUN] Would update elixire_supplier_retailer_mapping for storeID {store_id}")

    # 5. Update Sales Data (Main & Aux)
    update_data = {
        "$set": {
            "clientSupplierID": supplier_id,
            "updatedTime": datetime.now(timezone.utc),
            "updatedBy": CREATED_BY
        }
    }
    # User said "the storeID must be replaced with the clientSupplierID"
    # Actually, storeID field probably should be unset or kept as original if needed.
    # Looking at the sample records, they have both storeID and clientSupplierID.
    # User says "storeID must be replaced with the clientSupplierID". 
    # I'll update clientSupplierID and possibly unset storeID if that's what's meant by "replaced".
    # But usually in these systems, clientSupplierID is the field used for suppliers that are clients.
    
    # Let's see the examples again.
    # Example 1: clientSupplierID: "SUP202650332", storeID: None
    # Example 2: clientSupplierID: "ELXS220819", storeID: "ELXS220819"
    # So if we are converting store to supplier, we should set clientSupplierID to the new SUP id.
    
    for db_name, col in SALES_COLS.items():
        if not DRY_RUN:
            res = col.update_many({"storeID": store_id}, update_data, session=session)
            logger.info(f"Updated sales in {db_name}: {res.modified_count} records")
        else:
            logger.info(f"[DRY RUN] Would update b2b_sales_details in {db_name} for storeID {store_id}")

    for db_name, col in PAYMENT_COLS.items():
        if not DRY_RUN:
            res = col.update_many({"storeID": store_id}, update_data, session=session)
            logger.info(f"Updated payments in {db_name}: {res.modified_count} records")
        else:
            logger.info(f"[DRY RUN] Would update b2b_sales_payment_transaction_log in {db_name} for storeID {store_id}")

    # 6. Delete Store
    if not DRY_RUN:
        store_details_col.delete_one({"_id": store_doc["_id"]}, session=session)
        logger.info(f"Deleted store record {store_id}")
    else:
        logger.info(f"[DRY RUN] Would delete store record {store_id}")

    return supplier_id

def run_migration():
    logger.info(f"Starting migration from {FILENAME}...")
    try:
        df = pd.read_excel(FILENAME)
    except Exception as e:
        logger.error(f"Failed to read Excel file {FILENAME}: {e}")
        return

    stats = {
        "rows_processed": 0,
        "stores_matched": 0,
        "suppliers_created": 0,
        "errors": 0
    }

    for index, row in df.iterrows():
        stats["rows_processed"] += 1
        parsed = parse_excel_row(row)
        logger.info(f"Row {index+1}: Matching for {parsed['storeName']}...")
        
        stores = match_store(parsed)
        if not stores:
            logger.warning(f"  No match found for {parsed['storeName']}")
            continue
        
        stats["stores_matched"] += len(stores)
        store_ids = [s["storeID"] for s in stores]
        logger.info(f"  Matched StoreIDs: {', '.join(store_ids)}")

        for store_doc in stores:
            try:
                # Use session for atomicity if not dry run
                if not DRY_RUN:
                    with client.start_session() as session:
                        with session.start_transaction():
                            sup_id = migrate_store_to_supplier(store_doc, session=session)
                            stats["suppliers_created"] += 1
                else:
                    migrate_store_to_supplier(store_doc)
                    stats["suppliers_created"] += 1
            except Exception as e:
                logger.error(f"  Error processing store {store_doc['storeID']}: {e}")
                stats["errors"] += 1

    logger.info("=" * 40)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 40)
    logger.info(f"Dry Run: {DRY_RUN}")
    logger.info(f"Rows processed from Excel: {stats['rows_processed']}")
    logger.info(f"Stores matched in DB:      {stats['stores_matched']}")
    logger.info(f"Suppliers created:         {stats['suppliers_created']}")
    logger.info(f"Errors encountered:        {stats['errors']}")
    logger.info("=" * 40)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="Run the actual migration (default is dry-run)")
    args = parser.parse_args()

    if args.run:
        DRY_RUN = False
        logger.info("!!! ACTUALLY RUNNING MIGRATION !!!")
    else:
        logger.info("DRY RUN MODE - No changes will be made to the DB")

    run_migration()
