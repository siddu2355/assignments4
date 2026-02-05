import pandas as pd
import pymongo
from datetime import datetime
from typing import Dict, Optional

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
purchase_sku_matching_result = db["purchase_sku_matching_result"]

def extract_medicine_name(row: pd.Series) -> Optional[str]:
    """Extract medicine name from the appropriate suggestion column based on result value"""
    result_value = row.iloc[0]  # First column (result)
    
    if result_value == 6:
        return None
    
    # Try to get from suggestion_(result value) column
    suggestion_col = f"suggestion_{result_value}"
    if suggestion_col in row and pd.notna(row[suggestion_col]):
        return str(row[suggestion_col])
    
    # If no suggestion column, try the result column itself
    if pd.notna(result_value):
        return str(result_value)
    
    return None

def create_medicine_sku_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """Create a mapping of all medicine names from suggestion_1 to suggestion_5 to their SKU IDs"""
    # Collect all unique medicine names from suggestion columns
    medicine_names = set()
    
    for i in range(1, 6):  # suggestion_1 to suggestion_5
        suggestion_col = f"suggestion_{i}"
        if suggestion_col in df.columns:
            medicine_names.update(df[suggestion_col].dropna().astype(str).unique())
    
    print(f"Found {len(medicine_names)} unique medicine names to map")
    
    # Create mapping by querying database once for all medicine names
    medicine_sku_mapping = {}
    
    # Query in batches to avoid memory issues
    medicine_list = list(medicine_names)
    batch_size = 1000
    
    for i in range(0, len(medicine_list), batch_size):
        batch = medicine_list[i:i + batch_size]
        
        # Query database for this batch
        query = {"medicine_name": {"$in": batch}}
        results = product_details_300923.find(query, {"medicine_name": 1, "skuID": 1})
        
        for result in results:
            if "medicine_name" in result and "skuID" in result:
                medicine_sku_mapping[result["medicine_name"]] = result["skuID"]
    
    print(f"Mapped {len(medicine_sku_mapping)} medicine names to SKU IDs")
    return medicine_sku_mapping

def find_sku_id(medicine_name: str, medicine_sku_mapping: Dict[str, str]) -> Optional[str]:
    """Find SKU ID from mapping instead of database lookup"""
    return medicine_sku_mapping.get(medicine_name)

def process_excel_file(file_path: str, dry_run: bool = True) -> Dict:
    """Process Excel file and update SKU IDs"""
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded {len(df)} rows from Excel file")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return {"status": "error", "message": str(e)}
    
    # Create medicine name to SKU ID mapping upfront
    medicine_sku_mapping = create_medicine_sku_mapping(df)
    
    updates = []
    skipped_rows = 0
    not_found_skus = []
    
    for index, row in df.iterrows():
        medicine_name = extract_medicine_name(row)
        
        if not medicine_name:
            skipped_rows += 1
            continue
        
        sku_id = find_sku_id(medicine_name, medicine_sku_mapping)
        
        if not sku_id:
            not_found_skus.append(medicine_name)
            print(f"SKU not found for medicine: {medicine_name}")
            continue
        
        # Get bill_product from column B (index 1)
        bill_product = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else None
        
        if not bill_product:
            print(f"No bill_product found for row {index + 1}")
            continue
        
        update_data = {
            "bill_product": bill_product,
            "skuID": sku_id,
            "updatedBy": "siddu skuID update script",
            "updatedTime": datetime.now()
        }
        
        updates.append(update_data)
    
    print(f"Prepared {len(updates)} updates")
    print(f"Skipped {skipped_rows} rows (result = 6 or no medicine name)")
    print(f"SKU not found for {len(not_found_skus)} medicines")
    
    if not dry_run and updates:
        try:
            # Perform bulk updates
            bulk_ops = []
            for update in updates:
                bulk_ops.append(
                    pymongo.UpdateOne(
                        {"bill_product": update["bill_product"]},
                        {
                            "$set": {
                                "skuID": update["skuID"],
                                "updatedBy": update["updatedBy"],
                                "updatedTime": update["updatedTime"]
                            }
                        }
                    )
                )
                
                # Execute in batches of 1000 to avoid memory issues
                if len(bulk_ops) >= 1000:
                    result = purchase_sku_matching_result.bulk_write(bulk_ops)
                    print(f"Bulk update completed: {result.modified_count} documents modified")
                    bulk_ops = []
            
            # Execute remaining operations
            if bulk_ops:
                result = purchase_sku_matching_result.bulk_write(bulk_ops)
                print(f"Final bulk update completed: {result.modified_count} documents modified")
            
            return {
                "status": "success",
                "updates_performed": len(updates),
                "skipped_rows": skipped_rows,
                "not_found_skus": len(not_found_skus)
            }
            
        except Exception as e:
            print(f"Error during bulk update: {e}")
            return {"status": "error", "message": str(e)}
    
    elif dry_run:
        print("DRY RUN - No actual updates performed")
        return {
            "status": "dry_run",
            "updates_prepared": len(updates),
            "skipped_rows": skipped_rows,
            "not_found_skus": len(not_found_skus),
            "not_found_medicines": not_found_skus[:10]  # Show first 10
        }
    
    return {"status": "no_updates", "message": "No updates to perform"}

excel_file_path = r"C:\Important Documents\CareEco utils\ELIXIRE utils\Uttarakhand Medicos\suggestions_batch_1_rows.csv"  # Update this path
dry_run = True  # Set to False to execute actual updates

print(f"Processing file: {excel_file_path}")
print(f"Dry run mode: {dry_run}")
print("-" * 50)

result = process_excel_file(excel_file_path, dry_run=dry_run)

print("-" * 50)
print("Result:", result)