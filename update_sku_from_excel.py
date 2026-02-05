import pandas as pd
import pymongo
from datetime import datetime
from typing import Dict, Optional
import os
import glob

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
    
    # Handle None/NaN values
    if pd.isna(result_value):
        return None
    
    # Convert to string and handle both numeric and string values
    result_str = str(result_value).strip()
    
    # Skip if result is 6 (after converting to string)
    if result_str == "6":
        return None
    
    # Try to get from suggestion_(result value) column
    suggestion_col = f"suggestion_{result_str}"
    if suggestion_col in row and pd.notna(row[suggestion_col]):
        return str(row[suggestion_col])
    
    # If no suggestion column, try the result column itself
    if pd.notna(result_value):
        return result_str.strip()
    
    return None

def create_medicine_sku_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """Create a mapping of all medicine names from suggestion columns and direct entries in column 1 to their SKU IDs"""
    # Collect all unique medicine names from suggestion columns
    medicine_names = set()
    
    for i in range(1, 6):  # suggestion_1 to suggestion_5
        suggestion_col = f"suggestion_{i}"
        if suggestion_col in df.columns:
            medicine_names.update(df[suggestion_col].dropna().astype(str).unique())
    
    # Also collect medicine names from column 1 (result column) - but exclude numeric values and "6"
    result_col = df.iloc[:, 0]  # First column
    for value in result_col.dropna():
        str_value = str(value).strip()
        # Skip if it's a number (1-6) or empty
        if not str_value.isdigit() and str_value != "":
            medicine_names.add(str_value)
    
    # Strip all medicine names to ensure consistent matching
    medicine_names = {name.strip() for name in medicine_names if name.strip()}
    
    print(f"Found {len(medicine_names)} unique medicine names to map")
    
    # Create mapping by querying database once for all medicine names
    medicine_sku_mapping = {}
    
    # Query in batches to avoid memory issues
    medicine_list = list(medicine_names)
    batch_size = 1000
    
    for i in range(0, len(medicine_list), batch_size):
        batch = medicine_list[i:i + batch_size]
        
        # Query database for this batch - also include variations with/without spaces
        # Create a query that looks for both original and stripped versions
        query_conditions = []
        for med_name in batch:
            query_conditions.append({"medicine_name": med_name})
            # Also try with common space variations
            if med_name:
                query_conditions.append({"medicine_name": med_name + " "})
                query_conditions.append({"medicine_name": " " + med_name})
        
        query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
        results = product_details_300923.find(query, {"medicine_name": 1, "skuID": 1})
        
        for result in results:
            if "medicine_name" in result and "skuID" in result:
                # Strip both medicine_name and skuID to handle extra spaces
                clean_medicine_name = result["medicine_name"].strip()
                clean_sku_id = result["skuID"].strip()
                medicine_sku_mapping[clean_medicine_name] = clean_sku_id
    
    print(f"Mapped {len(medicine_sku_mapping)} medicine names to SKU IDs")
    return medicine_sku_mapping

def find_sku_id(medicine_name: str, medicine_sku_mapping: Dict[str, str]) -> Optional[str]:
    """Find SKU ID from mapping instead of database lookup"""
    return medicine_sku_mapping.get(medicine_name.strip())

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
            "updatedBy": "SIDDU-UTTARAKHAND-SKUID-SCRIPT",
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

def process_folder(folder_path: str, dry_run: bool = True) -> Dict:
    """Process all CSV/Excel files in a folder"""
    if not os.path.exists(folder_path):
        return {"status": "error", "message": f"Folder does not exist: {folder_path}"}
    
    # Find all CSV and Excel files in the folder
    file_patterns = ["*.csv", "*.xlsx", "*.xls"]
    files_to_process = []
    
    for pattern in file_patterns:
        files_to_process.extend(glob.glob(os.path.join(folder_path, pattern)))
    
    if not files_to_process:
        return {"status": "error", "message": f"No CSV or Excel files found in: {folder_path}"}
    
    print(f"Found {len(files_to_process)} files to process:")
    for file in files_to_process:
        print(f"  - {os.path.basename(file)}")
    print()
    
    total_results = {
        "status": "success",
        "total_files": len(files_to_process),
        "processed_files": 0,
        "total_updates": 0,
        "total_skipped": 0,
        "total_not_found": 0,
        "file_results": [],
        "errors": []
    }
    
    for file_path in files_to_process:
        print(f"\n{'='*60}")
        print(f"Processing file: {os.path.basename(file_path)}")
        print(f"{'='*60}")
        
        result = process_excel_file(file_path, dry_run=dry_run)
        total_results["file_results"].append({
            "file": os.path.basename(file_path),
            "result": result
        })
        
        if result["status"] in ["success", "dry_run"]:
            total_results["processed_files"] += 1
            if "updates_performed" in result:
                total_results["total_updates"] += result["updates_performed"]
            elif "updates_prepared" in result:
                total_results["total_updates"] += result["updates_prepared"]
            
            total_results["total_skipped"] += result["skipped_rows"]
            total_results["total_not_found"] += result["not_found_skus"]
        else:
            total_results["errors"].append({
                "file": os.path.basename(file_path),
                "error": result.get("message", "Unknown error")
            })
    
    return total_results

excel_file_path = r"C:\Important Documents\CareEco utils\ELIXIRE utils\Singh Pharmacy\suggestions_batch_2_rows_503-1002.csv"  # Update this path
dry_run = False  # Set to False to execute actual updates

# NEW: Folder processing option
folder_path = r"C:\Important Documents\CareEco utils\ELIXIRE utils\Uttarakhand Medicos"  # Update this path to your folder
use_folder_mode = True  # Set to True to process all files in folder, False to use single file

if use_folder_mode:
    print(f"Processing folder: {folder_path}")
    print(f"Dry run mode: {dry_run}")
    print("-" * 50)
    
    result = process_folder(folder_path, dry_run=dry_run)
else:
    print(f"Processing file: {excel_file_path}")
    print(f"Dry run mode: {dry_run}")
    print("-" * 50)
    
    result = process_excel_file(excel_file_path, dry_run=dry_run)

print("-" * 50)
print("Result:", result)