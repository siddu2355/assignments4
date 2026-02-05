import pandas as pd
import pymongo
from datetime import datetime
from typing import Dict, Optional, List
import os
import glob

# MongoDB connections
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

def extract_medicine_name_from_excel(row: pd.Series) -> Optional[str]:
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
    
    # Try to convert to int if it's a float like 1.0, 2.0, etc.
    try:
        if '.' in result_str and result_str.replace('.', '').isdigit():
            result_int = int(float(result_str))
            result_str = str(result_int)
    except (ValueError, TypeError):
        pass
    
    # Try to get from suggestion_(result value) column
    suggestion_col = f"suggestion_{result_str}"
    if suggestion_col in row and pd.notna(row[suggestion_col]):
        return str(row[suggestion_col]).strip()
    
    # If no suggestion column, try the result column itself (only if it's not a number)
    if pd.notna(result_value):
        # Check if it's not a numeric value (1, 2, 3, 4, 5, 6 or 1.0, 2.0, etc.)
        try:
            float(result_str)
            # If conversion to float succeeds, it's a number, so don't use it as medicine name
            return None
        except (ValueError, TypeError):
            # If conversion fails, it's a string, so use it as medicine name
            return result_str.strip()
    
    return None

def create_medicine_sku_mapping_from_excel(df: pd.DataFrame) -> Dict[str, str]:
    """Create a mapping of all medicine names from Excel file to their SKU IDs"""
    # Collect all unique medicine names from suggestion columns and column 1
    medicine_names = set()
    
    # From suggestion columns
    for i in range(1, 6):  # suggestion_1 to suggestion_5
        suggestion_col = f"suggestion_{i}"
        if suggestion_col in df.columns:
            medicine_names.update(df[suggestion_col].dropna().astype(str).unique())
    
    # From column 1 (result column) - exclude numeric values
    result_col = df.iloc[:, 0]  # First column
    for value in result_col.dropna():
        str_value = str(value).strip()
        if not str_value.isdigit() and str_value != "":
            medicine_names.add(str_value)
    
    # Strip all medicine names to ensure consistent matching
    medicine_names = {name.strip() for name in medicine_names if name.strip()}
    
    print(f"Found {len(medicine_names)} unique medicine names to map from Excel")
    
    # Create mapping by querying database
    medicine_sku_mapping = {}
    
    # Query in batches
    medicine_list = list(medicine_names)
    batch_size = 1000
    
    for i in range(0, len(medicine_list), batch_size):
        batch = medicine_list[i:i + batch_size]
        
        # Query with space variations
        query_conditions = []
        for med_name in batch:
            query_conditions.append({"medicine_name": med_name})
            if med_name:
                query_conditions.append({"medicine_name": med_name + " "})
                query_conditions.append({"medicine_name": " " + med_name})
        
        query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
        results = product_details_300923.find(query, {"medicine_name": 1, "skuID": 1})
        
        for result in results:
            if "medicine_name" in result and "skuID" in result:
                clean_medicine_name = result["medicine_name"].strip()
                clean_sku_id = result["skuID"].strip()
                medicine_sku_mapping[clean_medicine_name] = clean_sku_id
    
    print(f"Mapped {len(medicine_sku_mapping)} medicine names to SKU IDs")
    return medicine_sku_mapping

def find_sku_id(medicine_name: str, medicine_sku_mapping: Dict[str, str]) -> Optional[str]:
    """Find SKU ID from mapping"""
    return medicine_sku_mapping.get(medicine_name.strip())

def create_bill_product_to_sku_mapping(folder_path: str) -> Dict[str, str]:
    """Create mapping from bill_product (column 2) to skuID by processing all Excel files"""
    print("Creating bill_product to skuID mapping from Excel files...")
    
    # Find all CSV and Excel files in the folder
    file_patterns = ["*.csv", "*.xlsx", "*.xls"]
    files_to_process = []
    
    for pattern in file_patterns:
        files_to_process.extend(glob.glob(os.path.join(folder_path, pattern)))
    
    if not files_to_process:
        print(f"No CSV or Excel files found in: {folder_path}")
        return {}
    
    print(f"Processing {len(files_to_process)} Excel files...")
    
    bill_product_sku_mapping = {}
    
    for file_path in files_to_process:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Create medicine to SKU mapping for this file
            medicine_sku_mapping = create_medicine_sku_mapping_from_excel(df)
            
            # Map bill_product (column 2) to SKU ID
            for index, row in df.iterrows():
                # Get bill_product from column 2 (index 1)
                bill_product = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else None
                
                if not bill_product:
                    continue
                
                # Get medicine name from the row
                medicine_name = extract_medicine_name_from_excel(row)
                
                if medicine_name:
                    sku_id = find_sku_id(medicine_name, medicine_sku_mapping)
                    if sku_id:
                        bill_product_sku_mapping[bill_product] = sku_id
                        print(f"  Mapped: {bill_product} -> {sku_id} (medicine: {medicine_name})")
                    else:
                        print(f"  SKU not found for medicine: {medicine_name}")
                else:
                    print(f"  No medicine name found for bill_product: {bill_product}")
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue
    
    print(f"Created mapping for {len(bill_product_sku_mapping)} bill_products")
    return bill_product_sku_mapping

def get_empty_sku_records() -> List[Dict]:
    """Get records from purchase_sku_matching_result with empty skuID and updatedBy 'SIDDU-UTTARAKHAND'"""
    print("Fetching records with empty skuID and updatedBy 'SIDDU-UTTARAKHAND'...")
    
    query = {
        "skuID": "",
        "updatedBy": "SIDDU-UTTARAKHAND"
    }
    
    records = list(purchase_sku_matching_result.find(query))
    print(f"Found {len(records)} records to update")
    
    return records

def update_sku_ids(bill_product_sku_mapping: Dict[str, str], dry_run: bool = True) -> Dict:
    """Update SKU IDs for records that have matching bill_products"""
    records_to_update = get_empty_sku_records()
    
    if not records_to_update:
        return {"status": "no_records", "message": "No records found to update"}
    
    updates_performed = 0
    skipped_records = 0
    
    print(f"Processing {len(records_to_update)} records...")
    
    for record in records_to_update:
        bill_product = record.get("bill_product", "").strip()
        
        if not bill_product:
            skipped_records += 1
            continue
        
        sku_id = bill_product_sku_mapping.get(bill_product)
        
        if sku_id:
            print(f"Updating: {bill_product} -> {sku_id}")
            
            if not dry_run:
                purchase_sku_matching_result.update_one(
                    {"_id": record["_id"]},
                    {
                        "$set": {
                            "skuID": sku_id,
                            "updatedBy": "SIDDU-UTTARAKHAND-SKUID-SCRIPT",
                            "updatedTime": datetime.now()
                        }
                    }
                )
            
            updates_performed += 1
        else:
            print(f"No SKU mapping found for bill_product: {bill_product}")
            skipped_records += 1
    
    return {
        "status": "success",
        "total_records": len(records_to_update),
        "updates_performed": updates_performed,
        "skipped_records": skipped_records
    }

def main():
    # Configuration
    folder_path = r"C:\Important Documents\CareEco utils\ELIXIRE utils\Uttarakhand Medicos"
    dry_run = False  # Set to False to execute actual updates
    
    print("=" * 60)
    print("SKU ID Update Script for Uttarakhand Medicos")
    print("=" * 60)
    print(f"Folder path: {folder_path}")
    print(f"Dry run mode: {dry_run}")
    print("-" * 60)
    
    # Step 1: Create bill_product to skuID mapping from Excel files
    bill_product_sku_mapping = create_bill_product_to_sku_mapping(folder_path)
    
    if not bill_product_sku_mapping:
        print("No mappings created. Exiting.")
        return
    
    print("-" * 60)
    
    # Step 2: Update SKU IDs in database
    result = update_sku_ids(bill_product_sku_mapping, dry_run=dry_run)
    
    print("-" * 60)
    print("FINAL RESULT:")
    print(result)

if __name__ == "__main__":
    main()
