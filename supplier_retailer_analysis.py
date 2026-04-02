import pymongo
import pandas as pd
from datetime import datetime
from collections import defaultdict

# MongoDB connections (same as reference file)
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
elixire_supplier_retailer_mapping = db["elixire_supplier_retailer_mapping"]
elixire_store_details = db["elixire_store_details"]

def analyze_supplier_retailer_mapping(supplier_id):
    """
    Analyze supplier-retailer mappings for a given supplier ID.
    Groups stores by name and counts occurrences.
    """
    
    print(f"Analyzing mappings for supplier: {supplier_id}")
    
    # Get all mappings for the specified supplier
    mappings = list(elixire_supplier_retailer_mapping.find(
        {"supplierID": supplier_id},
        {"storeID": 1, "createdTime": 1, "updatedTime": 1}
    ))
    
    if not mappings:
        print(f"No mappings found for supplier: {supplier_id}")
        return None
    
    print(f"Found {len(mappings)} store mappings")
    
    # Extract all store IDs
    store_ids = [mapping["storeID"] for mapping in mappings]
    
    # Get store details for all store IDs
    store_details = list(elixire_store_details.find(
        {"storeID": {"$in": store_ids}},
        {
            "storeID": 1,
            "storeName": 1,
            "city": 1,
            "state": 1,
            "pin": 1,
            "address": 1,
            "storeContact": 1,
            "storeType": 1
        }
    ))
    
    # Create a mapping from storeID to store details
    store_dict = {store["storeID"]: store for store in store_details}
    
    # Group by store name and count occurrences
    store_name_groups = defaultdict(list)
    
    for mapping in mappings:
        store_id = mapping["storeID"]
        store_detail = store_dict.get(store_id, {})
        store_name = store_detail.get("storeName", "Unknown Store")
        
        store_name_groups[store_name].append({
            "storeID": store_id,
            "storeName": store_name,
            "city": store_detail.get("city", ""),
            "state": store_detail.get("state", ""),
            "pin": store_detail.get("pin", ""),
            "address": store_detail.get("address", ""),
            "storeContact": store_detail.get("storeContact", ""),
            "storeType": store_detail.get("storeType", ""),
            "mappingCreatedTime": mapping.get("createdTime", ""),
            "mappingUpdatedTime": mapping.get("updatedTime", "")
        })
    
    # Prepare data for Excel export
    analysis_data = []
    
    for store_name, stores in store_name_groups.items():
        count = len(stores)
        
        # Add summary row for each store name group
        analysis_data.append({
            "storeName": store_name,
            "storeID": f"TOTAL: {count} stores",
            "city": "",
            "state": "",
            "pin": "",
            "address": "",
            "storeContact": "",
            "storeType": "",
            "count": count,
            "mappingCreatedTime": "",
            "mappingUpdatedTime": "",
            "isSummary": True
        })
        
        # Add individual store details
        for store in stores:
            analysis_data.append({
                "storeName": store["storeName"],
                "storeID": store["storeID"],
                "city": store["city"],
                "state": store["state"],
                "pin": store["pin"],
                "address": store["address"],
                "storeContact": store["storeContact"],
                "storeType": store["storeType"],
                "count": 1,
                "mappingCreatedTime": store["mappingCreatedTime"],
                "mappingUpdatedTime": store["mappingUpdatedTime"],
                "isSummary": False
            })
    
    return analysis_data

def export_to_excel(data, supplier_id):
    """Export analysis data to Excel file"""
    
    if not data:
        print("No data to export")
        return
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"supplier_retailer_analysis_{supplier_id}_{timestamp}.xlsx"
    
    # Export to Excel
    df.to_excel(filename, index=False)
    print(f"Analysis exported to: {filename}")
    
    # Print summary statistics
    summary_rows = df[df["isSummary"] == True]
    total_stores = len(df[df["isSummary"] == False])
    unique_store_names = len(summary_rows)
    
    print(f"\nSummary for supplier {supplier_id}:")
    print(f"Total store mappings: {total_stores}")
    print(f"Unique store names: {unique_store_names}")
    print(f"Stores with duplicate names: {unique_store_names - total_stores if unique_store_names != total_stores else 0}")
    
    # Show stores with multiple occurrences
    duplicate_stores = summary_rows[summary_rows["count"] > 1]
    if not duplicate_stores.empty:
        print(f"\nStores with multiple occurrences:")
        for _, row in duplicate_stores.iterrows():
            print(f"  - {row['storeName']}: {row['count']} stores")
    
    return filename

def main():
    """Main function to run the analysis"""
    
    # Supplier ID to analyze (can be changed as needed)
    supplier_id = "SUP846521305"
    
    try:
        # Perform analysis
        analysis_data = analyze_supplier_retailer_mapping(supplier_id)
        
        if analysis_data:
            # Export to Excel
            filename = export_to_excel(analysis_data, supplier_id)
            print(f"\nAnalysis completed successfully!")
            print(f"Results saved to: {filename}")
        else:
            print("No data found for analysis")
            
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
