import json
import uuid
import random
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

product_docs = list(db["product_details_300923"].find({}, {"medicine_name": 1, "skuID": 1}))

NUM_PRODUCTS = 15000

def random_sku(i):
    return product_docs[i]["skuID"]


def random_medicine_name(i):
    return product_docs[i]["medicine_name"]


def random_inventory_details(sku_id: str, supplier_id: str, sku_name: str):
    count = random.choice([0, 1])
    details = []
    for _ in range(count):
        quantity = random.randint(0, 1000)
        mrp = round(random.uniform(10, 500), 2)
        ptr = round(mrp * random.uniform(0.5, 0.9), 2)
        pts = round(ptr * random.uniform(0.5, 0.9), 2)
        detail = {
            "b2bInventoryID": "EBI-" + str(uuid.uuid4()),
            "supplierID": supplier_id,
            "skuID": sku_id,
            "quantity": quantity,
            "ptr": ptr,
            "pts": pts,
            "mrp": mrp,
            "main": random.choice([0, 1]),
            "aux": 0,
            "skuName": sku_name,
            "gst": str(random.choice([0, 5, 18, 40])),
            "company": "",
            "division": "",
            "showQuantity": True,
            "allowOnlineSell": True,
            "supplierName": "CareEco Test Supplier Pvt Ltd 8-11-25",
            "minOrderAmount": 10,
            "isThirdParty": random.choice([False, False, False, True]),
            "creditAgreement": {
                "creditPeriodUnit": "Days",
                "creditPeriodValue": 0,
                "creditLimit": 0,
                "creditWorthiness": "Unknown",
            },
            "deliveryConditions": {
                "deliveryByType": "ByBeat",
                "deliveryByValue": 0,
            },
            "priceMarginCompanyLevelOverrides": [],
        }
        details.append(detail)
    return details


def make_product(i: int) -> dict:
    sku_id = random_sku(i)
    supplier_id = "SUP202650332"
    medicine_name = random_medicine_name(i)

    mrp = round(random.uniform(10, 500), 2)
    available_quantity = random.randint(0, 1000)
    has_inventory = 1 if available_quantity > 0 else 0

    # optional inventoryDetails
    inventory_details = random_inventory_details(sku_id, supplier_id, medicine_name)

    product = {
        "gst": str(random.choice([0, 5, 18, 40])),
        "hasInventory": has_inventory,
        "medicine_name": medicine_name,
        "mrp": str(mrp),
        "skuID": sku_id,
        "availableQuantity": available_quantity,
        "unique_suppliers": [supplier_id],
        "inventoryDetails": inventory_details,
    }

    return product


def main():
    products = [make_product(i) for i in range(NUM_PRODUCTS)]
    with open("products_15000.json", "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False)
    print(f"Generated products_15000.json with {NUM_PRODUCTS} products")


if __name__ == "__main__":
    main()
