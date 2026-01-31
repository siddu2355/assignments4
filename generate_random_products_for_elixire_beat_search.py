import json
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

product_docs = list(db["product_details_300923"].find({}, {"medicine_name": 1, "skuID": 1, "brand": 1, "pack": 1, "product_name": 1, "category": 1, "variant": 1, "subCategory": 1, "strength": 1, "form": 1, "gst": 1}))

NUM_PRODUCTS = 15000

def make_product(i: int) -> dict:
    doc = product_docs[i]

    sku_id = doc.get("skuID")
    medicine_name = doc.get("medicine_name")
    product_name = doc.get("product_name") or medicine_name
    pack = doc.get("pack")

    # Random generated fields
    mrp = round(random.uniform(10, 500), 2)
    ptr = round(random.uniform(10, 500), 2)
    available_quantity = random.randint(0, 1000)

    product = {
        "imageUrl": "",
        "margin": random.choice([0, 5, 18, 40]),
        "showQuantity": random.choice([True, False]),
        "discountRate": random.choice([0, 5, 18, 40]),
        "mrp": mrp,
        "ptr": ptr,
        "quantity": available_quantity,
        "medicine_name": medicine_name,
        "product_name": product_name,
        "skuID": sku_id,
        "pack": pack,
        "brand": doc.get("brand", ""),
        "category": doc.get("category", ""),
        "subCategory": doc.get("subCategory", ""),
        "variant": doc.get("variant", ""),
        "strength": doc.get("strength", ""),
        "form": doc.get("form", ""),
        "gst": doc.get("gst", 0),
    }

    return product

def main():
    products = [make_product(i) for i in range(NUM_PRODUCTS)]
    with open("products_15000_beat.json", "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False)
    print(f"Generated products_15000.json with {NUM_PRODUCTS} products")


if __name__ == "__main__":
    main()
