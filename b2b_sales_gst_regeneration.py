import math
import pymongo
from pymongo import InsertOne
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

supplier_id = "SUP212602"
# supplier_id = "SUP202650332"

b2b_sales_details = db["b2b_sales_details"]
b2b_sales_gst_table = db["b2b_sales_gst_table"]
elixire_store_details = db["elixire_store_details"]  
product_details_300923 = db["product_details_300923"]
product_details_categorized = db["product_details_categorized"]
elixire_document_deletion_audit = db["elixire_document_deletion_audit"]

def parse_double(value):
    try:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return 0.0
        num = float(value)
        if math.isnan(num) or math.isinf(num):
            return 0.0
        return round(num, 2)
    except (ValueError, TypeError):
        return 0.0

def parse_int(value):
    try:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return 0
        num = int(float(value))
        return num
    except (ValueError, TypeError):
        return 0

def get_store_gst_from_sale_or_store(sale_doc):
    store_id = sale_doc.get("storeID")
    if store_id:
        store_doc = elixire_store_details.find_one({"storeID": store_id}, {"GST": 1, "gst": 1})
        if store_doc:
            return store_doc.get("GST") or store_doc.get("gst") or ""
    return ""


def regenerate_gst_for_supplier(supplier_id):
    sales_cursor = list(b2b_sales_details.find({"supplierID": supplier_id}))

    all_sku_ids = []
    for sale in sales_cursor:
        for p in sale.get("productArray", []) or []:
            sku = p.get("skuID")
            if sku:
                all_sku_ids.append(sku)

    all_sku_ids = list(set(all_sku_ids))

    product_map = {}

    if all_sku_ids:
        cursor1 = product_details_300923.find(
            {"skuID": {"$in": all_sku_ids}},
            {"_id": 0, "skuID": 1, "medicine_name": 1, "packagingType": 1}
        )

        found_sku_ids = set()
        for p in cursor1:
            sku_id = p.get("skuID")
            if sku_id:
                found_sku_ids.add(sku_id)
                product_map[sku_id] = {
                    "medicine_name": (p.get("medicine_name") or "").strip(),
                    "packagingType": p.get("packagingType", "")
                }

        remaining_ids = list(set(all_sku_ids) - found_sku_ids)

        if remaining_ids:
            cursor2 = product_details_categorized.find(
                {"skuID": {"$in": remaining_ids}},
                {"_id": 0, "skuID": 1, "medicine_name": 1, "packagingType": 1}
            )

            for p in cursor2:
                sku_id = p.get("skuID")
                if sku_id:
                    product_map[sku_id] = {
                        "medicine_name": (p.get("medicine_name") or "").strip(),
                        "packagingType": p.get("packagingType", "")
                    }

    audit_entries = []
    sales_gst_cursor = b2b_sales_gst_table.find({"supplierID": supplier_id}, {"_id": 1})

    for doc in sales_gst_cursor:
        audit_entries.append({
            "deleted_id": doc.get("_id"),
            "db_name": "main",
            "collection_name": "b2b_sales_gst_table",
            "supplierID": supplier_id,
            "createdTime": datetime.now(),
            "updatedTime": datetime.now(),
        })

    if audit_entries:
        elixire_document_deletion_audit.insert_many(audit_entries)

    b2b_sales_gst_table.delete_many({"supplierID": supplier_id})

    vp_supplier_gst = "09AISPM8403D1Z1"
    bulk_writes = []

    for sale in sales_cursor:
        invoice_number = sale.get("invoiceNumber")
        store_gstin = get_store_gst_from_sale_or_store(sale)

        gst_dict = {}
        for item in sale.get("productArray", []) or []:
            sku_id = item.get("skuID")
            product_data = product_map.get(sku_id, {})
            hsn = item.get("hsn", "") or ""
            gst_rate = item.get("gstRate", 0) or 0
            packaging_type = product_data.get("packagingType", "")
            quantity = parse_int(item.get("orderQuantity", 0))
            amount_ex_gst = parse_double(item.get("taxableAmount", 0))
            gst_amount = parse_double(item.get("gstAmount", 0))

            sgst = cgst = igst = 0.0
            if store_gstin and vp_supplier_gst and str(store_gstin)[:2] == str(vp_supplier_gst)[:2]:
                # intra-state
                sgst = parse_double(gst_amount / 2)
                cgst = parse_double(gst_amount / 2)
            else:
                # inter-state
                igst = gst_amount

            gst_key = "_".join([str(x) for x in (invoice_number, hsn, gst_rate, packaging_type)])

            if gst_key in gst_dict:
                e = gst_dict[gst_key]
                e["quantity"] = e.get("quantity", 0) + quantity
                e["sgst"] = parse_double(e.get("sgst", 0) + sgst)
                e["cgst"] = parse_double(e.get("cgst", 0) + cgst)
                e["igst"] = parse_double(e.get("igst", 0) + igst)
                e["amountExGST"] = parse_double(e.get("amountExGST", 0) + amount_ex_gst)
            else:
                gst_dict[gst_key] = {
                    "salesDateTime": sale.get("salesDateTime"),
                    "invoiceNumber": invoice_number,
                    "hsn": hsn,
                    "gst": str(gst_rate),
                    "packagingType": packaging_type,
                    "quantity": quantity,
                    "gstKey": gst_key,
                    "sgst": sgst,
                    "cgst": cgst,
                    "igst": igst,
                    "amountExGST": amount_ex_gst,
                    "supplierID": supplier_id,
                    "createdTime": datetime.now(),
                    "updatedTime": datetime.now(),
                    "createdBy": "gst_regeneration_script",
                    "updatedBy": "gst_regeneration_script",
                    "createdByPos": "gst_regeneration_script",
                    "upatedByPos": "gst_regeneration_script",
                }

        for gst_transaction in gst_dict.values():
            bulk_writes.append(InsertOne(gst_transaction))

    if bulk_writes:
        b2b_sales_gst_table.bulk_write(bulk_writes, ordered=False)

regenerate_gst_for_supplier(supplier_id)
