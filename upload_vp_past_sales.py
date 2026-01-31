import re
import uuid
import pymongo
import pandas as pd
from pprint import pprint
from datetime import datetime
from dateutil import parser as date_parser

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

EXCEL_PATH = r"C:\Important Documents\CareEco utils\ELIXIRE B2B utils\VP surgicals\sales_pakka_last_year.xls"
SUPPLIER_ID = "SUP212602"

# connect to MongoDB
purchase_sku_matching_result = db["purchase_sku_matching_result"]
b2b_sales_details = db["b2b_sales_details"]
elixire_store_details = db["elixire_store_details"]
elixire_entity_details = db["elixire_entity_details"]

def build_entity_map():
    """
    Map normalized entityName → entityID
    """
    m = {}
    for doc in elixire_entity_details.find({}, {"entityName": 1, "entityID": 1}):
        name = doc.get("entityName", "").strip().lower()
        if name:
            m[name] = doc["entityID"]
    return m


def build_store_map():
    """
    Map entityID → storeID
    """
    m = {}
    for doc in elixire_store_details.find({}, {"entityID": 1, "storeID": 1}):
        eid = doc.get("entityID")
        sid = doc.get("storeID")
        if eid and sid:
            m[eid] = sid
    return m


ENTITY_MAP = build_entity_map()
STORE_MAP = build_store_map()

def find_entity_and_store(entity_name):
    """
    Returns (entityID, storeID) using prebuilt maps.
    """
    if not entity_name:
        return "", ""


    entity_id = ENTITY_MAP.get(entity_name.strip().lower(), "")

    if not entity_id:
        for name_key, eid in ENTITY_MAP.items():
            if entity_name.strip().lower() in name_key or name_key in entity_name.strip().lower():
                entity_id = eid
                break

    store_id = STORE_MAP.get(entity_id, "")

    return entity_id, store_id


def normalize_quantity(text):
    """
    Returns the first token that is digits only.
    Skips tokens containing *, letters, symbols, etc.
    """
    tokens = text.split()
    for token in tokens:
        if token.isdigit():  # pure numeric
            return token
    return None

# Helper: normalize product names for lookup
def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    # remove leading digits, dots, extra whitespace, leading hyphens
    s = re.sub(r'^[\d\.\-\s]+', '', s)
    # uppercase and collapse spaces
    s = re.sub(r'\s+', ' ', s).strip().upper()
    return s

# Load sku mapping into dict for faster lookup
def build_sku_map():
    sku_map = {}
    cursor = purchase_sku_matching_result.find({}, {"skuID":1, "bill_product":1, "medicine_name":1})
    for doc in cursor:
        if doc.get("bill_product"):
            sku_map[normalize_name(doc["bill_product"])] = doc.get("skuID")
    return sku_map

sku_map = build_sku_map()

# Read Excel into DataFrame (read all sheets / or first sheet)
# Use dtype=str so we can inspect raw cell strings
df = pd.read_excel(EXCEL_PATH, sheet_name=0, header=None, dtype=str, engine='xlrd')
df.fillna("", inplace=True)

# convert each row to a list of strings (strip)
rows = []
for _, r in df.iterrows():
    row = [str(x).strip() for x in r.tolist()]
    # if row entirely empty (all ""), skip
    if all(cell == "" for cell in row):
        rows.append(row)  # keep empty rows as separators (helpful)
    else:
        rows.append(row)

# regex to detect date like 11-12-2024 or 01-04-2024 etc.
date_re = re.compile(r'\b\d{1,2}-\d{1,2}-\d{4}\b')

# regex to detect invoice id like VP000001 (or starts with letters+digits)
invoice_re = re.compile(r'^[A-Z]{1,4}\d{1,10}$', re.IGNORECASE)

def parse_double(s):
    """Try to parse a numeric string. Return float or None."""
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    # remove commas, stray chars
    s_clean = re.sub(r'[^\d\.\-]', '', s)
    try:
        return float(s_clean)
    except:
        return None

# main parsing loop
current_date = None
current_invoice = None
current_invoice_items = []
invoices_to_insert = []

def flush_invoice():
    global current_invoice, current_invoice_items, current_date
    if not current_invoice:
        return
    # assemble document per your spec
    inv = current_invoice
    gross = inv.get("gross", 0.0)
    discount = inv.get("discount", 0.0)
    gst = inv.get("tax", 0.0)
    net = inv.get("net", 0.0)
    invoice_number = inv.get("invoiceNumber", "")
    # build productArray simplified objects
    product_array = []
    for it in current_invoice_items:
        product_array.append({
            "skuID": it.get("skuID", ""),
            "medicine_name": it.get("medicine_name", ""),
            "ptr": it.get("ptr", 0.0),
            "orderQuantity": int(it.get("orderQuantity", 0)),
            "totalAmount": parse_double(it.get("totalAmount", 0)),
        })
    doc = {
        "b2bSalesOrderID": f"EBSO-{uuid.uuid4()}",
        "additionalCharges": 0,
        "additionalDiscount": 0,
        "createdBy": "Siddu-Sales-Script",
        "createdByPos": "Siddu-Sales-Script",
        "createdTime": datetime.now(),
        "discountAmount": discount,
        "entityID": inv.get("entityID", ""),
        "invoiceNumber": invoice_number,
        "orderAmount": net,
        "orderStatus": "Delivered",
        "productArray": product_array,
        "salesDateTime": current_date if current_date else None,
        "storeID": inv.get("storeID", ""),
        "supplierID": SUPPLIER_ID,
        "syncDateTime": datetime.now(),
        "taxableAmount": gross,
        "updatedBy": "Siddu-Sales-Script",
        "updatedByPos": "Siddu-Sales-Script",
        "updatedTime": datetime.now(),
        "gstAmount": gst
    }
    invoices_to_insert.append(doc)
    # reset
    current_invoice = None
    current_invoice_items = []
    
skip_cf_block = False

# iterate rows
for i, row in enumerate(rows):
    # Skip C/F blocks until we reach a real product again
    if ("C/F" in row[0] or "C/F" in row[1] or "C/F" in row[2]):
        # enter skip block mode
        skip_cf_block = True
        continue

    # When skipping, ignore all rows until a product line appears
    if skip_cf_block:
        # product lines always have qty in row[2] and name in row[1]
        # row[1] contains product name, row[2] contains quantity
        cell = row[1].strip() if len(row) > 1 else ""

        if cell and (cell[0].isdigit()):
            skip_cf_block = False
        else:
            continue

    date_found = None
    m = date_re.search(str(row[0].strip()))
    if m:
        try:
            # try parsing to a datetime
            date_found = date_parser.parse(m.group(0), dayfirst=True).replace(hour=0, minute=0, second=0, microsecond=0)
        except:
            date_found = None
    
    if date_found:
        # flush previous invoice if any
        flush_invoice()
        current_date = date_found
        continue

    # invoice header detection (VP000001 style). Typically in col 0 or 1.
    inv_candidate = None

    first_cell = row[0].strip()
    if invoice_re.match(first_cell):
        inv_candidate = first_cell
        # read amounts from specific columns based on sample: gross at idx 3, discount idx4, tax idx5, net idx7
        # Safe-guard with try_parse_number across those indexes
        entity_name = row[1].strip()
        
        #TODO need to check if in any case entity id is missing...note down such cases and create entities for them
        found_entity_id, found_store_id = find_entity_and_store(entity_name)

        gross = parse_double(row[3]) if len(row) > 3 else 0.0
        discount = parse_double(row[4]) if len(row) > 4 else 0.0
        tax = parse_double(row[5]) if len(row) > 5 else 0.0
        net_amt = parse_double(row[7]) if len(row) > 7 else 0.0
        # flush previous invoice before starting a new one
        flush_invoice()
        current_invoice = {
            "invoiceNumber": inv_candidate,
            "gross": gross,
            "discount": discount ,
            "tax": tax,
            "net": net_amt,
            "entityID": found_entity_id,
            "storeID": found_store_id
        }

    if inv_candidate:
        continue

    if current_invoice is None:
        continue

    # find candidate name cell: try index 1 then 2 then 0
    product_name_cell = ""
    if row[1].strip() != "":
        product_name_cell = row[1].strip()

    if not product_name_cell:
        continue
    
    product_name = normalize_name(product_name_cell)

    order_qty = None
    ptr_value_candidate = None

    if row[2].strip():
        order_qty = int(normalize_quantity(row[2].strip()))
    
    if row[3].strip():
        ptr_value_candidate = round(float(row[3].strip().split()[0]), 2)

    # compute ptr:
    ptr = 0.0
    if ptr_value_candidate is not None and order_qty != 0:
        ptr = float(ptr_value_candidate) / float(order_qty)

    # Lookup skuID by normalized product_name
    sku_id = ""
    # try exact match first
    if product_name in sku_map:
        sku_id = sku_map[product_name]
    else:
        # try fuzzy matching by removing common tokens and matching startswith
        for key in sku_map.keys():
            if product_name.startswith(key) or key.startswith(product_name):
                sku_id = sku_map[key]
                break
        # otherwise attempt substring match
        if not sku_id:
            for key in sku_map.keys():
                if key in product_name or product_name in key:
                    sku_id = sku_map[key]
                    break

    item_obj = {
        "skuID": sku_id or "",
        "medicine_name": product_name,
        "ptr": round(ptr, 2),
        "orderQuantity": order_qty,
        "totalAmount": ptr_value_candidate
    }
    current_invoice_items.append(item_obj)

# flush final invoice
flush_invoice()

print(f"Prepared {len(invoices_to_insert)} invoice documents for insertion.")

if invoices_to_insert:
    result = b2b_sales_details.insert_many(invoices_to_insert)
    print(f"Inserted {len(result.inserted_ids)} documents into 'b2b_sales_details'.")
else:
    print("No invoices to insert.")

if invoices_to_insert:
    for i in invoices_to_insert:
        if i["invoiceNumber"] == "VP000027":
            pprint(i)