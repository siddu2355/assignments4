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

EXCEL_PATH = r"C:\Users\sathw\Downloads\purchase-3 pakka last year.xls"
SUPPLIER_ID = "SUP212602" # Used in final document

# Collections
purchase_sku_matching_result = db["purchase_sku_matching_result"]
b2b_purchase_invoice_details = db["b2b_purchase_invoice_details"]

# -------------- Helper utilities (re-used / adapted) --------------

def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    s = re.sub(r'^[\d\.\-\s]+', '', s)
    s = re.sub(r'\s+', ' ', s).strip().upper()
    return s

def build_sku_map():
    sku_map = {}
    cursor = purchase_sku_matching_result.find({}, {"skuID":1, "bill_product":1, "medicine_name":1})
    for doc in cursor:
        if doc.get("bill_product"):
            sku_map[normalize_name(doc["bill_product"])] = doc.get("skuID")
    return sku_map

sku_map = build_sku_map()

def find_sku_for_name(product_name_normalized):
    # exact
    if not product_name_normalized:
        return ""
    if product_name_normalized in sku_map:
        return sku_map[product_name_normalized]
    # startswith fuzzy
    for key in sku_map.keys():
        try:
            if product_name_normalized.startswith(key) or key.startswith(product_name_normalized):
                return sku_map[key]
        except Exception:
            continue
    # substring fuzzy
    for key in sku_map.keys():
        if key in product_name_normalized or product_name_normalized in key:
            return sku_map[key]
    return ""

def parse_double(s):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    s_clean = re.sub(r'[^\d\.\-]', '', s)
    try:
        return float(s_clean)
    except:
        return None

def parse_int(s):
    v = parse_double(s)
    if v is None:
        return None
    try:
        return int(round(v))
    except:
        return None

# Regex to find dates like 01-04-2025 or 1-4-2025
date_re = re.compile(r'\b\d{1,2}-\d{1,2}-\d{4}\b')

# Skip header/footer markers (col-1 content)
SKIP_MARKERS = ["V P SURGICAL", "BILL/ITEM WISE", "BILL/ITEM WISE PURCHASE STATEMENT"]  # check substrings

# ---------- Read Excel ----------
# we use header=None as input has no consistent header row for each section
df = pd.read_excel(EXCEL_PATH, sheet_name=0, header=None, dtype=str, engine='xlrd')
df.fillna("", inplace=True)

rows = []
for _, r in df.iterrows():
    row = [str(x).strip() for x in r.tolist()]
    # keep empty rows as separators (helpful)
    rows.append(row)

# ---------- Parsing state ----------
current_date = None               # datetime.date or None
current_invoice_number = None     # string
current_invoice_items = []
invoices_to_insert = []

skip_continued_counter = 0

def flush_current_invoice():
    global current_invoice_number, current_invoice_items, current_date
    if not current_invoice_number:
        return
    # compute orderAmount and paidAmount as sum(quantity * pts) across items
    order_amount = 0.0
    for it in current_invoice_items:
        q = it.get("quantity") or 0
        pts = it.get("pts") or 0.0
        order_amount += (q * pts)
    doc = {
        "b2bPurchaseInvoiceId": f"EBPI-{uuid.uuid4()}",
        "createdBy": "Siddu-Sales-Script",
        "createdByPos": "ELIXIRE_B2B",
        "createdTime": datetime.now(),
        "entityID": "",                       # per instruction
        "invoiceDate": current_date.strftime("%Y-%m-%d") if current_date else None,
        "invoiceNumber": str(current_invoice_number),
        "items": current_invoice_items,
        "orderAmount": round(order_amount, 2),
        "paidAmount": round(order_amount, 2),
        "paymentStatus": "Paid",
        "purchaseMode": "Credit",
        "supplierID": SUPPLIER_ID,
        "syncDateTime": datetime.now(),
        "updatedBy": "Siddu-Sales-Script",
        "updatedByPos": "ELIXIRE_B2B",
        "updatedTime": datetime.now()
    }
    invoices_to_insert.append(doc)
    # reset invoice
    current_invoice_number = None
    current_invoice_items = []

# helpers for detecting invoice-number rows (first column contains invoice number)
def is_invoice_number_cell(s):
    s = str(s).strip()
    if s == "":
        return False
    # treat numeric-only tokens as invoice numbers (e.g., "28", "977", "69")
    if s.isdigit():
        return True
    # sometimes invoice numbers can be alpha-numeric; if it looks like a short token without spaces:
    if re.match(r'^[A-Za-z0-9\-/]+$', s) and len(s) <= 10 and " " not in s:
        # avoid matching dates by ensuring it's not a date format
        if not date_re.search(s):
            return True
    return False

# iterate rows
for i, row in enumerate(rows):
    # global skip due to a 'continued' found earlier
    if skip_continued_counter > 0:
        skip_continued_counter -= 1
        continue

    # skip completely empty rows
    if all((cell == "" for cell in row)):
        continue

    # skip header/footer lines if first column contains listed markers
    first_col = row[0].upper() if len(row) > 0 else ""
    if any(first_col.lower() in marker.lower() for marker in SKIP_MARKERS):
        # flush any running invoice (some files start new invoice after header)
        # but instruction said skip it, don't change date/invoice
        continue

    # if any cell contains 'continued' (case-insensitive) -> skip next 7 rows and continue
    if any("continued" in (cell or "").lower() for cell in row):
        skip_continued_counter = 7
        continue

    # check for date in first column
    date_found = None
    m = date_re.search(str(row[0].strip()))
    if m:
        try:
            date_found_dt = date_parser.parse(m.group(0), dayfirst=True)
            # set current date (use only date part)
            date_found = date_found_dt.date()
        except Exception:
            date_found = None

    if date_found:
        # flush previous invoice (if any)
        flush_current_invoice()
        current_date = date_found
        continue

    # if no date set yet, we cannot process invoice lines -> skip
    if current_date is None:
        continue

    # skip header-like stray lines where first column empty or text like "Page No..2" etc
    if any("page" in (cell or "").lower() for cell in row):
        continue

    # detect invoice-number row
        # detect invoice-number row
    if is_invoice_number_cell(row[0]):
        # This starts a (possibly new) invoice number under current_date
        inv_candidate = str(row[0]).strip()

        # If invoice number changed, flush existing invoice
        if current_invoice_number is not None and str(current_invoice_number) != inv_candidate:
            flush_current_invoice()

        # set current invoice number (start or continue)
        current_invoice_number = inv_candidate

    # If we reach here and no current invoice number, skip rows (they belong to no invoice)
    if current_invoice_number is None:
        continue

    # Extract columns safely
    def col(idx):
        return row[idx].strip() if idx < len(row) else ""

    medicine_name_raw = col(1)
    if medicine_name_raw == "":
        # sometimes product name ends up in column 2 or 0 - but per your instruction it's column 2
        # attempt fallback to col 2 (index 2) or col 0 if nothing
        if len(row) > 2 and row[2].strip() != "":
            medicine_name_raw = row[2].strip()
        elif len(row) > 0:
            medicine_name_raw = row[0].strip()

    # If row still doesn't have a product name, skip
    if not medicine_name_raw:
        continue

    medicine_name = normalize_name(medicine_name_raw)

    batch = col(2)
    exp_raw = col(3)
    gst_raw = col(4)
    qty_raw = col(5)
    free_raw = col(6)
    pts_raw = col(7)
    discount_raw = col(8)

    # parse numerics
    gst_pct = parse_double(gst_raw) or 0.0
    quantity = parse_int(qty_raw) or 0
    free_qty = parse_int(free_raw) if free_raw != "" else 0
    pts = parse_double(pts_raw) or 0.0   # per item rate
    discount1_pct = parse_double(discount_raw) or 0.0

    # calculate effectivePurchasePriceExGST = pts - (pts * discount1 / 100)
    purchasePriceExGST = pts - (pts * (discount1_pct / 100.0))
    gst_amount = purchasePriceExGST * (gst_pct / 100.0)
    netPurchasePriceForPaidQty = (purchasePriceExGST + gst_amount)
    effectivePurchasePrice = parse_double(((purchasePriceExGST + gst_amount) * quantity) / (quantity + free_qty))

    # parse expDate to yyyy-mm-dd if possible (or keep empty string)
    exp_date_str = ""
    if exp_raw:
        try:
            # some values are like '10/26' meaning mm/yy — attempt to parse intelligently
            # try parser first
            parsed_exp = None
            # handle mm/yy -> convert to first day of month and year
            if re.match(r'^\d{1,2}/\d{2}$', exp_raw):
                parts = exp_raw.split('/')
                month = int(parts[0])
                year_two = int(parts[1])
                year = 2000 + year_two if year_two < 100 else year_two
                parsed_exp = datetime(year, month, 1)
            else:
                parsed_exp = date_parser.parse(exp_raw, dayfirst=False)
            if parsed_exp:
                exp_date_str = parsed_exp.strftime("%Y-%m-%d")
        except Exception:
            # fallback: keep raw string
            exp_date_str = exp_raw.strip()

    # find skuID using normalized product name
    sku_id = find_sku_for_name(medicine_name)

    # Amount per item: you asked for orderAmount/paidAmount to be qty * pts
    amount = parse_double((quantity + free_qty) * effectivePurchasePrice)

    item_obj = {
        "skuID": sku_id or "",
        "medicine_name": medicine_name,
        "pack": "",   # not provided explicitly; leave empty
        "batch": batch,
        "quantity": quantity,
        "mrp": None,
        "ptr": None,
        "pts": round(pts, 2),
        "gst": round(gst_pct, 2),
        "tax": round(gst_amount, 2),
        "expDate": exp_date_str,
        "discount1": round(discount1_pct, 2),
        "free": free_qty,
        "effectPurchasePrice": round(effectivePurchasePrice, 2),
        "effectivePurchasePriceExGST": round(purchasePriceExGST, 2),
        "Amount": amount
    }

    current_invoice_items.append(item_obj)

# flush any last invoice at EOF
flush_current_invoice()

print(f"Prepared {len(invoices_to_insert)} purchase invoice documents for insertion.")

# print a sample (invoiceNumber) for inspection:
if invoices_to_insert:
    # print first 2 for quick check
    pprint(invoices_to_insert[0])

# If you want to actually insert into MongoDB, uncomment below:
# if invoices_to_insert:
#     result = b2b_purchase_invoice_details.insert_many(invoices_to_insert)
#     print(f"Inserted {len(result.inserted_ids)} documents into 'b2b_purchase_invoice_details'.")
# else:
#     print("No invoices to insert.")
