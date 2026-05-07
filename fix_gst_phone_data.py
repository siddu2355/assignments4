import pymongo
import openpyxl
from datetime import datetime
import logging

# ─── Setup Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─── DB Clients ───────────────────────────────────────────────────────────────
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
main_db = client["main"]

store_col   = main_db["elixire_store_details"]
entity_col  = main_db["elixire_entity_details"]
user_col    = main_db["elixire_user"]

EXCEL_PATH  = "Hell.xlsx"
UPDATER_ID  = "siddu_fix_gst_phone_data"
UPDATER_POS = "siddu_fix_gst_phone_data"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def normalize_str(val) -> str:
    """Strip, lower, collapse whitespace."""
    if val is None:
        return ""
    return " ".join(str(val).strip().lower().split())


def parse_phones(raw) -> list[str]:
    """
    Parse a cell that might contain 1 or more space-separated phone numbers.
    Returns a list of cleaned 10-digit numbers (strips leading 0 / +91 if needed).
    """
    if not raw:
        return []
    phones = []
    for token in str(raw).split():
        token = token.strip()
        # Remove country code prefix
        if token.startswith("+91"):
            token = token[3:]
        if token.startswith("91") and len(token) == 12:
            token = token[2:]
        if token.startswith("0") and len(token) == 11:
            token = token[1:]
        if token.isdigit() and len(token) == 10:
            phones.append(token)
    return phones


def pick_user_phone(phones: list[str]) -> str | None:
    """Pick the first 10-digit number to use as the user's phoneNumber (+91 prefix)."""
    for p in phones:
        if len(p) == 10 and p.isdigit():
            return "+91" + p
    return None


# ─── Load Excel ───────────────────────────────────────────────────────────────

def load_excel_rows(path: str) -> list[dict]:
    """
    Load Hell.xlsx and return a list of dicts with keys:
      enriched_store_name, shop_no, street, locality, area, city, state
      phones (list[str]), gst (str)
    Only rows where Matched == True are considered.
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        # Columns (0-indexed):
        # 0: Original Store Name (col A, None header)
        # 1: Original Address   (col B, None header)
        # 2: Original Store Name (col C)
        # 3: Enriched Store Name (col D)
        # 4: Shop No             (col E)
        # 5: Street Name         (col F)
        # 6: Locality            (col G)
        # 7: Area                (col H)
        # 8: City                (col I)
        # 9: State               (col J)
        # 10: Pin                (col K)
        # 11: Matched            (col L)
        # 12: Phone Number       (col M)
        # 13: GSTIN              (col N)

        matched = row[11]
        if not matched:
            continue

        enriched_name = normalize_str(row[3])
        shop_no       = normalize_str(row[4])
        street        = normalize_str(row[5])
        locality      = normalize_str(row[6])
        area          = normalize_str(row[7])
        city          = normalize_str(row[8])
        state         = normalize_str(row[9])
        phones        = parse_phones(row[12])
        gst           = str(row[13]).strip() if row[13] else ""

        rows.append({
            "enriched_store_name": enriched_name,
            "shop_no":   shop_no,
            "street":    street,
            "locality":  locality,
            "area":      area,
            "city":      city,
            "state":     state,
            "phones":    phones,
            "gst":       gst,
        })

    logger.info(f"Loaded {len(rows)} matched rows from Excel.")
    return rows


# ─── Matching ─────────────────────────────────────────────────────────────────

def build_match_key(excel_row: dict) -> tuple:
    """Build a composite key from the excel row for matching against DB."""
    return (
        excel_row["enriched_store_name"],
        excel_row["shop_no"],
        excel_row["locality"],
        excel_row["street"],
        excel_row["area"],
        excel_row["city"],
        excel_row["state"],
    )


def store_match_key(store: dict) -> tuple:
    """Build a composite key from a store_details DB record."""
    return (
        normalize_str(store.get("storeName")),
        normalize_str(store.get("shopNo")),
        normalize_str(store.get("locality")),
        normalize_str(store.get("street")),
        normalize_str(store.get("area")),
        normalize_str(store.get("city")),
        normalize_str(store.get("state")),
    )


# ─── Core Fix Function ────────────────────────────────────────────────────────

def fix_gst_and_phone(dry_run: bool = True):
    """
    For each row in Hell.xlsx (Matched == True):
      1. Find the matching store in elixire_store_details.
      2. Skip if store has `live: True`.
      3. Update GST and storeContact (phone 1) in store_details.
      4. Update gst, pocContactNumber (phone 1), pocAlternateContactNumber (phone 2)
         in matching entity_details.
      5. Update phoneNumber in elixire_user where storeID array contains matching storeID.
    """
    excel_rows = load_excel_rows(EXCEL_PATH)

    # Pre-load all stores into memory (small enough collection)
    all_stores = list(store_col.find({}))
    logger.info(f"Fetched {len(all_stores)} stores from DB.")

    # Build index: match_key -> store doc
    store_index: dict[tuple, list] = {}
    for store in all_stores:
        key = store_match_key(store)
        store_index.setdefault(key, []).append(store)

    stats = {
        "matched":          0,
        "skipped_live":     0,
        "skipped_no_match": 0,
        "store_updated":    0,
        "entity_updated":   0,
        "user_updated":     0,
    }

    for excel_row in excel_rows:
        key = build_match_key(excel_row)
        candidates = store_index.get(key, [])

        if not candidates:
            logger.warning(
                f"NO MATCH in DB for: {excel_row['enriched_store_name']} | "
                f"shopNo={excel_row['shop_no']} | locality={excel_row['locality']} | "
                f"street={excel_row['street']} | area={excel_row['area']} | "
                f"city={excel_row['city']}"
            )
            stats["skipped_no_match"] += 1
            continue

        for store in candidates:
            store_id  = store.get("storeID", "")
            entity_id = store.get("entityID", "")

            # ── Guard: skip live stores ──────────────────────────────────────
            if store.get("live") is True:
                logger.info(f"SKIPPED (live=True): storeID={store_id}")
                stats["skipped_live"] += 1
                continue

            stats["matched"] += 1
            phones = excel_row["phones"]
            gst    = excel_row["gst"]

            phone1 = phones[0] if len(phones) >= 1 else None
            phone2 = phones[1] if len(phones) >= 2 else None

            # logger.info(
            #     f"Processing storeID={store_id} | entityID={entity_id} | "
            #     f"phone1={phone1} | phone2={phone2} | gst={gst}"
            # )

            # ── 1. Update elixire_store_details ──────────────────────────────
            store_update: dict = {}
            store_update["GST"] = gst or ""
            store_update["storeContact"] = phone1 or ""
            store_update["username"] = phone1 if phone1 and phone1.startswith("+") else ("+" + phone1 if phone1 and phone1.startswith("91") else ("+91" + phone1 if phone1 else None))

            if store_update:
                store_update["updatedBy"]     = UPDATER_ID
                store_update["updatedByPos"]  = UPDATER_POS
                store_update["updatedTime"]   = datetime.now()
                store_update["updatedReason"] = "GST and phone correction (fix_gst_phone_data.py)"

                if dry_run:
                    pass
                    # logger.info(f"  [DRY RUN] store_details update for {store_id}: {store_update}")
                else:
                    result = store_col.update_one(
                        {"_id": store["_id"]},
                        {"$set": store_update},
                    )
                    logger.info(
                        f"  store_details updated: storeID={store_id}, "
                        f"matched={result.matched_count}, modified={result.modified_count}"
                    )
                    stats["store_updated"] += result.modified_count

            # ── 2. Update elixire_entity_details ─────────────────────────────
            entity_update: dict = {}
            entity_update["gst"] = gst or ""
            entity_update["pocContactNumber"] = phone1 or ""
            entity_update["pocAlternateContactNumber"] = phone2 or ""

            if entity_update:
                entity_update["updatedBy"]     = UPDATER_ID
                entity_update["updatedByPos"]  = UPDATER_POS
                entity_update["updatedTime"]   = datetime.now()
                entity_update["updatedReason"] = "GST and phone correction (fix_gst_phone_data.py)"

                if dry_run:
                    pass
                    # logger.info(f"  [DRY RUN] entity_details update for {entity_id}: {entity_update}")
                else:
                    result = entity_col.update_one(
                        {"entityID": entity_id},
                        {"$set": entity_update},
                    )
                    logger.info(
                        f"  entity_details updated: entityID={entity_id}, "
                        f"matched={result.matched_count}, modified={result.modified_count}"
                    )
                    stats["entity_updated"] += result.modified_count

            # ── 3. Update elixire_user ────────────────────────────────────────
            user_phone = pick_user_phone(phones)
            if user_phone:
                if dry_run:
                    pass
                    # logger.info(
                    #     f"  [DRY RUN] user update: storeID.storeID={store_id} → phoneNumber={user_phone}"
                    # )
                else:
                    result = user_col.update_one(
                        {"storeID": {"$elemMatch": {"storeID": store_id}}},
                        {
                            "$set": {
                                "phoneNumber":  user_phone,
                                "updatedBy":    UPDATER_ID,
                                "updatedByPos": UPDATER_POS,
                                "updatedTime":  datetime.now(),
                            }
                        },
                    )
                    logger.info(
                        f"  user updated: storeID={store_id}, phoneNumber={user_phone}, "
                        f"matched={result.matched_count}, modified={result.modified_count}"
                    )
                    stats["user_updated"] += result.modified_count

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    if dry_run:
        logger.info("Mode: DRY RUN — no changes were written to DB")
    else:
        logger.info("Mode: LIVE — changes written to DB")
    logger.info(f"  Excel rows processed (matched):  {stats['matched']}")
    logger.info(f"  Skipped (live=True):             {stats['skipped_live']}")
    logger.info(f"  Skipped (no DB match):           {stats['skipped_no_match']}")
    if not dry_run:
        logger.info(f"  store_details docs modified:     {stats['store_updated']}")
        logger.info(f"  entity_details docs modified:    {stats['entity_updated']}")
        logger.info(f"  user docs modified:              {stats['user_updated']}")


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting GST & phone correction job...")

    # ── Set dry_run=True first to preview, then flip to False to commit ──────
    fix_gst_and_phone(dry_run=False)

    logger.info("Job completed.")
