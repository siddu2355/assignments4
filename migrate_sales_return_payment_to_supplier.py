import pymongo
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
DB_URI = "mongodb+srv://careeco_migration:0bkBr2KG8cNZN8AI@maincluster.vsonmgq.mongodb.net/"
CREATED_BY = "migrate_store_to_supplier_script"
CREATED_BY_POS = "ELIXIRE_MIGRATION"

# Dry Run Flag
DRY_RUN = True  # Default to True for safety

client = pymongo.MongoClient(DB_URI)
db_main = client["main"]
db_aux = client["aux"]
db_archive = client["archive"]

# Collections
supplier_details_col = db_main["supplier_details"]
archived_store_col = db_archive["archived_elixire_store_details"]

TARGET_COLS = [
    {"db": db_main, "name": "b2b_sales_payment_transaction_log"},
    {"db": db_aux, "name": "b2b_sales_payment_transaction_log"},
]

# The list of storeIDs to migrate
STORE_IDS = [
    "ELXS115512254", "ELXS115530790", "ELXS117092486", "ELXS117639967", "ELXS120601029",
    "ELXS121968798", "ELXS123879157", "ELXS129562942", "ELXS129929036", "ELXS131616552",
    "ELXS134420448", "ELXS135987839", "ELXS139092540", "ELXS139249637", "ELXS139689013",
    "ELXS140829298", "ELXS145459336", "ELXS148962994", "ELXS149876044", "ELXS152849161",
    "ELXS155014923", "ELXS156824157", "ELXS165823826", "ELXS169080651", "ELXS170979013",
    "ELXS172716", "ELXS172716_Nauroji", "ELXS180498451", "ELXS187505092", "ELXS188108204",
    "ELXS188119152", "ELXS190436163", "ELXS192287169", "ELXS192356697", "ELXS192767682",
    "ELXS193131495", "ELXS200258344", "ELXS203155051", "ELXS206309204", "ELXS208820380",
    "ELXS211715365", "ELXS215430833", "ELXS216047311", "ELXS223055773", "ELXS223101178",
    "ELXS225175548", "ELXS232260549", "ELXS236743730", "ELXS241957024", "ELXS241985934",
    "ELXS242591702", "ELXS249581026", "ELXS258118883", "ELXS261635877", "ELXS263397966",
    "ELXS267480056", "ELXS269644906", "ELXS271193558", "ELXS275169688", "ELXS277511647",
    "ELXS286707935", "ELXS290102211", "ELXS291069884", "ELXS292216379", "ELXS298418282",
    "ELXS302203358", "ELXS305489863", "ELXS306692822", "ELXS307486503", "ELXS312864487",
    "ELXS318351938", "ELXS319561242", "ELXS321125158", "ELXS321212894", "ELXS323290407",
    "ELXS324287738", "ELXS326905116", "ELXS327613885", "ELXS335471069", "ELXS340403732",
    "ELXS343639382", "ELXS345505437", "ELXS350663220", "ELXS357984208", "ELXS359334942",
    "ELXS363976424", "ELXS367772548", "ELXS370125043", "ELXS376430057", "ELXS380378365",
    "ELXS384528968", "ELXS386156057", "ELXS387154464", "ELXS389198231", "ELXS393914076",
    "ELXS402086952", "ELXS406775431", "ELXS409937928", "ELXS416283092", "ELXS422089769",
    "ELXS424342573", "ELXS424359039", "ELXS426984465", "ELXS427917010", "ELXS429619407",
    "ELXS437997362", "ELXS440390518", "ELXS448382145", "ELXS448387543", "ELXS450519810",
    "ELXS451066064", "ELXS452847969", "ELXS454944221", "ELXS457462678", "ELXS458792150",
    "ELXS464330742", "ELXS468213405", "ELXS477552317", "ELXS483640400", "ELXS494363885",
    "ELXS494939230", "ELXS499556182", "ELXS502509099", "ELXS506748167", "ELXS507750097",
    "ELXS512883103", "ELXS513823901", "ELXS516946144", "ELXS519390008", "ELXS519727361",
    "ELXS521409278", "ELXS521518281", "ELXS522433953", "ELXS526387637", "ELXS526855827",
    "ELXS535067269", "ELXS538672691", "ELXS540300902", "ELXS541195310", "ELXS547939342",
    "ELXS548087206", "ELXS551814745", "ELXS554216426", "ELXS568062098", "ELXS575351062",
    "ELXS576768649", "ELXS579239488", "ELXS582880088", "ELXS585728228", "ELXS595283071",
    "ELXS596711831", "ELXS600551605", "ELXS602216605", "ELXS603335834", "ELXS606635940",
    "ELXS608067786", "ELXS609458920", "ELXS615269312", "ELXS615520790", "ELXS617375829",
    "ELXS623032784", "ELXS628589445", "ELXS633072630", "ELXS633363230", "ELXS636819501",
    "ELXS640734587", "ELXS644733710", "ELXS646560957", "ELXS648360306", "ELXS649425530",
    "ELXS653331914", "ELXS667288782", "ELXS667778279", "ELXS670060360", "ELXS671188205",
    "ELXS675586492", "ELXS678451487", "ELXS678709005", "ELXS679088001", "ELXS680791958",
    "ELXS686412970", "ELXS689765093", "ELXS695097394", "ELXS695629226", "ELXS697421123",
    "ELXS700942002", "ELXS705080654", "ELXS705129052", "ELXS707471933", "ELXS708261810",
    "ELXS710198664", "ELXS715745125", "ELXS718357020", "ELXS718885222", "ELXS719312377",
    "ELXS720344576", "ELXS722459335", "ELXS724281538", "ELXS725877422", "ELXS726731169",
    "ELXS728245319", "ELXS731018745", "ELXS732209060", "ELXS732258895", "ELXS733436914",
    "ELXS734522972", "ELXS735739702", "ELXS740265236", "ELXS741641412", "ELXS742333986",
    "ELXS743189873", "ELXS743209560", "ELXS745781252", "ELXS747567422", "ELXS747897450",
    "ELXS755742385", "ELXS764513143", "ELXS766052970", "ELXS766439564", "ELXS770200664",
    "ELXS776102984", "ELXS776860901", "ELXS781281278", "ELXS785168457", "ELXS786022236",
    "ELXS786490035", "ELXS793495874", "ELXS793959810", "ELXS795542190", "ELXS811392812",
    "ELXS816109188", "ELXS817182804", "ELXS830975004", "ELXS831067931", "ELXS831416724",
    "ELXS833803846", "ELXS835416394", "ELXS844258369", "ELXS846248170", "ELXS847476116",
    "ELXS848795716", "ELXS849781836", "ELXS853110176", "ELXS856455633", "ELXS864119585",
    "ELXS864645887", "ELXS865929724", "ELXS879846217", "ELXS882795019", "ELXS884359427",
    "ELXS884502545", "ELXS891652039", "ELXS895383381", "ELXS899802327", "ELXS905250662",
    "ELXS906227706", "ELXS907990693", "ELXS913010081", "ELXS914101916", "ELXS914124368",
    "ELXS915345548", "ELXS929602", "ELXS931760633", "ELXS932627757", "ELXS933724922",
    "ELXS937449117", "ELXS941608494", "ELXS945027668", "ELXS947878238", "ELXS950331514",
    "ELXS953666030", "ELXS954288375", "ELXS954408940", "ELXS954921420", "ELXS957726274",
    "ELXS960038100", "ELXS964165959", "ELXS965173065", "ELXS968578540", "ELXS971956736",
    "ELXS975242944", "ELXS979591621", "ELXS981703497", "ELXS985698492", "ELXS986256800",
    "ELXS986603592", "ELXS989897555", "ELXS990014700", "ELXS998663077", "ELXS999444759"
]

QUERY = {
    "storeID": {"$in": STORE_IDS},
    "supplierID": "SUP212602"
}

def get_store_name(store_id):
    store_doc = archived_store_col.find_one({"storeID": store_id}, {"storeName": 1})
    if store_doc:
        return store_doc.get("storeName")
    return None

def get_new_supplier_id(store_name):
    if not store_name:
        return None
    supplier_doc = supplier_details_col.find_one({"entityName": store_name}, {"supplierID": 1})
    if supplier_doc:
        return supplier_doc.get("supplierID")
    return None

def run_migration():
    logger.info("Starting migration script...")
    
    # Store mapping to avoid redundant DB calls
    store_to_supplier_map = {}
    
    total_processed = 0
    total_updated = 0
    
    for target in TARGET_COLS:
        db = target["db"]
        col_name = target["name"]
        collection = db[col_name]
        
        logger.info(f"Processing collection: {db.name}.{col_name}")
        
        # Find records matching the query
        records = list(collection.find(QUERY))
        logger.info(f"  Found {len(records)} records matching the query.")
        
        for record in records:
            store_id = record.get("storeID")
            if not store_id:
                continue
            
            # Use cached mapping if available
            if store_id not in store_to_supplier_map:
                store_name = get_store_name(store_id)
                if not store_name:
                    logger.warning(f"    No storeName found for storeID: {store_id} in db_archive")
                    continue
                
                new_sup_id = get_new_supplier_id(store_name)
                if not new_sup_id:
                    logger.warning(f"    No supplier found for entityName: {store_name} in db_main")
                    continue
                
                store_to_supplier_map[store_id] = new_sup_id
            
            client_supplier_id = store_to_supplier_map[store_id]
            
            # Prepare update
            update_op = {
                "$unset": {"storeID": ""},
                "$set": {
                    "clientSupplierID": client_supplier_id,
                    "updatedBy": CREATED_BY,
                    "updatedByPos": CREATED_BY_POS,
                    "updatedTime": datetime.now() # User requested not to use UTC
                }
            }
            
            if not DRY_RUN:
                collection.update_one({"_id": record["_id"]}, update_op)
                total_updated += 1
            else:
                logger.debug(f"    [DRY RUN] Would update record {record['_id']}: storeID {store_id} -> clientSupplierID {client_supplier_id}")
            
            total_processed += 1

    logger.info("=" * 40)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 40)
    logger.info(f"Dry Run: {DRY_RUN}")
    logger.info(f"Records Found/Processed: {total_processed}")
    logger.info(f"Records Updated:        {total_updated}")
    logger.info(f"Unique Stores Mapped:    {len(store_to_supplier_map)}")
    logger.info("=" * 40)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="Run the actual migration (default is dry-run)")
    args = parser.parse_args()

    if args.run:
        DRY_RUN = False
        logger.info("!!! ACTUALLY RUNNING MIGRATION !!!")
    else:
        logger.info("DRY RUN MODE - No changes will be made to the DB")

    run_migration()
