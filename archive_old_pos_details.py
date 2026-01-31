import pymongo
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database clients
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

controls_db = client["controls"]
main_db = client["main"]

elixire_license_running_state = controls_db["elixire_license_running_state"]
elixire_pos_details = main_db["elixire_pos_details"]
elixire_pos_details_archive = main_db["elixire_pos_details_archive"]
elixire_document_deletion_audit = main_db["elixire_document_deletion_audit"]

def archive_old_pos_details(dry_run=True):
    """
    Archive POS details for POS IDs where ALL sessionStartTime records are older than 1 month
    
    Args:
        dry_run (bool): If True, only show what would be archived without actually doing it
    """
    try:
        # Calculate date threshold (1 month ago from now)
        one_month_ago = datetime.now() - timedelta(days=30)
        logger.info(f"Looking for POS IDs where ALL sessionStartTime records are older than: {one_month_ago}")
        
        # Step 1: Find all records in elixire_license_running_state and group by posID
        all_sessions = elixire_license_running_state.find({})
        
        # Group sessions by posID
        pos_sessions = {}
        for session in all_sessions:
            if "posID" in session:
                pos_id = session["posID"]
                if pos_id not in pos_sessions:
                    pos_sessions[pos_id] = []
                pos_sessions[pos_id].append(session)
        
        # Get all posIDs that have at least one session record
        pos_ids_with_sessions = set(pos_sessions.keys())
        
        # Find posIDs where ALL records are older than 1 month based on sessionStartTime
        pos_ids_old_sessions = set()
        for pos_id, sessions in pos_sessions.items():
            all_old = True
            for session in sessions:
                session_start_time = session.get("sessionStartTime")
                if session_start_time and session_start_time >= one_month_ago:
                    all_old = False
                    break
            if all_old:
                pos_ids_old_sessions.add(pos_id)
        
        # Step 2: Find all posIDs in elixire_pos_details to identify those without any session records
        all_pos_details = elixire_pos_details.find({}, {"posID": 1})
        pos_ids_in_details = set()
        for pos_detail in all_pos_details:
            if "posID" in pos_detail:
                pos_ids_in_details.add(pos_detail["posID"])
        
        # Find posIDs that exist in pos_details but have no session records
        pos_ids_no_sessions = pos_ids_in_details - pos_ids_with_sessions
        
        # Combine both sets: posIDs with old sessions AND posIDs with no sessions
        pos_ids_to_archive = pos_ids_old_sessions.union(pos_ids_no_sessions)
        
        logger.info(f"Found {len(pos_ids_old_sessions)} posIDs with old sessions")
        logger.info(f"Found {len(pos_ids_no_sessions)} posIDs with no session records")
        logger.info(f"Total {len(pos_ids_to_archive)} unique posIDs to archive")
        
        if not pos_ids_to_archive:
            logger.info("No old sessions or orphaned POS records found. Nothing to archive.")
            return
        
        # Step 3: Find and move records from elixire_pos_details to archive
        pos_ids_list = list(pos_ids_to_archive)
        pos_details_to_archive = elixire_pos_details.find({
            "posID": {"$in": pos_ids_list}
        })
        
        archived_count = 0
        audit_records = []
        pos_details_list = []
        
        for pos_detail in pos_details_to_archive:
            pos_details_list.append(pos_detail)
            archived_count += 1
        
        if dry_run:
            logger.info("=" * 60)
            logger.info("DRY RUN MODE - No actual changes will be made")
            logger.info("=" * 60)
            logger.info(f"Would archive {archived_count} POS details records")
            logger.info(f"Would create {archived_count} audit records")
            logger.info(f"POS IDs with old sessions: {len(pos_ids_old_sessions)}")
            logger.info(f"POS IDs with no session records: {len(pos_ids_no_sessions)}")
            logger.info(f"Total POS IDs to be archived: {', '.join(pos_ids_list)}")
            
            # Show details of records to be deleted
            logger.info("\nRecords that would be archived:")
            for i, pos_detail in enumerate(pos_details_list[:10]):  # Show first 10
                pos_id = pos_detail.get('posID', 'N/A')
                has_session = pos_id in pos_ids_with_sessions
                reason = "old sessions" if pos_id in pos_ids_old_sessions else "no session records"
                logger.info(f"  {i+1}. posID: {pos_id}, "
                           f"storeID: {pos_detail.get('storeID', 'N/A')}, "
                           f"reason: {reason}, "
                           f"_id: {pos_detail['_id']}")
            
            if len(pos_details_list) > 10:
                logger.info(f"  ... and {len(pos_details_list) - 10} more records")
            
            logger.info("\nTo perform actual archival, run with dry_run=False")
            return
        
        # Actual archival logic (only runs when dry_run=False)
        logger.info(f"Starting actual archival of {archived_count} records...")
        
        for pos_detail in pos_details_list:
            # Insert into archive collection
            pos_detail_copy = pos_detail.copy()
            pos_detail_copy["archivedTime"] = datetime.now()
            pos_detail_copy["archivedBy"] = "archive_old_pos_details.py"
            
            elixire_pos_details_archive.insert_one(pos_detail_copy)
            
            # Create audit record
            pos_id = pos_detail.get("posID", "")
            if pos_id in pos_ids_old_sessions:
                reason = "all sessionStartTime records older than 1 month"
            else:
                reason = "no session records found in elixire_license_running_state"
                
            audit_record = {
                "deleted_id": pos_detail["_id"],
                "db_name": "main",
                "collection_name": "elixire_pos_details",
                "storeID": pos_detail.get("storeID", ""),
                "posID": pos_id,
                "archivedReason": reason,
                "createdTime": datetime.now(),
                "updatedTime": datetime.now()
            }
            audit_records.append(audit_record)
            
            # Delete from original collection
            elixire_pos_details.delete_one({"_id": pos_detail["_id"]})
            
            archived_count += 1
            
            if archived_count:
                logger.info(f"Archived {archived_count} records...")
        
        # Step 3: Insert audit records
        if audit_records:
            elixire_document_deletion_audit.insert_many(audit_records)
            logger.info(f"Created {len(audit_records)} audit records")
        
        logger.info(f"Successfully archived {archived_count} POS details records")
        logger.info(f"Processed posIDs: {', '.join(pos_ids_list[:10])}{'...' if len(pos_ids_list) > 10 else ''}")
        
    except Exception as e:
        logger.error(f"Error during archival process: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting POS details archival process...")
    
    # Run in dry run mode by default
    archive_old_pos_details(dry_run=False)
    
    logger.info("Archival process completed.")
