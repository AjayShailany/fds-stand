
import logging
import os
import pandas as pd
from scraper import scrape_fda_standards
from fda_db_operations import FDADatabaseOperations, process_fda_standards
from pdf_html_generator import FDAStandardsProcessor
from config import setup_directories, DEFAULT_DOWNLOAD_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def run_full_pipeline(processor: FDAStandardsProcessor, db_ops: FDADatabaseOperations):
    """Run the FDA standards pipeline."""
    try:
        logger.info("Step 1: Scraping FDA standards metadata")
        scraped_df = scrape_fda_standards()
        
        if scraped_df.empty:
            logger.error("No data scraped, exiting pipeline")
            return False
        
        logger.info(f"Scraped {len(scraped_df)} standards")
        
        logger.info("Step 2: Syncing metadata to database")
        result = process_fda_standards(scraped_df)
        
        if not result['success']:
            logger.error(f"Database sync failed: {result['message']}")
            return False
        
        logger.info(f"Inserted {result.get('new_records', 0)} new records")
        logger.info(f"{result.get('pending_pdfs', 0)} documents need processing")
        
        if result.get('new_records', 0) == 0 and result.get('pending_pdfs', 0) == 0:
            logger.info("No new data or documents to process")
            return True
        
        logger.info("Step 3: Processing unprocessed standards")
        unprocessed_df = db_ops.get_unprocessed_standards()
        
        if not unprocessed_df.empty:
            logger.info(f"Processing {len(unprocessed_df)} unprocessed standards")
            processor.process_unprocessed_standards(unprocessed_df)
        else:
            logger.info("No unprocessed standards found")
        
        final_status = db_ops.get_sync_status()
        logger.info(f"Final sync status: {final_status}")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return False

def main():
    """Main pipeline execution."""
    db_ops = None
    try:
        pdf_path, html_path = setup_directories(DEFAULT_DOWNLOAD_DIR)
        logger.info(f"Using PDF directory: {pdf_path}, HTML directory: {html_path}")
        
        logger.info("Initializing database operations")
        db_ops = FDADatabaseOperations()
        
        initial_status = db_ops.get_sync_status()
        logger.info(f"Initial sync status: {initial_status}")
        
        force_reload = os.getenv("FORCE_DB_LOAD", "false").lower() == "true"
        if force_reload:
            logger.info("FORCE_DB_LOAD enabled, resetting S3 paths")
            db_ops.reset_s3_paths()
        
        processor = FDAStandardsProcessor(pdf_path, html_path, use_s3=True)
        
        success = run_full_pipeline(processor, db_ops)
        
        if success:
            logger.info("Pipeline completed successfully")
            return 0
        else:
            logger.error("Pipeline failed")
            return 1
            
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)
        return 1
    finally:
        if db_ops and hasattr(db_ops, 'engine'):
            db_ops.engine.dispose()

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)