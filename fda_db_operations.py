
import pandas as pd
import hashlib
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from typing import Tuple, Dict, Any

load_dotenv(override=True)
logger = logging.getLogger(__name__)


class FDADatabaseOperations:
    def __init__(self):
        self.db_name = os.getenv("DB_NAME", "lexim_gpt_dev")
        self.s3_bucket = 'lexim-international'
        self.s3_prefix = 'FDA_STANDARDS/'
        self.engine = self._create_engine()
        logger.info(f"Database initialized: {self.db_name}")

    def _create_engine(self):
        """Create SQLAlchemy engine."""
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT", "3306")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASS")
        conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{self.db_name}'
        return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

    def generate_unique_id(self, recognition_number: str,
                           standards_developing_organization: str,
                           standard_designation_number_and_date: str) -> str:
        """Generate MD5 hash as unique_id."""
        combined = f"{recognition_number}|{standards_developing_organization}|{standard_designation_number_and_date}"
        return hashlib.md5(combined.encode()).hexdigest()

    def log_duplicates(self, df: pd.DataFrame):
        """Log duplicate standard_title entries."""
        try:
            if 'standard_title' not in df.columns:
                logger.warning("No standard_title column in DataFrame")
                return

            duplicates = df[df.duplicated(subset=['standard_title'], keep=False)]
            if not duplicates.empty:
                duplicate_titles = duplicates['standard_title'].value_counts()
                for title, count in duplicate_titles.items():
                    if count > 1:
                        logger.warning(f"Duplicate standard_title found: '{title}' appears {count} times")
            else:
                logger.info("No duplicate standard_titles found")
        except Exception as e:
            logger.error(f"Error checking for duplicate standard_titles: {str(e)}")

    def check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check for duplicates and return only new records."""
        try:
            if df.empty:
                logger.warning("Empty DataFrame provided")
                return pd.DataFrame()

            required_cols = [
                'recognition_number',
                'standards_developing_organization',
                'standard_designation_number_and_date'
            ]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns: {missing_cols}")
                return pd.DataFrame()

            with self.engine.connect() as conn:
                existing_result = conn.execute(text("""
                    SELECT unique_id FROM fda_standards
                    WHERE unique_id IS NOT NULL
                """))
                existing_ids = {row[0] for row in existing_result.fetchall()}

            df = df.copy()
            df['unique_id'] = df.apply(
                lambda row: self.generate_unique_id(
                    str(row['recognition_number']),
                    str(row['standards_developing_organization']),
                    str(row['standard_designation_number_and_date'])
                ), axis=1
            )

            new_records = df[~df['unique_id'].isin(existing_ids)]
            logger.info(f"Total: {len(df)}, Existing: {len(existing_ids)}, New: {len(new_records)}")
            return new_records

        except Exception as e:
            logger.error(f"Error checking duplicates: {str(e)}")
            return pd.DataFrame()

    def insert_new_standards(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Insert new standards with unique_id."""
        try:
            new_df = self.check_duplicates(df)
            if new_df.empty:
                return True, "No new records to insert"

            new_df = new_df.copy()
            new_df['aws_bucket'] = self.s3_bucket
            new_df['aws_key'] = None
            new_df['aws_html_key'] = None

            if 'date_of_entry' in new_df.columns:
                new_df['date_of_entry'] = pd.to_datetime(
                    new_df['date_of_entry'], format='%m/%d/%Y', errors='coerce'
                ).dt.strftime('%Y-%m-%d')

            defaults = {
                'standard_title': '',
                'specialty_task_group_area': 'UNKNOWN',
                'aws_key': None,
                'aws_html_key': None,
            }

            for col, default in defaults.items():
                if col not in new_df.columns:
                    new_df[col] = default
                else:
                    if default is None:
                        new_df[col] = new_df[col].where(pd.notna(new_df[col]), None)
                    else:
                        new_df[col] = new_df[col].fillna(default)

            new_df.to_sql(
                name='fda_standards',
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=500,
                method='multi'
            )

            logger.info(f"Inserted {len(new_df)} new records")
            return True, f"Inserted {len(new_df)} records"

        except Exception as e:
            logger.error(f"Error inserting standards: {str(e)}")
            return False, f"Insert failed: {str(e)}"

    def update_s3_paths(self, title_link: str, pdf_filename: str, html_filename: str) -> bool:
        """Update database with S3 paths for PDF and HTML."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    UPDATE fda_standards
                    SET aws_key = :pdf_key,
                        aws_html_key = :html_key
                    WHERE title_link = :url
                """), {
                    'pdf_key': f"{self.s3_prefix}PDF/{pdf_filename}",
                    'html_key': f"{self.s3_prefix}HTML/{html_filename}",
                    'url': title_link
                })

                if result.rowcount > 0:
                    logger.info(f"Updated S3 paths for: {title_link}")
                    return True

                logger.warning(f"No record found for {title_link}")
                return False

        except Exception as e:
            logger.error(f"Error updating S3 paths: {str(e)}")
            return False

    def get_unprocessed_standards(self) -> pd.DataFrame:
        """Get standards without PDF or HTML in S3."""
        try:
            query = """
                SELECT recognition_number, standard_title, title_link, unique_id
                FROM fda_standards
                WHERE (aws_key IS NULL OR aws_key = '')
                OR (aws_html_key IS NULL OR aws_html_key = '')
            """
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn)

            if not df.empty:
                df['pdf_filename'] = df.apply(
                    lambda row: f"{row['recognition_number']}_{'_'.join(row['standard_title'].split()[:3]).replace('/', '_').replace('\\', '_')}.pdf",
                    axis=1
                )
                df['html_filename'] = df.apply(
                    lambda row: f"{row['recognition_number']}_{'_'.join(row['standard_title'].split()[:3]).replace('/', '_').replace('\\', '_')}.html",
                    axis=1
                )
                logger.info(f"Found {len(df)} unprocessed standards")

            return df

        except Exception as e:
            logger.error(f"Error getting unprocessed standards: {str(e)}")
            return pd.DataFrame()

    def get_sync_status(self) -> Dict[str, Any]:
        """Get synchronization status."""
        try:
            with self.engine.connect() as conn:
                db_total = conn.execute(text("SELECT COUNT(*) FROM fda_standards")).scalar() or 0
                db_processed = conn.execute(text("""
                    SELECT COUNT(*) FROM fda_standards
                    WHERE aws_key IS NOT NULL AND aws_key != ''
                      AND aws_html_key IS NOT NULL AND aws_html_key != ''
                """)).scalar() or 0

                return {
                    'db_total': db_total,
                    'db_processed': db_processed,
                    'pending': db_total - db_processed
                }

        except Exception as e:
            logger.error(f"Error getting sync status: {str(e)}")
            return {'db_total': 0, 'db_processed': 0, 'pending': 0}

    def reset_s3_paths(self):
        """Reset AWS paths."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    UPDATE fda_standards
                    SET aws_key = NULL, aws_html_key = NULL
                """))
                logger.info(f"Reset S3 paths for {result.rowcount} records")
                return True

        except Exception as e:
            logger.error(f"Error resetting S3 paths: {str(e)}")
            return False

    def __del__(self):
        """Cleanup DB engine."""
        if hasattr(self, 'engine') and self.engine:
            self.engine.dispose()


def process_fda_standards(df: pd.DataFrame) -> Dict[str, Any]:
    """Process FDA standards data with deduplication."""
    db_ops = None
    try:
        db_ops = FDADatabaseOperations()
        status = db_ops.get_sync_status()
        logger.info(f"Sync status: {status}")

        db_ops.log_duplicates(df)

        success, message = db_ops.insert_new_standards(df)
        if not success:
            return {"success": False, "message": message}

        unprocessed = db_ops.get_unprocessed_standards()

        return {
            "success": True,
            "message": message,
            "new_records": len(df) if success else 0,
            "pending_pdfs": len(unprocessed),
            "sync_status": status,
            "unprocessed_df": unprocessed
        }

    except Exception as e:
        logger.error(f"Error processing standards: {str(e)}")
        return {"success": False, "message": str(e)}

    finally:
        if db_ops and hasattr(db_ops, 'engine'):
            db_ops.engine.dispose()
















# import pandas as pd
# import hashlib
# import logging
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# from typing import Tuple, Dict, Any

# load_dotenv(override=True)

# logger = logging.getLogger(__name__)

# class FDADatabaseOperations:
#     def __init__(self):
#         self.db_name = os.getenv("DB_NAME", "lexim_gpt_dev")
#         self.s3_bucket = 'lexim-international'
#         self.s3_prefix = 'FDA_STANDARDS/'
#         self.engine = self._create_engine()
#         logger.info(f"Database initialized: {self.db_name}")

#     def _create_engine(self):
#         """Create SQLAlchemy engine."""
#         db_host = os.getenv("DB_HOST")
#         db_port = os.getenv("DB_PORT", "3306")
#         db_user = os.getenv("DB_USER")
#         db_password = os.getenv("DB_PASS")
#         conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{self.db_name}'
#         return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

#     def generate_unique_id(self, recognition_number: str, standards_developing_organization: str, 
#                           standard_designation_number_and_date: str) -> str:
#         """Generate MD5 hash as unique_id."""
#         combined = f"{recognition_number}|{standards_developing_organization}|{standard_designation_number_and_date}"
#         return hashlib.md5(combined.encode()).hexdigest()

#     def log_duplicates(self, df: pd.DataFrame):
#         """Log duplicate standard_title entries."""
#         try:
#             if 'standard_title' not in df.columns:
#                 logger.warning("No standard_title column in DataFrame")
#                 return

#             duplicates = df[df.duplicated(subset=['standard_title'], keep=False)]
#             if not duplicates.empty:
#                 duplicate_titles = duplicates['standard_title'].value_counts()
#                 for title, count in duplicate_titles.items():
#                     if count > 1:
#                         logger.warning(f"Duplicate standard_title found: '{title}' appears {count} times")
#             else:
#                 logger.info("No duplicate standard_titles found")

#         except Exception as e:
#             logger.error(f"Error checking for duplicate standard_titles: {str(e)}")

#     def check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
#         """Check for duplicates and return only new records."""
#         try:
#             if df.empty:
#                 logger.warning("Empty DataFrame provided")
#                 return pd.DataFrame()
            
#             required_cols = ['recognition_number', 'standards_developing_organization', 'standard_designation_number_and_date']
#             missing_cols = [col for col in required_cols if col not in df.columns]
#             if missing_cols:
#                 logger.error(f"Missing columns: {missing_cols}")
#                 return pd.DataFrame()
            
#             with self.engine.connect() as conn:
#                 existing_result = conn.execute(text("""
#                     SELECT unique_id FROM fda_standards 
#                     WHERE unique_id IS NOT NULL
#                 """))
#                 existing_ids = {row[0] for row in existing_result.fetchall()}
                
#                 df = df.copy()
#                 df['unique_id'] = df.apply(
#                     lambda row: self.generate_unique_id(
#                         str(row['recognition_number']),
#                         str(row['standards_developing_organization']),
#                         str(row['standard_designation_number_and_date'])
#                     ), axis=1
#                 )
                
#                 new_records = df[~df['unique_id'].isin(existing_ids)]
#                 logger.info(f"Total records: {len(df)}, Existing: {len(existing_ids)}, New: {len(new_records)}")
#                 return new_records
                
#         except Exception as e:
#             logger.error(f"Error checking duplicates: {str(e)}")
#             return pd.DataFrame()

#     def insert_new_standards(self, df: pd.DataFrame) -> Tuple[bool, str]:
#         """Insert new standards with unique_id."""
#         try:
#             new_df = self.check_duplicates(df)
            
#             if new_df.empty:
#                 return True, "No new records to insert"
            
#             new_df = new_df.copy()
#             new_df['aws_bucket'] = self.s3_bucket
#             new_df['aws_key'] = None
#             new_df['aws_html_key'] = None
            
#             if 'date_of_entry' in new_df.columns:
#                 new_df['date_of_entry'] = pd.to_datetime(
#                     new_df['date_of_entry'], format='%m/%d/%Y', errors='coerce'
#                 ).dt.strftime('%Y-%m-%d')

#             defaults = {
#                 'standard_title': '',
#                 'specialty_task_group_area': 'UNKNOWN',
#                 'aws_key': None,
#                 'aws_html_key': None,
#             }
#             for col, default in defaults.items():
#                 if col not in new_df.columns:
#                     new_df[col] = default
#                 else:
#                     if default is None:
#                         new_df[col] = new_df[col].where(pd.notna(new_df[col]), None)
#                     else:
#                         new_df[col] = new_df[col].fillna(default)

#             new_df.to_sql(
#                 name='fda_standards',
#                 con=self.engine,
#                 if_exists='append',
#                 index=False,
#                 chunksize=500,
#                 method='multi'
#             )
            
#             logger.info(f"Inserted {len(new_df)} new records")
#             return True, f"Inserted {len(new_df)} records"
            
#         except Exception as e:
#             logger.error(f"Error inserting standards: {str(e)}")
#             return False, f"Insert failed: {str(e)}"

#     def update_s3_paths(self, title_link: str, pdf_filename: str, html_filename: str) -> bool:
#         """Update database with S3 paths for PDF and HTML."""
#         try:
#             with self.engine.begin() as conn:
#                 result = conn.execute(text("""
#                     UPDATE fda_standards 
#                     SET aws_key = :pdf_key, aws_html_key = :html_key
#                     WHERE title_link = :url
#                 """), {
#                     'pdf_key': f"{self.s3_prefix}PDF/{pdf_filename}",
#                     'html_key': f"{self.s3_prefix}HTML/{html_filename}",
#                     'url': title_link
#                 })
                
#                 if result.rowcount > 0:
#                     logger.info(f"Updated S3 paths for PDF: {pdf_filename}, HTML: {html_filename}")
#                     return True
#                 logger.warning(f"No record found for {title_link}")
#                 return False
                
#         except Exception as e:
#             logger.error(f"Error updating S3 paths: {str(e)}")
#             return False


# def get_unprocessed_standards(self) -> pd.DataFrame:
#     """Get standards without PDF or HTML in S3, including date_of_entry."""
#     try:
#         query = """
#             SELECT recognition_number, standard_title, title_link, unique_id, date_of_entry
#             FROM fda_standards 
#             WHERE (aws_key IS NULL OR aws_key = '') OR (aws_html_key IS NULL OR aws_html_key = '')
#         """
#         with self.engine.connect() as conn:
#             df = pd.read_sql_query(text(query), conn)
            
#         if not df.empty:
#             # Convert date_of_entry to MM/DD/YYYY format for consistency
#             if 'date_of_entry' in df.columns:
#                 df['date_of_entry'] = pd.to_datetime(
#                     df['date_of_entry'], errors='coerce'
#                 ).dt.strftime('%m/%d/%Y').fillna('N/A')
            
#             df['pdf_filename'] = df.apply(
#                 lambda row: f"{row['recognition_number']}_{'_'.join(row['standard_title'].split()[:3]).replace('/', '_').replace('\\', '_')}.pdf",
#                 axis=1
#             )
#             df['html_filename'] = df.apply(
#                 lambda row: f"{row['recognition_number']}_{'_'.join(row['standard_title'].split()[:3]).replace('/', '_').replace('\\', '_')}.html",
#                 axis=1
#             )
#             logger.info(f"Found {len(df)} unprocessed standards")
#             logger.info(f"Unprocessed standards sample:\n{df[['recognition_number', 'date_of_entry', 'title_link']].head().to_dict()}")
#             if '7-197' in df['recognition_number'].values:
#                 logger.info(f"CLSI M35-A2 (7-197) data:\n{df[df['recognition_number'] == '7-197'][['recognition_number', 'date_of_entry', 'title_link']].to_dict()}")
#             return df
#         return pd.DataFrame()
#     except Exception as e:
#         logger.error(f"Error getting unprocessed standards: {str(e)}")
#         return pd.DataFrame()

 
# def get_sync_status(self) -> Dict[str, Any]:
#         """Get synchronization status."""
#         try:
#             with self.engine.connect() as conn:
#                 db_total = conn.execute(text("SELECT COUNT(*) FROM fda_standards")).scalar() or 0
#                 db_processed = conn.execute(text("""
#                     SELECT COUNT(*) FROM fda_standards 
#                     WHERE aws_key IS NOT NULL AND aws_key != '' 
#                     AND aws_html_key IS NOT NULL AND aws_html_key != ''
#                 """)).scalar() or 0
                
#                 return {
#                     'db_total': db_total,
#                     'db_processed': db_processed,
#                     'pending': db_total - db_processed
#                 }
#         except Exception as e:
#             logger.error(f"Error getting sync status: {str(e)}")
#             return {'db_total': 0, 'db_processed': 0, 'pending': 0}

# def reset_s3_paths(self):
#         """Reset S3 paths to force reprocessing."""
#         try:
#             with self.engine.begin() as conn:
#                 result = conn.execute(text("""
#                     UPDATE fda_standards 
#                     SET aws_key = NULL, aws_html_key = NULL
#                 """))
#                 logger.info(f"Reset S3 paths for {result.rowcount} records")
#                 return True
#         except Exception as e:
#             logger.error(f"Error resetting S3 paths: {str(e)}")
#             return False

# def __del__(self):
#         """Clean up resources."""
#         if hasattr(self, 'engine') and self.engine:
#             self.engine.dispose()

# def process_fda_standards(df: pd.DataFrame) -> Dict[str, Any]:
#     """Process FDA standards data with deduplication."""
#     db_ops = None
#     try:
#         db_ops = FDADatabaseOperations()
#         status = db_ops.get_sync_status()
#         logger.info(f"Sync status: {status}")
        
#         # Log duplicates before inserting
#         db_ops.log_duplicates(df)
        
#         success, message = db_ops.insert_new_standards(df)
#         if not success:
#             return {"success": False, "message": message}
        
#         unprocessed = db_ops.get_unprocessed_standards()
#         return {
#             "success": True,
#             "message": message,
#             "new_records": len(df) if success else 0,
#             "pending_pdfs": len(unprocessed),
#             "sync_status": status,
#             "unprocessed_df": unprocessed
#         }
        
#     except Exception as e:
#         logger.error(f"Error processing standards: {str(e)}")
#         return {"success": False, "message": str(e)}
#     finally:
#         if db_ops and hasattr(db_ops, 'engine'):
#             db_ops.engine.dispose()