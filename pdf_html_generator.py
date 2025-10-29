import requests
from bs4 import BeautifulSoup
import pandas as pd
from fpdf import FPDF
import os
import logging
import time
import re
import unicodedata
from s3_operations import S3Operations
from config import HEADERS, validate_s3_config
from fda_db_operations import FDADatabaseOperations
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ---------------------- PDF CLASS ----------------------
class StandardsPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_font("Arial", size=10)

    def header(self):
        self.set_font("Arial", 'B', 12)
        self.cell(0, 10, 'FDA Standard', 0, 1, 'C')

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

# ---------------------- MAIN PROCESSOR ----------------------
class FDAStandardsProcessor:
    def __init__(self, pdf_path: str, html_path: str, use_s3: bool = False):
        self.pdf_path = pdf_path
        self.html_path = html_path
        self.use_s3 = use_s3 and validate_s3_config()
        self.s3_ops = S3Operations() if self.use_s3 else None
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        os.makedirs(self.pdf_path, exist_ok=True)
        os.makedirs(self.html_path, exist_ok=True)
        logger.info(f"Processor initialized: PDF={self.pdf_path}, HTML={self.html_path}, S3={self.use_s3}")

    # --------------------------------------------------------------------- #
    # Helper – turn any date‑like object into a printable string
    # --------------------------------------------------------------------- #
    def _format_date(self, value) -> str:
        """Convert pandas Timestamp / NaT / string → 'YYYY‑MM‑DD' or 'N/A'."""
        if pd.isna(value):
            return "N/A"
        if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
            return value.strftime('%Y-%m-%d') if pd.notna(value) else "N/A"
        return str(value).strip() or "N/A"

    def sanitize_text(self, text):
        if text is None:
            return "N/A"
        text = unicodedata.normalize("NFKD", str(text))
        return text.encode("latin-1", "replace").decode("latin-1")

    def sanitize_filename(self, filename):
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = filename.replace('\n', '_').replace('\r', '_').strip()
        return filename

    # --------------------------------------------------------------------- #
    # Extraction – unchanged except final date normalisation
    # --------------------------------------------------------------------- #
    def extract_detailed_data(self, url: str, row: pd.Series) -> dict:
        """Extract detailed data from a standard page, with fallback to DataFrame."""
        try:
            logger.debug(f"Row columns: {row.index.tolist()}")

            data = {
                "FR_Recognition_Number": None,
                "Date_of_Entry": row.get('date_of_entry', None),   # <-- may be Timestamp
                "Standard": None,
                "Scope_Abstract": None,
                "Extent_of_Recognition": None,
                "Standards_Development_Organization": {"Acronym": None, "Name": None, "Website": None}
            }
            logger.info(f"Initial Date_of_Entry from DataFrame: {data['Date_of_Entry']}")

            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # ---------- Date of Entry ----------
            date_labels = ["Date of Entry", "Publication Date", "Posted Date", "Effective Date"]
            date_found = False
            for label in date_labels:
                elem = soup.find(string=re.compile(label, re.IGNORECASE))
                if elem:
                    td = elem.find_parent("td")
                    if td:
                        sibling = td.find_next_sibling("td")
                        if sibling and sibling.text.strip():
                            data["Date_of_Entry"] = sibling.text.strip()
                            date_found = True
                            logger.info(f"Extracted Date_of_Entry from page: {data['Date_of_Entry']}")
                            break
                    parent_text = elem.find_parent().get_text(strip=True) if elem.find_parent() else ""
                    m = re.search(r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b', parent_text)
                    if m:
                        data["Date_of_Entry"] = m.group(1)
                        date_found = True
                        logger.info(f"Extracted Date_of_Entry from nearby text: {data['Date_of_Entry']}")
                        break

            if not date_found and data["Date_of_Entry"] is None:
                logger.warning(f"No valid Date_of_Entry found on page, using DataFrame value: {row.get('date_of_entry', 'N/A')}")
            elif date_found:
                logger.info(f"Overriding with page Date_of_Entry: {data['Date_of_Entry']}")
            else:
                logger.info(f"Retaining DataFrame Date_of_Entry: {data['Date_of_Entry']}")

            # ---------- Other fields ----------
            for field, label in [("FR_Recognition_Number", "FR Recognition Number")]:
                elem = soup.find(string=label)
                if elem:
                    td = elem.find_parent("td").find_next_sibling("td")
                    data[field] = td.text.strip() if td else None

            std_elem = soup.find("td", string="Standard")
            if std_elem:
                tbl = std_elem.find_next("table")
                data["Standard"] = tbl.get_text(" ", strip=True) if tbl else None

            for field, label in [("Scope_Abstract", "Scope/Abstract"),
                                 ("Extent_of_Recognition", "Extent of Recognition")]:
                elem = soup.find("span", string=label)
                if elem:
                    tbl = elem.find_next("table")
                    data[field] = tbl.get_text(" ", strip=True) if tbl else None

            sdo_elem = soup.find("span", string="Standards Development Organization")
            if sdo_elem:
                tr = sdo_elem.find_next("table").find("tr")
                tds = tr.find_all("td") if tr else []
                if len(tds) >= 3:
                    data["Standards_Development_Organization"] = {
                        "Acronym": tds[0].text.strip(),
                        "Name": tds[1].text.strip(),
                        "Website": tds[2].find("a")["href"] if tds[2].find("a") else None
                    }

            # ---------- FINAL DATE NORMALISATION ----------
            data["Date_of_Entry"] = self._format_date(data["Date_of_Entry"])

            return data

        except Exception as e:
            logger.error(f"Error extracting data from {url}: {str(e)}")
            # Fallback – still normalise the date
            fallback = {
                "FR_Recognition_Number": None,
                "Date_of_Entry": row.get('date_of_entry', None),
                "Standard": None,
                "Scope_Abstract": None,
                "Extent_of_Recognition": None,
                "Standards_Development_Organization": {"Acronym": None, "Name": None, "Website": None}
            }
            fallback["Date_of_Entry"] = self._format_date(fallback["Date_of_Entry"])
            logger.info(f"Exception fallback Date_of_Entry: {fallback['Date_of_Entry']}")
            return fallback

    # --------------------------------------------------------------------- #
    # PDF / HTML generation – unchanged (they now receive a clean string)
    # --------------------------------------------------------------------- #
    def generate_pdf(self, data: dict, filename: str) -> str:
        try:
            filename = self.sanitize_filename(filename)
            local_path = os.path.join(self.pdf_path, filename)
            pdf = StandardsPDF()
            pdf.add_page()

            pdf.set_font("Arial", 'B', 12)
            pdf.cell(0, 10, "Standard Information", 0, 1)

            for key, value in data.items():
                if key == "Standards_Development_Organization":
                    pdf.set_font("Arial", 'B', 10)
                    pdf.cell(0, 8, "Standards Development Organization", 0, 1)
                    for subkey, subvalue in value.items():
                        pdf.set_font("Arial", '', 9)
                        pdf.cell(50, 6, f"{self.sanitize_text(subkey)}:", 0, 0)
                        pdf.multi_cell(0, 6, self.sanitize_text(subvalue))
                else:
                    pdf.set_font("Arial", 'B', 10)
                    pdf.cell(50, 6, f"{self.sanitize_text(key)}:", 0, 0)
                    pdf.set_font("Arial", '', 9)
                    pdf.multi_cell(0, 6, self.sanitize_text(value))

            pdf.output(local_path)
            logger.info(f"Generated PDF: {local_path}")
            return local_path

        except Exception as e:
            logger.error(f"Error generating PDF {filename}: {str(e)}")
            return None

    def generate_html(self, data: dict, filename: str) -> str:
        try:
            filename = self.sanitize_filename(filename)
            local_path = os.path.join(self.html_path, filename)
            html_content = "<html><head><title>FDA Standard</title></head><body>"
            html_content += "<h1>FDA Standard Information</h1>"

            for key, value in data.items():
                if key == "Standards_Development_Organization":
                    html_content += "<h2>Standards Development Organization</h2><ul>"
                    for subkey, subvalue in value.items():
                        html_content += f"<li><b>{self.sanitize_text(subkey)}:</b> {self.sanitize_text(subvalue)}</li>"
                    html_content += "</ul>"
                else:
                    html_content += f"<p><b>{self.sanitize_text(key)}:</b> {self.sanitize_text(value)}</p>"

            html_content += "</body></html>"

            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"Generated HTML: {local_path}")
            return local_path

        except Exception as e:
            logger.error(f"Error generating HTML {filename}: {str(e)}")
            return None

    # --------------------------------------------------------------------- #
    # The rest of the class (process_standard, threading, cleanup) is untouched
    # --------------------------------------------------------------------- #
    def process_standard(self, row):
        try:
            url = row['title_link']
            pdf_filename = self.sanitize_filename(row['pdf_filename'])
            html_filename = self.sanitize_filename(row['html_filename'])

            if self.use_s3:
                pdf_s3_key = f"{self.s3_ops.prefix}PDF/{pdf_filename}"
                html_s3_key = f"{self.s3_ops.prefix}HTML/{html_filename}"
                if self.s3_ops.file_exists(pdf_s3_key) and self.s3_ops.file_exists(html_s3_key):
                    logger.info(f"Skipping {url}: PDF and HTML already exist in S3")
                    FDADatabaseOperations().update_s3_paths(url, pdf_filename, html_filename)
                    return True

            data = self.extract_detailed_data(url, row)
            if not data:
                logger.warning(f"No data extracted for {url}")
                return False

            pdf_path = self.generate_pdf(data, pdf_filename)
            html_path = self.generate_html(data, html_filename)

            if self.use_s3:
                pdf_s3_key = f"{self.s3_ops.prefix}PDF/{pdf_filename}"
                html_s3_key = f"{self.s3_ops.prefix}HTML/{html_filename}"

                if self.s3_ops.upload_file(pdf_path, pdf_s3_key, 'application/pdf'):
                    if self.s3_ops.upload_file(html_path, html_s3_key, 'text/html'):
                        FDADatabaseOperations().update_s3_paths(url, pdf_filename, html_filename)
                        os.remove(pdf_path)
                        os.remove(html_path)
                        return True
                    else:
                        logger.error(f"Failed to upload HTML for {url}")
                        return False
                else:
                    logger.error(f"Failed to upload PDF for {url}")
                    return False
            return True

        except Exception as e:
            logger.error(f"Error processing {row.get('recognition_number', 'unknown')}: {str(e)}")
            return False

    def process_unprocessed_standards(self, df: pd.DataFrame):
        if df.empty:
            logger.info("No unprocessed standards")
            return

        successful = 0
        max_workers = min(8, max(2, os.cpu_count() or 4))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_row = {executor.submit(self.process_standard, row): row for _, row in df.iterrows()}
            for future in as_completed(future_to_row):
                if future.result():
                    successful += 1
                time.sleep(0.1)

        logger.info(f"Processed {successful}/{len(df)} standards")

    def __del__(self):
        if hasattr(self, 'session') and self.session:
            self.session.close()











# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from fpdf import FPDF
# import os
# import logging
# import time
# import re
# import unicodedata
# from s3_operations import S3Operations
# from config import HEADERS, validate_s3_config
# from fda_db_operations import FDADatabaseOperations
# from concurrent.futures import ThreadPoolExecutor, as_completed

# logger = logging.getLogger(__name__)

# # ---------------------- PDF CLASS ----------------------
# class StandardsPDF(FPDF):
#     def __init__(self):
#         super().__init__()
#         self.set_font("Arial", size=10)

#     def header(self):
#         self.set_font("Arial", 'B', 12)
#         self.cell(0, 10, 'FDA Standard', 0, 1, 'C')

#     def footer(self):
#         self.set_y(-15)
#         self.set_font("Arial", 'I', 8)
#         self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

# # ---------------------- MAIN PROCESSOR ----------------------
# class FDAStandardsProcessor:
#     def __init__(self, pdf_path: str, html_path: str, use_s3: bool = False):
#         self.pdf_path = pdf_path
#         self.html_path = html_path
#         self.use_s3 = use_s3 and validate_s3_config()
#         self.s3_ops = S3Operations() if self.use_s3 else None
#         self.session = requests.Session()
#         self.session.headers.update(HEADERS)
#         os.makedirs(self.pdf_path, exist_ok=True)
#         os.makedirs(self.html_path, exist_ok=True)
#         logger.info(f"Processor initialized: PDF={self.pdf_path}, HTML={self.html_path}, S3={self.use_s3}")

#     def sanitize_text(self, text):
#         if text is None:
#             return "N/A"
#         text = unicodedata.normalize("NFKD", str(text))
#         return text.encode("latin-1", "replace").decode("latin-1")

#     def sanitize_filename(self, filename):
#         filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
#         filename = filename.replace('\n', '_').replace('\r', '_').strip()
#         return filename

#     def extract_detailed_data(self, url: str, row: pd.Series) -> dict:
#         """Extract detailed data from a standard page, with fallback to DataFrame."""
#         try:
#             # Log available columns in row for debugging
#             logger.debug(f"Row columns: {row.index.tolist()}")
            
#             # Initialize data with DataFrame date_of_entry as default
#             data = {
#                 "FR_Recognition_Number": None,
#                 "Date_of_Entry": row.get('date_of_entry', None),  # Default to DataFrame value
#                 "Standard": None,
#                 "Scope_Abstract": None,
#                 "Extent_of_Recognition": None,
#                 "Standards_Development_Organization": {"Acronym": None, "Name": None, "Website": None}
#             }
#             logger.info(f"Initial Date_of_Entry from DataFrame: {data['Date_of_Entry']}")

#             response = self.session.get(url, timeout=15)
#             response.raise_for_status()
#             soup = BeautifulSoup(response.content, 'html.parser')

#             # Try multiple labels for Date_of_Entry
#             date_labels = ["Date of Entry", "Publication Date", "Posted Date", "Effective Date"]
#             date_found = False
#             for label in date_labels:
#                 elem = soup.find(string=re.compile(label, re.IGNORECASE))
#                 if elem:
#                     td = elem.find_parent("td")
#                     if td:
#                         sibling = td.find_next_sibling("td")
#                         if sibling and sibling.text.strip():
#                             data["Date_of_Entry"] = sibling.text.strip()
#                             date_found = True
#                             logger.info(f"Extracted Date_of_Entry from page: {data['Date_of_Entry']}")
#                             break
#                     # Try finding date in nearby text
#                     parent_text = elem.find_parent().get_text(strip=True) if elem.find_parent() else ""
#                     date_match = re.search(r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b', parent_text)
#                     if date_match:
#                         data["Date_of_Entry"] = date_match.group(1)
#                         date_found = True
#                         logger.info(f"Extracted Date_of_Entry from nearby text: {data['Date_of_Entry']}")
#                         break

#             if not date_found and data["Date_of_Entry"] is None:
#                 logger.warning(f"No valid Date_of_Entry found on page, using DataFrame value: {row.get('date_of_entry', 'N/A')}")
#             elif date_found:
#                 logger.info(f"Overriding with page Date_of_Entry: {data['Date_of_Entry']}")
#             else:
#                 logger.info(f"Retaining DataFrame Date_of_Entry: {data['Date_of_Entry']}")

#             # Extract other fields
#             for field, label in [
#                 ("FR_Recognition_Number", "FR Recognition Number"),
#             ]:
#                 elem = soup.find(string=label)
#                 if elem:
#                     td = elem.find_parent("td").find_next_sibling("td")
#                     data[field] = td.text.strip() if td else None

#             standard_elem = soup.find("td", string="Standard")
#             if standard_elem:
#                 table = standard_elem.find_next("table")
#                 data["Standard"] = table.get_text(" ", strip=True) if table else None

#             for field, label in [
#                 ("Scope_Abstract", "Scope/Abstract"),
#                 ("Extent_of_Recognition", "Extent of Recognition")
#             ]:
#                 elem = soup.find("span", string=label)
#                 if elem:
#                     table = elem.find_next("table")
#                     data[field] = table.get_text(" ", strip=True) if table else None

#             sdo_elem = soup.find("span", string="Standards Development Organization")
#             if sdo_elem:
#                 row = sdo_elem.find_next("table").find("tr")
#                 tds = row.find_all("td") if row else []
#                 if len(tds) >= 3:
#                     data["Standards_Development_Organization"] = {
#                         "Acronym": tds[0].text.strip(),
#                         "Name": tds[1].text.strip(),
#                         "Website": tds[2].find("a")["href"] if tds[2].find("a") else None
#                     }

#             return data

#         except Exception as e:
#             logger.error(f"Error extracting data from {url}: {str(e)}")
#             # Fallback to DataFrame for Date_of_Entry on error
#             data = {
#                 "FR_Recognition_Number": None,
#                 "Date_of_Entry": row.get('date_of_entry', None),
#                 "Standard": None,
#                 "Scope_Abstract": None,
#                 "Extent_of_Recognition": None,
#                 "Standards_Development_Organization": {"Acronym": None, "Name": None, "Website": None}
#             }
#             logger.info(f"Exception fallback to DataFrame date_of_entry: {data['Date_of_Entry']}")
#             return data

#     def generate_pdf(self, data: dict, filename: str) -> str:
#         """Generate PDF from standard data."""
#         try:
#             filename = self.sanitize_filename(filename)
#             local_path = os.path.join(self.pdf_path, filename)
#             pdf = StandardsPDF()
#             pdf.add_page()

#             pdf.set_font("Arial", 'B', 12)
#             pdf.cell(0, 10, "Standard Information", 0, 1)

#             for key, value in data.items():
#                 if key == "Standards_Development_Organization":
#                     pdf.set_font("Arial", 'B', 10)
#                     pdf.cell(0, 8, "Standards Development Organization", 0, 1)
#                     for subkey, subvalue in value.items():
#                         pdf.set_font("Arial", '', 9)
#                         pdf.cell(50, 6, f"{self.sanitize_text(subkey)}:", 0, 0)
#                         pdf.multi_cell(0, 6, self.sanitize_text(subvalue))
#                 else:
#                     pdf.set_font("Arial", 'B', 10)
#                     pdf.cell(50, 6, f"{self.sanitize_text(key)}:", 0, 0)
#                     pdf.set_font("Arial", '', 9)
#                     pdf.multi_cell(0, 6, self.sanitize_text(value))

#             pdf.output(local_path)
#             logger.info(f"Generated PDF: {local_path}")
#             return local_path

#         except Exception as e:
#             logger.error(f"Error generating PDF {filename}: {str(e)}")
#             return None

#     def generate_html(self, data: dict, filename: str) -> str:
#         """Generate HTML from standard data."""
#         try:
#             filename = self.sanitize_filename(filename)
#             local_path = os.path.join(self.html_path, filename)
#             html_content = "<html><head><title>FDA Standard</title></head><body>"
#             html_content += "<h1>FDA Standard Information</h1>"

#             for key, value in data.items():
#                 if key == "Standards_Development_Organization":
#                     html_content += "<h2>Standards Development Organization</h2><ul>"
#                     for subkey, subvalue in value.items():
#                         html_content += f"<li><b>{self.sanitize_text(subkey)}:</b> {self.sanitize_text(subvalue)}</li>"
#                     html_content += "</ul>"
#                 else:
#                     html_content += f"<p><b>{self.sanitize_text(key)}:</b> {self.sanitize_text(value)}</p>"

#             html_content += "</body></html>"

#             with open(local_path, 'w', encoding='utf-8') as f:
#                 f.write(html_content)
#             logger.info(f"Generated HTML: {local_path}")
#             return local_path

#         except Exception as e:
#             logger.error(f"Error generating HTML {filename}: {str(e)}")
#             return None

#     def process_standard(self, row):
#         """Process a single standard row."""
#         try:
#             url = row['title_link']
#             pdf_filename = self.sanitize_filename(row['pdf_filename'])
#             html_filename = self.sanitize_filename(row['html_filename'])

#             # Check if files already exist in S3
#             if self.use_s3:
#                 pdf_s3_key = f"{self.s3_ops.prefix}PDF/{pdf_filename}"
#                 html_s3_key = f"{self.s3_ops.prefix}HTML/{html_filename}"
#                 if self.s3_ops.file_exists(pdf_s3_key) and self.s3_ops.file_exists(html_s3_key):
#                     logger.info(f"Skipping {url}: PDF and HTML already exist in S3")
#                     FDADatabaseOperations().update_s3_paths(url, pdf_filename, html_filename)
#                     return True

#             data = self.extract_detailed_data(url, row)
#             if not data:
#                 logger.warning(f"No data extracted for {url}")
#                 return False

#             pdf_path = self.generate_pdf(data, pdf_filename)
#             html_path = self.generate_html(data, html_filename)

#             if self.use_s3:
#                 pdf_s3_key = f"{self.s3_ops.prefix}PDF/{pdf_filename}"
#                 html_s3_key = f"{self.s3_ops.prefix}HTML/{html_filename}"

#                 if self.s3_ops.upload_file(pdf_path, pdf_s3_key, 'application/pdf'):
#                     if self.s3_ops.upload_file(html_path, html_s3_key, 'text/html'):
#                         FDADatabaseOperations().update_s3_paths(url, pdf_filename, html_filename)
#                         os.remove(pdf_path)
#                         os.remove(html_path)
#                         return True
#                     else:
#                         logger.error(f"Failed to upload HTML for {url}")
#                         return False
#                 else:
#                     logger.error(f"Failed to upload PDF for {url}")
#                     return False
#             return True

#         except Exception as e:
#             logger.error(f"Error processing {row.get('recognition_number', 'unknown')}: {str(e)}")
#             return False

#     def process_unprocessed_standards(self, df: pd.DataFrame):
#         """Process unprocessed standards to generate PDF and HTML."""
#         if df.empty:
#             logger.info("No unprocessed standards")
#             return

#         successful = 0
#         max_workers = min(8, max(2, os.cpu_count() or 4))
#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             future_to_row = {executor.submit(self.process_standard, row): row for _, row in df.iterrows()}
#             for future in as_completed(future_to_row):
#                 if future.result():
#                     successful += 1
#                 time.sleep(0.1)

#         logger.info(f"Processed {successful}/{len(df)} standards")

#     def __del__(self):
#         """Clean up resources."""
#         if hasattr(self, 'session') and self.session:
#             self.session.close()










