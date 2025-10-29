import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import logging
import time
from config import FDA_BASE_URL, HEADERS, COLUMNS

logger = logging.getLogger(__name__)

def fetch_page(start: int = 1, session: requests.Session = None) -> str:
    """Fetch a page of FDA standards results."""
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
    
    params = {
        "standardsearch": "1",
        "start_search": str(start),
        "pagenum": 500
    }
    resp = session.get(FDA_BASE_URL, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text

def extract_table_rows(soup: BeautifulSoup, header_template=None):
    """Extract table rows from FDA standards results page with carry logic."""
    table = soup.find("table", {"id": "stds-results-table"})
    if not table:
        return [], header_template
      
    rows = []
    carry = [None, None, None, None]  # to handle 3-column continuation rows
    
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        row_texts = [td.get_text(strip=True) for td in tds]

        # Skip header rows
        if header_template is None and any("Date" in txt for txt in row_texts):
            header_template = row_texts
            continue
        if header_template and row_texts == header_template:
            continue

        if len(tds) >= 7:
            a_tag = tds[6].find("a")
            row = {
                "date_of_entry": row_texts[0],
                "specialty_task_group_area": row_texts[1],
                "recognition_number": row_texts[2],
                "extent_of_recognition": row_texts[3],
                "standards_developing_organization": row_texts[4],
                "standard_designation_number_and_date": row_texts[5],
                "standard_title": a_tag.get_text(strip=True) if a_tag else row_texts[6],
                "title_link": urljoin(FDA_BASE_URL, a_tag["href"]) if a_tag else ""
            }
            rows.append(row)
            carry = row_texts[:4]

        elif len(tds) == 3:  # continuation row
            a_tag = tds[2].find("a")
            row = {
                "date_of_entry": carry[0],
                "specialty_task_group_area": carry[1],
                "recognition_number": carry[2],
                "extent_of_recognition": carry[3],
                "standards_developing_organization": row_texts[0],
                "standard_designation_number_and_date": row_texts[1],
                "standard_title": a_tag.get_text(strip=True) if a_tag else row_texts[2],
                "title_link": urljoin(FDA_BASE_URL, a_tag["href"]) if a_tag else ""
            }
            rows.append(row)

    return rows, header_template

def scrape_fda_standards() -> pd.DataFrame:
    """Scrape FDA standards metadata to DataFrame with pagination and carry logic."""
    logger.info("Scraping FDA standards")
    all_rows = []
    start = 1
    header_template = None
    session = requests.Session()
    session.headers.update(HEADERS)

    while True:
        logger.info(f"Scraping page starting at record {start}...")
        try:
            html = fetch_page(start, session)
            soup = BeautifulSoup(html, "html.parser")
            page_rows, header_template = extract_table_rows(soup, header_template)

            if not page_rows:
                logger.info("No more rows found, stopping")
                break

            all_rows.extend(page_rows)

            if len(page_rows) < 500:
                break  # last page
            start += 500
            time.sleep(1)  # Reduced sleep time for faster scraping

        except Exception as e:
            logger.error(f"Error scraping page starting at {start}: {str(e)}")
            break

    session.close()
    df = pd.DataFrame(all_rows, columns=COLUMNS).fillna("").astype(str)
    logger.info(f"Scraped {len(df)} total standards")
    return df