# test_scrape.py
from scraper import scrape_fda_standards
from pdf_html_generator import FDAStandardsProcessor

df = scrape_fda_standards()
print(df['date_of_entry'].head())   # → 2024-12-31  etc.

processor = FDAStandardsProcessor(pdf_path="pdfs", html_path="htmls")
sample = df.iloc[0]
data = processor.extract_detailed_data(sample['title_link'], sample)
print(data["Date_of_Entry"])        # → 2024-12-31