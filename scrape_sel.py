import os
import pandas as pd
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import re
import time

# Load environment variables
load_dotenv()
openai_key = os.getenv("OPENAI_APIKEY")

graph_config = {
    "llm": {
        "api_key": openai_key,
        "model": "openai/gpt-4o-mini",
    },
    "verbose": True,
    "headless": False,
}

def json_to_dataframe(json_data):
    try:
        if 'Rates for fuel acquired' in json_data:
            rates_data = json_data['Rates for fuel acquired']
            all_rows = []
            for table_name, table in rates_data.items():
                title = table['Period']
                for entry in table['Data']:
                    row = {'Title': title}
                    row.update(entry)
                    all_rows.append(row)
            df = pd.DataFrame(all_rows)
            return df
        else:
            raise KeyError("Key 'Rates for fuel acquired' not found in JSON data.")
    except Exception as e:
        print(f"Error converting JSON to DataFrame: {e}")
        return None

def extract_dates(title):
    date_pattern = r'(\d{1,2} \w+ \d{4}) to (\d{1,2} \w+ \d{4})'
    match = re.search(date_pattern, title)
    if match:
        start_date = datetime.strptime(match.group(1), '%d %B %Y').strftime('%m/%d/%Y')
        end_date = datetime.strptime(match.group(2), '%d %B %Y').strftime('%m/%d/%Y')
        return pd.Series([start_date, end_date])
    return pd.Series([None, None])

def clean_rate(rate):
    return float(rate.split(' ')[0])

fuel_type_mapping = {
    'Liquid fuels (for example, diesel or petrol)': 'FT1',
    'Blended fuels: B5, B20, E10': 'FT2',
    'Liquefied petroleum gas (LPG)': 'FT3',
    'Liquefied natural gas (LNG) or compressed natural gas (CNG)': 'FT4',
    'Blended fuel: E85': 'FT5',
    'B100': 'FT6'
}

def update_rates_table():
    # Load the data from update.xlsx
    try:
        update_df = pd.read_excel("update.xlsx")
    except FileNotFoundError:
        print("update.xlsx not found.")
        return

    # Load the data from FTC Rates.xlsx (sheet: rates)
    try:
        ftc_df = pd.read_excel("FTC Rates.xlsx", sheet_name="rates")
    except FileNotFoundError:
        print("FTC Rates.xlsx not found.")
        return

    # Ensure Start Date and End Date are in timestamp format for both DataFrames
    update_df['Start Date'] = pd.to_datetime(update_df['Start Date'], errors='coerce')
    update_df['End Date'] = pd.to_datetime(update_df['End Date'], errors='coerce')
    ftc_df['Start Date'] = pd.to_datetime(ftc_df['Start Date'], errors='coerce')
    ftc_df['End Date'] = pd.to_datetime(ftc_df['End Date'], errors='coerce')

    # Check if any rows in update_df are not in ftc_df
    new_entries = update_df[~update_df.apply(tuple, axis=1).isin(ftc_df.apply(tuple, axis=1))]

    if new_entries.empty:
        print("No updates at the moment")
    else:
        # Append the new entries to FTC Rates.xlsx
        with pd.ExcelWriter("FTC Rates.xlsx", mode="a", if_sheet_exists="replace") as writer:
            combined_df = pd.concat([ftc_df, new_entries], ignore_index=True)
            combined_df.to_excel(writer, sheet_name="rates", index=False)
        print("New entries appended to FTC Rates.xlsx")

def main():
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Run in headless mode if needed
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    try:
        driver.get("https://www.ato.gov.au/businesses-and-organisations/income-deductions-and-concessions/incentives-and-concessions/fuel-schemes/fuel-tax-credits-business/rates-business/")

        # Wait and click on the first link containing "from" in href
        wait = WebDriverWait(driver, 10)
        link_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href*="from"]')))
        link_element.click()
        time.sleep(3)
        loaded_page_url = driver.current_url
        print("Loaded Page URL:", loaded_page_url)
        driver.quit()

        # Integrate with SmartScraperGraph
        smart_scraper_graph = SmartScraperGraph(
            prompt="Extract all the available tables containing the 'Rates for fuel acquired' from the page.",
            source=loaded_page_url,
            config=graph_config
        )

        result = smart_scraper_graph.run()
        print(result)

        df = json_to_dataframe(result)
        print("DataFrame Output:", df)  

        if df is None:
            print("DataFrame is None, rerunning the script...")
            return

        df_r1 = df.copy()
        df_r2 = df.copy()

        df_r1['Road Type'] = 'R1'
        df_r1['Road'] = 'On-Road'
        df_r1['Rate'] = df_r1['Used in heavy vehicles'].apply(clean_rate)

        df_r2['Road Type'] = 'R2'
        df_r2['Road'] = 'Off-Road'
        df_r2['Rate'] = df_r2['All other business uses'].apply(clean_rate)

        result_df = pd.concat([df_r1, df_r2], ignore_index=True)
        result_df[['Start Date', 'End Date']] = result_df['Title'].apply(extract_dates)
        result_df['Fuel Type'] = result_df['Eligible fuel type'].map(fuel_type_mapping)
        result_df['Unit'] = 'cents per liter'
        result_df['Fuel'] = result_df['Eligible fuel type']
        final_df = result_df[[
            'Start Date', 'End Date', 'Fuel Type', 'Road Type', 'Unit', 'Rate', 'Fuel', 'Road'
        ]]

        final_df.to_excel("update.xlsx", index=False)
        print("Transformed Output:", final_df) 

        if df is not None and not df.empty:
            excel_file = 'fuel_rates.xlsx'
            df.to_excel(excel_file, index=False)
            print(f"DataFrame exported to {excel_file}")
            update_rates_table()
        else:
            print("DataFrame is empty, not exporting.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
