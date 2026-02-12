from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from pathlib import Path
import time

# Set up data directory & report date
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)
report_date = "03/31/2022"


class FFIECDownloader:
    """
    Download Call Report data from FFIEC using Selenium
    """
    
    def __init__(self):
        self.data_dir = DATA_DIR
        
    def download_call_report(self, report_date: str = '03/31/2022'):
        """
        Download Call Report data from FFIEC
        
        Parameters:
        -----------
        report_date : str
            Quarter end date in MM/DD/YYYY format (e.g., "12/31/2024")
        """
        # Set Chrome download preferences
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": str(self.data_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Initialize browser
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            print("Opening FFIEC page...")
            driver.get("https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx")
            
            # Wait for page to load
            wait = WebDriverWait(driver, 10)
            
            # Step 1: Select "Call Reports -- Single Period" from Available Products dropdown
            print("Selecting Call Reports -- Single Period...")
            products_dropdown = wait.until(
                EC.presence_of_element_located((By.ID, "ListBox1"))
            )
            select_products = Select(products_dropdown)
            select_products.select_by_value("ReportingSeriesSinglePeriod")
            time.sleep(2)  # Wait for page to update
            
            # Step 2: Select the reporting period date
            print(f"Selecting reporting period: {report_date}...")
            date_dropdown = wait.until(
                EC.presence_of_element_located((By.ID, "DatesDropDownList"))
            )
            select_date = Select(date_dropdown)
            
            # Print all available dates for debugging
            available_dates = [option.text for option in select_date.options]
            print(f"Available dates: {available_dates}")
            
            # Check if requested date exists
            if report_date in available_dates:
                select_date.select_by_visible_text(report_date)
                print(f"✓ Selected: {report_date}")
            else:
                print(f"✗ Date {report_date} not available!")
                print(f"  Using most recent date: {available_dates[0]}")
                select_date.select_by_index(0)  # Select first (most recent)
            
            time.sleep(1)
            
            # Step 3: Ensure "Tab Delimited" is selected
            print("Ensuring Tab Delimited format is selected...")
            tab_delimited_radio = driver.find_element(By.ID, "TSVRadioButton")
            if not tab_delimited_radio.is_selected():
                tab_delimited_radio.click()
            time.sleep(1)
            
            # Step 4: Click the download button
            print("Clicking download button...")
            download_button = driver.find_element(By.ID, "Download_0")
            download_button.click()
            
            # Wait for download to complete
            print("Downloading... (this may take a few minutes)")
            time.sleep(15)  # Adjust based on file size and internet speed
            
            print(f"Download complete! Check {self.data_dir} for the file.")
            
        except Exception as e:
            print(f"Error: {e}")
            # Take screenshot for debugging
            driver.save_screenshot(str(self.data_dir / "error_screenshot.png"))
            print(f"Error screenshot saved to {self.data_dir / 'error_screenshot.png'}")
            
        finally:
            driver.quit()


if __name__ == "__main__":
    downloader = FFIECDownloader()
    downloader.download_call_report(report_date)
