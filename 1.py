import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import time
import re

# Set up logging
logging.basicConfig(
    filename="bypass_script.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def setup_driver():
    """Set up Chrome WebDriver with console logging"""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    logger.info("WebDriver initialized (non-headless)")
    return driver

def get_countdown_time(driver):
    """Dynamically extract the countdown time from the page"""
    try:
        countdown_span = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "tp-time"))
        )
        countdown_text = countdown_span.text
        countdown_seconds = int(re.search(r'\d+', countdown_text).group())
        logger.info(f"Detected countdown time: {countdown_seconds} seconds")
        return countdown_seconds
    except Exception as e:
        logger.warning(f"Could not detect countdown time, defaulting to 15 seconds: {str(e)}")
        return 15  # Default to 15 seconds as per your structure

def close_popups(driver):
    """Close all popups with retry mechanism and handle stale elements"""
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='popup' and @id='popup']"))
        )
        max_attempts = 5
        for attempt in range(max_attempts):
            popups = driver.find_elements(By.XPATH, "//div[@class='popup' and @id='popup']//div[@class='close']")
            if not popups or not any(p.is_displayed() for p in popups):
                logger.info("No more popups to close")
                break
            for popup in popups:
                if popup.is_displayed():
                    try:
                        driver.execute_script("arguments[0].click();", popup)  # JS click for reliability
                        logger.info("Closed a popup advertisement")
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Failed to click popup close button: {str(e)}")
            time.sleep(1)
        
        # Verify no popups remain
        remaining_popups = driver.find_elements(By.XPATH, "//div[@class='popup' and @id='popup']")
        if remaining_popups and any(p.is_displayed() for p in remaining_popups):
            logger.error("Popups still visible after all attempts")
            return False
        
        # Remove overlay
        try:
            overlay = driver.find_element(By.ID, "overlay")
            if overlay.is_displayed():
                driver.execute_script("arguments[0].style.display = 'none';", overlay)
                logger.info("Removed overlay blocking clicks")
        except:
            logger.info("No overlay found or already hidden")
        
        return True
    except Exception as e:
        logger.error(f"Failed to close popups: {str(e)}")
        return False

def bypass_page(driver, url, step_number, total_steps=4):
    """Bypass a single page with dynamic countdown, popups, and proceed/get link button"""
    logger.info(f"Processing Step {step_number}/{total_steps}: {url}")
    driver.get(url)
    
    try:
        # Wait for page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Get and wait for the countdown
        countdown_seconds = get_countdown_time(driver)
        time.sleep(countdown_seconds + 2)  # Add 2-second buffer
        
        # Close all popups
        if not close_popups(driver):
            raise Exception("Failed to close popups, aborting step")
        
        # Additional wait on Page 2 to stabilize
        if step_number == 2:
            logger.info("Waiting an additional 5 seconds on Page 2 to stabilize...")
            time.sleep(5)
        
        # Click the proceed or get link button
        proceed_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "tp-snp2"))
        )
        
        if step_number < 4:
            # Steps 1-3: Get next URL from form action
            next_url = driver.find_element(By.XPATH, "//form[@name='tp']").get_attribute("action")
            logger.info(f"Found 'Click here to proceed' button. Next URL: {next_url}")
            driver.execute_script("arguments[0].click();", proceed_button)
            time.sleep(3)
            WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
            return next_url
        else:
            # Step 4: Handle "Get Link" and trigger redirect
            try:
                get_link_element = driver.find_element(By.XPATH, "//a[button[@id='tp-snp2']]")
                get_link = get_link_element.get_attribute("href")
                if not get_link.startswith("http"):
                    get_link = f"https://keedabankingnews.com{get_link}"
                logger.info(f"Found 'Get Link' button. Intermediate URL: {get_link}")
            except:
                get_link = url.rstrip('/') + "/includes/open.php?id=HucM6"
                logger.info(f"No explicit 'Get Link' href found, constructed URL: {get_link}")
            
            driver.execute_script("arguments[0].click();", proceed_button)
            time.sleep(3)
            driver.get(get_link)
            return get_link
            
    except Exception as e:
        logger.error(f"Error on step {step_number}: {str(e)}")
        return None

def bypass_shortlink(initial_url):
    """Main function to bypass all steps and get final Telegram link"""
    driver = setup_driver()
    current_url = initial_url
    step = 1
    previous_urls = set()
    
    try:
        while step <= 4:
            if current_url in previous_urls:
                logger.error(f"Loop detected at step {step}: {current_url}")
                return None
            previous_urls.add(current_url)
            
            next_url = bypass_page(driver, current_url, step)
            if not next_url:
                logger.error(f"Failed to find next URL at step {step}")
                break
            
            current_url = next_url
            step += 1
            time.sleep(2)  # Brief pause between steps
        
        if step > 4:
            logger.info("Waiting for final Telegram redirect...")
            WebDriverWait(driver, 30).until(
                lambda driver: "t.me" in driver.current_url.lower() or "telegram.org" in driver.current_url.lower()
            )
            final_url = driver.current_url
            logger.info(f"Final Telegram URL: {final_url}")
            return final_url
        else:
            logger.error("Did not complete all steps")
            return None
            
    except Exception as e:
        logger.error(f"Error in bypass process: {str(e)}")
        return None
        
    finally:
        driver.quit()
        logger.info("WebDriver closed")

def main():
    initial_url = "https://adrinolinks.in/HucM6"
    logger.info(f"Starting bypass process with URL: {initial_url}")
    final_url = bypass_shortlink(initial_url)
    
    if final_url:
        print(f"Successfully bypassed all steps. Final Telegram destination: {final_url}")
        logger.info(f"Successfully bypassed all steps. Final Telegram destination: {final_url}")
    else:
        print("Failed to complete bypass process. Check logs for details.")
        logger.error("Failed to complete bypass process")

if __name__ == "__main__":
    main()