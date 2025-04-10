import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def bypass_adrinolink(url):
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--headless")  # optional: hide browser window

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        driver.get(url)
        time.sleep(2)

        for step in range(1, 5):
            logging.info(f"🚀 Step {step}/4")

            try:
                # Wait for countdown to finish and a continue button to appear
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "countdown"))
                )
                logging.info("⏳ Countdown detected. Waiting 1 sec (faking timer)...")
                time.sleep(1)

                # Try clicking the button if it's a <button> tag
                try:
                    button = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button#countdown, a#countdown"))
                    )
                    logging.info(f"✅ Clicking countdown button: {button.text}")
                    button.click()
                except:
                    logging.warning("⚠️ Countdown button not found or not clickable")

                # Wait for redirection
                time.sleep(2)

            except Exception as e:
                logging.warning(f"⛔ Step {step} skipped or error: {e}")
                continue

        # Final Get Link
        try:
            logging.info("🧲 Waiting for final 'Get Link' button...")
            get_link = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'open.php')]"))
            )
            final_url = get_link.get_attribute("href")
            logging.info(f"✅ Final URL: {final_url}")
            return final_url
        except:
            logging.error("❌ Failed to find final Get Link button.")
            return None

    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        return None
    finally:
        driver.quit()


if __name__ == "__main__":
    short_url = "https://adrinolinks.in/HucM6"
    result = bypass_adrinolink(short_url)

    if result:
        print("\n✅ Bypass Successful!")
        print("🔗 Final Telegram Link:", result)
    else:
        print("\n❌ Bypass Failed.")
