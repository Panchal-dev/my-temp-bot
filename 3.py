import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup logging
logging.basicConfig(
    filename='bypass.log',
    filemode='w',
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', "%H:%M:%S")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def wait_and_click(driver, by, value, timeout=30):
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value))).click()
        logging.info(f"Clicked on element: {value}")
    except Exception as e:
        logging.warning(f"Failed to click element {value}: {str(e)}")


def close_popups(driver, attempts=3):
    logging.info("Checking for popups...")
    for _ in range(attempts):
        try:
            buttons = driver.find_elements(By.CLASS_NAME, "close")
            for btn in buttons:
                try:
                    btn.click()
                    logging.info("Popup closed.")
                    time.sleep(1)
                except:
                    continue
        except:
            break


def wait_countdown(seconds=9):
    logging.info(f"Waiting {seconds} seconds for countdown...")
    time.sleep(seconds + 1)


def handle_step_page(driver, step_num):
    logging.info(f"Processing Step {step_num}/4")
    wait_countdown()

    close_popups(driver)

    try:
        wait_and_click(driver, By.ID, "tp-snp2", timeout=5)
    except:
        try:
            wait_and_click(driver, By.XPATH, "//button[contains(text(), 'Click here to proceed')]")
        except:
            logging.warning("Fallback: 'Click here to proceed' button not found.")

    time.sleep(2)


def handle_final_page(driver):
    logging.info("Processing Final Page (Step 4/4)...")
    wait_countdown()
    close_popups(driver, attempts=5)

    # Try clicking "Get Link" button multiple times if needed
    max_retries = 5
    for attempt in range(max_retries):
        try:
            button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Get Link')]"))
            )
            driver.execute_script("arguments[0].click();", button)
            logging.info(f"Clicked 'Get Link' (attempt {attempt+1})")
            time.sleep(3)

            # If redirected, return the new URL
            current_url = driver.current_url
            if "t.me" in current_url or "telegram.me" in current_url:
                logging.info(f"✅ Final URL: {current_url}")
                return current_url
        except Exception as e:
            logging.warning(f"Retrying Get Link click... ({attempt+1}/{max_retries}) - {str(e)}")
            close_popups(driver)
            time.sleep(2)

    logging.error("❌ Failed to click 'Get Link' after multiple attempts.")
    return None


def bypass_adrinolink(start_url):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    # options.add_argument("--headless")  # Uncomment for headless mode

    logging.info("Launching Chrome browser...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(start_url)

    final_url = None

    try:
        for step in range(1, 5):
            if step < 4:
                handle_step_page(driver, step)
            else:
                final_url = handle_final_page(driver)

    except Exception as e:
        logging.error(f"Error during bypass: {str(e)}")

    finally:
        driver.quit()

    return final_url


if __name__ == "__main__":
    adrinolinks_url = "https://adrinolinks.in/HucM6"
    result_url = bypass_adrinolink(adrinolinks_url)

    if result_url:
        logging.info(f"\n✅ Final Telegram Link: {result_url}")
        print(f"\n✅ Final Telegram Link: {result_url}")
    else:
        logging.error("❌ Failed to retrieve the final link.")
        print("❌ Failed to retrieve the final link.")
