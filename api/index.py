import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, send_file, jsonify
import telegram
import shutil
import logging
import time

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = "8049812102:AAEJPIZfH-lUqUXflj8BcOKuQWFZI5629Bc"
base_url = "https://desifakes.com"
save_dir = "/tmp/HTML_Pages"  # Use temporary directory
merge_dir = "/tmp/Merge"
error_log_file = os.path.join(merge_dir, "error.txt")
os.makedirs(save_dir, exist_ok=True)
os.makedirs(merge_dir, exist_ok=True)

PROXY = {
    "http": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594",
    "https": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594"
}

unique_images = set()

def init_html(file_path, title):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"""<!DOCTYPE html><html><head><title>{title}</title></head><body>""")

def append_to_html(file_path, content):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(content)

def close_html(file_path):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write("</body></html>")

def log_error(url, error_message):
    with open(error_log_file, "a", encoding="utf-8") as f:
        f.write(f"{url} - {error_message}\n")

def generate_links(start_year, end_year, username):
    links = []
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        url = f"https://desifakes.com/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}&c[title_only]=0&o=date"
        links.append((year, url))
    return links

def fetch_total_pages(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=5, proxies=PROXY)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            pagination = soup.find('div', class_='pageNav')
            return min(2, max(int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit())) if pagination else 1
        except Exception as e:
            if attempt == max_retries - 1:
                log_error(url, str(e))
                return 1
            time.sleep(1)
    return 1

def scrape_post_links(search_url):
    try:
        response = requests.get(search_url, timeout=5, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return list(dict.fromkeys(urljoin(base_url, link['href']) for link in soup.find_all('a', href=True) 
                                 if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
    except Exception as e:
        log_error(search_url, str(e))
        return []

def add_media(media_url, year):
    if media_url.startswith("data:image") or "addonflare/awardsystem/icons/" in media_url or any(keyword in media_url.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"]):
        return
    media_url = urljoin(base_url, media_url) if media_url.startswith("/") else media_url
    if media_url not in unique_images:
        unique_images.add(media_url)
        append_to_html(f"HTML_Pages/{year}/images.html", f'<div><img src="{media_url}" alt="Image" style="max-width:80%;height:auto;"></div>')

def process_post(post_link, year, username):
    try:
        response = requests.get(post_link, timeout=5, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        post_id = re.search(r'post-(\d+)', post_link).group(1) if re.search(r'post-(\d+)', post_link) else None
        articles = [soup.find('article', {'data-content': f'post-{post_id}', 'id': f'js-post-{post_id}'})] if post_id else soup.find_all('article')
        
        username_lower = username.lower()
        articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        
        for article in articles:
            for img in article.find_all('img', src=True):
                add_media(img['src'], year)
    except Exception as e:
        log_error(post_link, str(e))

def process_year(year, search_links, username):
    year_dir = f"HTML_Pages/{year}"
    os.makedirs(year_dir, exist_ok=True)
    init_html(f"{year_dir}/images.html", f"{year} Images Links")
    
    for search_link in search_links:
        total_pages = fetch_total_pages(search_link)
        for page in range(1, total_pages + 1):
            post_links = scrape_post_links(f"{search_link}&page={page}")
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_post, post, year, username) for post in post_links]
                for _ in as_completed(futures):
                    pass
    close_html(f"{year_dir}/images.html")

def merge_html_files(years):
    merge_file_path = f"{merge_dir}/images.html"
    init_html(merge_file_path, "Merged Images Links")
    for year in years:
        year_file_path = f"HTML_Pages/{year}/images.html"
        if os.path.exists(year_file_path) and os.path.getsize(year_file_path) > 0:
            with open(year_file_path, "r", encoding="utf-8") as f:
                content = re.search(r'<body>(.*?)</body>', f.read(), re.DOTALL)
                if content:
                    append_to_html(merge_file_path, content.group(1))
    close_html(merge_file_path)

def process_username(username):
    try:
        logger.info(f"Starting processing for username: {username}")
        start_year, end_year = 2024, 2025
        all_links = generate_links(start_year, end_year, username)
        links_by_year = {year: [link for y, link in all_links if y == year] for year in range(start_year, end_year + 1)}
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(lambda y: process_year(y, links_by_year[y], username), links_by_year.keys())
        
        years_descending = sorted(range(start_year, end_year + 1), reverse=True)
        merge_html_files(years_descending)
        
        shutil.rmtree(save_dir, ignore_errors=True)
        file_path = f"{merge_dir}/images.html"
        logger.info(f"Processing completed, file path: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error in process_username: {str(e)}")
        raise

bot = telegram.Bot(token=TOKEN)

def send_file(chat_id, file_path):
    try:
        with open(file_path, 'rb') as f:
            bot.send_document(chat_id=chat_id, document=f)
    except Exception as e:
        logger.error(f"Error sending file: {str(e)}")
        raise

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update = telegram.Update.de_json(request.get_json(force=True), bot)
        if not update or not update.message:
            logger.error("Invalid update received")
            return jsonify({"status": "error", "message": "Invalid update"}), 400
        
        chat_id = update.message.chat_id
        text = update.message.text
        logger.info(f"Received message: {text} from chat_id: {chat_id}")
        
        if text.startswith('/start'):
            bot.send_message(chat_id=chat_id, text="Welcome! Send me a username to process.")
        else:
            bot.send_message(chat_id=chat_id, text=f"Processing username: {text}...")
            file_path = process_username(text)
            send_file(chat_id, file_path)
            bot.send_message(chat_id=chat_id, text="Processing complete! Here’s your images.html file.")
        
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)