import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import shutil
from flask import Flask, request, send_file, jsonify
import threading

app = Flask(__name__)

# Telegram Bot Token and Chat ID (replace with yours)
TOKEN = "8049812102:AAEJPIZfH-lUqUXflj8BcOKuQWFZI5629Bc"
CHAT_ID = "1285451259"

# Base URL and directories
base_url = "https://desifakes.com"
save_dir = "HTML_Pages"
merge_dir = "Merge"
error_log_file = os.path.join(merge_dir, "error.txt")
os.makedirs(save_dir, exist_ok=True)
os.makedirs(merge_dir, exist_ok=True)

# Proxy settings
PROXY = {
    "http": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594",
    "https": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594"
}

# Global sets for unique media
unique_images = set()

# HTML utilities
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

# Generate search links
def generate_links(start_year, end_year, username):
    links = []
    for year in range(start_year, end_year + 1):
        months = [("01-01", "03-31"), ("04-01", "06-30"), ("07-01", "09-30"), ("10-01", "12-31")]
        for start_month, end_month in months:
            start_date = f"{year}-{start_month}"
            end_date = f"{year}-{end_month}"
            url = f"https://desifakes.com/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}&c[title_only]=0&o=date"
            links.append((year, url))
    return links

# Fetch total pages
def fetch_total_pages(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10, proxies=PROXY)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            pagination = soup.find('div', class_='pageNav')
            return max(int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()) if pagination else 1
        except Exception as e:
            if attempt == max_retries - 1:
                log_error(url, str(e))
                return 1
            time.sleep(1)
    return 1

# Scrape post links
def scrape_post_links(search_url):
    try:
        response = requests.get(search_url, timeout=10, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return list(dict.fromkeys(urljoin(base_url, link['href']) for link in soup.find_all('a', href=True) 
                                 if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
    except Exception as e:
        log_error(search_url, str(e))
        return []

# Add media with deduplication
def add_media(media_url, year):
    if media_url.startswith("data:image") or "addonflare/awardsystem/icons/" in media_url or any(keyword in media_url.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"]):
        return
    media_url = urljoin(base_url, media_url) if media_url.startswith("/") else media_url
    if media_url not in unique_images:
        unique_images.add(media_url)
        append_to_html(f"HTML_Pages/{year}/images.html", f'<div><img src="{media_url}" alt="Image" style="max-width:80%;height:auto;"></div>')

# Process individual post
def process_post(post_link, year, username):
    try:
        response = requests.get(post_link, timeout=10, proxies=PROXY)
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

# Process year
def process_year(year, search_links, username):
    year_dir = f"HTML_Pages/{year}"
    os.makedirs(year_dir, exist_ok=True)
    init_html(f"{year_dir}/images.html", f"{year} Images Links")
    
    for search_link in search_links:
        total_pages = fetch_total_pages(search_link)
        for page in range(1, total_pages + 1):
            post_links = scrape_post_links(f"{search_link}&page={page}")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(process_post, post, year, username) for post in post_links]
                for _ in as_completed(futures):
                    pass
    close_html(f"{year_dir}/images.html")

# Merge HTML files
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

# Main processing function
def process_username(username):
    start_year, end_year = 2019, 2025
    all_links = generate_links(start_year, end_year, username)
    links_by_year = {year: [link for y, link in all_links if y == year] for year in range(start_year, end_year + 1)}
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda y: process_year(y, links_by_year[y], username), links_by_year.keys())
    
    years_descending = sorted(range(start_year, end_year + 1), reverse=True)
    merge_html_files(years_descending)
    
    shutil.rmtree(save_dir, ignore_errors=True)
    return f"{merge_dir}/images.html"

# Telegram Bot Logic
bot = telegram.Bot(token=TOKEN)

def send_file(chat_id, file_path):
    with open(file_path, 'rb') as f:
        bot.send_document(chat_id=chat_id, document=f)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telegram.Update.de_json(request.get_json(force=True), bot)
    chat_id = update.message.chat_id
    text = update.message.text
    
    if text.startswith('/start'):
        bot.send_message(chat_id=chat_id, text="Welcome! Send me a username to process.")
    else:
        bot.send_message(chat_id=chat_id, text=f"Processing username: {text}...")
        file_path = process_username(text)
        send_file(chat_id, file_path)
        bot.send_message(chat_id=chat_id, text="Processing complete! Here’s your images.html file.")
    
    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)