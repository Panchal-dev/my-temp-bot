import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import Fore, Style, init
import shutil
from datetime import datetime, timedelta
import time
import logging
from flask import Flask, request, jsonify
from io import BytesIO
import telebot
import traceback

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = 10  # Increased from 5 for speed
MAX_PAGES_PER_SEARCH = 10

# Telegram Bot Setup
try:
    TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
except KeyError:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    raise
except Exception as e:
    logger.error(f"Failed to initialize bot: {str(e)}")
    raise

# Proxy configuration
PROXY = {"http": "http://146.56.142.114:1080", "https": "http://146.56.142.114:1080"}
FALLBACK_PROXIES = [
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://172.188.122.92:80", "https": "http://172.188.122.92:80"},
    {"http": "http://157.230.40.77:1004", "https": "http://157.230.40.77:1004"}
]

BASE_URL = "https://desifakes.com"

class ScraperError(Exception):
    pass

# Global task tracking
active_tasks = {}

# Allowed chat IDs
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

def make_request(url, method='get', **kwargs):
    initial_timeout = 10
    timeout_increment = 5
    proxies = [PROXY] + FALLBACK_PROXIES

    for proxy_idx, proxy in enumerate(proxies):
        for attempt in range(MAX_RETRIES):
            current_timeout = initial_timeout + (attempt * timeout_increment)
            try:
                response = requests.request(
                    method,
                    url,
                    proxies=proxy,
                    timeout=current_timeout,
                    **kwargs
                )
                response.raise_for_status()
                logger.info(f"Success with proxy {proxy_idx + 1} on attempt {attempt + 1} for {url}")
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url} with proxy {proxy_idx + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1 and proxy_idx == len(proxies) - 1:
                    raise ScraperError(f"All proxies failed for {url} after {MAX_RETRIES} attempts: {str(e)}")
                elif attempt == MAX_RETRIES - 1:
                    logger.info(f"Switching to next proxy {proxy_idx + 2}")
                    break
                time.sleep(1 * (attempt + 1))

def generate_year_link(year, username, title_only=False):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    url = f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}"
    url += "&c[title_only]=1" if title_only else "&c[title_only]=0"
    url += "&o=date"  # Newest first
    return url

def split_url(url, start_date, end_date, max_pages=MAX_PAGES_PER_SEARCH):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        total_pages = max([int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]) if pagination else 1
        if total_pages < max_pages:
            return [url]
        
        total_days = (end_dt - start_dt).days
        mid_dt = start_dt + timedelta(days=total_days // 2)
        first_range = (start_date, mid_dt.strftime("%Y-%m-%d"))
        second_range = ((mid_dt + timedelta(days=1)).strftime("%Y-%m-%d"), end_date)
        
        first_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={first_range[0]}", url)
        first_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={first_range[1]}", first_url)
        second_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={second_range[0]}", url)
        second_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={second_range[1]}", second_url)
        
        return split_url(first_url, first_range[0], first_range[1]) + split_url(second_url, second_range[0], second_range[1])
    except Exception as e:
        logger.error(f"Split error for {url}: {str(e)}")
        return [url]

def scrape_post_links(search_url):
    try:
        response = make_request(search_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        return list(dict.fromkeys(urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True) 
                                 if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
    except Exception as e:
        logger.error(f"Failed to scrape post links for {search_url}: {str(e)}")
        return []

def process_post(post_link, username, year, media_by_year):
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        post_id = re.search(r'post-(\d+)', post_link).group(1) if re.search(r'post-(\d+)', post_link) else None
        articles = [soup.find('article', {'data-content': f'post-{post_id}', 'id': f'js-post-{post_id}'})] if post_id else soup.find_all('article')
        
        username_lower = username.lower()
        articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        
        for article in articles:
            for media in article.find_all(['img', 'video', 'source'], src=True):
                src = urljoin(BASE_URL, media['src']) if media['src'].startswith("/") else media['src']
                if (src.startswith("data:image") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                if media.name == 'img':
                    media_type = "gifs" if src.endswith(".gif") else "images"
                    media_by_year[year][media_type].append(src)
                elif media.name in ['video', 'source']:
                    media_by_year[year]['videos'].append(src)
    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")

def process_year(year, search_url, username, media_by_year):
    total_pages = 0
    try:
        response = make_request(search_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        total_pages = max([int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]) if pagination else 1
    except Exception as e:
        logger.error(f"Failed to fetch total pages for {search_url}: {str(e)}")
        total_pages = 1

    urls_to_process = split_url(search_url, f"{year}-01-01", f"{year}-12-31") if total_pages >= MAX_PAGES_PER_SEARCH else [search_url]
    
    for url in urls_to_process:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for page in range(total_pages, 0, -1):  # Descending order
                page_url = f"{url}&page={page}"
                post_links = scrape_post_links(page_url)
                for post in post_links:
                    futures.append(executor.submit(process_post, post, username, year, media_by_year))
            
            for future in as_completed(futures):
                future.result()  # Ensure all posts are processed

def create_html(file_type, media_by_year, username, start_year, end_year):
    html_content = f"""<!DOCTYPE html><html><head>
    <title>{username} - {file_type.capitalize()} Links</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        img {{ max-width: 80%; height: auto; }}
        video {{ max-width: 100%; }}
    </style>
    </head>
    <body>
    <h1>{username} - {file_type.capitalize()} Links ({start_year}-{end_year})</h1>"""
    
    total_items = 0
    seen_urls = set()  # Deduplicate by full URL
    for year in sorted(media_by_year.keys(), reverse=True):
        items = media_by_year[year][file_type]
        if items:
            html_content += f"<h2>{year}</h2>"
            for item in items:  # Items already in order from scraping
                if item not in seen_urls:
                    seen_urls.add(item)
                    total_items += 1
                    if file_type == "images":
                        html_content += f'<div><img src="{item}" alt="Image" style="max-width:80%;height:auto;"></div>'
                    elif file_type == "videos":
                        html_content += f'<p><video controls style="max-width:100%;"><source src="{item}" type="video/mp4"></video></p>'
                    elif file_type == "gifs":
                        html_content += f'<div><img src="{item}" alt="GIF" style="max-width:80%;height:auto;"></div>'
    
    html_content += "</body></html>"
    return html_content if total_items > 0 else None

def send_telegram_message(chat_id, text, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except Exception as e:
            logger.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(1 * (attempt + 1))

def send_telegram_document(chat_id, file_buffer, filename, caption):
    for attempt in range(MAX_RETRIES):
        try:
            file_buffer.seek(0)
            return bot.send_document(chat_id=chat_id, document=file_buffer, visible_file_name=filename, caption=caption[:1024])
        except Exception as e:
            logger.warning(f"Send document attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(1 * (attempt + 1))

def cancel_task(chat_id):
    if chat_id in active_tasks:
        executor, futures = active_tasks[chat_id]
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False)
        del active_tasks[chat_id]
        return True
    return False

@app.route('/telegram', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json()
        if not update or 'message' not in update:
            return '', 200

        chat_id = update['message']['chat']['id']
        message_id = update['message'].get('message_id')
        text = update['message'].get('text', '').strip()

        if chat_id not in ALLOWED_CHAT_IDS:
            send_telegram_message(chat_id=chat_id, text="❌ Restricted to specific users.", reply_to_message_id=message_id)
            return '', 200

        if not text:
            send_telegram_message(chat_id=chat_id, text="Please send a search query", reply_to_message_id=message_id)
            return '', 200

        if text.lower() == '/stop':
            if cancel_task(chat_id):
                send_telegram_message(chat_id=chat_id, text="✅ Scraping stopped", reply_to_message_id=message_id)
            else:
                send_telegram_message(chat_id=chat_id, text="ℹ️ No active scraping to stop", reply_to_message_id=message_id)
            return '', 200

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            send_telegram_message(chat_id=chat_id, text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: 'Akshra Singh' n 2023 2025", reply_to_message_id=message_id)
            return '', 200

        if chat_id in active_tasks:
            send_telegram_message(chat_id=chat_id, text="⚠️ Scraping already running. Use /stop to cancel.", reply_to_message_id=message_id)
            return '', 200

        if parts[0] == '/start':
            username = ' '.join(parts[1:-3]) if len(parts) > 4 else parts[1]
            title_only_idx = 2 if len(parts) <= 4 else len(parts) - 3
        else:
            username = ' '.join(parts[:-3]) if len(parts) > 3 else parts[0]
            title_only_idx = 1 if len(parts) <= 3 else len(parts) - 3
        
        title_only = parts[title_only_idx].lower() == 'y' if len(parts) > title_only_idx else False
        start_year = int(parts[-2]) if len(parts) > 2 else 2019
        end_year = int(parts[-1]) if len(parts) > 1 else datetime.now().year

        progress_msg = send_telegram_message(chat_id=chat_id, text=f"🔍 Processing '{username}' ({start_year}-{end_year})...")

        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        active_tasks[chat_id] = (executor, [])

        try:
            media_by_year = {year: {'images': [], 'videos': [], 'gifs': []} for year in range(start_year, end_year + 1)}
            years = list(range(end_year, start_year - 1, -1))
            
            futures = []
            for year in years:
                search_url = generate_year_link(year, username, title_only)
                future = executor.submit(process_year, year, search_url, username, media_by_year)
                futures.append(future)
            
            active_tasks[chat_id] = (executor, futures)
            for future in as_completed(futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                future.result()
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"🔍 Processing '{username}' ({start_year}-{end_year}): {years.index(year) + 1}/{len(years)} years done")

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
            any_sent = False
            for file_type in ["images", "videos", "gifs"]:
                html_content = create_html(file_type, media_by_year, username, start_year, end_year)
                if html_content:
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                    total_items = sum(len(media_by_year[year][file_type]) for year in media_by_year)
                    send_telegram_document(chat_id=chat_id, file_buffer=html_file, filename=html_file.name, caption=f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})")
                    any_sent = True

            if not any_sent:
                send_telegram_message(chat_id=chat_id, text=f"⚠️ No media found for '{username}' ({start_year}-{end_year})")

        except ScraperError as e:
            if str(e) == "Task cancelled by user":
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"🛑 Scraping stopped for '{username}'")
            else:
                raise
        except Exception as e:
            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"❌ Error for '{username}': {str(e)}")
            logger.error(f"Error processing {username}: {str(e)}\n{traceback.format_exc()}")
        finally:
            if chat_id in active_tasks:
                executor, _ = active_tasks[chat_id]
                del active_tasks[chat_id]
                executor.shutdown(wait=False)

        return '', 200
    
    except Exception as e:
        logger.critical(f"Unhandled error: {str(e)}\n{traceback.format_exc()}")
        if 'chat_id' in locals() and 'progress_msg' in locals():
            try:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"❌ Critical error: {str(e)}")
            except:
                pass
        if chat_id in active_tasks:
            executor, _ = active_tasks[chat_id]
            del active_tasks[chat_id]
            executor.shutdown(wait=False)
        return '', 200

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

def set_webhook():
    railway_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
    if railway_url:
        webhook_url = f"https://{railway_url}/telegram"
        bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.error("RAILWAY_PUBLIC_DOMAIN not set")

if __name__ == "__main__":
    set_webhook()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)