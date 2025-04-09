import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from flask import Flask, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot
from datetime import datetime, timedelta
import time
import logging
import shutil

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
BASE_URL = "https://desifakes.com"
SAVE_DIR = "HTML_Pages"
MERGE_DIR = "Merge"
ERROR_LOG_FILE = os.path.join(MERGE_DIR, "error.txt")
MAX_RETRIES = 3
MAX_WORKERS = 6

# Proxy configuration
PROXY = {
    "http": "http://34.143.143.61:7777",
    "https": "http://34.143.143.61:7777"
}

# Allowed chat IDs (replace with your Telegram chat IDs)
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

# Global task tracking
active_tasks = {}

# HTML initialization with styling
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
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url} - {error_message}\n")

# Generate search links for a full year
def generate_year_link(year, username, title_only=False):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    url = f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}"
    url += "&c[title_only]=1" if title_only else "&c[title_only]=0"
    url += "&o=date"  # Ensures newest first (Dec 31 to Jan 1)
    return url

# Split URL if pages exceed max_pages
def split_url(url, start_date, end_date, max_pages=10):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_pages = fetch_total_pages(url)
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
        log_error(url, f"Split error: {str(e)}")
        return [url]

# Fetch total pages with retry logic
def fetch_total_pages(url, max_retries=MAX_RETRIES):
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
        return list(dict.fromkeys(urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True) 
                                 if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
    except Exception as e:
        log_error(search_url, str(e))
        return []

# Add media without deduplication, preserving site order
def add_media(media_url, media_type, year):
    if media_url.startswith("data:image") or "addonflare/awardsystem/icons/" in media_url or any(keyword in media_url.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"]):
        return
    media_url = urljoin(BASE_URL, media_url) if media_url.startswith("/") else media_url
    
    if media_type == "image":
        append_to_html(f"{SAVE_DIR}/{year}/images.html", f'<div><img src="{media_url}" alt="Image" style="max-width:80%;height:auto;"></div>')
    elif media_type == "video":
        append_to_html(f"{SAVE_DIR}/{year}/videos.html", f'<p><video controls style="max-width:100%;"><source src="{media_url}" type="video/mp4"></video></p>')
    elif media_type == "gif":
        append_to_html(f"{SAVE_DIR}/{year}/gifs.html", f'<p><a href="{media_url}" target="_blank">View GIF</a></p>')

# Process individual post, preserving site order
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
            # Process media in the order it appears in the HTML
            for media in article.find_all(['img', 'video', 'source', 'a'], recursive=True):
                if media.name == 'img' and media.get('src'):
                    src = media['src']
                    if media.get('data-url'):  # Handle watermarked images
                        src = media['data-url']
                    add_media(src, "gif" if src.endswith(".gif") else "image", year)
                elif media.name == 'video' and media.get('src'):
                    add_media(media['src'], "video", year)
                elif media.name == 'source' and media.get('src'):
                    add_media(media['src'], "video", year)
                elif media.name == 'a' and media.get('href'):
                    href = media['href']
                    if any(href.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm', '.mov']):
                        media_type = "gif" if href.endswith(".gif") else "image" if href.endswith(('.jpg', '.jpeg', '.png')) else "video"
                        add_media(href, media_type, year)
    except Exception as e:
        log_error(post_link, str(e))

# Process year
def process_year(year, search_url, username, chat_id):
    year_dir = f"{SAVE_DIR}/{year}"
    os.makedirs(year_dir, exist_ok=True)
    for file_type in ["images", "videos", "gifs"]:
        init_html(f"{year_dir}/{file_type}.html", f"{year} {file_type.capitalize()} Links")
    
    total_pages = fetch_total_pages(search_url)
    urls_to_process = split_url(search_url, f"{year}-01-01", f"{year}-12-31") if total_pages >= 10 else [search_url]
    
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []
    total_posts = 0
    
    for url in urls_to_process:
        total_pages = fetch_total_pages(url)
        for page in range(total_pages, 0, -1):  # Reverse order for Dec 31 to Jan 1
            post_links = scrape_post_links(f"{url}&page={page}")
            total_posts += len(post_links)
            for post in post_links:
                future = executor.submit(process_post, post, year, username)
                futures.append(future)
    
    active_tasks[chat_id] = (executor, futures)
    
    processed_count = 0
    for future in as_completed(futures):
        if chat_id not in active_tasks:
            raise Exception("Task cancelled by user")
        future.result()
        processed_count += 1
        if processed_count % 10 == 0 or processed_count == total_posts:
            bot.edit_message_text(chat_id=chat_id, message_id=active_tasks[chat_id][2], 
                                 text=f"🔍 Processing '{username}' ({year}): {processed_count}/{total_posts} posts")
    
    for file_type in ["images", "videos", "gifs"]:
        close_html(f"{year_dir}/{file_type}.html")

# Merge HTML files
def merge_html_files(file_type, years, merge_dir):
    merge_file_path = f"{merge_dir}/{file_type}_temp.html"
    init_html(merge_file_path, f"Merged {file_type.capitalize()} Links")
    for year in years:
        year_file_path = f"{SAVE_DIR}/{year}/{file_type}.html"
        if os.path.exists(year_file_path) and os.path.getsize(year_file_path) > 0:
            with open(year_file_path, "r", encoding="utf-8") as f:
                content = re.search(r'<body>(.*?)</body>', f.read(), re.DOTALL)
                if content:
                    append_to_html(merge_file_path, content.group(1))
    close_html(merge_file_path)
    return merge_file_path

# Deduplicate merged file by full src path, preserving order
def deduplicate_html(file_type, temp_file_path, final_file_path):
    with open(temp_file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    seen_urls = set()
    unique_content = []
    
    for element in soup.body.find_all(recursive=False):
        url = None
        if file_type == "images" and element.name == "div" and element.img:
            url = element.img.get("src")
        elif file_type == "videos" and element.name == "p" and element.video and element.video.source:
            url = element.video.source.get("src")
        elif file_type == "gifs" and element.name == "p" and element.a:
            url = element.a.get("href")
        
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_content.append(str(element))
    
    init_html(final_file_path, f"Merged {file_type.capitalize()} Links")
    append_to_html(final_file_path, "\n".join(unique_content))
    close_html(final_file_path)
    os.remove(temp_file_path)

# Telegram bot utilities
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
        executor, futures, _ = active_tasks[chat_id]
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
        start_year = int(parts[-2]) if len(parts) > 2 else 2025
        end_year = int(parts[-1]) if len(parts) > 1 else 2025

        os.makedirs(SAVE_DIR, exist_ok=True)
        os.makedirs(MERGE_DIR, exist_ok=True)

        progress_msg = send_telegram_message(chat_id=chat_id, text=f"🔍 Processing '{username}' ({start_year}-{end_year})...")
        active_tasks[chat_id] = (None, [], progress_msg.message_id)

        try:
            years = list(range(end_year, start_year - 1, -1))
            for year in years:
                search_url = generate_year_link(year, username, title_only)
                process_year(year, search_url, username, chat_id)

            any_sent = False
            for file_type in ["images", "videos", "gifs"]:
                temp_file_path = merge_html_files(file_type, years, MERGE_DIR)
                final_file_path = f"{MERGE_DIR}/{file_type}.html"
                deduplicate_html(file_type, temp_file_path, final_file_path)
                
                if os.path.exists(final_file_path) and os.path.getsize(final_file_path) > 0:
                    with open(final_file_path, 'rb') as f:
                        html_file = BytesIO(f.read())
                        html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                        total_items = len(BeautifulSoup(f.read(), 'html.parser').body.find_all(recursive=False))
                        send_telegram_document(chat_id, html_file, html_file.name, 
                                              f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})")
                    any_sent = True

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            if not any_sent:
                send_telegram_message(chat_id=chat_id, text=f"⚠️ No media found for '{username}' ({start_year}-{end_year})")

        except Exception as e:
            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, 
                                 text=f"❌ Error for '{username}': {str(e)}")
            logger.error(f"Error processing {username}: {str(e)}")
        finally:
            if chat_id in active_tasks:
                executor, _, _ = active_tasks[chat_id]
                if executor:
                    executor.shutdown(wait=False)
                del active_tasks[chat_id]
            shutil.rmtree(SAVE_DIR, ignore_errors=True)

        return '', 200
    
    except Exception as e:
        logger.critical(f"Unhandled error: {str(e)}")
        if 'chat_id' in locals() and 'progress_msg' in locals():
            try:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"❌ Critical error: {str(e)}")
            except:
                pass
        if chat_id in active_tasks:
            executor, _, _ = active_tasks[chat_id]
            if executor:
                executor.shutdown(wait=False)
            del active_tasks[chat_id]
        return '', 200

@app.route('/health', methods=['GET'])
def health_check():
    from flask import jsonify
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

def set_webhook():
    railway_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
    if railway_url:
        webhook_url = f"https://{railway_url}/telegram"
        bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.error("RAILWAY_PUBLIC_DOMAIN not set")

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

if __name__ == "__main__":
    set_webhook()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)