import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from flask import Flask, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot
from datetime import datetime, timedelta
import time
import logging
import shutil
import json
from telebot.apihelper import ApiTelegramException

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
MAX_WORKERS = 10  # 5 for PROXY_GROUP_1, 5 for PROXY_GROUP_2
TELEGRAM_RATE_LIMIT_DELAY = 2  # Seconds between Telegram API calls

# Proxy configuration
# Proxy configuration
PROXY_GROUP_1 = {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"}
PROXY_GROUP_2 = {"http": "http://42.114.11.82:8080", "https": "http://42.114.11.82:8080"}
FALLBACK_PROXIES = [
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://34.143.143.61:7777", "https": "http://34.143.143.61:7777"},
    {"http": "http://171.240.118.170:5134", "https": "http://171.240.118.170:5134"},
    {"http": "http://15.235.9.224:28003", "https": "http://15.235.9.224:28003"},
    {"http": "http://171.240.118.170:5134", "https": "http://171.240.118.170:5134"},
    {"http": "http://219.135.229.196:7890", "https": "http://219.135.229.196:7890"},
    {"http": "http://219.135.229.196:7890", "https": "http://219.135.229.196:7890"},
    {"http": "http://219.135.229.196:7890", "https": "http://219.135.229.196:7890"},
    {"http": "http://34.143.143.61:7777", "https": "http://34.143.143.61:7777"}
]

ALLOWED_CHAT_IDS = {5809601894, 1285451259}
active_tasks = {}

# HTML initialization with masonry styling for images and gifs
def init_html(file_path, title, media_urls=None):
    media_urls = media_urls or []
    if not all(isinstance(url, str) for url in media_urls):
        logger.error(f"media_urls contains non-string items: {media_urls}")
        raise ValueError("All media URLs must be strings")
    media_urls_json = json.dumps(media_urls)
    if "images.html" in file_path or "gifs.html" in file_path:
        media_type = "Images" if "images.html" in file_path else "GIFs"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: black;
            color: white;
        }}
        h1 {{
            text-align: center;
        }}
        .masonry-container {{
            display: flex;
            gap: 15px;
            justify-content: center;
        }}
        .column {{
            display: flex;
            flex-direction: column;
            gap: 15px;
        }}
        .column img {{
            width: 100%;
            height: auto;
            display: block;
            border-radius: 6px;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="masonry-container" id="masonry"></div>

    <script>
        const mediaUrls = {media_urls_json};

        const columns = 3;
        const masonry = document.getElementById('masonry');

        const colDivs = [];
        for (let i = 0; i < columns; i++) {{
            const col = document.createElement('div');
            col.className = 'column';
            masonry.appendChild(col);
            colDivs.push(col);
        }}

        mediaUrls.forEach((url, index) => {{
            const img = document.createElement('img');
            img.src = url;
            colDivs[index % columns].appendChild(img);
        }});
    </script>
</body>
</html>""")
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html><html><head><title>{title}</title></head><body style="background-color: black; color: white;">""")

def append_to_html(file_path, content):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(content)
        logger.info(f"Appended to {file_path}: {content[:50]}...")

def close_html(file_path):
    if not ("images.html" in file_path or "gifs.html" in file_path):
        with open(file_path, "a", encoding="utf-8") as f:
            f.write("</body></html>")

def log_error(url, error_message):
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url} - {error_message}\n")

def make_request(url, proxy_group, max_retries=MAX_RETRIES):
    proxies = proxy_group
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10, proxies=proxies)
            response.raise_for_status()
            logger.info(f"Success with {proxies['http']} on attempt {attempt + 1} for {url}")
            return response
        except Exception as e:
            logger.warning(f"Failed with {proxies['http']} on attempt {attempt + 1} for {url}: {str(e)}")
            if attempt == max_retries - 1:
                for fallback_proxy in FALLBACK_PROXIES:
                    try:
                        response = requests.get(url, timeout=10, proxies=fallback_proxy)
                        response.raise_for_status()
                        logger.info(f"Success with fallback {fallback_proxy['http']} for {url}")
                        return response
                    except Exception as fb_e:
                        logger.warning(f"Fallback {fallback_proxy['http']} failed for {url}: {str(fb_e)}")
                log_error(url, f"All proxies failed: {str(e)}")
                raise
            time.sleep(1)
    raise Exception("Request failed after retries")

def generate_year_link(year, username, title_only=False):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    url = f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}"
    url += "&c[title_only]=1" if title_only else "&c[title_only]=0"
    url += "&o=date"  # Newest first
    return url

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

def fetch_total_pages(url, proxy_group=PROXY_GROUP_1):
    response = make_request(url, proxy_group)
    soup = BeautifulSoup(response.text, 'html.parser')
    pagination = soup.find('div', class_='pageNav')
    return max(int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()) if pagination else 1

def scrape_post_links(search_url, proxy_group=PROXY_GROUP_1):
    response = make_request(search_url, proxy_group)
    soup = BeautifulSoup(response.text, 'html.parser')
    return list(dict.fromkeys(urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True) 
                             if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))

def normalize_url(url):
    parsed = urlparse(url)
    return parsed.scheme + "://" + parsed.netloc + parsed.path

def add_media(media_url, media_type, year, media_list=None):
    exclude_keywords = ["addonflare/awardsystem/icons/", "ozzmodz_badges_badge", "premium", "likes", "avatars"]
    if media_url.startswith("data:image") or any(keyword in media_url.lower() for keyword in exclude_keywords):
        logger.info(f"Filtered out {media_url} due to exclusion rules")
        return
    media_url = urljoin(BASE_URL, media_url) if media_url.startswith("/") else media_url
    
    if media_type in ["image", "gif"] and media_list is not None:
        logger.info(f"Adding {media_type} to list: {media_url}")
        media_list.append(media_url)
    elif media_type == "video":
        append_to_html(f"{SAVE_DIR}/{year}/videos.html", f'<p><video controls style="max-width:100%;"><source src="{media_url}" type="video/mp4"></video></p>')

def process_post(post_link, year, username, proxy_group, image_list, gif_list):
    try:
        response = make_request(post_link, proxy_group)
        soup = BeautifulSoup(response.text, 'html.parser')
        post_id = re.search(r'post-(\d+)', post_link).group(1) if re.search(r'post-(\d+)', post_link) else None
        articles = [soup.find('article', {'data-content': f'post-{post_id}', 'id': f'js-post-{post_id}'})] if post_id else soup.find_all('article')
        
        username_lower = username.lower()
        articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        
        for article in articles:
            for media in article.find_all(['img', 'video', 'source', 'a'], recursive=True):
                if media.name == 'img' and media.get('src'):
                    src = media['src']
                    if media.get('data-url'):
                        src = media['data-url']
                    media_type = "gif" if src.lower().endswith(".gif") else "image"
                    logger.info(f"Found {media_type}: {src}")
                    add_media(src, media_type, year, gif_list if media_type == "gif" else image_list)
                elif media.name == 'video' and media.get('src'):
                    logger.info(f"Found video: {media['src']}")
                    add_media(media['src'], "video", year)
                elif media.name == 'source' and media.get('src'):
                    logger.info(f"Found video source: {media['src']}")
                    add_media(media['src'], "video", year)
                elif media.name == 'a' and media.get('href'):
                    href = media['href']
                    if any(href.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm', '.mov']):
                        media_type = "gif" if href.lower().endswith('.gif') else "image" if href.endswith(('.jpg', '.jpeg', '.png')) else "video"
                        logger.info(f"Found {media_type} from link: {href}")
                        add_media(href, media_type, year, gif_list if media_type == "gif" else image_list)
    except Exception as e:
        log_error(post_link, str(e))

def process_year(year, search_url, username, chat_id):
    year_dir = f"{SAVE_DIR}/{year}"
    os.makedirs(year_dir, exist_ok=True)
    
    for file_type in ["videos"]:
        init_html(f"{year_dir}/{file_type}.html", f"{year} {file_type.capitalize()} Links")
    
    total_pages = fetch_total_pages(search_url, PROXY_GROUP_1)
    urls_to_process = split_url(search_url, f"{year}-01-01", f"{year}-12-31") if total_pages >= 10 else [search_url]
    
    executor1 = ThreadPoolExecutor(max_workers=5)
    executor2 = ThreadPoolExecutor(max_workers=5)
    futures = []
    image_list = []  # List of URLs in original order
    gif_list = []    # List of URLs in original order
    
    for url in urls_to_process:
        total_pages = fetch_total_pages(url, PROXY_GROUP_1)
        for page in range(total_pages, 0, -1):  # Newest to oldest
            post_links = scrape_post_links(f"{url}&page={page}", PROXY_GROUP_1)
            for i, post in enumerate(post_links):
                proxy_group = PROXY_GROUP_1 if i < len(post_links) // 2 else PROXY_GROUP_2
                executor = executor1 if i < len(post_links) // 2 else executor2
                future = executor.submit(process_post, post, year, username, proxy_group, image_list, gif_list)
                futures.append(future)
    
    _, _, progress_msg_id = active_tasks[chat_id]
    active_tasks[chat_id] = ((executor1, executor2), futures, progress_msg_id)
    
    for future in as_completed(futures):
        if chat_id not in active_tasks:
            raise Exception("Task cancelled by user")
        future.result()
    
    # Deduplicate while preserving order
    seen_urls = set()
    unique_image_urls = []
    for url in image_list:
        normalized_url = normalize_url(url)
        if normalized_url not in seen_urls:
            seen_urls.add(normalized_url)
            unique_image_urls.append(url)
            logger.info(f"Kept image: {url}")
    
    seen_urls = set()
    unique_gif_urls = []
    for url in gif_list:
        normalized_url = normalize_url(url)
        if normalized_url not in seen_urls:
            seen_urls.add(normalized_url)
            unique_gif_urls.append(url)
            logger.info(f"Kept GIF: {url}")
    
    init_html(f"{year_dir}/images.html", f"{username} - Images Links ({year}-{year})", unique_image_urls)
    init_html(f"{year_dir}/gifs.html", f"{username} - GIFs Links ({year}-{year})", unique_gif_urls)
    
    for file_type in ["videos"]:
        close_html(f"{year_dir}/{file_type}.html")
        logger.info(f"Closed {year_dir}/{file_type}.html")

def merge_html_files(file_type, years, merge_dir):
    if file_type in ["images", "gifs"]:
        return None
    merge_file_path = f"{merge_dir}/{file_type}_temp.html"
    init_html(merge_file_path, f"Merged {file_type.capitalize()} Links")
    for year in years:
        year_file_path = f"{SAVE_DIR}/{year}/{file_type}.html"
        if os.path.exists(year_file_path) and os.path.getsize(year_file_path) > 0:
            with open(year_file_path, "r", encoding="utf-8") as f:
                content = re.search(r'<body>(.*?)</body>', f.read(), re.DOTALL)
                if content:
                    append_to_html(merge_file_path, content.group(1))
                    logger.info(f"Merged {year_file_path} into {merge_file_path}")
    close_html(merge_file_path)
    return merge_file_path

def deduplicate_html(file_type, temp_file_path, final_file_path):
    if file_type in ["images", "gifs"] or not temp_file_path:
        return
    with open(temp_file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    seen_urls = set()
    unique_content = []
    
    for element in soup.body.find_all(recursive=False):
        url = None
        if file_type == "videos" and element.name == "p" and element.video and element.video.source:
            url = normalize_url(element.video.source.get("src"))
        
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_content.append(str(element))
            logger.info(f"Kept unique {file_type}: {url}")
    
    init_html(final_file_path, f"Merged {file_type.capitalize()} Links")
    append_to_html(final_file_path, "\n".join(unique_content))
    close_html(final_file_path)
    os.remove(temp_file_path)

def send_telegram_message(chat_id, text, max_retries=MAX_RETRIES, **kwargs):
    for attempt in range(max_retries):
        try:
            response = bot.send_message(chat_id=chat_id, text=text, **kwargs)
            time.sleep(TELEGRAM_RATE_LIMIT_DELAY)
            return response
        except ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = int(e.result_json.get('parameters', {}).get('retry_after', 3))
                logger.warning(f"429 Too Many Requests: retrying after {retry_after} seconds")
                time.sleep(retry_after)
                continue
            logger.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1 * (attempt + 1))
    raise Exception("Failed to send message after retries")

def send_telegram_document(chat_id, file_buffer, filename, caption, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            file_buffer.seek(0)
            response = bot.send_document(chat_id=chat_id, document=file_buffer, visible_file_name=filename, caption=caption[:1024])
            time.sleep(TELEGRAM_RATE_LIMIT_DELAY)
            return response
        except ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = int(e.result_json.get('parameters', {}).get('retry_after', 3))
                logger.warning(f"429 Too Many Requests: retrying after {retry_after} seconds")
                time.sleep(retry_after)
                continue
            logger.warning(f"Send document attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1 * (attempt + 1))
    raise Exception("Failed to send document after retries")

def cancel_task(chat_id):
    if chat_id in active_tasks:
        (executor1, executor2), futures, _ = active_tasks[chat_id]
        for future in futures:
            future.cancel()
        executor1.shutdown(wait=False)
        executor2.shutdown(wait=False)
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
                send_telegram_message(chat_id=chat_id, text="ℹ️ No active scraping to stop --10", reply_to_message_id=message_id)
            return '', 200

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            send_telegram_message(chat_id=chat_id, text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: 'Rakul Preet Singh' n 2023 2025", reply_to_message_id=message_id)
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
            for file_type in ["images", "gifs", "videos"]:
                if file_type in ["images", "gifs"]:
                    for year in years:
                        final_file_path = f"{SAVE_DIR}/{year}/{file_type}.html"
                        if os.path.exists(final_file_path) and os.path.getsize(final_file_path) > 0:
                            with open(final_file_path, 'rb') as f:
                                html_file = BytesIO(f.read())
                            html_file.name = f"{username.replace(' ', '_')}_{file_type}_{year}.html"
                            with open(final_file_path, 'r', encoding='utf-8') as f2:
                                soup = BeautifulSoup(f2.read(), 'html.parser')
                                script_tag = soup.find('script')
                                if script_tag:
                                    urls_match = re.search(r'const mediaUrls = (\[.*?\]);', script_tag.string, re.DOTALL)
                                    total_items = len(json.loads(urls_match.group(1))) if urls_match else 0
                                else:
                                    total_items = 0
                            send_telegram_document(chat_id, html_file, html_file.name, 
                                                  f"Found {total_items} {file_type} for '{username}' ({year})")
                            logger.info(f"Sent {file_type}_{year}.html with {total_items} items")
                            any_sent = True
                else:
                    temp_file_path = merge_html_files(file_type, years, MERGE_DIR)
                    final_file_path = f"{MERGE_DIR}/{file_type}.html"
                    deduplicate_html(file_type, temp_file_path, final_file_path)
                    
                    if os.path.exists(final_file_path) and os.path.getsize(final_file_path) > 0:
                        with open(final_file_path, 'rb') as f:
                            html_file = BytesIO(f.read())
                        html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                        with open(final_file_path, 'r', encoding='utf-8') as f2:
                            soup = BeautifulSoup(f2.read(), 'html.parser')
                            total_items = len(soup.body.find_all(recursive=False)) if soup.body else 0
                        send_telegram_document(chat_id, html_file, html_file.name, 
                                              f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})")
                        logger.info(f"Sent {file_type}.html with {total_items} items")
                        any_sent = True
                    else:
                        logger.info(f"No {file_type} found or file empty: {final_file_path}")

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            if not any_sent:
                send_telegram_message(chat_id=chat_id, text=f"⚠️ No media found for '{username}' ({start_year}-{end_year})")

        except Exception as e:
            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, 
                                 text=f"❌ Error for '{username}': {str(e)}")
            logger.error(f"Error processing {username}: {str(e)}")
        finally:
            if chat_id in active_tasks:
                (executor1, executor2), _, _ = active_tasks[chat_id]
                if executor1:
                    executor1.shutdown(wait=False)
                if executor2:
                    executor2.shutdown(wait=False)
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
            (executor1, executor2), _, _ = active_tasks[chat_id]
            if executor1:
                executor1.shutdown(wait=False)
            if executor2:
                executor2.shutdown(wait=False)
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