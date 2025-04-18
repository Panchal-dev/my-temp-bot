import os
import aiohttp
import aioflask
from flask import request
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from telegram import Bot, Update
from telegram.ext import Application
import time
import logging
import shutil
import json
import asyncio
from telegram.error import TelegramError

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = aioflask.Flask(__name__)

# Configuration
BASE_URL = "https://desifakes.com"
SAVE_DIR = "HTML_Pages"
MERGE_DIR = "Merge"
ERROR_LOG_FILE = os.path.join(MERGE_DIR, "error.txt")
MAX_RETRIES = 3
MAX_WORKERS = 9
TELEGRAM_RATE_LIMIT_DELAY = 0
THREAD_SUBMISSION_DELAY = 0.5

# Proxy configuration
PROXY_GROUP_1 = {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"}
PROXY_GROUP_2 = {"http": "http://34.143.143.61:7777", "https": "http://34.143.143.61:7777"}
PROXY_GROUP_3 = {"http": "http://146.56.142.114:1080", "https": "http://146.56.142.114:1080"}
FALLBACK_PROXIES = [
    {"http": "http://171.240.118.170:5170", "https": "http://171.240.118.170:5170"},
    {"http": "http://116.108.21.141:10020", "https": "http://116.108.21.141:10020"},
    {"http": "http://172.167.161.8:8080", "https": "http://172.167.161.8:8080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://34.143.143.61:7777", "https": "http://34.143.143.61:7777"},
    {"http": "http://146.56.142.114:1080", "https": "http://146.56.142.114:1080"},
]

ALLOWED_CHAT_IDS = {5809601894, 1285451259}
active_tasks = {}
page_cache = {}

# HTML initialization
def init_html(file_path, title, image_urls=None):
    if "images.html" in file_path:
        image_urls = image_urls or []
        if not all(isinstance(url, str) for url in image_urls):
            logger.error(f"image_urls contains non-string items: {image_urls}")
            raise ValueError("All image URLs must be strings")
        image_urls_json = json.dumps(image_urls)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: black; color: white; }}
        h1 {{ text-align: center; color: white; }}
        .masonry-container {{ display: flex; gap: 15px; justify-content: center; }}
        .column {{ display: flex; flex-direction: column; gap: 15px; }}
        .column img {{ width: 100%; height: auto; display: block; border-radius: 6px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="masonry-container" id="masonry"></div>
    <script>
        const imageUrls = {image_urls_json};
        const columns = 3;
        const masonry = document.getElementById('masonry');
        const colDivs = [];
        for (let i = 0; i < columns; i++) {{
            const col = document.createElement('div');
            col.className = 'column';
            masonry.appendChild(col);
            colDivs.push(col);
        }}
        imageUrls.forEach((url, index) => {{
            const img = document.createElement('img');
            img.src = url;
            colDivs[index % columns].appendChild(img);
        }});
    </script>
</body>
</html>""")
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: white; color: black; }}
        h1 {{ text-align: center; color: black; }}
        h2 {{ font-size: 24px; margin: 20px 0 10px; color: #333; text-align: center; }}
        .year-section {{ margin-bottom: 20px; }}
        video, img {{ max-width: 100%; height: auto; display: block; margin: 10px auto; border-radius: 6px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
""")

def append_to_html(file_path, content):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(content)
        logger.info(f"Appended to {file_path}: {content[:50]}...")

def close_html(file_path):
    if "images.html" not in file_path:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write("</body></html>")

def log_error(url, error_message):
    os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url} - {error_message}\n")
        logger.info(f"Logged error for {url}: {error_message}")

async def make_request(url, proxy_group, max_retries=MAX_RETRIES):
    proxy = proxy_group.get("http")
    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.get(url, proxy=proxy, timeout=10) as response:
                    response.raise_for_status()
                    text = await response.text()
                    logger.info(f"Success with {proxy} on attempt {attempt + 1} for {url}")
                    return text
            except Exception as e:
                logger.warning(f"Failed with {proxy} on attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == max_retries - 1:
                    for fallback_proxy in FALLBACK_PROXIES:
                        fallback = fallback_proxy.get("http")
                        try:
                            async with session.get(url, proxy=fallback, timeout=10) as response:
                                response.raise_for_status()
                                text = await response.text()
                                logger.info(f"Success with fallback {fallback} for {url}")
                                return text
                        except Exception as fb_e:
                            logger.warning(f"Fallback {fallback} failed for {url}: {str(fb_e)}")
                    log_error(url, f"All proxies failed: {str(e)}")
                    raise
                await asyncio.sleep(1)

def generate_year_link(year, username, title_only=False):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    url = f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}"
    url += "&c[title_only]=1" if title_only else "&c[title_only]=0"
    url += "&o=date"
    logger.info(f"Generated search URL for {year}: {url}")
    return url

def split_url(url, start_date, end_date, max_pages=10):
    try:
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_pages = asyncio.run(fetch_total_pages(url))
        logger.info(f"Total pages for {url}: {total_pages}")
        if total_pages < max_pages:
            return [url]
        total_days = (end_dt - start_dt).days
        mid_dt = start_dt + timedelta(days=total_days // 2)
        first_range = (start_date, mid_dt.strftime("%Y-%m-%d"))
        second_range = ((mid_dt + timedelta(days=1)).strftime("%Y-%m-%d"), end_date)
        first_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={first_range[0]}", url)
        first_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={first_range[1]}", url)
        second_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={second_range[0]}", url)
        second_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={second_range[1]}", url)
        return split_url(first_url, first_range[0], first_range[1]) + split_url(second_url, second_range[0], second_range[1])
    except Exception as e:
        log_error(url, f"Split error: {str(e)}")
        logger.error(f"Split error for {url}: {str(e)}")
        return [url]

async def fetch_total_pages(url, proxy_group=PROXY_GROUP_1):
    if url in page_cache:
        logger.info(f"Using cached total pages for {url}: {page_cache[url]}")
        return page_cache[url]
    try:
        text = await make_request(url, proxy_group)
        soup = BeautifulSoup(text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        total_pages = max(int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()) if pagination else 1
        page_cache[url] = total_pages
        logger.info(f"Fetched total pages for {url}: {total_pages}")
        return total_pages
    except Exception as e:
        log_error(url, f"Fetch total pages error: {str(e)}")
        logger.error(f"Error fetching total pages for {url}: {str(e)}")
        return 1

async def scrape_post_links(search_url, proxy_group=PROXY_GROUP_1):
    try:
        text = await make_request(search_url, proxy_group)
        soup = BeautifulSoup(text, 'html.parser')
        links = list(dict.fromkeys(urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True)
                                   if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
        logger.info(f"Found {len(links)} post links for {search_url}")
        return links
    except Exception as e:
        log_error(search_url, f"Scrape post links error: {str(e)}")
        logger.error(f"Error scraping post links for {search_url}: {str(e)}")
        return []

def add_media(media_url, media_type, year, image_list=None, video_list=None, gif_list=None, seen_urls=None):
    exclude_keywords = ["addonflare/awardsystem/icons/", "ozzmodz_badges_badge/", "avatars/"]
    if media_url.startswith("data:image") or any(keyword in media_url.lower() for keyword in exclude_keywords):
        logger.info(f"Filtered out {media_url} due to exclusion rules")
        return False
    media_url = urljoin(BASE_URL, media_url) if media_url.startswith("/") else media_url
    if seen_urls is not None and media_url in seen_urls:
        logger.info(f"Skipped duplicate in-year {media_type}: {media_url}")
        return False
    if media_type == "image" and image_list is not None:
        logger.info(f"Adding image: {media_url} for year {year}")
        image_list.append((media_url, year))
        if seen_urls is not None:
            seen_urls.add(media_url)
        return True
    elif media_type == "video" and video_list is not None:
        logger.info(f"Adding video: {media_url} for year {year}")
        video_list.append((f'<p><video controls style="max-width:100%;"><source src="{media_url}" type="video/mp4"></video></p>', year))
        if seen_urls is not None:
            seen_urls.add(media_url)
        return True
    elif media_type == "gif" and gif_list is not None:
        logger.info(f"Adding GIF: {media_url} for year {year}")
        gif_list.append((f'<div><img src="{media_url}" alt="GIF" style="max-width:100%;height:auto;"></div>', year))
        if seen_urls is not None:
            seen_urls.add(media_url)
        return True
    logger.info(f"Rejected {media_type}: {media_url} for year {year}")
    return False

async def process_post(post_link, year, username, proxy_group, image_list, video_list, gif_list):
    try:
        logger.info(f"Processing post: {post_link}")
        text = await make_request(post_link, proxy_group)
        soup = BeautifulSoup(text, 'html.parser')
        post_id = re.search(r'post-(\d+)', post_link).group(1) if re.search(r'post-(\d+)', post_link) else None
        articles = [soup.find('article', {'data-content': f'post-{post_id}', 'id': f'js-post-{post_id}'})] if post_id else soup.find_all('article')
        username_lower = username.lower()
        articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        seen_urls = set()
        for article in articles:
            media_found = []
            for media in article.find_all(['img', 'video', 'source', 'a'], recursive=True):
                media_url = None
                media_type = None
                if media.name == 'img' and media.get('src'):
                    media_url = media['src']
                    media_type = "gif" if media_url.lower().endswith(".gif") else "image"
                elif media.name == 'video' and (media.get('src') or media.find('source')):
                    media_url = media.get('src') or (media.find('source').get('src') if media.find('source') else None)
                    media_type = "video" if media_url else None
                elif media.name == 'source' and media.get('src'):
                    media_url = media['src']
                    media_type = "video"
                elif media.name == 'a' and media.get('href'):
                    href = media['href']
                    if href.lower().endswith('.gif'):
                        media_url = href
                        media_type = "gif"
                    elif href.lower().endswith(('.mp4', '.webm', '.mov')):
                        media_url = href
                        media_type = "video"
                    elif href.lower().endswith(('.jpg', '.jpeg', '.png')):
                        media_url = href
                        media_type = "image"
                if media_url and media_type:
                    media_found.append((media_url, media_type))
                    added = add_media(
                        media_url,
                        media_type,
                        year,
                        image_list if media_type == "image" else None,
                        video_list if media_type == "video" else None,
                        gif_list if media_type == "gif" else None,
                        seen_urls
                    )
                    if not added:
                        logger.info(f"Skipped {media_type}: {media_url} in post {post_link}")
            logger.info(f"Post {post_link}: Found {len(media_found)} media items: {media_found}")
    except Exception as e:
        log_error(post_link, f"Process post error: {str(e)}")
        logger.error(f"Error processing post {post_link}: {str(e)}")

async def process_year(year, search_url, username, chat_id, image_list, video_list, gif_list):
    try:
        total_pages = await fetch_total_pages(search_url, PROXY_GROUP_1)
        urls_to_process = split_url(search_url, f"{year}-01-01", f"{year}-12-31") if total_pages >= 10 else [search_url]
        executor1 = ThreadPoolExecutor(max_workers=3)
        executor2 = ThreadPoolExecutor(max_workers=3)
        executor3 = ThreadPoolExecutor(max_workers=3)
        futures = []
        for url in urls_to_process:
            total_pages = await fetch_total_pages(url, PROXY_GROUP_1)
            logger.info(f"Processing {total_pages} pages for {url}")
            for page in range(total_pages, 0, -1):  # Reverse for Dec 31 to Jan 1
                post_links = await scrape_post_links(f"{url}&page={page}", PROXY_GROUP_1)
                third = len(post_links) // 3
                for i, post in enumerate(post_links):
                    if i < third:
                        proxy_group = PROXY_GROUP_1
                        executor = executor1
                    elif i < 2 * third:
                        proxy_group = PROXY_GROUP_2
                        executor = executor2
                    else:
                        proxy_group = PROXY_GROUP_3
                        executor = executor3
                    future = executor.submit(asyncio.run, process_post(post, year, username, proxy_group, image_list, video_list, gif_list))
                    futures.append(future)
                    await asyncio.sleep(THREAD_SUBMISSION_DELAY)
        _, _, progress_msg_id = active_tasks[chat_id]
        active_tasks[chat_id] = ((executor1, executor2, executor3), futures, progress_msg_id)
        for future in as_completed(futures):
            if chat_id not in active_tasks:
                logger.info(f"Task cancelled for chat_id {chat_id}")
                raise Exception("Task cancelled by user")
            future.result()
    except Exception as e:
        logger.error(f"Error processing year {year}: {str(e)}")
        raise

def merge_and_deduplicate(file_type, media_list, merge_dir, username, start_year, end_year):
    if not media_list:
        logger.info(f"No {file_type} to process")
        return None
    logger.info(f"Processing {len(media_list)} {file_type} items before deduplication")
    seen_urls = set()
    unique_media = []
    for content, year in media_list:
        url = content
        if file_type != "images":
            soup = BeautifulSoup(content, 'html.parser')
            if file_type == "videos" and soup.video and soup.video.source:
                url = soup.video.source.get("src")
            elif file_type == "gifs" and soup.img:
                url = soup.img.get("src")
        if url not in seen_urls:
            seen_urls.add(url)
            unique_media.append((content, year))
            logger.info(f"Kept {file_type}: {url} for year {year}")
        else:
            logger.info(f"Skipped cross-year duplicate {file_type}: {url}")
    if not unique_media:
        logger.info(f"No unique {file_type} after deduplication")
        return None
    logger.info(f"Writing {len(unique_media)} unique {file_type} to HTML")
    final_file_path = os.path.join(merge_dir, f"{file_type}.html")
    if file_type == "images":
        init_html(final_file_path, f"{username} - Images Links ({start_year}-{end_year})", [content for content, _ in unique_media])
    else:
        init_html(final_file_path, f"Merged {file_type.capitalize()} Links ({start_year}-{end_year})")
        current_year = None
        for content, year in unique_media:
            if year != current_year:
                append_to_html(final_file_path, f'<div class="year-section"><h2>{year}</h2></div>')
                current_year = year
            append_to_html(final_file_path, content)
        close_html(final_file_path)
    return final_file_path

async def send_telegram_message(bot, chat_id, text, max_retries=MAX_RETRIES, **kwargs):
    for attempt in range(max_retries):
        try:
            response = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            logger.info(f"Sent message to chat_id {chat_id}: {text[:50]}...")
            await asyncio.sleep(TELEGRAM_RATE_LIMIT_DELAY)
            return response
        except TelegramError as e:
            if 'Too Many Requests' in str(e):
                retry_after = 3
                logger.warning(f"429 Too Many Requests: retrying after {retry_after} seconds")
                await asyncio.sleep(retry_after)
                continue
            logger.error(f"Send message attempt {attempt + 1} failed for chat_id {chat_id}: {str(e)}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1 * (attempt + 1))
    raise Exception("Failed to send message after retries")

async def send_telegram_document(bot, chat_id, file_buffer, filename, caption, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            file_buffer.seek(0)
            response = await bot.send_document(chat_id=chat_id, document=file_buffer, filename=filename, caption=caption[:1024])
            logger.info(f"Sent document {filename} to chat_id {chat_id}")
            await asyncio.sleep(TELEGRAM_RATE_LIMIT_DELAY)
            return response
        except TelegramError as e:
            if 'Too Many Requests' in str(e):
                retry_after = 3
                logger.warning(f"429 Too Many Requests: retrying after {retry_after} seconds")
                await asyncio.sleep(retry_after)
                continue
            logger.error(f"Send document attempt {attempt + 1} failed for chat_id {chat_id}: {str(e)}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1 * (attempt + 1))
    raise Exception("Failed to send document after retries")

async def cancel_task(bot, chat_id):
    if chat_id in active_tasks:
        (executor1, executor2, executor3), futures, progress_msg_id = active_tasks[chat_id]
        for future in futures:
            future.cancel()
        executor1.shutdown(wait=False)
        executor2.shutdown(wait=False)
        executor3.shutdown(wait=False)
        if progress_msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=progress_msg_id)
                logger.info(f"Deleted progress message for chat_id {chat_id}")
            except TelegramError as e:
                logger.error(f"Failed to delete progress message for chat_id {chat_id}: {str(e)}")
        del active_tasks[chat_id]
        logger.info(f"Cancelled task for chat_id {chat_id}")
        return True
    logger.info(f"No active task to cancel for chat_id {chat_id}")
    return False

@app.route('/telegram', methods=['POST'])
async def telegram_webhook():
    bot = app.bot
    try:
        update = request.get_json()
        if not update or 'message' not in update:
            logger.info("Received empty or invalid update")
            return '', 200
        chat_id = update['message']['chat']['id']
        message_id = update['message'].get('message_id')
        text = update['message'].get('text', '').strip()
        logger.info(f"Received command: {text} from chat_id: {chat_id}")
        if chat_id not in ALLOWED_CHAT_IDS:
            await send_telegram_message(bot, chat_id=chat_id, text="❌ Restricted to specific users.", reply_to_message_id=message_id)
            return '', 200
        if not text:
            await send_telegram_message(bot, chat_id=chat_id, text="Please send a search query", reply_to_message_id=message_id)
            return '', 200
        if text.lower() == '/stop':
            if await cancel_task(bot, chat_id):
                await send_telegram_message(bot, chat_id=chat_id, text="✅ Scraping stopped", reply_to_message_id=message_id)
            else:
                await send_telegram_message(bot, chat_id=chat_id, text="ℹ️ No active scraping to stop", reply_to_message_id=message_id)
            return '', 200
        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            await send_telegram_message(bot, chat_id=chat_id, text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: 'Madhuri Dixit' n 2019 2025", reply_to_message_id=message_id)
            return '', 200
        if chat_id in active_tasks:
            await send_telegram_message(bot, chat_id=chat_id, text="⚠️ Scraping already running. Use /stop to cancel.", reply_to_message_id=message_id)
            return '', 200
        if parts[0] == '/start':
            username = ' '.join(parts[1:-3]) if len(parts) > 4 else parts[1]
            title_only_idx = 2 if len(parts) <= 4 else len(parts) - 3
        else:
            username = ' '.join(parts[:-3]) if len(parts) > 3 else parts[0]
            title_only_idx = 1 if len(parts) <= 3 else len(parts) - 3
        title_only = parts[title_only_idx].lower() == 'y' if len(parts) > title_only_idx else False
        start_year = int(parts[-2]) if len(parts) > 2 else 2019
        end_year = int(parts[-1]) if len(parts) > 1 else 2025
        logger.info(f"Starting scrape for username: {username}, title_only: {title_only}, years: {start_year}-{end_year}")
        os.makedirs(SAVE_DIR, exist_ok=True)
        os.makedirs(MERGE_DIR, exist_ok=True)
        try:
            progress_msg = await send_telegram_message(bot, chat_id=chat_id, text=f"🔍 Processing '{username}' ({start_year}-{end_year})...")
            active_tasks[chat_id] = (None, [], progress_msg.message_id)
            logger.info(f"Sent progress message for chat_id {chat_id}, message_id: {progress_msg.message_id}")
        except Exception as e:
            logger.error(f"Failed to send progress message for chat_id {chat_id}: {str(e)}")
            await send_telegram_message(bot, chat_id=chat_id, text=f"❌ Failed to start: {str(e)}", reply_to_message_id=message_id)
            return '', 200
        try:
            years = list(range(end_year, start_year - 1, -1))
            image_list = []
            video_list = []
            gif_list = []
            for year in years:
                search_url = generate_year_link(year, username, title_only)
                await process_year(year, search_url, username, chat_id, image_list, video_list, gif_list)
            logger.info(f"Collected {len(image_list)} images, {len(video_list)} videos, {len(gif_list)} GIFs")
            any_sent = False
            for file_type, media_list in [("images", image_list), ("videos", video_list), ("gifs", gif_list)]:
                final_file_path.ConcurrentModificationError = merge_and_deduplicate(file_type, media_list, MERGE_DIR, username, start_year, end_year)
                if final_file_path and os.path.exists(final_file_path) and os.path.getsize(final_file_path) > 0:
                    with open(final_file_path, 'rb') as f:
                        html_file = BytesIO(f.read())
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                    with open(final_file_path, 'r', encoding='utf-8') as f2:
                        soup = BeautifulSoup(f2.read(), 'html.parser')
                        if file_type == "images":
                            script_tag = soup.find('script')
                            total_items = len(json.loads(re.search(r'const imageUrls = (\[.*?\]);', script_tag.string, re.DOTALL).group(1))) if script_tag else 0
                        else:
                            total_items = len(soup.body.find_all(['p', 'div'], recursive=False)) - len(soup.body.find_all('div', class_='year-section')) if soup.body else 0
                    await send_telegram_document(bot, chat_id, html_file, html_file.name,
                                                f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})")
                    logger.info(f"Sent {file_type}.html with {total_items} items to chat_id {chat_id}")
                    any_sent = True
                else:
                    logger.info(f"No {file_type} found or file empty: {final_file_path}")
            await bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            logger.info(f"Deleted progress message for chat_id {chat_id}")
            if not any_sent:
                await send_telegram_message(bot, chat_id=chat_id, text=f"⚠️ No media found for '{username}' ({start_year}-{end_year})")
        except Exception as e:
            logger.error(f"Error processing {username} for chat_id {chat_id}: {str(e)}")
            await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id,
                                       text=f"❌ Error for '{username}': {str(e)}")
        finally:
            if chat_id in active_tasks:
                (executor1, executor2, executor3), _, _ = active_tasks[chat_id]
                if executor1:
                    executor1.shutdown(wait=False)
                if executor2:
                    executor2.shutdown(wait=False)
                if executor3:
                    executor3.shutdown(wait=False)
                del active_tasks[chat_id]
                logger.info(f"Cleaned up task for chat_id {chat_id}")
            shutil.rmtree(SAVE_DIR, ignore_errors=True)
            logger.info(f"Removed temporary directory {SAVE_DIR}")
        return '', 200
    except Exception as e:
        logger.critical(f"Unhandled error in webhook for chat_id {chat_id if 'chat_id' in locals() else 'unknown'}: {str(e)}")
        if 'chat_id' in locals() and 'message_id' in locals():
            try:
                await send_telegram_message(bot, chat_id=chat_id, text=f"❌ Critical error: {str(e)}", reply_to_message_id=message_id)
            except Exception as send_e:
                logger.error(f"Failed to send error message to chat_id {chat_id}: {str(send_e)}")
        if 'chat_id' in locals() and chat_id in active_tasks:
            await cancel_task(bot, chat_id)
        return '', 200

@app.route('/health', methods=['GET'])
async def health_check():
    from flask import jsonify
    from datetime import datetime
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

async def set_webhook():
    railway_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
    if railway_url:
        webhook_url = f"https://{railway_url}/telegram"
        try:
            await app.bot.set_webhook(url=webhook_url)
            logger.info(f"Webhook set to: {webhook_url}")
        except TelegramError as e:
            logger.error(f"Failed to set webhook: {str(e)}")
    else:
        logger.error("RAILWAY_PUBLIC_DOMAIN not set")

def init_bot():
    try:
        TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        app.bot = bot
        logger.info("Bot initialized successfully")
        asyncio.run(set_webhook())
    except KeyError:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize bot: {str(e)}")
        raise

if __name__ == "__main__":
    init_bot()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)