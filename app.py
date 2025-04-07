import os
import requests
from bs4 import BeautifulSoup
import re
import logging
from urllib.parse import urljoin
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot
from datetime import datetime, timedelta
import traceback
import time

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = 5
MAX_PAGES_PER_SEARCH = 10  # Threshold for splitting

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
PROXY =  {"http": "http://157.230.40.77:1004", "https": "http://157.230.40.77:1004"}
FALLBACK_PROXIES = [
    {"http": "http://146.56.142.114:1080", "https": "http://146.56.142.114:1080"},
    {"http": "http://45.140.143.77:18080", "https": "http://45.140.143.77:18080"},
    {"http": "http://172.188.122.92:80", "https": "http://172.188.122.92:80"}
]

BASE_URL = "https://desifakes.com"

class ScraperError(Exception):
    pass

# Global task tracking
active_tasks = {}

# Allowed chat IDs
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

def make_request(url, method='get', **kwargs):
    initial_timeout = 11
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

def generate_links(start_year, end_year, username, title_only=False):
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")
    
    current_date = datetime.now()
    current_year = current_date.year
    start_year = max(2010, min(start_year, current_year))
    end_year = min(current_year, max(end_year, start_year))
    
    links = []
    for year in range(end_year, start_year - 1, -1):  # Descending order
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31" if year < current_year or current_date.strftime("%Y-%m-%d") > f"{year}-12-31" else current_date.strftime("%Y-%m-%d")
        url = (
            f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}"
            f"&c[newer_than]={start_date}"
            f"&c[older_than]={end_date}"
            f"&c[title_only]={1 if title_only else 0}"
            "&o=date"
        )
        links.append((year, url, start_date, end_date))
    logger.info(f"Generated {len(links)} search URLs for {username}")
    return links

def split_url(url, start_date, end_date, max_pages=MAX_PAGES_PER_SEARCH):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        total_pages = max([int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]) if pagination else 1
        if total_pages < max_pages:
            return [(url, start_date, end_date)]
        
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
        return [(url, start_date, end_date)]

def fetch_page_data(url, page=None):
    try:
        full_url = f"{url}&page={page}" if page else url
        response = make_request(full_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = list(dict.fromkeys(urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True) 
                                  if 'threads/' in link['href'] and not link['href'].startswith('#') and 'page-' not in link['href']))
        pagination = soup.find('div', class_='pageNav')
        total_pages = max([int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]) if pagination else 1
        return links, total_pages
    except Exception as e:
        logger.error(f"Failed to fetch page data for {full_url}: {str(e)}")
        return [], 1

def process_post(post_link, username, start_year, end_year, media_by_date, global_seen):
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date = None
        date_elem = soup.find('time', class_='u-dt')
        if date_elem and 'datetime' in date_elem.attrs:
            try:
                post_date = datetime.strptime(date_elem['datetime'], "%Y-%m-%dT%H:%M:%S%z")
                date = post_date.strftime("%Y-%m-%d")
                year = post_date.year
            except ValueError:
                pass
        
        if not date and date_elem:
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_elem.get_text(strip=True))
            if match:
                date = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                year = int(match.group(1))
        
        if not date:
            year = start_year
            date = f"{year}-01-01"
        
        if year < start_year or year > end_year:
            return
        
        post_id = re.search(r'post-(\d+)', post_link)
        articles = [soup.find('article', {'data-content': f'post-{post_id.group(1)}', 'id': f'js-post-{post_id.group(1)}'})] if post_id else soup.find_all('article')
        username_lower = username.lower()
        filtered_articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        
        for article in filtered_articles:
            for img in article.find_all('img', src=True):
                src = urljoin(BASE_URL, img['src']) if img['src'].startswith("/") else img['src']
                if (src.startswith("data:image") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                media_type = "gifs" if src.endswith(".gif") else "images"
                if src not in global_seen[media_type]:
                    global_seen[media_type].add(src)
                    if date not in media_by_date[year][media_type]:
                        media_by_date[year][media_type][date] = []
                    media_by_date[year][media_type][date].append(src)
            
            for video in article.find_all('video', src=True):
                src = urljoin(BASE_URL, video['src']) if video['src'].startswith("/") else video['src']
                if src not in global_seen['videos']:
                    global_seen['videos'].add(src)
                    if date not in media_by_date[year]['videos']:
                        media_by_date[year]['videos'][date] = []
                    media_by_date[year]['videos'][date].append(src)
            
            for source in article.find_all('source', src=True):
                src = urljoin(BASE_URL, source['src']) if source['src'].startswith("/") else source['src']
                if src not in global_seen['videos']:
                    global_seen['videos'].add(src)
                    if date not in media_by_date[year]['videos']:
                        media_by_date[year]['videos'][date] = []
                    media_by_date[year]['videos'][date].append(src)
            
            for link in article.find_all('a', href=True):
                href = urljoin(BASE_URL, link['href']) if link['href'].startswith("/") else link['href']
                if any(href.lower().endswith(ext) for ext in ['.mp4', '.webm', '.mov']):
                    if href not in global_seen['videos']:
                        global_seen['videos'].add(href)
                        if date not in media_by_date[year]['videos']:
                            media_by_date[year]['videos'][date] = []
                        media_by_date[year]['videos'][date].append(href)
    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")

def create_html(file_type, media_by_date, username, start_year, end_year):
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
    for year in sorted(media_by_date.keys(), reverse=True):
        items_by_date = media_by_date[year][file_type]
        if items_by_date:
            html_content += f"<h2>{year}</h2>"
            for date in sorted(items_by_date.keys(), reverse=True):
                items = items_by_date[date]
                total_items += len(items)
                html_content += f"<h3>{date}</h3>"
                for item in items:
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
            media_by_date = {year: {'images': {}, 'videos': {}, 'gifs': {}} for year in range(start_year, end_year + 1)}
            all_post_links = []
            seen_links = set()
            global_seen = {'images': set(), 'videos': set(), 'gifs': set()}
            
            search_links = generate_links(start_year, end_year, username, title_only)
            if not search_links:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"⚠️ No search URLs for '{username}'")
                return '', 200

            # Fetch initial page and total pages concurrently
            page_futures = {}
            url_page_cache = {}
            for year, search_link, start_date, end_date in search_links:
                urls_to_process = split_url(search_link, start_date, end_date)
                for url, s_date, e_date in urls_to_process:
                    if url not in url_page_cache:
                        future = executor.submit(fetch_page_data, url)
                        page_futures[future] = (url, s_date, e_date)
            
            for future in as_completed(page_futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                url, _, _ = page_futures[future]
                links, total_pages = future.result()
                url_page_cache[url] = total_pages
                all_post_links.extend(link for link in links if link not in seen_links)
                seen_links.update(links)

            # Fetch remaining pages concurrently
            additional_futures = {}
            for url, total_pages in url_page_cache.items():
                for page in range(2, total_pages + 1):
                    future = executor.submit(fetch_page_data, url, page)
                    additional_futures[future] = url
            
            for future in as_completed(additional_futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                links, _ = future.result()
                all_post_links.extend(link for link in links if link not in seen_links)
                seen_links.update(links)

            if not all_post_links:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"⚠️ No posts found for '{username}' ({start_year}-{end_year})")
                return '', 200

            logger.info(f"Processing {len(all_post_links)} unique post links")
            post_futures = [executor.submit(process_post, link, username, start_year, end_year, media_by_date, global_seen) for link in all_post_links]
            active_tasks[chat_id] = (executor, post_futures)

            processed_count = 0
            total_posts = len(all_post_links)
            for future in as_completed(post_futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                future.result()
                processed_count += 1
                if processed_count % 10 == 0 or processed_count == total_posts:
                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"🔍 Processing '{username}' ({start_year}-{end_year}): {processed_count}/{total_posts} posts")

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
            any_sent = False
            for file_type in ["images", "videos", "gifs"]:
                html_content = create_html(file_type, media_by_date, username, start_year, end_year)
                if html_content:
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                    total_items = sum(len(media_by_date[year][file_type][date]) for year in media_by_date for date in media_by_date[year][file_type])
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