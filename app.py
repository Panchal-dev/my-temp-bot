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
REQUEST_TIMEOUT = 20
MAX_WORKERS = 4  # Works with Railway’s 1 vCPU
MAX_PAGES_PER_SEARCH = 9

try:
    TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
except KeyError:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    raise
except Exception as e:
    logger.error(f"Failed to initialize bot: {str(e)}")
    raise

# List of proxies to try sequentially
PROXIES = [
    {
        "http": "http://34.143.143.61:7777",
        "https": "http://34.143.143.61:7777"
    },
    {
        "http": "http://45.140.143.77:18080",
        "https": "http://45.140.143.77:18080"
    },
    {
        "http": "http://34.143.143.61:7777",
        "https": "http://34.143.143.61:7777"
    },
    {
        "http": "http://172.188.122.92:80",
        "https": "http://172.188.122.92:80"
    },
    {
        "http": "http://49.51.232.22:13001",
        "https": "http://49.51.232.22:13001"
    }
]

BASE_URL = "https://desifakes.com"

class ScraperError(Exception):
    pass

# Global task tracking
active_tasks = {}

# Allowed chat IDs
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

def make_request(url, method='get', **kwargs):
    # Try each proxy in the PROXIES list
    for proxy_idx, proxy in enumerate(PROXIES):
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Attempting request to {url} with proxy {proxy_idx + 1}: {proxy['http']}")
                response = requests.request(
                    method,
                    url,
                    proxies=proxy,
                    timeout=REQUEST_TIMEOUT,
                    **kwargs
                )
                response.raise_for_status()
                logger.info(f"Success with proxy {proxy_idx + 1} on attempt {attempt + 1}")
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url} with proxy {proxy_idx + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Proxy {proxy_idx + 1} failed after {MAX_RETRIES} attempts")
                    if proxy_idx == len(PROXIES) - 1:  # Last proxy
                        raise ScraperError(f"All proxies failed after {MAX_RETRIES} attempts each: {str(e)}")
                    else:
                        logger.info(f"Switching to next proxy {proxy_idx + 2}")
                        break  # Move to next proxy
                time.sleep(1 * (attempt + 1))

def generate_links(start_year, end_year, username, title_only=False):
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")
    
    current_date = datetime.now()
    current_year = current_date.year
    start_year = max(2010, min(start_year, current_year))
    end_year = min(current_year, max(end_year, start_year))
    
    links = []
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31" if year < current_year else current_date.strftime("%Y-%m-%d")
        url = (
            f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}"
            f"&c[newer_than]={start_date}"
            f"&c[older_than]={end_date}"
            f"&c[title_only]={1 if title_only else 0}"
            "&o=date"
        )
        links.append((year, url))
    return links

def split_url(url, start_date, end_date, max_pages=MAX_PAGES_PER_SEARCH):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_pages = fetch_total_pages(url)
        if total_pages <= max_pages:
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

def fetch_total_pages(url):
    try:
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        if not pagination:
            return 1
        page_numbers = [int(link.text.strip()) for link in pagination.find_all('a') 
                       if link.text.strip().isdigit()]
        return max(page_numbers) if page_numbers else 1
    except Exception as e:
        logger.error(f"Error fetching pages for {url}: {str(e)}")
        return 1

def scrape_post_links(search_url):
    try:
        response = make_request(search_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        seen = set()
        for link in soup.find_all('a', href=True):
            href = link['href']
            if ('threads/' in href and 
                not href.startswith('#') and 
                'page-' not in href):
                full_url = urljoin(BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    links.append(full_url)
        return links
    except Exception as e:
        logger.error(f"Failed to scrape post links: {str(e)}")
        return []

def process_post(post_link, username, start_year, end_year, media_by_year, global_seen):
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract post date with multiple fallbacks
        year = None
        date_elem = soup.find('time', class_='u-dt')
        if date_elem and 'datetime' in date_elem.attrs:
            try:
                post_date = datetime.strptime(date_elem['datetime'], "%Y-%m-%dT%H:%M:%S%z")
                year = post_date.year
                logger.info(f"Parsed datetime {post_date} for {post_link}, year: {year}")
            except ValueError:
                logger.warning(f"Invalid datetime format for {post_link}: {date_elem['datetime']}")
        
        if not year and date_elem:
            date_text = date_elem.get_text(strip=True)
            match = re.search(r'(\d{4})', date_text)
            if match:
                year = int(match.group(1))
                logger.info(f"Parsed year {year} from text for {post_link}")
        
        if not year:
            year = start_year
            logger.warning(f"Date not found for {post_link}, defaulting to {year}")

        if year < start_year or year > end_year:
            logger.info(f"Skipping {post_link} - year {year} outside range {start_year}-{end_year}")
            return
        
        post_id = re.search(r'post-(\d+)', post_link)
        articles = []
        if post_id:
            article = soup.find('article', {
                'data-content': f'post-{post_id.group(1)}',
                'id': f'js-post-{post_id.group(1)}'
            })
            if article:
                articles.append(article)
        else:
            articles = soup.find_all('article')
        
        username_lower = username.lower()
        filtered_articles = [
            article for article in articles 
            if article and (username_lower in article.get_text(separator=" ").lower() or 
                           username_lower in article.get('data-author', '').lower())
        ]
        
        for article in filtered_articles:
            media_order = []
            
            for img in article.find_all('img', src=True):
                src = img['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                if (src.startswith("data:image") or 
                    "addonflare/awardsystem/icons/" in src or
                    any(kw in src.lower() for kw in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                if src.endswith(".gif"):
                    media_order.append(('gifs', full_src))
                else:
                    media_order.append(('images', full_src))
            
            for video in article.find_all('video'):
                if video.get('src'):
                    full_src = urljoin(BASE_URL, video['src']) if video['src'].startswith("/") else video['src']
                    logger.info(f"Found video tag src: {full_src}")
                    media_order.append(('videos', full_src))
                for source in video.find_all('source', src=True):
                    full_src = urljoin(BASE_URL, source['src']) if source['src'].startswith("/") else source['src']
                    logger.info(f"Found video source: {full_src}")
                    media_order.append(('videos', full_src))
            
            for source in article.find_all('source', src=True):
                full_src = urljoin(BASE_URL, source['src']) if source['src'].startswith("/") else source['src']
                logger.info(f"Found standalone source: {full_src}")
                media_order.append(('videos', full_src))
            
            for link in article.find_all('a', href=True):
                href = link['href']
                full_href = urljoin(BASE_URL, href) if href.startswith("/") else href
                if any(full_href.lower().endswith(ext) for ext in ['.mp4', '.webm', '.mov']):
                    logger.info(f"Found video link: {full_href}")
                    media_order.append(('videos', full_href))
            
            # Append media in order, using global seen set for deduplication
            for media_type, url in media_order:
                if url not in global_seen[media_type]:
                    global_seen[media_type].add(url)
                    media_by_year[year][media_type].append(url)

    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")

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
    years_descending = sorted(media_by_year.keys(), reverse=True)
    for year in years_descending:
        items = media_by_year[year][file_type]
        if items:
            html_content += f"<h2>{year}</h2>"
            total_items += len(items)
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
            logger.warning(f"Attempt {attempt + 1} failed to send message: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(1 * (attempt + 1))

def send_telegram_document(chat_id, file_buffer, filename, caption):
    for attempt in range(MAX_RETRIES):
        try:
            file_buffer.seek(0)
            return bot.send_document(
                chat_id=chat_id,
                document=file_buffer,
                visible_file_name=filename,
                caption=caption[:1024]
            )
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed to send document: {str(e)}")
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
            send_telegram_message(
                chat_id=chat_id,
                text="❌ This bot is restricted to specific users only.",
                reply_to_message_id=message_id
            )
            return '', 200

        if not text:
            send_telegram_message(
                chat_id=chat_id,
                text="Please send a search query",
                reply_to_message_id=message_id
            )
            return '', 200

        if text.lower() == '/stop':
            if cancel_task(chat_id):
                send_telegram_message(
                    chat_id=chat_id,
                    text="✅ Scraping process stopped immediately",
                    reply_to_message_id=message_id
                )
            else:
                send_telegram_message(
                    chat_id=chat_id,
                    text="ℹ️ No active scraping process to stop",
                    reply_to_message_id=message_id
                )
            return '', 200

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            send_telegram_message(
                chat_id=chat_id,
                text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: 'Saiee Manjrekar' n 2019 2025\nUse /stop to cancel",
                reply_to_message_id=message_id
            )
            return '', 200

        if chat_id in active_tasks:
            send_telegram_message(
                chat_id=chat_id,
                text="⚠️ A scraping process is already running. Use /stop to cancel it first.",
                reply_to_message_id=message_id
            )
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

        progress_msg = send_telegram_message(
            chat_id=chat_id,
            text=f"🔍 Processing '{username}' ({start_year}-{end_year})..."
        )

        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        active_tasks[chat_id] = (executor, [])

        try:
            media_by_year = {year: {'images': [], 'videos': [], 'gifs': []} 
                           for year in range(start_year, end_year + 1)}
            all_post_links = []
            seen_links = set()
            global_seen = {'images': set(), 'videos': set(), 'gifs': set()}  # Global deduplication
            
            search_links = generate_links(start_year, end_year, username, title_only)
            if not search_links:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"⚠️ No search URLs generated for '{username}'"
                )
                return '', 200

            for year, search_link in search_links:
                total_pages = fetch_total_pages(search_link)
                urls_to_process = split_url(search_link, *re.search(r"c\[newer_than\]=(\d{4}-\d{2}-\d{2})&c\[older_than\]=(\d{4}-\d{2}-\d{2})", search_link).groups()) if total_pages > MAX_PAGES_PER_SEARCH else [search_link]
                
                for url in urls_to_process:
                    pages = fetch_total_pages(url)
                    for page in range(1, pages + 1):
                        if chat_id not in active_tasks:
                            raise ScraperError("Task cancelled by user")
                        post_links = scrape_post_links(f"{url}&page={page}")
                        for link in post_links:
                            if link not in seen_links:
                                seen_links.add(link)
                                all_post_links.append(link)

            if not all_post_links:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"⚠️ No posts found for '{username}' ({start_year}-{end_year})"
                )
                return '', 200

            futures = [
                executor.submit(process_post, link, username, start_year, end_year, media_by_year, global_seen)
                for link in all_post_links
            ]
            active_tasks[chat_id] = (executor, futures)

            processed_count = 0
            total_posts = len(all_post_links)
            for future in as_completed(futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                future.result()
                processed_count += 1
                if processed_count % 10 == 0 or processed_count == total_posts:
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=f"🔍 Processing '{username}' ({start_year}-{end_year}): {processed_count}/{total_posts} posts"
                    )

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
            any_sent = False
            for file_type in ["images", "videos", "gifs"]:
                html_content = create_html(file_type, media_by_year, username, start_year, end_year)
                if html_content:
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                    total_items = sum(len(media_by_year[year][file_type]) for year in media_by_year)
                    
                    send_telegram_document(
                        chat_id=chat_id,
                        file_buffer=html_file,
                        filename=html_file.name,
                        caption=f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})"
                    )
                    any_sent = True

            if not any_sent:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"⚠️ No media found for '{username}' ({start_year}-{end_year})"
                )

        except ScraperError as e:
            if str(e) == "Task cancelled by user":
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"🛑 Scraping stopped for '{username}'"
                )
            else:
                raise
        except Exception as e:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"❌ Error for '{username}': {str(e)}"
            )
        finally:
            if chat_id in active_tasks:
                executor, _ = active_tasks[chat_id]
                del active_tasks[chat_id]
                executor.shutdown(wait=False)

        return '', 200
    
    except Exception as e:
        logger.critical(f"Unhandled error: {str(e)}\n{traceback.format_exc()}")
        try:
            if 'progress_msg' in locals():
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"❌ Error: {str(e)}"
                )
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)  # debug=False for production