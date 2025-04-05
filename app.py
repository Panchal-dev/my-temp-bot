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
from collections import defaultdict

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
MAX_WORKERS = 5
MAX_PAGES_PER_SEARCH = 10

try:
    TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
except KeyError:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    raise
except Exception as e:
    logger.error(f"Failed to initialize bot: {str(e)}")
    raise

PROXY = {
    "http": "http://kigxipuw:4p9dfn30rig0@38.153.152.244:9594",
    "https": "http://kigxipuw:4p9dfn30rig0@38.153.152.244:9594"
}

BASE_URL = "https://desifakes.com"

class ScraperError(Exception):
    pass

# Global task tracking
active_tasks = {}

# Allowed chat IDs
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

def make_request(url, method='get', **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.request(
                method,
                url,
                proxies=PROXY,
                timeout=REQUEST_TIMEOUT,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed after {MAX_RETRIES} attempts: {str(e)}")
            time.sleep(1 * (attempt + 1))

def generate_links(start_year, end_year, username, title_only=False):
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")
    
    current_year = datetime.now().year
    start_year = max(2010, min(start_year, current_year))
    end_year = min(current_year, max(end_year, start_year))
    
    links = []
    for year in range(start_year, end_year + 1):
        months = [("01-01", "03-31"), ("04-01", "06-30"), 
                 ("07-01", "09-30"), ("10-01", "12-31")]
        for start_month, end_month in months:
            if year == current_year:
                current_date = datetime.now()
                quarter_end = datetime.strptime(f"{year}-{end_month}", "%Y-%m-%d")
                if quarter_end > current_date:
                    continue
                
            url = (
                f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}"
                f"&c[newer_than]={year}-{start_month}"
                f"&c[older_than]={year}-{end_month}"
                f"&c[title_only]={1 if title_only else 0}"
                "&o=date"
            )
            links.append((year, url, f"{year}-{start_month}", f"{year}-{end_month}"))
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
        links = set()
        for link in soup.find_all('a', href=True):
            href = link['href']
            if ('threads/' in href and 
                not href.startswith('#') and 
                'page-' not in href):
                full_url = urljoin(BASE_URL, href)
                links.add(full_url)
        return list(links)
    except Exception as e:
        logger.error(f"Failed to scrape post links: {str(e)}")
        return []

def extract_post_date(post_link):
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        time_tag = soup.find('time')
        if time_tag and 'datetime' in time_tag.attrs:
            return datetime.strptime(time_tag['datetime'], "%Y-%m-%dT%H:%M:%S%z").date()
    except Exception as e:
        logger.error(f"Failed to extract date from {post_link}: {str(e)}")
    return None

def process_post(post_link, username, year, media_by_date):
    try:
        post_date = extract_post_date(post_link)
        if not post_date:
            return

        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        
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
            if username_lower in article.get_text(separator=" ").lower() or 
               username_lower in article.get('data-author', '').lower()
        ]
        
        for article in filtered_articles:
            for img in article.find_all('img', src=True):
                src = img['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                if (src.startswith("data:image") or 
                    "addonflare/awardsystem/icons/" in src or
                    any(kw in src.lower() for kw in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                if src.endswith(".gif"):
                    media_by_date[post_date]['gifs'].add(full_src)
                else:
                    media_by_date[post_date]['images'].add(full_src)
            
            for media in [*article.find_all('video', src=True), 
                         *article.find_all('source', src=True)]:
                src = media['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                media_by_date[post_date]['videos'].add(full_src)
        
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
        .date-section {{ margin-bottom: 30px; border-bottom: 1px solid #ccc; padding-bottom: 10px; }}
        .date-header {{ font-size: 1.2em; font-weight: bold; margin-bottom: 10px; }}
        .media-container {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 15px; }}
        .media-item {{ flex: 1 1 200px; }}
    </style>
    </head>
    <body>
    <h1>{username} - {file_type.capitalize()} Links ({start_year}-{end_year})</h1>"""
    
    total_items = 0
    
    # Group by year and month
    year_month_groups = defaultdict(lambda: defaultdict(list))
    for date in sorted(media_by_date.keys(), reverse=True):
        items = media_by_date[date][file_type]
        if items:
            year_month_groups[date.year][date.month].extend((date, items))
    
    # Generate HTML content in descending order
    for year in sorted(year_month_groups.keys(), reverse=True):
        html_content += f"<h2>{year}</h2>"
        for month in sorted(year_month_groups[year].keys(), reverse=True):
            month_name = datetime(year, month, 1).strftime("%B")
            html_content += f"<h3>{month_name}</h3>"
            
            # Sort dates within month in descending order
            for date, items in sorted(year_month_groups[year][month], key=lambda x: x[0], reverse=True):
                date_str = date.strftime("%Y-%m-%d")
                html_content += f"""
                <div class="date-section">
                    <div class="date-header">{date_str}</div>
                    <div class="media-container">"""
                
                for item in items:
                    total_items += 1
                    if file_type == "images":
                        html_content += f'<div class="media-item"><img src="{item}" alt="Image" style="max-width:100%;height:auto;"></div>'
                    elif file_type == "videos":
                        html_content += f'<div class="media-item"><video controls style="max-width:100%;"><source src="{item}" type="video/mp4"></video></div>'
                    elif file_type == "gifs":
                        html_content += f'<div class="media-item"><a href="{item}" target="_blank">View GIF</a></div>'
                
                html_content += "</div></div>"
    
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
                text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: 'Madhuri Dixit' y 2020 2023\nUse /stop to cancel",
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

        # Parse username and parameters
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
            text=f"🔍 Searching for '{username}' ({start_year}-{end_year})..."
        )

        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        active_tasks[chat_id] = (executor, [])

        try:
            # Store media by date
            media_by_date = defaultdict(lambda: {'images': set(), 'videos': set(), 'gifs': set()})
            all_post_links = set()
            
            search_links = generate_links(start_year, end_year, username, title_only)
            if not search_links:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"⚠️ No search URLs generated for '{username}'"
                )
                return '', 200

            for year, search_link, start_date, end_date in search_links:
                total_pages = fetch_total_pages(search_link)
                urls_to_process = split_url(search_link, start_date, end_date) if total_pages > MAX_PAGES_PER_SEARCH else [search_link]
                
                for url in urls_to_process:
                    pages = fetch_total_pages(url)
                    for page in range(1, pages + 1):
                        if chat_id not in active_tasks:
                            raise ScraperError("Task cancelled by user")
                        post_links = scrape_post_links(f"{url}&page={page}")
                        all_post_links.update(post_links)

            if not all_post_links:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"⚠️ No posts found for '{username}' ({start_year}-{end_year})"
                )
                return '', 200

            futures = [
                executor.submit(process_post, link, username, year, media_by_date)
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
                html_content = create_html(file_type, media_by_date, username, start_year, end_year)
                if html_content:
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}.html"
                    total_items = sum(len(items[file_type]) for items in media_by_date.values())
                    
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
    app.run(host="0.0.0.0", port=port)