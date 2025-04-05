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
REQUEST_TIMEOUT = 10
MAX_WORKERS = 5
MAX_PAGES_PER_SEARCH = 2

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
            links.append((year, url))
    return links

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

def process_post(post_link, username, unique_images, unique_videos, unique_gifs):
    try:
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
        
        images, videos, gifs = [], [], []
        for article in filtered_articles:
            for img in article.find_all('img', src=True):
                src = img['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                if (src.startswith("data:image") or 
                    "addonflare/awardsystem/icons/" in src or
                    any(kw in src.lower() for kw in ["avatars", "badge", "premium", "likes"])):
                    continue
                if src.endswith(".gif") and full_src not in unique_gifs:
                    unique_gifs.add(full_src)
                    gifs.append(full_src)
                elif full_src not in unique_images:
                    unique_images.add(full_src)
                    images.append(full_src)
            
            for media in [*article.find_all('video', src=True), 
                         *article.find_all('source', src=True)]:
                src = media['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                if full_src not in unique_videos:
                    unique_videos.add(full_src)
                    videos.append(full_src)
        
        return images, videos, gifs
    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")
        return [], [], []

def create_html(file_type, items, username):
    if not items:
        return None
        
    html_content = f"""<!DOCTYPE html><html><head>
    <title>{username} - {file_type.capitalize()}</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .item {{ margin: 20px 0; padding: 10px; border: 1px solid #eee; }}
        img, video {{ max-width: 80%; height: auto; display: block; }}
        a {{ color: #0066cc; text-decoration: none; }}
    </style>
    </head>
    <body>
    <h1>{username} - {file_type.capitalize()} ({len(items)})</h1>"""
    
    for item in items:
        if file_type == "images":
            html_content += f'<div class="item"><img src="{item}" alt="Image"><a href="{item}" target="_blank">View Original</a></div>'
        elif file_type == "videos":
            html_content += f'<div class="item"><video controls><source src="{item}" type="video/mp4"></video><a href="{item}" target="_blank">Download Video</a></div>'
        elif file_type == "gifs":
            html_content += f'<div class="item"><img src="{item}" alt="GIF"><a href="{item}" target="_blank">View Original GIF</a></div>'
    
    html_content += "</body></html>"
    return html_content

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
                    text="✅ Scraping process stopped",
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
                text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: Madhuri y 2020 2023\nUse /stop to cancel",
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

        username = parts[1] if parts[0] == '/start' else parts[0]
        title_only = parts[2].lower() == 'y' if len(parts) > 2 else False
        start_year = int(parts[3]) if len(parts) > 3 else 2019
        end_year = int(parts[4]) if len(parts) > 4 else datetime.now().year

        progress_msg = send_telegram_message(
            chat_id=chat_id,
            text=f"🔍 Searching for {username} ({start_year}-{end_year})..."
        )

        # Initialize sets for this specific task
        unique_images, unique_videos, unique_gifs = set(), set(), set()
        all_post_links = set()
        
        search_links = generate_links(start_year, end_year, username, title_only)
        if not search_links:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"⚠️ No search URLs generated for {username}"
            )
            return '', 200

        for year, search_link in search_links:
            total_pages = fetch_total_pages(search_link)
            pages_to_scrape = min(total_pages, MAX_PAGES_PER_SEARCH)
            for page in range(1, pages_to_scrape + 1):
                post_links = scrape_post_links(f"{search_link}&page={page}")
                all_post_links.update(post_links)

        if not all_post_links:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"⚠️ No posts found for {username} ({start_year}-{end_year})"
            )
            return '', 200

        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        futures = [
            executor.submit(process_post, link, username, unique_images, unique_videos, unique_gifs)
            for link in all_post_links
        ]
        
        active_tasks[chat_id] = (executor, futures)

        try:
            processed_count = 0
            total_posts = len(all_post_links)
            for future in as_completed(futures):
                if future.cancelled():
                    raise ScraperError("Task cancelled by user")
                future.result()
                processed_count += 1
                if processed_count % 10 == 0:  # Update progress every 10 posts
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=f"🔍 Processing {username} ({start_year}-{end_year}): {processed_count}/{total_posts} posts"
                    )

            results = {
                "images": list(unique_images),
                "videos": list(unique_videos),
                "gifs": list(unique_gifs)
            }
            
            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
            any_sent = False
            for file_type, items in results.items():
                if items:
                    html_content = create_html(file_type, items, username)
                    if html_content:
                        html_file = BytesIO(html_content.encode('utf-8'))
                        html_file.name = f"{username}_{file_type}.html"
                        
                        send_telegram_document(
                            chat_id=chat_id,
                            file_buffer=html_file,
                            filename=html_file.name,
                            caption=f"Found {len(items)} {file_type} for {username} ({start_year}-{end_year})"
                        )
                        any_sent = True

            if not any_sent:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"⚠️ No media found for {username} ({start_year}-{end_year})"
                )

        except ScraperError as e:
            if str(e) == "Task cancelled by user":
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text="🛑 Scraping stopped by user"
                )
            else:
                raise
        except Exception as e:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"❌ Error: {str(e)}"
            )
        finally:
            if chat_id in active_tasks:
                del active_tasks[chat_id]
            executor.shutdown(wait=True)

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
            del active_tasks[chat_id]
        return '', 200

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)