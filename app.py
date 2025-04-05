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
REQUEST_TIMEOUT = 15  # Increased timeout for better reliability
MAX_WORKERS = 8       # Increased workers for faster processing
MAX_PAGES_PER_SEARCH = 15  # Increased pages per search
DEDUPE_CACHE_SIZE = 10000  # Size of URL deduplication cache

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

# Global task tracking and deduplication
active_tasks = {}
seen_urls = set()  # Global URL deduplication cache

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
                if full_url not in seen_urls:
                    links.add(full_url)
                    seen_urls.add(full_url)
                    if len(seen_urls) > DEDUPE_CACHE_SIZE:
                        seen_urls.clear()
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

def process_post(post_link, username, media_by_date):
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
            # Deduplicate media URLs within the same post
            post_media = {'images': set(), 'videos': set(), 'gifs': set()}
            
            for img in article.find_all('img', src=True):
                src = img['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                if (src.startswith("data:image") or 
                    "addonflare/awardsystem/icons/" in src or
                    any(kw in src.lower() for kw in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                if src.endswith(".gif"):
                    post_media['gifs'].add(full_src)
                else:
                    post_media['images'].add(full_src)
            
            for media in [*article.find_all('video', src=True), 
                         *article.find_all('source', src=True)]:
                src = media['src']
                full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                post_media['videos'].add(full_src)
            
            # Add to global storage only if not empty
            for media_type, urls in post_media.items():
                if urls:
                    media_by_date[post_date][media_type].update(urls)
        
    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")

def create_html(file_type, media_by_date, username, start_year, end_year):
    html_content = f"""<!DOCTYPE html><html><head>
    <title>{username} - {file_type.capitalize()} Links</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            line-height: 1.6;
        }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #3498db; margin-top: 30px; }}
        h3 {{ color: #16a085; margin-top: 20px; }}
        .date-section {{ 
            margin-bottom: 30px; 
            border-bottom: 1px solid #ecf0f1; 
            padding-bottom: 15px; 
        }}
        .date-header {{ 
            font-size: 1.1em; 
            font-weight: bold; 
            margin-bottom: 10px; 
            color: #7f8c8d;
        }}
        .media-container {{ 
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 15px;
        }}
        .media-item {{ 
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 5px;
            background: #f9f9f9;
        }}
        .media-item img {{
            max-width: 100%;
            height: auto;
            display: block;
        }}
        .media-item video {{
            max-width: 100%;
            display: block;
        }}
        .gif-link {{
            display: block;
            padding: 10px;
            background: #3498db;
            color: white;
            text-align: center;
            text-decoration: none;
            border-radius: 4px;
        }}
        .gif-link:hover {{
            background: #2980b9;
        }}
        .summary {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
    </style>
    </head>
    <body>
    <div class="summary">
        <h1>{username} - {file_type.capitalize()} Links ({start_year}-{end_year})</h1>
        <p>Total {file_type}: {sum(len(items[file_type]) for items in media_by_date.values())}</p>
    </div>"""
    
    # Group by year and month
    year_month_groups = defaultdict(lambda: defaultdict(list))
    for date in sorted(media_by_date.keys(), reverse=True):
        items = media_by_date[date][file_type]
        if items:
            year_month_groups[date.year][date.month].append((date, items))
    
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
                    if file_type == "images":
                        html_content += f'''
                        <div class="media-item">
                            <img src="{item}" alt="Image" loading="lazy">
                        </div>'''
                    elif file_type == "videos":
                        html_content += f'''
                        <div class="media-item">
                            <video controls>
                                <source src="{item}" type="video/mp4">
                                Your browser does not support the video tag.
                            </video>
                        </div>'''
                    elif file_type == "gifs":
                        html_content += f'''
                        <div class="media-item">
                            <a href="{item}" class="gif-link" target="_blank">View GIF</a>
                        </div>'''
                
                html_content += "</div></div>"
    
    html_content += """
    <script>
        // Lazy loading for better performance
        document.addEventListener("DOMContentLoaded", function() {
            const lazyImages = [].slice.call(document.querySelectorAll("img[loading='lazy']"));
            
            if ("IntersectionObserver" in window) {
                let lazyImageObserver = new IntersectionObserver(function(entries, observer) {
                    entries.forEach(function(entry) {
                        if (entry.isIntersecting) {
                            let lazyImage = entry.target;
                            lazyImage.src = lazyImage.dataset.src;
                            lazyImageObserver.unobserve(lazyImage);
                        }
                    });
                });

                lazyImages.forEach(function(lazyImage) {
                    lazyImageObserver.observe(lazyImage);
                });
            }
        });
    </script>
    </body></html>"""
    
    return html_content if any(len(items[file_type]) > 0 for items in media_by_date.values()) else None

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
                caption=caption[:1024],
                timeout=30
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
    global seen_urls
    seen_urls = set()  # Reset URL cache for each new request
    
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

            # First pass: collect all post links
            for year, search_link, start_date, end_date in search_links:
                total_pages = fetch_total_pages(search_link)
                urls_to_process = split_url(search_link, start_date, end_date) if total_pages > MAX_PAGES_PER_SEARCH else [search_link]
                
                for url in urls_to_process:
                    pages = min(fetch_total_pages(url), MAX_PAGES_PER_SEARCH)
                    for page in range(1, pages + 1):
                        if chat_id not in active_tasks:
                            raise ScraperError("Task cancelled by user")
                        
                        try:
                            post_links = scrape_post_links(f"{url}&page={page}")
                            all_post_links.update(post_links)
                            
                            # Update progress
                            bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=progress_msg.message_id,
                                text=f"🔍 Found {len(all_post_links)} posts for '{username}' ({start_year}-{end_year})"
                            )
                        except Exception as e:
                            logger.error(f"Error scraping page {page} of {url}: {str(e)}")
                            continue

            if not all_post_links:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"⚠️ No posts found for '{username}' ({start_year}-{end_year})"
                )
                return '', 200

            # Second pass: process all posts
            futures = []
            batch_size = 50
            post_list = list(all_post_links)
            
            for i in range(0, len(post_list), batch_size):
                batch = post_list[i:i + batch_size]
                futures.append(
                    executor.submit(
                        lambda posts: [process_post(post, username, media_by_date) for post in posts],
                        batch
                    )
                )
            
            active_tasks[chat_id] = (executor, futures)

            processed_count = 0
            total_posts = len(post_list)
            last_update = 0
            
            for future in as_completed(futures):
                if chat_id not in active_tasks:
                    raise ScraperError("Task cancelled by user")
                
                future.result()
                processed_count += len(batch)
                
                # Update progress every 10% or when completed
                progress_percent = int((processed_count / total_posts) * 100)
                if progress_percent >= last_update + 10 or processed_count == total_posts:
                    last_update = progress_percent
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=f"🔍 Processing '{username}' ({start_year}-{end_year}): {processed_count}/{total_posts} posts ({progress_percent}%)"
                    )

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
            # Generate and send results
            any_sent = False
            for file_type in ["images", "videos", "gifs"]:
                html_content = create_html(file_type, media_by_date, username, start_year, end_year)
                if html_content:
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username.replace(' ', '_')}_{file_type}_{start_year}-{end_year}.html"
                    total_items = sum(len(items[file_type]) for items in media_by_date.values())
                    
                    try:
                        send_telegram_document(
                            chat_id=chat_id,
                            file_buffer=html_file,
                            filename=html_file.name,
                            caption=f"Found {total_items} {file_type} for '{username}' ({start_year}-{end_year})"
                        )
                        any_sent = True
                    except Exception as e:
                        logger.error(f"Failed to send {file_type} document: {str(e)}")
                        send_telegram_message(
                            chat_id=chat_id,
                            text=f"⚠️ Failed to send {file_type} results for '{username}'"
                        )

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
                logger.error(f"Scraper error: {str(e)}")
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"❌ Error processing '{username}': {str(e)}"
                )
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}\n{traceback.format_exc()}")
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"❌ Unexpected error for '{username}': {str(e)}"
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
                    text=f"❌ Critical error: {str(e)}"
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
    return jsonify({
        "status": "healthy",
        "time": datetime.now().isoformat(),
        "active_tasks": len(active_tasks),
        "memory_usage": f"{os.getpid()} - {psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024:.2f}MB"
    })

if __name__ == "__main__":
    import psutil
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)