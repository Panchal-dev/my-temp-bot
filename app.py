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
MAX_PAGES_PER_SEARCH = 2  # Limit pages to avoid timeouts

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
    """Custom exception for scraping errors"""
    pass

def make_request(url, method='get', **kwargs):
    """Robust request handler with retries"""
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
            time.sleep(1 * (attempt + 1))  # Exponential backoff

def generate_links(start_year, end_year, username, title_only=False):
    """Generate search URLs with error checking"""
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")
    
    # Ensure years are within valid range
    current_year = datetime.now().year
    start_year = max(2010, start_year)
    end_year = min(current_year, end_year)
    
    if start_year > end_year:
        start_year, end_year = end_year, start_year

    links = []
    for year in range(start_year, end_year + 1):
        months = [("01-01", "03-31"), ("04-01", "06-30"), 
                 ("07-01", "09-30"), ("10-01", "12-31")]
        for start_month, end_month in months:
            try:
                # Skip future quarters for current year
                if year == current_year:
                    current_month = datetime.now().month
                    quarter_end_month = int(end_month.split('-')[1])
                    if quarter_end_month > current_month:
                        continue
                
                url = (
                    f"{BASE_URL}/search/39143295/?q={username.replace(' ', '+')}"
                    f"&c[newer_than]={year}-{start_month}"
                    f"&c[older_than]={year}-{end_month}"
                    f"&c[title_only]={1 if title_only else 0}"
                    "&o=date"
                )
                links.append((year, url))
            except Exception as e:
                logger.error(f"Error generating URL for {year}-{start_month}: {str(e)}")
                continue
    return links

def fetch_total_pages(url):
    """Get total pages with fault tolerance"""
    try:
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        if not pagination:
            return 1
            
        page_numbers = []
        for link in pagination.find_all('a'):
            try:
                num = int(link.text.strip())
                page_numbers.append(num)
            except (ValueError, AttributeError):
                continue
        
        return max(page_numbers) if page_numbers else 1
    except Exception as e:
        logger.error(f"Error fetching pages for {url}: {str(e)}")
        return 1

def scrape_post_links(search_url):
    """Extract post links with comprehensive error handling"""
    try:
        response = make_request(search_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            try:
                href = link['href']
                if ('threads/' in href and 
                    not href.startswith('#') and 
                    'page-' not in href):
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in links:  # Avoid duplicates
                        links.append(full_url)
            except Exception as e:
                logger.warning(f"Error processing link {href}: {str(e)}")
                continue
                
        return links
    except Exception as e:
        logger.error(f"Failed to scrape post links: {str(e)}")
        return []

def process_post(post_link, username, unique_images, unique_videos, unique_gifs):
    """Process individual post with full error containment"""
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find relevant articles
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
        
        # Filter by username
        username_lower = username.lower()
        filtered_articles = []
        for article in articles:
            try:
                article_text = article.get_text(separator=" ").lower()
                article_author = article.get('data-author', '').lower()
                if username_lower in article_text or username_lower in article_author:
                    filtered_articles.append(article)
            except Exception:
                continue
        
        # Extract media
        images, videos, gifs = [], [], []
        
        for article in filtered_articles:
            # Process images
            for img in article.find_all('img', src=True):
                try:
                    src = img['src']
                    full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                    
                    # Skip invalid images
                    if (src.startswith("data:image") or 
                        "addonflare/awardsystem/icons/" in src or
                        any(kw in src.lower() for kw in ["avatars", "badge", "premium", "likes"])):
                        continue
                        
                    # Categorize media
                    if src.endswith(".gif"):
                        if full_src not in unique_gifs:
                            unique_gifs.add(full_src)
                            gifs.append(full_src)
                    elif full_src not in unique_images:
                        unique_images.add(full_src)
                        images.append(full_src)
                except Exception as e:
                    logger.warning(f"Error processing image: {str(e)}")
                    continue
            
            # Process videos
            for media in [*article.find_all('video', src=True), 
                         *article.find_all('source', src=True)]:
                try:
                    src = media['src']
                    full_src = urljoin(BASE_URL, src) if src.startswith("/") else src
                    if full_src not in unique_videos:
                        unique_videos.add(full_src)
                        videos.append(full_src)
                except Exception as e:
                    logger.warning(f"Error processing video: {str(e)}")
                    continue
        
        return images, videos, gifs
    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")
        return [], [], []

def create_html(file_type, items):
    """Generate HTML with validation"""
    if not items:
        return None
        
    try:
        html_content = f"""<!DOCTYPE html><html><head>
        <title>{file_type.capitalize()} Links</title>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .item {{ margin: 20px 0; padding: 10px; border: 1px solid #eee; }}
            img, video {{ max-width: 100%; height: auto; display: block; }}
            a {{ color: #0066cc; text-decoration: none; }}
        </style>
        </head>
        <body>
        <h1>{file_type.capitalize()} ({len(items)})</h1>"""
        
        for item in items:
            if file_type == "images":
                html_content += f'''
                <div class="item">
                    <img src="{item}" alt="Image" onerror="this.style.display='none'">
                    <div><a href="{item}" target="_blank">View Original</a></div>
                </div>'''
            elif file_type == "videos":
                html_content += f'''
                <div class="item">
                    <video controls>
                        <source src="{item}" type="video/mp4">
                        Your browser does not support the video tag.
                    </video>
                    <div><a href="{item}" target="_blank">Download Video</a></div>
                </div>'''
            elif file_type == "gifs":
                html_content += f'''
                <div class="item">
                    <img src="{item}" alt="GIF" onerror="this.style.display='none'">
                    <div><a href="{item}" target="_blank">View Original GIF</a></div>
                </div>'''
        
        html_content += "</body></html>"
        return html_content
    except Exception as e:
        logger.error(f"Failed to generate HTML: {str(e)}")
        return None

def send_telegram_message(chat_id, text, **kwargs):
    """Safe message sender with retries"""
    for attempt in range(MAX_RETRIES):
        try:
            return bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            if "blocked" in str(e):
                logger.warning(f"Bot blocked by user {chat_id}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed to send message: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(1 * (attempt + 1))

def send_telegram_document(chat_id, file_buffer, filename, caption):
    """Safe document sender with retries"""
    for attempt in range(MAX_RETRIES):
        try:
            file_buffer.seek(0)  # Rewind buffer
            return bot.send_document(
                chat_id=chat_id,
                document=file_buffer,
                visible_file_name=filename,
                caption=caption[:1024]  # Truncate long captions
            )
        except telebot.apihelper.ApiTelegramException as e:
            logger.warning(f"Attempt {attempt + 1} failed to send document: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(1 * (attempt + 1))

@app.route('/telegram', methods=['POST'])
def telegram_webhook():
    """Main webhook handler with full error containment"""
    try:
        update = request.get_json()
        if not update:
            logger.warning("Empty request received")
            return '', 200

        # Validate request structure
        if 'message' not in update or 'chat' not in update['message']:
            logger.warning("Invalid message structure")
            return '', 200

        chat_id = update['message']['chat']['id']
        message_id = update['message'].get('message_id')
        text = update['message'].get('text', '').strip()

        # Input validation
        if not text:
            try:
                send_telegram_message(
                    chat_id=chat_id,
                    text="Please send a search query",
                    reply_to_message_id=message_id
                )
            except Exception as e:
                logger.error(f"Failed to send help message: {str(e)}")
            return '', 200

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            try:
                send_telegram_message(
                    chat_id=chat_id,
                    text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: john y 2020 2022",
                    reply_to_message_id=message_id
                )
            except Exception as e:
                logger.error(f"Failed to send usage: {str(e)}")
            return '', 200

        # Parse parameters with defaults
        try:
            username = parts[1] if parts[0] == '/start' else parts[0]
            title_only = parts[2].lower() == 'y' if len(parts) > 2 else False
            start_year = int(parts[3]) if len(parts) > 3 else 2019
            end_year = int(parts[4]) if len(parts) > 4 else datetime.now().year
            
            # Validate years
            if start_year < 2010 or end_year > datetime.now().year + 1:
                raise ValueError("Invalid year range")
        except Exception as e:
            logger.error(f"Parameter error: {str(e)}")
            try:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"⚠️ Invalid parameters: {str(e)}\n\nUsage: username [title_only y/n] [start_year] [end_year]",
                    reply_to_message_id=message_id
                )
            except Exception as e:
                logger.error(f"Failed to send error: {str(e)}")
            return '', 200

        # Send initial processing message
        try:
            progress_msg = send_telegram_message(
                chat_id=chat_id,
                text=f"🔍 Searching for {username} ({start_year}-{end_year})..."
            )
        except Exception as e:
            logger.error(f"Failed to send progress message: {str(e)}")
            return '', 200

        # Scrape content
        unique_images, unique_videos, unique_gifs = set(), set(), set()
        all_post_links = []
        
        try:
            search_links = generate_links(start_year, end_year, username, title_only)
            if not search_links:
                raise ScraperError("No search URLs generated")
            
            logger.info(f"Generated {len(search_links)} search URLs for years {start_year}-{end_year}")
            
            for year, search_link in search_links:
                try:
                    total_pages = fetch_total_pages(search_link)
                    pages_to_scrape = min(total_pages, MAX_PAGES_PER_SEARCH)
                    logger.info(f"Processing {pages_to_scrape} pages for year {year}")
                    
                    for page in range(1, pages_to_scrape + 1):
                        try:
                            post_links = scrape_post_links(f"{search_link}&page={page}")
                            all_post_links.extend(post_links)
                            logger.info(f"Found {len(post_links)} posts on page {page} for year {year}")
                        except Exception as e:
                            logger.error(f"Failed to scrape page {page}: {str(e)}")
                            continue
                except Exception as e:
                    logger.error(f"Failed to process search link {search_link}: {str(e)}")
                    continue
            
            # Remove duplicate post links
            all_post_links = list(dict.fromkeys(all_post_links))
            
            if not all_post_links:
                raise ScraperError("No posts found matching criteria")
            
            logger.info(f"Total unique posts to process: {len(all_post_links)}")
            
            # Process posts in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for link in all_post_links:
                    futures.append(executor.submit(
                        process_post,
                        link,
                        username,
                        unique_images,
                        unique_videos,
                        unique_gifs
                    ))
                
                for future in as_completed(futures):
                    try:
                        future.result()  # Just wait for completion
                    except Exception as e:
                        logger.warning(f"Post processing failed: {str(e)}")
                        continue
            
            # Prepare results
            results = {
                "images": list(unique_images),
                "videos": list(unique_videos),
                "gifs": list(unique_gifs)
            }
            
            # Send results
            any_sent = False
            for file_type, items in results.items():
                if not items:
                    continue
                
                try:
                    html_content = create_html(file_type, items)
                    if not html_content:
                        continue
                    
                    html_file = BytesIO(html_content.encode('utf-8'))
                    html_file.name = f"{username}_{file_type}.html"
                    
                    send_telegram_document(
                        chat_id=chat_id,
                        file_buffer=html_file,
                        filename=html_file.name,
                        caption=f"Found {len(items)} {file_type} for {username}"
                    )
                    any_sent = True
                except Exception as e:
                    logger.error(f"Failed to send {file_type}: {str(e)}")
                    continue
            
            if not any_sent:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"⚠️ No media found for {username} ({start_year}-{end_year})"
                )
            
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            try:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"❌ Failed to complete search: {str(e)}"
                )
            except Exception as e:
                logger.error(f"Failed to send error message: {str(e)}")
            return '', 200
        
        # Clean up
        try:
            if 'progress_msg' in locals():
                bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete progress message: {str(e)}")
        
        return '', 200
    
    except Exception as e:
        logger.critical(f"Unhandled error in webhook: {str(e)}\n{traceback.format_exc()}")
        return '', 200

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint for health checks"""
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)