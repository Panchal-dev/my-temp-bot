import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot  # Using pytelegrambotapi (sync)
from datetime import datetime, timedelta

app = Flask(__name__)

# Telegram Bot Token from environment variable
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Proxy Configuration (update if needed)
PROXY = {
    "http": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594",
    "https": "http://ybmonqoz:tpcfq425wvfw@38.153.152.244:9594"
}

# Base URL
base_url = "https://desifakes.com"

def generate_links(start_year, end_year, username, title_only=False):
    links = []
    for year in range(start_year, end_year + 1):
        months = [("01-01", "03-31"), ("04-01", "06-30"), ("07-01", "09-30"), ("10-01", "12-31")]
        for start_month, end_month in months:
            start_date = f"{year}-{start_month}"
            end_date = f"{year}-{end_month}"
            url = f"https://desifakes.com/search/39143295/?q={username.replace(' ', '+')}&c[newer_than]={start_date}&c[older_than]={end_date}"
            url += "&c[title_only]=1" if title_only else "&c[title_only]=0"
            url += "&o=date"
            links.append((year, url))
    return links

def fetch_total_pages(url):
    try:
        response = requests.get(url, timeout=5, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pageNav')
        return max(int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()) if pagination else 1
    except Exception:
        return 1

def scrape_post_links(search_url):
    try:
        response = requests.get(search_url, timeout=5, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return list(dict.fromkeys(urljoin(base_url, link['href']) for link in soup.find_all('a', href=True)
                                 if 'threads/' in link['href'] and not link['href'].startswith('#') and not 'page-' in link['href']))
    except Exception:
        return []

def process_post(post_link, username, unique_images, unique_videos, unique_gifs):
    try:
        response = requests.get(post_link, timeout=5, proxies=PROXY)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        post_id = re.search(r'post-(\d+)', post_link).group(1) if re.search(r'post-(\d+)', post_link) else None
        articles = [soup.find('article', {'data-content': f'post-{post_id}', 'id': f'js-post-{post_id}'})] if post_id else soup.find_all('article')
        
        username_lower = username.lower()
        articles = [a for a in articles if a and (username_lower in a.get_text(separator=" ").lower() or username_lower in a.get('data-author', '').lower())]
        
        images, videos, gifs = [], [], []
        for article in articles:
            for img in article.find_all('img', src=True):
                src = urljoin(base_url, img['src']) if img['src'].startswith("/") else img['src']
                if (src.startswith("data:image") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    continue
                if img['src'].endswith(".gif") and src not in unique_gifs:
                    unique_gifs.add(src)
                    gifs.append(src)
                elif src not in unique_images:
                    unique_images.add(src)
                    images.append(src)
            for video in article.find_all('video', src=True):
                src = urljoin(base_url, video['src']) if video['src'].startswith("/") else video['src']
                if src not in unique_videos:
                    unique_videos.add(src)
                    videos.append(src)
            for source in article.find_all('source', src=True):
                src = urljoin(base_url, source['src']) if source['src'].startswith("/") else source['src']
                if src not in unique_videos:
                    unique_videos.add(src)
                    videos.append(src)
        return images, videos, gifs
    except Exception:
        return [], [], []

def create_html(file_type, items):
    html_content = f"""<!DOCTYPE html><html><head><title>{file_type.capitalize()} Links</title>
    <style>body{{font-family:Arial,sans-serif;}}.item{{margin:10px;text-align:center;}}</style></head><body>
    <h1>{file_type.capitalize()} ({len(items)})</h1>"""
    
    for item in items:
        if file_type == "images":
            html_content += f'<div class="item"><img src="{item}" alt="Image" style="max-width:80%;height:auto;"></div>'
        elif file_type == "videos":
            html_content += f'<div class="item"><video controls style="max-width:100%;"><source src="{item}" type="video/mp4"></video></div>'
        elif file_type == "gifs":
            html_content += f'<div class="item"><a href="{item}" target="_blank">View GIF</a><br><img src="{item}" style="max-width:80%;height:auto;"></div>'
    
    html_content += "</body></html>"
    return html_content

@app.route('/telegram', methods=['POST'])
def telegram_webhook():  # Removed async
    update = request.get_json()
    if not update or 'message' not in update or 'text' not in update['message']:
        return '', 200

    chat_id = update['message']['chat']['id']
    message_id = update['message']['message_id']
    text = update['message']['text'].strip()

    # Parse input
    parts = text.split()
    if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
        bot.send_message(chat_id=chat_id, text="Usage: username [title_only y/n] [start_year] [end_year]\nExample: john y 2020 2022", reply_to_message_id=message_id)
        return '', 200

    username = parts[1] if parts[0] == '/start' else parts[0]
    title_only = parts[2].lower() == 'y' if len(parts) > 2 else False
    start_year = int(parts[3]) if len(parts) > 3 else 2019
    end_year = int(parts[4]) if len(parts) > 4 else 2025

    # Send processing message
    progress_msg = bot.send_message(chat_id=chat_id, text=f"📸 Scraping for {username} ({start_year}-{end_year})...")

    # Scrape and process
    unique_images, unique_videos, unique_gifs = set(), set(), set()
    all_links = generate_links(start_year, end_year, username, title_only)
    all_post_links = []
    for _, search_link in all_links:
        total_pages = fetch_total_pages(search_link)
        for page in range(1, min(total_pages + 1, 3)):  # Limit to 2 pages per range
            all_post_links.extend(scrape_post_links(f"{search_link}&page={page}"))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_post, link, username, unique_images, unique_videos, unique_gifs) for link in all_post_links]
        for future in as_completed(futures):
            imgs, vids, gifs = future.result()
            unique_images.update(imgs)
            unique_videos.update(vids)
            unique_gifs.update(gifs)

    # Generate and send HTML files
    for file_type, items in [("images", unique_images), ("videos", unique_videos), ("gifs", unique_gifs)]:
        if items:
            html_content = create_html(file_type, items)
            html_file = BytesIO(html_content.encode('utf-8'))
            html_file.name = f"{file_type}.html"
            bot.send_document(chat_id=chat_id, document=html_file, filename=f"{file_type}.html", caption=f"{file_type.capitalize()} for {username}")

    # Clean up
    bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
    return '', 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))