import os
import re
import json
import requests
import logging
from io import BytesIO
from PIL import Image
import concurrent.futures

BASE_DIR = "/Users/rishipandey/Projects/intv-nextjs/firebase-imported/python-script-for-mexico"
LOG_FILE = os.path.join(BASE_DIR, "scrape.log")

# Setup logging
logger = logging.getLogger("downloader")
logger.setLevel(logging.INFO)
if getattr(logger, 'handlers', None):
    logger.handlers.clear()

# Add to the same log file as the scraper, but append
file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

BASE_DIR = "/Users/rishipandey/Projects/intv-nextjs/firebase-imported/python-script-for-mexico"
SCHEDULE_DIR = os.path.join(BASE_DIR, "schedule")
IMAGES_DIR = os.path.join(BASE_DIR, "downloaded-images")
CDN_BASE = "https://cdn.programaciontv.com.mx/wp-content/uploads/downloaded-images"

def slugify(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

def download_and_compress(url, local_path):
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # If already exists, skip
    if os.path.exists(local_path):
        return True, "Already downloaded"
        
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        img = Image.open(BytesIO(response.content))
        if img.mode != 'RGB':
            img = img.convert('RGB')
            
        # Compress to webp, target < 8kb. Resize to a max 400x400 to help reduce size heavily
        img.thumbnail((300, 300))
        
        img.save(local_path, "webp", quality=30, method=6)
        
        # Check size
        size_kb = os.path.getsize(local_path) / 1024
        return True, f"{size_kb:.1f} KB"
    except Exception as e:
        return False, str(e)

def main():
    if not os.path.exists(SCHEDULE_DIR):
        logger.error("No schedule directory found.")
        return

    # original_url -> new_cdn_url mapping
    url_to_cdn = {}
    
    # original_url -> local path for downloading
    download_tasks = {}
    
    json_files = [f for f in os.listdir(SCHEDULE_DIR) if f.endswith('.json')]
    modified_files = []
    
    # Pass 1: Gather URLs and map them
    logger.info("Scanning JSON files for logos...")
    for filename in json_files:
        filepath = os.path.join(SCHEDULE_DIR, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        channel = data.get("channel", filename.replace('.json', ''))
        changed = False
        
        for key, value in data.items():
            if isinstance(value, list) and key != "channel":
                for show in value:
                    original_url = show.get("logo")
                    
                    if not original_url or not original_url.startswith("http") or original_url.startswith(CDN_BASE):
                        continue
                        
                    if original_url in url_to_cdn:
                        # Already mapped this URL
                        show["logo"] = url_to_cdn[original_url]
                        changed = True
                    else:
                        # Map new URL
                        show_name = show.get("show", "unknown-show")
                        slug = slugify(show_name)
                        
                        # Handle duplicate slugs for different original URLs to prevent saving two totally different images to one slug path
                        local_path = os.path.join(IMAGES_DIR, channel, f"{slug}.webp")
                        counter = 1
                        while local_path in download_tasks.values():
                            local_path = os.path.join(IMAGES_DIR, channel, f"{slug}-{counter}.webp")
                            counter += 1
                            
                        # compute cdn path
                        rel_path = os.path.relpath(local_path, IMAGES_DIR)
                        rel_path = rel_path.replace(os.sep, '/')
                        cdn_url = f"{CDN_BASE}/{rel_path}"
                        
                        url_to_cdn[original_url] = cdn_url
                        download_tasks[original_url] = local_path
                        
                        show["logo"] = cdn_url
                        changed = True
                        
        if changed:
            modified_files.append((filepath, data))

    # Download stuff
    logger.info(f"Found {len(download_tasks)} unique images to download.")
    successful = 0
    failed = 0
    
    # Configurable threading
    max_workers = int(os.environ.get("IMAGE_WORKERS", "40"))
    
    if download_tasks:
        logger.info(f"Starting downloads with {max_workers} threads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(download_and_compress, orig_url, loc_path): orig_url
                for orig_url, loc_path in download_tasks.items()
            }
            
            for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
                orig_url = futures[future]
                try:
                    success, result = future.result()
                except Exception as e:
                    success, result = False, str(e)
                    
                if success:
                    successful += 1
                    if idx % 50 == 0 or idx == len(futures):
                        logger.info(f"[{idx}/{len(futures)}] Downloaded: {result}")
                else:
                    failed += 1
                    logger.error(f"Failed to download {orig_url}: {result}")
                    
    logger.info(f"Download summary: {successful} successful, {failed} failed.")
    
    # Save modified JSONs
    if modified_files:
        logger.info(f"Updating {len(modified_files)} JSON files...")
        for filepath, data in modified_files:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
                
    logger.info("Done! Image scraping & replacement complete.")

if __name__ == "__main__":
    main()
