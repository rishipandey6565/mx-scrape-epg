import os
import re
import json
import requests
import logging
from io import BytesIO
from PIL import Image
import concurrent.futures
import sqlite3
import hashlib
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
console_handler.setFormatter(logging.Formatter('%(terminal)s'))
console_handler.addFilter(lambda record: hasattr(record, 'terminal'))
logger.propagate = False
logger.addHandler(file_handler)
logger.addHandler(console_handler)

SCHEDULE_DIR = os.environ.get("SCHEDULE_DIR", os.path.join(BASE_DIR, "schedule"))
IMAGES_DIR = os.path.join(BASE_DIR, "downloaded-images")
CDN_BASE = os.environ.get("IMAGE_CDN_BASE", "https://cdn.programaciontv.com.mx/wp-content/uploads/downloaded-images").rstrip("/")
DB_PATH = os.environ.get("IMAGE_DB_PATH", os.path.join(BASE_DIR, "image-state", "images.sqlite3"))
FALLBACK_URL = "https://programaciontv.com.mx/wp-content/uploads/2026/01/pexels-caleboquendo-8254900.webp"

def slugify(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

def download_and_compress(url, local_path):
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # If already exists, skip
    if valid_image(local_path):
        return True, "Already downloaded"
        
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        img = Image.open(BytesIO(response.content))
        if img.mode != 'RGB':
            img = img.convert('RGB')
            
        # Compress to webp, target < 8kb. Resize to a max 400x400 to help reduce size heavily
        img.thumbnail((300, 300))
        
        temporary = local_path + ".tmp"
        img.save(temporary, "webp", quality=30, method=6)
        os.replace(temporary, local_path)
        
        # Check size
        size_kb = os.path.getsize(local_path) / 1024
        return True, f"{size_kb:.1f} KB"
    except Exception as e:
        if os.path.exists(local_path + ".tmp"):
            os.remove(local_path + ".tmp")
        return False, str(e)

def valid_image(path):
    try:
        with Image.open(path) as image:
            image.verify()
        return True
    except (OSError, ValueError):
        return False


def show_key(name):
    return ' '.join(unicodedata.normalize('NFKC', name).casefold().split())


def image_groups(shows):
    """Within one channel: join identical URLs first, then identical titles."""
    parents = list(range(len(shows)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for field in ('logo', 'show'):
        seen = {}
        for index, show in enumerate(shows):
            value = show.get(field, '')
            if field == 'show':
                value = show_key(value)
            if value:
                if value in seen:
                    parents[root(index)] = root(seen[value])
                else:
                    seen[value] = index
    groups = {}
    for index, show in enumerate(shows):
        groups.setdefault(root(index), []).append(show)
    return list(groups.values())


def aliases(group):
    # URL matches take precedence over title matches in the persistent index.
    return list(dict.fromkeys(
        [('url', s['logo']) for s in group] +
        [('name', show_key(s.get('show', ''))) for s in group if show_key(s.get('show', ''))]
    ))


def save_json(path, data):
    temporary = str(path) + '.tmp'
    with open(temporary, 'w', encoding='utf-8') as target:
        json.dump(data, target, ensure_ascii=False, separators=(',', ':'))
    os.replace(temporary, path)


def main():
    os.makedirs(SCHEDULE_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    database = sqlite3.connect(DB_PATH)
    database.execute("""CREATE TABLE IF NOT EXISTS image_aliases (
        channel TEXT NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL,
        path TEXT NOT NULL, PRIMARY KEY(channel, kind, value))""")
    documents = {}
    jobs = []
    # Reserve paths already associated with other shows, including previous runs.
    reserved = {row[0] for row in database.execute('SELECT DISTINCT path FROM image_aliases')}
    successful = failed = reused = 0
    try:
        for path in sorted(Path(SCHEDULE_DIR).glob('*.json')):
            if path.name == 'all-channel.json':
                continue
            channel = path.stem  # Always the first name from filter.txt.
            with path.open(encoding='utf-8') as source:
                data = json.load(source)
            documents[channel] = data
            shows = []
            for value in data.values():
                if not isinstance(value, list):
                    continue
                for show in value:
                    url = show.get('logo', '')
                    if not url or not url.startswith(('http://', 'https://')) or url == FALLBACK_URL:
                        continue
                    if 'fallback' in url.lower():
                        show['logo'] = FALLBACK_URL
                    else:
                        shows.append(show)
            for group in image_groups(shows):
                keys = aliases(group)
                candidates = []
                for kind, value in keys:
                    row = database.execute(
                        'SELECT path FROM image_aliases WHERE channel=? AND kind=? AND value=?',
                        (channel, kind, value)).fetchone()
                    if row:
                        candidates.append(row[0])
                # Adopt already-downloaded images when upgrading from the old script.
                for show in group:
                    if show['logo'].startswith(CDN_BASE + '/'):
                        candidates.append(show['logo'][len(CDN_BASE) + 1:])
                    legacy = channel + '/' + (slugify(show.get('show', '')) or 'unknown-show') + '.webp'
                    if legacy not in reserved:
                        candidates.append(legacy)
                relative = None
                for candidate in dict.fromkeys(candidates):
                    local = Path(IMAGES_DIR) / candidate
                    if not local.resolve().is_relative_to(Path(IMAGES_DIR).resolve()):
                        continue
                    if valid_image(local):
                        relative = candidate
                        break
                if relative is not None:
                    reused += 1
                    record_image(database, channel, keys, relative, group)
                    reserved.add(relative)
                    continue
                stem = slugify(group[0].get('show', '')) or 'unknown-show'
                relative = f'{channel}/{stem}.webp'
                if relative in reserved:
                    digest = hashlib.sha256(repr(keys).encode()).hexdigest()[:12]
                    relative = f'{channel}/{stem}-{digest}.webp'
                reserved.add(relative)
                jobs.append((channel, keys, relative, group))

        logger.info('Image download plan', extra={'terminal': f'Images: {reused} reused, {len(jobs)} to download.'})
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.environ.get('IMAGE_WORKERS', '40'))) as executor:
            futures = {
                executor.submit(download_and_compress, group[0]['logo'], os.path.join(IMAGES_DIR, relative)):
                (channel, keys, relative, group)
                for channel, keys, relative, group in jobs
            }
            for future in concurrent.futures.as_completed(futures):
                channel, keys, relative, group = futures[future]
                try:
                    success, detail = future.result()
                except Exception as exc:
                    success, detail = False, str(exc)
                if success:
                    successful += 1
                    record_image(database, channel, keys, relative, group)
                else:
                    failed += 1
                    logger.error(f'Image failed for {channel}: {detail}')
                    for show in group:
                        show['logo'] = FALLBACK_URL
        database.commit()
        for channel, data in documents.items():
            save_json(Path(SCHEDULE_DIR) / f'{channel}.json', data)
        # Rebuild from the final channel files so duplicate aliases and failures
        # have identical logo URLs in both individual and combined schedules.
        today = datetime.now(timezone(timedelta(hours=-6))).date().isoformat()
        combined = {channel: data[today] for channel, data in documents.items() if data.get(today)}
        save_json(Path(SCHEDULE_DIR) / 'all-channel.json', combined)
        logger.info('Images complete', extra={'terminal': f'Images: {successful} saved, {reused} reused, {failed} failed.'})
    finally:
        database.close()


def record_image(database, channel, keys, relative, group):
    database.executemany(
        'INSERT OR REPLACE INTO image_aliases (channel, kind, value, path) VALUES (?, ?, ?, ?)',
        [(channel, kind, value, relative) for kind, value in keys])
    for show in group:
        show['logo'] = CDN_BASE + '/' + relative


if __name__ == "__main__":
    main()
