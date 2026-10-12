import os
import re
import json
import logging
from datetime import datetime, timedelta, timezone
import concurrent.futures

import gzip
import xml.etree.ElementTree as ET
import importlib.util
import subprocess
import sys


def install_dependencies():
    """Install missing packages using this interpreter, including the image job."""
    packages = {"requests": "requests", "bs4": "beautifulsoup4", "PIL": "Pillow"}
    missing = [package for module, package in packages.items()
               if importlib.util.find_spec(module) is None]
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])


if __name__ == "__main__":
    install_dependencies()

import requests
from bs4 import BeautifulSoup

DEFAULT_URL = 'https://iptv-epg.org/files/epg-mx.xml'


def timestamp(value):
    parts = value.split()
    dt = datetime.strptime(parts[0], '%Y%m%d%H%M%S')
    return dt.replace(tzinfo=timezone.utc) if len(parts) == 1 else datetime.strptime(value, '%Y%m%d%H%M%S %z')


def localized(element, tag):
    nodes = element.findall(tag)
    return next((n.text or '' for n in nodes if n.get('lang', '').startswith('es')), nodes[0].text or '' if nodes else '')


def localized_values(element, tag):
    nodes = element.findall(tag)
    spanish = [node for node in nodes if node.get('lang', '').lower().startswith('es')]
    return list(dict.fromkeys((node.text or '').strip() for node in (spanish or nodes) if (node.text or '').strip()))


class XMLTVFeed:
    def __init__(self, data):
        if data.startswith(b'\x1f\x8b'):
            data = gzip.decompress(data)
        root = ET.fromstring(data)
        if root.tag != 'tv':
            raise ValueError('Expected an XMLTV <tv> document')
        self.channels = {}
        self.names = {}
        self.ids = {}
        for channel in root.findall('channel'):
            cid = channel.attrib['id']
            self.channels[cid] = []
            self.ids.setdefault(cid.casefold(), set()).add(cid)
            name = localized(channel, 'display-name') or cid.removesuffix('.mx')
            self.names[cid] = re.sub(r'^MX\s*-\s*', '', name.strip(), flags=re.I)
        self.skipped = 0
        for programme in root.findall('programme'):
            try:
                start = timestamp(programme.attrib['start'])
                stop = timestamp(programme.attrib['stop']) if programme.get('stop') else None
                if stop is not None and stop <= start:
                    raise ValueError('Invalid programme duration')
                icon = programme.find('icon')
                show = dict(name=localized(programme, 'title') or 'Unknown',
                            category='/'.join(localized_values(programme, 'category')),
                            release_year=(programme.findtext('date') or '').strip()[:4],
                            description=localized(programme, 'desc'),
                            episode_number=localized(programme, 'episode-num'),
                            logo=icon.get('src', '') if icon is not None else '',
                            start_dt=start, end_dt=stop)
                self.channels.setdefault(programme.attrib['channel'], []).append(show)
            except (KeyError, ValueError, IndexError):
                self.skipped += 1
        for cid, shows in self.channels.items():
            shows.sort(key=lambda s: s['start_dt'])
            for i, show in enumerate(shows):
                if show['end_dt'] is None:
                    following = next((s['start_dt'] for s in shows[i + 1:] if s['start_dt'] > show['start_dt']), None)
                    show['end_dt'] = following or show['start_dt'] + timedelta(hours=1)

    @classmethod
    def download(cls, url=DEFAULT_URL):
        response = requests.get(url, timeout=(15, 90))
        response.raise_for_status()
        # requests already handles HTTP Content-Encoding; magic bytes identify .gz bodies.
        return cls(response.content)

    def channel_id(self, xmltv_id):
        requested = xmltv_id if xmltv_id.lower().endswith('.mx') else xmltv_id + '.mx'
        if requested in self.channels:
            return requested
        matches = self.ids.get(requested.casefold(), set())
        return next(iter(matches)) if len(matches) == 1 else None

    def shows(self, xmltv_id, tz):
        cid = self.channel_id(xmltv_id)
        return [dict(show, start_dt=show['start_dt'].astimezone(tz),
                     end_dt=show['end_dt'].astimezone(tz)) for show in self.channels.get(cid, [])]


def read_channels(path):
    channels = {}
    with open(path, encoding='utf-8') as source:
        for number, line in enumerate(source, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            slug, separator, xmltv_id = line.partition('=')
            slug, xmltv_id = slug.strip(), xmltv_id.strip()
            if not separator or not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', slug) or not xmltv_id:
                raise ValueError(f'{path}:{number}: expected mi.tv-slug=XMLTV-ID')
            if slug in channels:
                raise ValueError(f'{path}:{number}: duplicate channel {slug}')
            channels[slug] = xmltv_id
    return channels


def format_xmltv_schedule(channel, feed, xmltv_id, now=None):
    now = now or datetime.now(MX_TZ)
    today = now.date()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    cutoff = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=MX_TZ) + timedelta(hours=23)
    cid = feed.channel_id(xmltv_id)
    output = {'channel': feed.names.get(cid, xmltv_id)}
    seen = set()
    for show in feed.shows(xmltv_id, MX_TZ):
        date = show['start_dt'].date()
        if date not in (today, tomorrow) and not (date == yesterday and show['end_dt'] > cutoff):
            continue
        item = {
            'show': show['name'],
            'category': show['category'],
            'logo': show['logo'],
            'start': show['start_dt'].strftime('%H:%M'),
            'end': show['end_dt'].strftime('%H:%M'),
            'release_year': show['release_year'],
            'description': show['description'],
            'episode_number': show['episode_number'],
        }
        identity = (show['start_dt'], show['end_dt'], json.dumps(item, sort_keys=True))
        if identity not in seen:
            output.setdefault(date.isoformat(), []).append(item)
            seen.add(identity)
    return output


CDN_BASE = os.environ.get("IMAGE_CDN_BASE", "https://cdn.programaciontv.com.mx/wp-content/uploads/downloaded-images").rstrip("/")

def slugify(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

# Configure logging to overwrite the scrape.log file on every run
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "scrape.log")

# Setup logging
logger = logging.getLogger("scraper")
logger.setLevel(logging.INFO)
# Prevent duplicate logs if running multiple times in same process
if getattr(logger, 'handlers', None):
    logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(logging.Formatter('%(terminal)s'))
console_handler.addFilter(lambda record: hasattr(record, 'terminal'))
logger.propagate = False
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# We will use UTC-6 for Mexico City
MX_TZ = timezone(timedelta(hours=-6))

def parse_time_str(time_str):
    time_str = time_str.strip().lower()
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except ValueError:
        return datetime.strptime(time_str, "%I:%M%p").time()

def fetch_schedule_page(channel, rel_day):
    # rel_day can be 'ayer', '', 'manana'
    url = f"https://mi.tv/mx/async/channel/{channel}/{rel_day}/-360" if rel_day else f"https://mi.tv/mx/async/channel/{channel}/-360"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html"
    }
    response = requests.get(url, headers=headers, timeout=(15, 45))
    if response.status_code == 200:
        return response.text
    return ""

def parse_page_items(html, logical_start_date):
    if not html:
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    shows = soup.select("ul.broadcasts li a.program-link")
    
    parsed_shows = []
    current_date = logical_start_date
    prev_time = None
    
    for show in shows:
        # Extract basic info
        time_tag = show.select_one("span.time")
        if not time_tag:
            continue
        time_str = time_tag.text.strip()
        parsed_t = parse_time_str(time_str)
        
        # Determine if we crossed midnight
        if prev_time is not None and parsed_t < prev_time:
            # We crossed midnight
            current_date += timedelta(days=1)
        
        prev_time = parsed_t
        start_dt = datetime.combine(current_date, parsed_t).replace(tzinfo=MX_TZ)
        
        name_tag = show.select_one("h2")
        name = name_tag.text.strip() if name_tag else "Unknown"
        
        cat_tag = show.select_one("span.sub-title")
        category = cat_tag.text.strip() if cat_tag else ""
        
        img_div = show.select_one("div.image")
        logo = ""
        if img_div and "background-image: url(" in img_div.get("style", ""):
            style = img_div["style"]
            start_idx = style.find("url('") + 5
            end_idx = style.find("')", start_idx)
            if start_idx != 4 and end_idx != -1:
                logo = style[start_idx:end_idx]
        elif img_div and "background-image: url(" in img_div.get("style", "").replace('"', "'"):
             # Handle double quotes if any
             style = img_div["style"].replace('"', "'")
             start_idx = style.find("url('") + 5
             end_idx = style.find("')", start_idx)
             if start_idx != 4 and end_idx != -1:
                 logo = style[start_idx:end_idx]
            
        parsed_shows.append({
            "name": name,
            "start_dt": start_dt,
            "category": category,
            "logo": logo
        })
        
    return parsed_shows

def generate_channel_schedule(channel):
    now = datetime.now(MX_TZ)
    # The 'ayer' page starts on yesterday
    yesterday = (now - timedelta(days=1)).date()
    today = now.date()
    tomorrow = (now + timedelta(days=1)).date()
    
    all_shows = []
    
    # Yesterday
    html_ayer = fetch_schedule_page(channel, "ayer")
    all_shows.extend(parse_page_items(html_ayer, yesterday))
    
    # Today
    html_hoy = fetch_schedule_page(channel, "")
    all_shows.extend(parse_page_items(html_hoy, today))
    
    # Tomorrow
    html_manana = fetch_schedule_page(channel, "manana")
    all_shows.extend(parse_page_items(html_manana, tomorrow))
    
    # Filter duplicates due to overlapping boundaries
    unique_shows = {}
    for s in all_shows:
        # Use start_dt and name as unique key to prevent overlaps
        # since yesterday's page and today's page may share some shows at boundaries
        key = (s["start_dt"], s["name"])
        if key not in unique_shows:
            unique_shows[key] = s
            
    # Sort by start_dt
    sorted_shows = sorted(unique_shows.values(), key=lambda x: x["start_dt"])
    
    # Calculate end dates
    for i in range(len(sorted_shows) - 1):
        sorted_shows[i]["end_dt"] = sorted_shows[i+1]["start_dt"]
    
    # For the last show, we assume a default 60 mins duration
    if sorted_shows:
        sorted_shows[-1]["end_dt"] = sorted_shows[-1]["start_dt"] + timedelta(minutes=60)
        
    return format_schedule(channel, sorted_shows, now)


def format_schedule(channel, sorted_shows, now=None):
    now = now or datetime.now(MX_TZ)
    yesterday = (now - timedelta(days=1)).date()
    today = now.date()
    tomorrow = (now + timedelta(days=1)).date()
    # Keep only shows occurring today or tomorrow
    # Group them by date
    final_output = {
        "channel": channel,
    }
    
    yesterday_str = yesterday.isoformat()
    today_str = today.isoformat()
    tomorrow_str = tomorrow.isoformat()
    
    final_output[yesterday_str] = []
    final_output[today_str] = []
    final_output[tomorrow_str] = []
    
    yesterday_23 = datetime.combine(yesterday, datetime.strptime("23:00", "%H:%M").time()).replace(tzinfo=MX_TZ)
    
    for s in sorted_shows:
        s_date = s["start_dt"].date()
        date_list = None
        
        if s_date == today:
            date_list = final_output[today_str]
        elif s_date == tomorrow:
            date_list = final_output[tomorrow_str]
        elif s_date == yesterday and s["end_dt"] > yesterday_23:
            date_list = final_output[yesterday_str]
            
        if date_list is not None:
            new_show = {
                "show": s["name"],
                "category": s["category"],
                "logo": s["logo"],
                "start": s["start_dt"].strftime("%H:%M"),
                "end": s["end_dt"].strftime("%H:%M")
            }
            if date_list and date_list[-1]["show"] == new_show["show"] and date_list[-1]["category"] == new_show["category"] and date_list[-1]["end"] == new_show["start"]:
                date_list[-1]["end"] = new_show["end"]
            else:
                date_list.append(new_show)
            
    if not final_output[yesterday_str]:
        del final_output[yesterday_str]
    if not final_output[today_str]:
        del final_output[today_str]
    if not final_output[tomorrow_str]:
        del final_output[tomorrow_str]
        
    return final_output

def process_channel(channel, schedule_dir, primary=None, xmltv_id=None):
    try:
        schedule = {}
        today = datetime.now(MX_TZ).date().isoformat()
        if primary is not None:
            try:
                schedule = format_xmltv_schedule(channel, primary, xmltv_id or channel)
            except Exception as exc:
                logger.warning(f"IPTV failed for {channel}: {exc}")
        source = "IPTV"
        if schedule.get(today):
            logger.info(f"[IPTV] {channel}: Using XMLTV schedule.")
        else:
            logger.info(f"[FALLBACK] {channel}: No usable IPTV schedule today; trying mi.tv.")
            source = "mi.tv"
            schedule = generate_channel_schedule(channel)
        if schedule:
            # Check if there's actually any item
            has_items = sum(len(shows) for key, shows in schedule.items() if isinstance(shows, list)) > 0
            if not has_items:
                return channel, False, "No shows today or tomorrow", None
                
            json_path = os.path.join(schedule_dir, f"{channel}.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(schedule, f, ensure_ascii=False, separators=(',', ':'))
                
            total_items = sum(len(shows) for key, shows in schedule.items() if isinstance(shows, list))
            logger.info(f"Saved {channel} from {source}: {total_items} items",
                        extra={"terminal": f"{channel} — {source}"})
            return channel, True, total_items, schedule
        else:
            return channel, False, "No schedule parsed", None
    except Exception as e:
        return channel, False, str(e), None

def main():
    if not os.path.exists(BASE_DIR):
        logger.error(f"Base dir {BASE_DIR} not found.", extra={"terminal": "Failed: project directory missing."})
        return
        
    schedule_dir = os.environ.get("SCHEDULE_DIR", os.path.join(BASE_DIR, "schedule"))
    if not os.path.exists(schedule_dir):
        os.makedirs(schedule_dir)
        
    filter_file = os.path.join(BASE_DIR, "filter.txt")
    if not os.path.exists(filter_file):
        logger.error(f"Error: {filter_file} not found.", extra={"terminal": "Failed: filter.txt missing."})
        return
        
    try:
        channels = read_channels(filter_file)
    except ValueError as exc:
        logger.error(str(exc), extra={"terminal": f"Invalid filter.txt: {exc}"})
        return
    primary = None
    try:
        primary = XMLTVFeed.download(os.environ.get("XMLTV_URL", DEFAULT_URL))
        logger.info(f"Loaded IPTV primary: {len(primary.channels)} channels; {primary.skipped} invalid programmes skipped.")
    except Exception as exc:
        logger.warning(f"IPTV unavailable; falling back to mi.tv: {exc}",
                       extra={"terminal": "IPTV unavailable — using mi.tv."})

    successful = []
    failed = []
    
    # Read configure from env or default based on gh actions capabilities
    max_workers = int(os.environ.get("SCRAPE_WORKERS", "20"))
    
    all_channels_today = {}
    today_str = datetime.now(MX_TZ).date().isoformat()
    
    # Process multiple channels in parallel
    logger.info(f"Starting schedule scraping for {len(channels)} channels using {max_workers} workers...",
                extra={"terminal": f"Scraping {len(channels)} channels..."})
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_channel = {
            executor.submit(process_channel, ch, schedule_dir, primary, channels[ch]): ch for ch in channels
        }
        for future in concurrent.futures.as_completed(future_to_channel):
            ch = future_to_channel[future]
            success, result, schedule = False, "Unknown Error", None
            try:
                ch, success, result, schedule = future.result()
            except Exception as e:
                result = str(e)
                
            if success:
                logger.info(f"[SUCCESS] {ch}: Saved {result} items.")
                successful.append(ch)
                if schedule and today_str in schedule:
                    today_shows = []
                    for show in schedule[today_str]:
                        show_copy = show.copy()
                        original_logo = show_copy.get("logo")
                        if original_logo and original_logo.startswith("http") and not original_logo.startswith(CDN_BASE):
                            slug = slugify(show_copy["show"])
                            show_copy["logo"] = f"{CDN_BASE}/{ch}/{slug}.webp"
                        today_shows.append(show_copy)
                    all_channels_today[ch] = today_shows
            else:
                logger.error(f"[FAILED] {ch}: {result}", extra={"terminal": f"{ch} — FAILED"})
                failed.append(ch)
                
    if all_channels_today:
        all_channel_path = os.path.join(schedule_dir, "all-channel.json")
        with open(all_channel_path, "w", encoding="utf-8") as f:
            json.dump(all_channels_today, f, ensure_ascii=False, separators=(',', ':'))
        logger.info(f"Saved all-channel.json with {len(all_channels_today)} channels for today.")
                
    # Print the epg.logo summary
    summary_msg = "\n" + "="*40 + "\n"
    summary_msg += "EPG.LOGO SUMMARY - SCRAPING COMPLETED\n"
    summary_msg += "="*40 + "\n"
    summary_msg += f"Successfully configured channels ({len(successful)}):\n"
    summary_msg += f"  {', '.join(successful) if successful else 'None'}\n"
    summary_msg += "-"*40 + "\n"
    
    if failed:
        summary_msg += f"FAILED CHANNELS ({len(failed)}) >>> REQUIRES ATTENTION:\n"
        for fc in failed:
            summary_msg += f"  - {fc}\n"
    else:
        summary_msg += "ALL CHANNELS PROCESSED SUCCESSFULLY!\n"
    summary_msg += "="*40
    
    logger.info(summary_msg, extra={"terminal": f"Done: {len(successful)} saved, {len(failed)} failed."})

if __name__ == "__main__":
    main()
