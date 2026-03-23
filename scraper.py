import os
import re
import json
import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import concurrent.futures

CDN_BASE = "https://cdn.programaciontv.com.mx/wp-content/uploads/downloaded-images"

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
console_handler.setFormatter(formatter)
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
    response = requests.get(url, headers=headers)
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
            slug = slugify(s["name"])
            logo_url = f"{CDN_BASE}/{channel}/{slug}.webp"
            
            new_show = {
                "show": s["name"],
                "category": s["category"],
                "logo": logo_url,
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

def process_channel(channel, schedule_dir):
    try:
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
            return channel, True, total_items, schedule
        else:
            return channel, False, "No schedule parsed", None
    except Exception as e:
        return channel, False, str(e), None

def main():
    if not os.path.exists(BASE_DIR):
        logger.error(f"Base dir {BASE_DIR} not found.")
        return
        
    schedule_dir = os.path.join(BASE_DIR, "schedule")
    if not os.path.exists(schedule_dir):
        os.makedirs(schedule_dir)
        
    filter_file = os.path.join(BASE_DIR, "filter.txt")
    if not os.path.exists(filter_file):
        logger.error(f"Error: {filter_file} not found.")
        return
        
    with open(filter_file, "r") as f:
        channels = [line.strip() for line in f if line.strip()]
        
    successful = []
    failed = []
    
    # Read configure from env or default based on gh actions capabilities
    max_workers = int(os.environ.get("SCRAPE_WORKERS", "20"))
    
    all_channels_today = {}
    today_str = datetime.now(MX_TZ).date().isoformat()
    
    # Process multiple channels in parallel
    logger.info(f"Starting schedule scraping for {len(channels)} channels using {max_workers} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_channel = {
            executor.submit(process_channel, ch, schedule_dir): ch for ch in channels
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
                    all_channels_today[ch] = schedule[today_str]
            else:
                logger.error(f"[FAILED] {ch}: {result}")
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
    
    logger.info(summary_msg)

if __name__ == "__main__":
    main()
