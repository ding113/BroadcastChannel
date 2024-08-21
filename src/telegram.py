import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import html
from datetime import datetime
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("telegram_scraper.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)

@dataclass
class TelegramPost:
    message_id: str
    date: str
    text: str
    photo_url: str

class TelegramChannelScraper:
    def __init__(self, channel_name: str, start_id: Optional[str] = None):
        self.channel_name: str = channel_name
        self.base_url: str = f"https://t.me/s/{channel_name}"
        self.posts: Dict[str, TelegramPost] = {}
        self.oldest_id: Optional[str] = start_id
        self.newest_id: Optional[str] = None
        self.empty_page_count: int = 0
        self.max_empty_pages: int = 3

    async def get_page_content(self, session: aiohttp.ClientSession, before: Optional[str] = None) -> Optional[str]:
        url = self.base_url
        if before:
            url += f"?before={before}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Cache-Control": "no-cache",
        }
        for _ in range(3):  # Retry 3 times
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    response.raise_for_status()
                    return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Request failed: {e}. Retrying...")
                await asyncio.sleep(2)
        logger.error("Failed to get page content after 3 retries")
        return None

    def parse_message(self, message_html: str) -> TelegramPost:
        parsers = [self.parse_with_bs4, self.parse_with_regex]
        result = TelegramPost(message_id="", date="", text="", photo_url="")
        for parser in parsers:
            parsed = parser(message_html)
            for key, value in parsed.items():
                if not getattr(result, key) and value:
                    setattr(result, key, value)
        return result

    def parse_with_bs4(self, message_html: str) -> Dict[str, str]:
        soup = BeautifulSoup(message_html, "html.parser")
        text_elem = soup.select_one(".tgme_widget_message_text")
        text = text_elem.get_text(strip=True) if text_elem else ""
        photo = soup.select_one(".tgme_widget_message_photo_wrap")
        photo_url = photo["style"].split("'")[1] if photo and "style" in photo.attrs else ""
        time_element = soup.select_one("time")
        date = time_element.get("datetime") if time_element else ""
        message_element = soup.select_one(".tgme_widget_message")
        message_id = message_element["data-post"].split("/")[-1] if message_element and "data-post" in message_element.attrs else ""
        return {"text": text, "photo_url": photo_url, "date": date, "message_id": message_id}

    def parse_with_regex(self, message_html: str) -> Dict[str, str]:
        text_pattern = re.compile(r'<div class="tgme_widget_message_text js-message_text" dir="auto">(.*?)</div>', re.DOTALL)
        photo_pattern = re.compile(r'<a class="tgme_widget_message_photo_wrap.*?background-image:url\(\'(.*?)\'\)')
        date_pattern = re.compile(r'<time datetime="(.*?)">')
        message_id_pattern = re.compile(r'data-post=".*?/(\d+)"')
        
        text = text_pattern.search(message_html)
        text = html.unescape(re.sub("<[^<]+?>", "", text.group(1))).strip() if text else ""
        
        photo = photo_pattern.search(message_html)
        photo_url = photo.group(1) if photo else ""
        
        date = date_pattern.search(message_html)
        date = date.group(1) if date else ""
        
        message_id = message_id_pattern.search(message_html)
        message_id = message_id.group(1) if message_id else ""
        
        return {"text": text, "photo_url": photo_url, "date": date, "message_id": message_id}

    def save_to_json(self, filename: str) -> None:
        sorted_posts = sorted(self.posts.values(), key=lambda x: int(x.message_id), reverse=True)
        with open(filename, "w", encoding="utf-8") as jsonfile:
            json.dump([post.__dict__ for post in sorted_posts], jsonfile, ensure_ascii=False, indent=2)

    async def scrape_channel(self, session: aiohttp.ClientSession) -> None:
        while True:
            try:
                page_content = await self.get_page_content(session, self.oldest_id)
                if not page_content:
                    break

                soup = BeautifulSoup(page_content, "html.parser")
                messages = soup.select(".tgme_widget_message_wrap")

                if not messages:
                    self.empty_page_count += 1
                    if self.empty_page_count >= self.max_empty_pages:
                        logger.info(f"No new messages for {self.max_empty_pages} consecutive pages. Stopping.")
                        break
                    continue
                else:
                    self.empty_page_count = 0

                new_messages_count = 0
                for message in messages:
                    try:
                        parsed_message = self.parse_message(str(message))
                        message_id = parsed_message.message_id
                        if message_id and (parsed_message.text or parsed_message.photo_url):
                            if message_id not in self.posts:
                                self.posts[message_id] = parsed_message
                                new_messages_count += 1
                                if not self.newest_id or int(message_id) > int(self.newest_id):
                                    self.newest_id = message_id
                                if not self.oldest_id or int(message_id) < int(self.oldest_id):
                                    self.oldest_id = message_id
                        else:
                            logger.info(f"Skipping empty or invalid message, ID: {message_id}")
                    except Exception as e:
                        logger.error(f"Error parsing message: {str(e)}")

                logger.info(f"Scraped {new_messages_count} new messages from {self.channel_name}. Total: {len(self.posts)}")
                self.save_to_json(f"{self.channel_name}_posts.json")

                if new_messages_count == 0:
                    self.empty_page_count += 1
                    if self.empty_page_count >= self.max_empty_pages:
                        logger.info(f"No new messages for {self.max_empty_pages} consecutive pages. Stopping.")
                        break
                else:
                    self.empty_page_count = 0

                if self.oldest_id and int(self.oldest_id) <= 1:
                    logger.info("Reached the earliest message. Stopping.")
                    break

                await asyncio.sleep(1)  # Be nice to the server
            except Exception as e:
                logger.error(f"Error during scraping process: {str(e)}")
                break

        logger.info(f"Scraping completed for {self.channel_name}. Total messages saved: {len(self.posts)}")

class TelegramMultiChannelScraper:
    def __init__(self, channel_names: List[str], start_ids: Optional[Dict[str, str]] = None):
        self.scrapers = [
            TelegramChannelScraper(channel, start_ids.get(channel) if start_ids else None)
            for channel in channel_names
        ]

    async def scrape_all_channels(self) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = [scraper.scrape_channel(session) for scraper in self.scrapers]
            await asyncio.gather(*tasks)

async def main():
    channel_names = input("Enter Telegram channel names (comma-separated, without @): ").split(",")
    channel_names = [name.strip() for name in channel_names]
    
    start_ids = {}
    for channel in channel_names:
        start_id = input(f"Enter start message ID for {channel} (leave empty for latest): ").strip()
        if start_id:
            start_ids[channel] = start_id

    multi_scraper = TelegramMultiChannelScraper(channel_names, start_ids)
    
    try:
        await multi_scraper.scrape_all_channels()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logger.info("Program finished")

if __name__ == "__main__":
    asyncio.run(main())