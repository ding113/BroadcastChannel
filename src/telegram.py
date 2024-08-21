import requests
from bs4 import BeautifulSoup
import lxml.html
import re
import time
import html
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TelegramChannelScraper:
    def __init__(self, channel_name, start_id=None):
        self.channel_name = channel_name
        self.base_url = f"https://t.me/s/{channel_name}"
        self.session = requests.Session()
        self.txt_filename = f"{channel_name}_posts.txt"
        self.posts = {}
        self.oldest_id = start_id
        self.newest_id = None
        self.empty_page_count = 0
        self.max_empty_pages = 3  # 允许连续空页面的最大数量

    def get_page_content(self, before=None):
        url = self.base_url
        if before:
            url += f"?before={before}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Cache-Control': 'no-cache'
        }
        for _ in range(3):  # 重试3次
            try:
                response = self.session.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logging.warning(f"请求失败: {e}. 正在重试...")
                time.sleep(2)
        logging.error("3次重试后仍未能获取页面内容")
        return None

    def parse_message(self, message_html):
        parsers = [self.parse_with_bs4, self.parse_with_lxml, self.parse_with_regex, self.parse_with_custom]
        result = {
            'message_id': '',
            'date': '',
            'text': '',
            'photo_url': ''
        }
        for parser in parsers:
            parsed = parser(message_html)
            for key, value in parsed.items():
                if not result[key] and value:
                    result[key] = value
        return result

    def parse_with_bs4(self, message_html):
        soup = BeautifulSoup(message_html, 'html.parser')
        text = soup.select_one('.tgme_widget_message_text')
        text = text.get_text(strip=True) if text else ''
        photo = soup.select_one('.tgme_widget_message_photo_wrap')
        photo_url = photo['style'].split("'")[1] if photo and 'style' in photo.attrs else ''
        time_element = soup.select_one('time')
        date = time_element.get('datetime') if time_element else ''
        message_element = soup.select_one('.tgme_widget_message')
        message_id = message_element['data-post'].split('/')[-1] if message_element and 'data-post' in message_element.attrs else ''
        return {'text': text, 'photo_url': photo_url, 'date': date, 'message_id': message_id}

    def parse_with_lxml(self, message_html):
        root = lxml.html.fromstring(message_html)
        text = root.cssselect('.tgme_widget_message_text')
        text = text[0].text_content().strip() if text else ''
        photo = root.cssselect('.tgme_widget_message_photo_wrap')
        photo_url = photo[0].get('style').split("'")[1] if photo and 'style' in photo[0].attrib else ''
        time_element = root.cssselect('time')
        date = time_element[0].get('datetime') if time_element else ''
        message_element = root.cssselect('.tgme_widget_message')
        message_id = message_element[0].get('data-post').split('/')[-1] if message_element and 'data-post' in message_element[0].attrib else ''
        return {'text': text, 'photo_url': photo_url, 'date': date, 'message_id': message_id}

    def parse_with_regex(self, message_html):
        text_pattern = re.compile(r'<div class="tgme_widget_message_text js-message_text" dir="auto">(.*?)</div>', re.DOTALL)
        photo_pattern = re.compile(r'<a class="tgme_widget_message_photo_wrap.*?background-image:url\(\'(.*?)\'\)')
        date_pattern = re.compile(r'<time datetime="(.*?)">')
        message_id_pattern = re.compile(r'data-post=".*?/(\d+)"')
        
        text = text_pattern.search(message_html)
        text = html.unescape(re.sub('<[^<]+?>', '', text.group(1))).strip() if text else ''
        
        photo = photo_pattern.search(message_html)
        photo_url = photo.group(1) if photo else ''
        
        date = date_pattern.search(message_html)
        date = date.group(1) if date else ''
        
        message_id = message_id_pattern.search(message_html)
        message_id = message_id.group(1) if message_id else ''
        
        return {'text': text, 'photo_url': photo_url, 'date': date, 'message_id': message_id}

    def parse_with_custom(self, message_html):
        text = ''
        photo_url = ''
        date = ''
        message_id = ''
        
        text_match = re.search(r'<div class="tgme_widget_message_text js-message_text" dir="auto">(.*?)</div>', message_html, re.DOTALL)
        if text_match:
            text = html.unescape(re.sub('<[^<]+?>', '', text_match.group(1))).strip()
        
        photo_match = re.search(r'<a class="tgme_widget_message_photo_wrap.*?background-image:url\(\'(.*?)\'\)', message_html)
        if photo_match:
            photo_url = photo_match.group(1)
        
        date_match = re.search(r'<time datetime="(.*?)">', message_html)
        if date_match:
            date = date_match.group(1)
        
        message_id_match = re.search(r'data-post=".*?/(\d+)"', message_html)
        if message_id_match:
            message_id = message_id_match.group(1)
        
        return {'text': text, 'photo_url': photo_url, 'date': date, 'message_id': message_id}

    def save_to_txt(self):
        sorted_posts = sorted(self.posts.values(), key=lambda x: int(x['message_id']), reverse=True)
        with open(self.txt_filename, 'w', encoding='utf-8') as txtfile:
            for post in sorted_posts:
                txtfile.write(f"ID: {post.get('message_id', 'N/A')}\n")
                txtfile.write(f"日期: {post.get('date', 'N/A')}\n")
                txtfile.write(f"文本: {post.get('text', 'N/A')}\n")
                if 'photo_url' in post and post['photo_url']:
                #    txtfile.write(f"图片URL: {post['photo_url']}\n")
                txtfile.write('***\n\n')

    def scrape_channel(self):
        while True:
            try:
                page_content = self.get_page_content(self.oldest_id)
                if not page_content:
                    break

                soup = BeautifulSoup(page_content, 'html.parser')
                messages = soup.select('.tgme_widget_message_wrap')

                if not messages:
                    self.empty_page_count += 1
                    if self.empty_page_count >= self.max_empty_pages:
                        logging.info(f"连续 {self.max_empty_pages} 页没有新消息，停止抓取")
                        break
                    continue
                else:
                    self.empty_page_count = 0

                new_messages_count = 0
                for message in messages:
                    try:
                        parsed_message = self.parse_message(str(message))
                        message_id = parsed_message.get('message_id')
                        if message_id and (parsed_message.get('text') or parsed_message.get('photo_url')):
                            if message_id not in self.posts:
                                self.posts[message_id] = parsed_message
                                new_messages_count += 1
                                if not self.newest_id or int(message_id) > int(self.newest_id):
                                    self.newest_id = message_id
                                if not self.oldest_id or int(message_id) < int(self.oldest_id):
                                    self.oldest_id = message_id
                        else:
                            logging.info(f"跳过空消息或无效消息，ID: {message_id}")
                    except Exception as e:
                        logging.error(f"解析消息时出错: {str(e)}")

                logging.info(f"本页抓取了 {new_messages_count} 条新消息，总计 {len(self.posts)} 条")
                self.save_to_txt()  # 每页都保存一次

                if new_messages_count == 0:
                    self.empty_page_count += 1
                    if self.empty_page_count >= self.max_empty_pages:
                        logging.info(f"连续 {self.max_empty_pages} 页没有新消息，停止抓取")
                        break
                else:
                    self.empty_page_count = 0

                if self.oldest_id and int(self.oldest_id) <= 1:
                    logging.info("已经抓取到最早的消息，停止抓取")
                    break

                time.sleep(1)  # 对服务器友好
            except Exception as e:
                logging.error(f"抓取过程中出错: {str(e)}")
                break

        logging.info(f"抓取完成，共保存 {len(self.posts)} 条消息到 {self.txt_filename}")

if __name__ == "__main__":
    channel_name = input("请输入Telegram频道名称 (不包含@): ")
    start_id = input("请输入起始消息ID (留空从最新开始): ") or None
    scraper = TelegramChannelScraper(channel_name, start_id)
    try:
        scraper.scrape_channel()
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    finally:
        scraper.save_to_txt()
        logging.info(f"程序结束，已保存的消息数：{len(scraper.posts)}")