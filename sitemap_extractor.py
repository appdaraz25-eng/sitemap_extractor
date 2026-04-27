import sys
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import pandas as pd
from datetime import datetime
import pytz
from typing import List, Set, Dict
import os
from bs4 import BeautifulSoup
import re
from pathlib import Path
import logging
import requests
import random
import sqlite3
from dateutil import parser as date_parser
import validators
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HEADLESS MODE DETECTION
# Run with:  python sitemap_extractor.py --headless
#            python sitemap_extractor.py --headless --sites sites.txt
#            python sitemap_extractor.py --headless --async-mode --percent 50
# GUI mode:  python sitemap_extractor.py   (no arguments)
# ─────────────────────────────────────────────────────────────────────────────

HEADLESS_MODE = "--headless" in sys.argv


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNALS STUB  –  replaces PyQt signals when running headless
# ══════════════════════════════════════════════════════════════════════════════

class HeadlessSignals:
    """Mimics WorkerSignals but just prints to stdout — no Qt needed."""

    def emit_log(self, message, level="info"):
        icons = {"error": "❌", "warning": "⚠️", "success": "✅", "info": "ℹ️"}
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {icons.get(level, 'ℹ️')} {message}", flush=True)

    # Create callable sub-objects that mimic signal.emit()
    class _Sig:
        def __init__(self, fn=None):
            self._fn = fn or (lambda *a, **kw: None)
        def emit(self, *args, **kwargs):
            self._fn(*args, **kwargs)
        def connect(self, fn):
            self._fn = fn

    def __init__(self):
        outer = self

        self.log = self._Sig(lambda msg, lvl="info": outer.emit_log(msg, lvl))
        self.error = self._Sig(lambda msg: outer.emit_log(msg, "error"))
        self.progress_update = self._Sig(
            lambda cur, tot, desc: print(
                f"  ⏳ {desc}: {cur}/{tot} ({int(cur/tot*100) if tot else 0}%)",
                flush=True
            )
        )
        self.finished = self._Sig()
        self.extraction_complete = self._Sig(
            lambda domain, new, total: outer.emit_log(
                f"✅ {domain}: {new} new URLs extracted (total found: {total})", "success"
            )
        )
        self.site_complete = self._Sig(
            lambda domain, ok: outer.emit_log(
                f"{'✅' if ok else '❌'} Site {'completed' if ok else 'FAILED'}: {domain}",
                "success" if ok else "error"
            )
        )


# ══════════════════════════════════════════════════════════════════════════════
#  ULTRA-FAST ASYNC EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

class UltraFastSitemapExtractor:
    """Ultra-fast asynchronous sitemap extractor."""

    def __init__(self, domain: str, output_file: str, db_file: str, signals, process_percentage: float = 100.0):
        self.domain = domain.rstrip('/')
        self.output_file = output_file
        self.db_file = db_file
        self.signals = signals
        self.process_percentage = process_percentage
        self.existing_urls = self.load_existing_urls()
        self.all_urls: Set[str] = set()
        self.urls_metadata: Dict = {}
        self.batch_size = 50
        self.semaphore = None

    def load_existing_urls(self) -> Set[str]:
        if os.path.exists(self.output_file):
            try:
                df = pd.read_excel(self.output_file)
                existing = set(df['url'].tolist())
                self.signals.log.emit(f"📚 Loaded {len(existing)} existing URLs from Excel", "info")
                return existing
            except Exception as e:
                self.signals.log.emit(f"Error loading existing file: {e}", "error")
        return set()

    def init_database(self):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    published_date TEXT,
                    article_content TEXT,
                    extracted_date TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON news_articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
            conn.commit()
            conn.close()
            self.signals.log.emit(f"🗄️ Database initialized: {self.db_file}", "info")
        except Exception as e:
            self.signals.log.emit(f"Error initializing database: {e}", "error")

    async def fetch_robots_txt(self, session: aiohttp.ClientSession) -> str:
        robots_url = f"{self.domain}/robots.txt"
        try:
            async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            self.signals.log.emit(f"Could not fetch robots.txt: {e}", "warning")
        return ""

    def extract_sitemap_from_robots(self, robots_content: str) -> List[str]:
        sitemaps = []
        pattern = r'[Ss]itemap:\s*(https?://\S+)'
        matches = re.findall(pattern, robots_content)
        if matches:
            sitemaps.extend(matches)
            self.signals.log.emit(f"🔍 Found {len(matches)} sitemap(s) in robots.txt", "info")
        else:
            default_sitemaps = [
                f"{self.domain}/sitemap.xml",
                f"{self.domain}/sitemap_index.xml",
                f"{self.domain}/sitemap/sitemap.xml",
                f"{self.domain}/sitemap/sitemap_index.xml",
                f"{self.domain}/sitemap-0.xml",
                f"{self.domain}/sitemap1.xml",
                f"{self.domain}/news_sitemap.xml",
                f"{self.domain}/sitemap_news.xml"
            ]
            sitemaps.extend(default_sitemaps)
            self.signals.log.emit("No sitemaps in robots.txt, trying common locations", "info")
        return list(set(sitemaps))

    async def fetch_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str):
        try:
            async with self.semaphore:
                async with session.get(sitemap_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        content = await response.text()
                        return content, sitemap_url
                    else:
                        self.signals.log.emit(f"Failed to fetch {sitemap_url}: HTTP {response.status}", "warning")
        except Exception as e:
            self.signals.log.emit(f"Error fetching {sitemap_url}: {e}", "warning")
        return "", sitemap_url

    def parse_sitemap_with_metadata(self, content: str, sitemap_url: str):
        urls = set()
        subsitemaps = set()
        url_metadata = {}
        try:
            root = ET.fromstring(content)
            namespaces = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9',
            }
            if root.find('.//ns:sitemap', namespaces) is not None:
                for sitemap_elem in root.findall('.//ns:sitemap', namespaces):
                    loc = sitemap_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        subsitemaps.add(loc.text)
            else:
                for url_elem in root.findall('.//ns:url', namespaces):
                    loc = url_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        url = loc.text
                        urls.add(url)
                        metadata = {}
                        news_date_elem = url_elem.find('.//news:publication_date', namespaces)
                        if news_date_elem is not None and news_date_elem.text:
                            metadata['news_date'] = news_date_elem.text
                        news_title_elem = url_elem.find('.//news:title', namespaces)
                        if news_title_elem is not None and news_title_elem.text:
                            metadata['news_title'] = news_title_elem.text
                        lastmod_elem = url_elem.find('ns:lastmod', namespaces)
                        if lastmod_elem is not None and lastmod_elem.text:
                            metadata['lastmod'] = lastmod_elem.text
                        date_from_url = self.extract_date_from_url(url)
                        if date_from_url:
                            metadata['url_date'] = date_from_url
                        url_metadata[url] = metadata
        except Exception as e:
            self.signals.log.emit(f"Error parsing sitemap XML: {e}", "error")
        return urls, subsitemaps, url_metadata

    def extract_date_from_url(self, url: str):
        patterns = [
            r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
            r'/(\d{4})-(\d{1,2})-(\d{1,2})/',
            r'/(\d{4})(\d{2})(\d{2})/',
            r'/(\d{4})[/-](\d{1,2})[/-](\d{1,2})'
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return datetime(year, month, day).strftime("%Y-%m-%d")
                except:
                    continue
        return None

    def parse_date_from_string(self, date_str: str):
        try:
            return date_parser.parse(date_str)
        except:
            return None

    async def crawl_sitemap_tree(self, session: aiohttp.ClientSession, sitemap_url: str):
        visited_sitemaps = set()
        to_visit = [sitemap_url]
        while to_visit and len(visited_sitemaps) < 200:
            current = to_visit.pop(0)
            if current in visited_sitemaps:
                continue
            visited_sitemaps.add(current)
            self.signals.progress_update.emit(
                len(visited_sitemaps), len(to_visit) + len(visited_sitemaps), "Processing sitemaps"
            )
            content, _ = await self.fetch_sitemap(session, current)
            if not content:
                continue
            urls, subsitemaps, url_metadata = self.parse_sitemap_with_metadata(content, current)
            for url in urls:
                if url not in self.existing_urls:
                    self.all_urls.add(url)
                    if url in url_metadata:
                        self.urls_metadata[url] = url_metadata[url]
            for submap in subsitemaps:
                if submap not in visited_sitemaps:
                    to_visit.append(submap)

    def select_urls_by_percentage(self, urls_list: List[str]) -> List[str]:
        if self.process_percentage >= 100.0:
            return urls_list
        urls_to_process = max(1, int((self.process_percentage / 100.0) * len(urls_list)))
        selected = random.sample(urls_list, urls_to_process)
        self.signals.log.emit(
            f"🎯 Processing {self.process_percentage}% of total URLs ({urls_to_process}/{len(urls_list)})", "info"
        )
        return selected

    def extract_date_from_article_content(self, article_content: str):
        if not article_content:
            return None
        date_patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
        ]
        for pattern in date_patterns:
            match = re.search(pattern, article_content, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if match.group(1).isdigit() and match.group(3).isdigit():
                            day = int(match.group(1))
                            month = self.get_month_number(match.group(2))
                            year = int(match.group(3))
                            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                                return datetime(year, month, day)
                        elif match.group(1).isalpha():
                            month = self.get_month_number(match.group(1))
                            day = int(match.group(2))
                            year = int(match.group(3))
                            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                                return datetime(year, month, day)
                    elif len(match.groups()) == 1:
                        dt = self.parse_date_from_string(match.group(1))
                        if dt and dt.year > 2000:
                            return dt
                except:
                    continue
        return None

    def get_month_number(self, month_str: str) -> int:
        months = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
            'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'october': 10, 'oct': 10,
            'november': 11, 'nov': 11, 'december': 12, 'dec': 12
        }
        return months.get(month_str.lower(), 1)

    def extract_date_from_html(self, soup: BeautifulSoup):
        date_selectors = [
            ('meta[property="article:published_time"]', 'content'),
            ('meta[name="article:published_time"]', 'content'),
            ('meta[name="date"]', 'content'),
            ('meta[property="og:article:published_time"]', 'content'),
            ('meta[name="published_date"]', 'content'),
            ('time[datetime]', 'datetime'),
            ('[itemprop="datePublished"]', 'content'),
        ]
        for selector, attr in date_selectors:
            try:
                for elem in soup.select(selector):
                    date_str = elem.get(attr, '')
                    if date_str:
                        parsed = self.parse_date_from_string(date_str)
                        if parsed and parsed.year > 2000:
                            return parsed
            except:
                continue
        return None

    def get_best_published_date(self, url: str, metadata: Dict, article_content: str = "", soup=None) -> str:
        best_date = None
        if 'news_date' in metadata and metadata['news_date']:
            parsed = self.parse_date_from_string(metadata['news_date'])
            if parsed and parsed.year > 2000:
                best_date = parsed
        if not best_date and 'url_date' in metadata and metadata['url_date']:
            parsed = self.parse_date_from_string(metadata['url_date'])
            if parsed:
                best_date = parsed
        if not best_date and 'lastmod' in metadata and metadata['lastmod']:
            parsed = self.parse_date_from_string(metadata['lastmod'])
            if parsed:
                best_date = parsed
        if not best_date and article_content:
            parsed = self.extract_date_from_article_content(article_content)
            if parsed:
                best_date = parsed
        if not best_date and soup:
            html_date = self.extract_date_from_html(soup)
            if html_date:
                best_date = html_date
        if not best_date:
            return ""
        return best_date.strftime("%d %b %Y, %I:%M %p")

    def extract_article_content(self, soup: BeautifulSoup) -> str:
        article_paragraphs = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                article_paragraphs.append(text)
        for selector in ['article', '[class*="article"]', '[class*="content"]', '[class*="post"]', 'main']:
            try:
                for container in soup.select(selector):
                    for p in container.find_all('p'):
                        text = p.get_text(strip=True)
                        if text and len(text) > 20 and text not in article_paragraphs:
                            article_paragraphs.append(text)
            except:
                continue
        if article_paragraphs:
            article_text = '\n\n'.join(article_paragraphs)
            return (article_text[:10000] + "...") if len(article_text) > 10000 else article_text
        return ""

    def extract_title(self, soup: BeautifulSoup, metadata: Dict) -> str:
        if 'news_title' in metadata and metadata['news_title']:
            return metadata['news_title']
        h1 = soup.find('h1')
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)[:500]
        title = soup.find('title')
        if title and title.get_text(strip=True):
            return title.get_text(strip=True)[:500]
        return ""

    async def extract_page_metadata_async(self, session: aiohttp.ClientSession, url: str) -> Dict:
        metadata = {'url': url, 'title': '', 'published_date': '', 'article_content': ''}
        try:
            async with self.semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        url_metadata = self.urls_metadata.get(url, {})
                        metadata['title'] = self.extract_title(soup, url_metadata)
                        metadata['article_content'] = self.extract_article_content(soup)
                        metadata['published_date'] = self.get_best_published_date(
                            url, url_metadata, metadata['article_content'], soup
                        )
        except Exception as e:
            self.signals.log.emit(f"Error extracting from {url[:80]}: {e}", "warning")
        return metadata

    async def process_all_urls_ultrafast(self) -> List[Dict]:
        all_new_urls = [u for u in self.all_urls if u not in self.existing_urls]
        if not all_new_urls:
            self.signals.log.emit("No new URLs to process", "info")
            return []
        urls_to_process = self.select_urls_by_percentage(all_new_urls)
        self.signals.log.emit(f"🚀 Processing {len(urls_to_process)} new URLs ultra-fast...", "info")
        results = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(urls_to_process), self.batch_size):
                batch = urls_to_process[i:i + self.batch_size]
                tasks = [self.extract_page_metadata_async(session, url) for url in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in batch_results:
                    if isinstance(result, dict):
                        results.append(result)
                self.signals.progress_update.emit(i + len(batch), len(urls_to_process), "Extracting metadata")
        return results

    def save_to_database(self, metadata_list: List[Dict]):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            new_count = 0
            for metadata in metadata_list:
                if metadata['article_content']:
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO news_articles
                            (url, title, published_date, article_content, extracted_date)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            metadata['url'],
                            metadata['title'][:500] if metadata['title'] else '',
                            metadata['published_date'] or None,
                            metadata['article_content'],
                            datetime.now().isoformat()
                        ))
                        if cursor.rowcount > 0:
                            new_count += 1
                    except Exception as e:
                        self.signals.log.emit(f"Error saving {metadata['url']} to DB: {e}", "warning")
            conn.commit()
            conn.close()
            if new_count > 0:
                self.signals.log.emit(f"💾 Saved {new_count} articles to database: {self.db_file}", "success")
        except Exception as e:
            self.signals.log.emit(f"Error saving to database: {e}", "error")

    def save_to_excel(self, metadata_list: List[Dict]):
        all_data = []
        if os.path.exists(self.output_file):
            try:
                existing_df = pd.read_excel(self.output_file)
                all_data = existing_df.to_dict('records')
            except:
                pass
        new_count = 0
        for metadata in metadata_list:
            if metadata['url'] not in self.existing_urls:
                all_data.append({
                    'serial': len(all_data) + 1,
                    'url': metadata['url'],
                    'title': metadata.get('title', ''),
                    'published_date': metadata.get('published_date', '')
                })
                new_count += 1
        if all_data:
            df = pd.DataFrame(all_data)[['serial', 'url', 'title', 'published_date']]
            df.to_excel(self.output_file, index=False)
            self.signals.log.emit(f"✅ Saved {new_count} new URLs to Excel: {self.output_file}", "success")
        else:
            self.signals.log.emit("No new data to save to Excel", "info")

    async def extract_all_sitemaps(self):
        self.semaphore = asyncio.Semaphore(50)
        self.init_database()
        self.signals.log.emit(f"🌐 Starting ultra-fast extraction for {self.domain}", "info")
        self.signals.log.emit(f"📊 Processing mode: {self.process_percentage}% of total URLs", "info")
        async with aiohttp.ClientSession() as session:
            robots_content = await self.fetch_robots_txt(session)
            sitemap_list = self.extract_sitemap_from_robots(robots_content)
            if not sitemap_list:
                self.signals.error.emit("No sitemaps found!")
                return
            self.signals.log.emit(f"📑 Processing {len(sitemap_list)} sitemap(s)...", "info")
            for sitemap in sitemap_list:
                await self.crawl_sitemap_tree(session, sitemap)
            self.signals.log.emit(f"📊 Total unique URLs found: {len(self.all_urls)}", "info")
            metadata_results = await self.process_all_urls_ultrafast()
            self.save_to_excel(metadata_results)
            self.save_to_database(metadata_results)
            self.signals.extraction_complete.emit(self.domain, len(metadata_results), len(self.all_urls))


# ══════════════════════════════════════════════════════════════════════════════
#  NORMAL SYNC EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

class NormalSitemapExtractor:
    """Normal synchronous sitemap extractor."""

    def __init__(self, domain: str, output_file: str, db_file: str, signals, process_percentage: float = 100.0):
        self.domain = domain.rstrip('/')
        self.output_file = output_file
        self.db_file = db_file
        self.signals = signals
        self.process_percentage = process_percentage
        self.existing_urls = self.load_existing_urls()
        self.all_urls: Set[str] = set()
        self.urls_metadata: Dict = {}

    def load_existing_urls(self) -> Set[str]:
        if os.path.exists(self.output_file):
            try:
                df = pd.read_excel(self.output_file)
                existing = set(df['url'].tolist())
                self.signals.log.emit(f"Loaded {len(existing)} existing URLs", "info")
                return existing
            except Exception as e:
                self.signals.log.emit(f"Error loading existing file: {e}", "error")
        return set()

    def init_database(self):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    published_date TEXT,
                    article_content TEXT,
                    extracted_date TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON news_articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
            conn.commit()
            conn.close()
            self.signals.log.emit(f"Database initialized: {self.db_file}", "info")
        except Exception as e:
            self.signals.log.emit(f"Error initializing database: {e}", "error")

    def select_urls_by_percentage(self, urls_list: List[str]) -> List[str]:
        if self.process_percentage >= 100.0:
            return urls_list
        urls_to_process = max(1, int((self.process_percentage / 100.0) * len(urls_list)))
        selected = random.sample(urls_list, urls_to_process)
        self.signals.log.emit(
            f"Processing {self.process_percentage}% of total URLs ({urls_to_process}/{len(urls_list)})", "info"
        )
        return selected

    def fetch_robots_txt(self) -> str:
        try:
            response = requests.get(f"{self.domain}/robots.txt", timeout=10)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            self.signals.log.emit(f"Could not fetch robots.txt: {e}", "warning")
        return ""

    def extract_sitemap_from_robots(self, robots_content: str) -> List[str]:
        sitemaps = []
        matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots_content)
        if matches:
            sitemaps.extend(matches)
        else:
            sitemaps.extend([
                f"{self.domain}/sitemap.xml",
                f"{self.domain}/sitemap_index.xml",
                f"{self.domain}/sitemap/sitemap.xml",
                f"{self.domain}/news_sitemap.xml"
            ])
        return list(set(sitemaps))

    def fetch_sitemap(self, sitemap_url: str):
        try:
            response = requests.get(sitemap_url, timeout=15)
            if response.status_code == 200:
                return response.text, sitemap_url
        except Exception as e:
            self.signals.log.emit(f"Error fetching {sitemap_url}: {e}", "warning")
        return "", sitemap_url

    def parse_sitemap_with_metadata(self, content: str):
        urls = set()
        subsitemaps = set()
        url_metadata = {}
        try:
            root = ET.fromstring(content)
            namespaces = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9'
            }
            if root.find('.//ns:sitemap', namespaces) is not None:
                for sitemap_elem in root.findall('.//ns:sitemap', namespaces):
                    loc = sitemap_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        subsitemaps.add(loc.text)
            else:
                for url_elem in root.findall('.//ns:url', namespaces):
                    loc = url_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        url = loc.text
                        urls.add(url)
                        metadata = {}
                        news_date_elem = url_elem.find('.//news:publication_date', namespaces)
                        if news_date_elem is not None and news_date_elem.text:
                            metadata['news_date'] = news_date_elem.text
                        url_metadata[url] = metadata
        except Exception as e:
            self.signals.log.emit(f"Error parsing sitemap: {e}", "error")
        return urls, subsitemaps, url_metadata

    def extract_date_from_url(self, url: str):
        for pattern in [r'/(\d{4})/(\d{1,2})/(\d{1,2})/', r'/(\d{4})-(\d{1,2})-(\d{1,2})/']:
            match = re.search(pattern, url)
            if match:
                try:
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year}-{month:02d}-{day:02d}"
                except:
                    continue
        return None

    def parse_date_from_string(self, date_str: str):
        try:
            return date_parser.parse(date_str)
        except:
            return None

    def extract_date_from_article_content(self, article_content: str):
        if not article_content:
            return None
        patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{2})/(\d{2})/(\d{4})',
        ]
        months = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        for pattern in patterns:
            match = re.search(pattern, article_content, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if match.group(1).isdigit() and not match.group(2).isdigit():
                            day = int(match.group(1))
                            month = months.get(match.group(2).lower(), 1)
                            year = int(match.group(3))
                            return datetime(year, month, day)
                        elif match.group(1).isdigit() and match.group(2).isdigit():
                            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                except:
                    continue
        return None

    def get_best_published_date(self, url: str, metadata: Dict, article_content: str = "") -> str:
        best_date = None
        if 'news_date' in metadata and metadata['news_date']:
            dt = self.parse_date_from_string(metadata['news_date'])
            if dt and dt.year > 2000:
                best_date = dt
        if not best_date:
            date_from_url = self.extract_date_from_url(url)
            if date_from_url:
                dt = self.parse_date_from_string(date_from_url)
                if dt:
                    best_date = dt
        if not best_date and article_content:
            dt = self.extract_date_from_article_content(article_content)
            if dt:
                best_date = dt
        return best_date.strftime("%d %b %Y, %I:%M %p") if best_date else ""

    def extract_article_content(self, soup: BeautifulSoup) -> str:
        article_paragraphs = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                article_paragraphs.append(text)
        if article_paragraphs:
            article_text = '\n\n'.join(article_paragraphs)
            return (article_text[:10000] + "...") if len(article_text) > 10000 else article_text
        return ""

    def extract_title(self, soup: BeautifulSoup) -> str:
        h1 = soup.find('h1')
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)[:500]
        title = soup.find('title')
        if title and title.get_text(strip=True):
            return title.get_text(strip=True)[:500]
        return ""

    def extract_page_metadata(self, url: str) -> Dict:
        metadata = {'url': url, 'title': '', 'published_date': '', 'article_content': ''}
        try:
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                metadata['title'] = self.extract_title(soup)
                metadata['article_content'] = self.extract_article_content(soup)
                url_metadata = self.urls_metadata.get(url, {})
                metadata['published_date'] = self.get_best_published_date(url, url_metadata, metadata['article_content'])
        except:
            pass
        return metadata

    def crawl_sitemap_tree(self, sitemap_url: str):
        visited_sitemaps = set()
        to_visit = [sitemap_url]
        while to_visit:
            current = to_visit.pop(0)
            if current in visited_sitemaps:
                continue
            visited_sitemaps.add(current)
            self.signals.progress_update.emit(
                len(visited_sitemaps), len(to_visit) + len(visited_sitemaps), "Processing sitemaps"
            )
            content, _ = self.fetch_sitemap(current)
            if not content:
                continue
            urls, subsitemaps, url_metadata = self.parse_sitemap_with_metadata(content)
            for url in urls:
                if url not in self.existing_urls:
                    self.all_urls.add(url)
                    if url in url_metadata:
                        self.urls_metadata[url] = url_metadata[url]
            for submap in subsitemaps:
                if submap not in visited_sitemaps:
                    to_visit.append(submap)

    def process_all_urls(self) -> List[Dict]:
        all_new_urls = [u for u in self.all_urls if u not in self.existing_urls]
        if not all_new_urls:
            return []
        urls_to_process = self.select_urls_by_percentage(all_new_urls)
        self.signals.log.emit(f"Processing {len(urls_to_process)} URLs...", "info")
        results = []
        for i, url in enumerate(urls_to_process):
            results.append(self.extract_page_metadata(url))
            self.signals.progress_update.emit(i + 1, len(urls_to_process), "Extracting metadata")
        return results

    def save_to_excel(self, metadata_list: List[Dict]):
        all_data = []
        if os.path.exists(self.output_file):
            try:
                all_data = pd.read_excel(self.output_file).to_dict('records')
            except:
                pass
        for metadata in metadata_list:
            if metadata['url'] not in self.existing_urls:
                all_data.append({
                    'serial': len(all_data) + 1,
                    'url': metadata['url'],
                    'title': metadata.get('title', ''),
                    'published_date': metadata.get('published_date', '')
                })
        if all_data:
            df = pd.DataFrame(all_data)[['serial', 'url', 'title', 'published_date']]
            df.to_excel(self.output_file, index=False)
            self.signals.log.emit(f"Data saved to {self.output_file}", "success")

    def save_to_database(self, metadata_list: List[Dict]):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            for metadata in metadata_list:
                if metadata['article_content']:
                    cursor.execute('''
                        INSERT OR IGNORE INTO news_articles
                        (url, title, published_date, article_content, extracted_date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        metadata['url'],
                        metadata['title'][:500] if metadata['title'] else '',
                        metadata['published_date'] or None,
                        metadata['article_content'],
                        datetime.now().isoformat()
                    ))
            conn.commit()
            conn.close()
            self.signals.log.emit(f"Data saved to database: {self.db_file}", "success")
        except Exception as e:
            self.signals.log.emit(f"Error saving to database: {e}", "error")

    def extract_all_sitemaps(self):
        try:
            self.init_database()
            self.signals.log.emit(f"Starting normal extraction for {self.domain}", "info")
            self.signals.log.emit(f"Processing mode: {self.process_percentage}% of total URLs", "info")
            robots_content = self.fetch_robots_txt()
            sitemap_list = self.extract_sitemap_from_robots(robots_content)
            if not sitemap_list:
                self.signals.error.emit("No sitemaps found!")
                return
            self.signals.log.emit(f"Processing {len(sitemap_list)} sitemap(s)...", "info")
            for sitemap in sitemap_list:
                self.crawl_sitemap_tree(sitemap)
            self.signals.log.emit(f"Total unique URLs found: {len(self.all_urls)}", "info")
            metadata_results = self.process_all_urls()
            self.save_to_excel(metadata_results)
            self.save_to_database(metadata_results)
            self.signals.extraction_complete.emit(self.domain, len(metadata_results), len(self.all_urls))
        except Exception as e:
            self.signals.error.emit(f"Extraction failed: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
#  HEADLESS RUNNER  –  used by GitHub Actions (no GUI needed)
# ══════════════════════════════════════════════════════════════════════════════

def generate_filename_from_url(url: str, ext: str) -> str:
    """Auto-generate a safe filename from a domain URL."""
    domain_name = re.sub(r'^https?://(www\.)?', '', url)
    domain_name = re.sub(r'\.[^.]+$', '', domain_name)
    domain_name = re.sub(r'[^a-zA-Z0-9]', '_', domain_name)
    return f"{domain_name}{ext}"


def load_sites_from_file(filepath: str) -> List[Dict]:
    """Read sites.txt and return list of site dicts."""
    sites = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        for url in lines:
            if validators.url(url):
                sites.append({
                    'domain': url,
                    'output_file': generate_filename_from_url(url, '.xlsx'),
                    'db_file': generate_filename_from_url(url, '.db'),
                })
            else:
                logger.warning(f"Skipping invalid URL: {url}")
    except FileNotFoundError:
        logger.error(f"Sites file not found: {filepath}")
    return sites


def run_headless(sites_file: str, use_async: bool, percent: float):
    """
    Main headless entry point.
    Called when --headless flag is present (e.g. from GitHub Actions).
    """
    print("=" * 65, flush=True)
    print("  🤖  Sitemap Extractor — HEADLESS / GitHub Actions Mode", flush=True)
    bdt = pytz.timezone('Asia/Dhaka')
    now_bdt = datetime.now(bdt).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"  🕐  Run time (Bangladesh): {now_bdt}", flush=True)
    print(f"  📂  Sites file          : {sites_file}", flush=True)
    print(f"  ⚡  Async mode          : {use_async}", flush=True)
    print(f"  📊  Process percentage  : {percent}%", flush=True)
    print("=" * 65, flush=True)

    sites = load_sites_from_file(sites_file)
    if not sites:
        print("❌ No valid sites found. Exiting.", flush=True)
        sys.exit(1)

    print(f"\n✅ Loaded {len(sites)} site(s) to process\n", flush=True)

    signals = HeadlessSignals()
    success_count = 0
    fail_count = 0

    for site in sites:
        domain = site['domain']
        output_file = site['output_file']
        db_file = site['db_file']

        print(f"\n{'─'*60}", flush=True)
        print(f"🌐 Processing: {domain}", flush=True)
        print(f"📁 Excel: {output_file}  |  🗄️ DB: {db_file}", flush=True)
        print(f"{'─'*60}", flush=True)

        try:
            if use_async:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    extractor = UltraFastSitemapExtractor(domain, output_file, db_file, signals, percent)
                    loop.run_until_complete(extractor.extract_all_sitemaps())
                finally:
                    loop.close()
            else:
                extractor = NormalSitemapExtractor(domain, output_file, db_file, signals, percent)
                extractor.extract_all_sitemaps()

            signals.site_complete.emit(domain, True)
            success_count += 1

        except Exception as e:
            signals.site_complete.emit(domain, False)
            signals.error.emit(f"Failed to process {domain}: {e}")
            fail_count += 1

    print(f"\n{'='*65}", flush=True)
    print(f"🎉 All done!  ✅ Success: {success_count}  ❌ Failed: {fail_count}", flush=True)
    print(f"{'='*65}", flush=True)

    if fail_count > 0:
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
#  GUI MODE  –  only imported / launched when NOT headless
# ══════════════════════════════════════════════════════════════════════════════

def run_gui():
    """Launch the full PyQt6 GUI application."""
    # Import Qt only in GUI mode so headless runs don't need a display
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QPushButton, QLineEdit, QTextEdit,
        QCheckBox, QDoubleSpinBox, QSlider, QProgressBar, QTableWidget,
        QTableWidgetItem, QTabWidget, QDialog, QFileDialog, QMessageBox,
        QGroupBox
    )
    from PyQt6.QtCore import Qt, QRunnable, QThreadPool, QObject, pyqtSignal, pyqtSlot
    from PyQt6.QtGui import QFont
    import qasync

    # ── Qt Signals ────────────────────────────────────────────────────────────
    class WorkerSignals(QObject):
        finished = pyqtSignal()
        error = pyqtSignal(str)
        progress_update = pyqtSignal(int, int, str)
        log = pyqtSignal(str, str)
        extraction_complete = pyqtSignal(str, int, int)
        site_complete = pyqtSignal(str, bool)

    # ── Worker thread ─────────────────────────────────────────────────────────
    class SitemapWorker(QRunnable):
        def __init__(self, sites, use_async, process_percentage):
            super().__init__()
            self.sites = sites
            self.use_async = use_async
            self.process_percentage = process_percentage
            self.signals = WorkerSignals()
            self.is_running = True
            self.setAutoDelete(True)

        @pyqtSlot()
        def run(self):
            try:
                for site in self.sites:
                    if not self.is_running:
                        break
                    domain = site['domain']
                    output_file = site['output_file']
                    db_file = site['db_file']
                    self.signals.log.emit(f"\n{'='*60}", "info")
                    self.signals.log.emit(f"🌐 Processing site: {domain}", "info")
                    self.signals.log.emit(f"📁 Excel: {output_file}", "info")
                    self.signals.log.emit(f"🗄️ Database: {db_file}", "info")
                    try:
                        if self.use_async:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                extractor = UltraFastSitemapExtractor(
                                    domain, output_file, db_file, self.signals, self.process_percentage
                                )
                                loop.run_until_complete(extractor.extract_all_sitemaps())
                            finally:
                                loop.close()
                        else:
                            extractor = NormalSitemapExtractor(
                                domain, output_file, db_file, self.signals, self.process_percentage
                            )
                            extractor.extract_all_sitemaps()
                        self.signals.site_complete.emit(domain, True)
                    except Exception as e:
                        self.signals.site_complete.emit(domain, False)
                        self.signals.error.emit(f"Failed to process {domain}: {str(e)}")
            except Exception as e:
                self.signals.error.emit(str(e))
            finally:
                self.signals.finished.emit()

    # ── Add Sites Dialog ──────────────────────────────────────────────────────
    class AddSitesDialog(QDialog):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setWindowTitle("Add Websites")
            self.setModal(True)
            self.setMinimumSize(600, 480)
            layout = QVBoxLayout()
            from PyQt6.QtWidgets import QTabWidget
            tab_widget = QTabWidget()

            # Single site
            single_tab = QWidget()
            sl = QVBoxLayout(single_tab)
            sl.addWidget(QLabel("Enter website URL:"))
            self.single_url = QLineEdit()
            self.single_url.setPlaceholderText("https://example.com")
            sl.addWidget(self.single_url)
            self.single_custom = QCheckBox("Use custom file naming")
            sl.addWidget(self.single_custom)
            xl = QHBoxLayout()
            xl.addWidget(QLabel("Excel file:"))
            self.single_excel = QLineEdit()
            self.single_excel.setEnabled(False)
            xl.addWidget(self.single_excel)
            sl.addLayout(xl)
            dl = QHBoxLayout()
            dl.addWidget(QLabel("Database file:"))
            self.single_db = QLineEdit()
            self.single_db.setEnabled(False)
            dl.addWidget(self.single_db)
            sl.addLayout(dl)
            self.single_custom.toggled.connect(self.single_excel.setEnabled)
            self.single_custom.toggled.connect(self.single_db.setEnabled)
            sl.addStretch()
            tab_widget.addTab(single_tab, "Single Site")

            # Multiple sites
            multi_tab = QWidget()
            ml = QVBoxLayout(multi_tab)
            ml.addWidget(QLabel("Enter multiple websites (one per line):"))
            self.multi_urls = QTextEdit()
            self.multi_urls.setPlaceholderText("https://example1.com\nhttps://example2.com")
            self.multi_urls.setMaximumHeight(140)
            ml.addWidget(self.multi_urls)
            import_btn = QPushButton("📂 Import URLs from Text File")
            import_btn.clicked.connect(self.import_from_file)
            ml.addWidget(import_btn)
            ml.addStretch()
            tab_widget.addTab(multi_tab, "Multiple Sites")
            layout.addWidget(tab_widget)

            btn_layout = QHBoxLayout()
            ok_btn = QPushButton("Add Sites")
            ok_btn.clicked.connect(self.accept)
            cancel_btn = QPushButton("Cancel")
            cancel_btn.clicked.connect(self.reject)
            btn_layout.addWidget(ok_btn)
            btn_layout.addWidget(cancel_btn)
            layout.addLayout(btn_layout)
            self.setLayout(layout)

        def import_from_file(self):
            path, _ = QFileDialog.getOpenFileName(self, "Select Text File", "", "Text Files (*.txt)")
            if path:
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        self.multi_urls.setText(f.read())
                except Exception as e:
                    QMessageBox.critical(self, "Error", str(e))

        def get_sites(self):
            sites = []
            raw = self.multi_urls.toPlainText().strip()
            urls = [u.strip() for u in raw.split('\n') if u.strip()] if raw else []
            if not urls:
                url = self.single_url.text().strip()
                if url:
                    urls = [url]
            for url in urls:
                if validators.url(url):
                    excel_file = generate_filename_from_url(url, '.xlsx')
                    db_file = generate_filename_from_url(url, '.db')
                    sites.append({'domain': url, 'output_file': excel_file, 'db_file': db_file})
                elif url:
                    QMessageBox.warning(self, "Invalid URL", f"'{url}' is not a valid URL")
            return sites

    # ── Site list widget ──────────────────────────────────────────────────────
    class SiteListWidget(QWidget):
        def __init__(self):
            super().__init__()
            self.sites = []
            layout = QVBoxLayout()
            self.table = QTableWidget()
            self.table.setColumnCount(4)
            self.table.setHorizontalHeaderLabels(["Website URL", "Excel File", "DB File", "Status"])
            self.table.horizontalHeader().setStretchLastSection(True)
            layout.addWidget(self.table)
            btn_layout = QHBoxLayout()
            for label, slot in [("➕ Add Site(s)", self.add_sites),
                                  ("❌ Remove Selected", self.remove_site),
                                  ("🗑 Clear All", self.clear_all)]:
                btn = QPushButton(label)
                btn.clicked.connect(slot)
                btn_layout.addWidget(btn)
            btn_layout.addStretch()
            layout.addLayout(btn_layout)
            self.setLayout(layout)

        def add_sites(self):
            dialog = AddSitesDialog(self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                new_sites = dialog.get_sites()
                existing_domains = {s['domain'] for s in self.sites}
                added = 0
                for site in new_sites:
                    if site['domain'] not in existing_domains:
                        self.sites.append(site)
                        row = self.table.rowCount()
                        self.table.insertRow(row)
                        self.table.setItem(row, 0, QTableWidgetItem(site['domain']))
                        self.table.setItem(row, 1, QTableWidgetItem(site['output_file']))
                        self.table.setItem(row, 2, QTableWidgetItem(site['db_file']))
                        self.table.setItem(row, 3, QTableWidgetItem("Pending"))
                        added += 1
                if added:
                    QMessageBox.information(self, "Success", f"Added {added} site(s)")

        def remove_site(self):
            row = self.table.currentRow()
            if row >= 0:
                self.table.removeRow(row)
                del self.sites[row]

        def clear_all(self):
            if QMessageBox.question(self, "Confirm", "Clear all sites?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
                self.table.setRowCount(0)
                self.sites.clear()

        def update_site_status(self, domain, status):
            for row in range(self.table.rowCount()):
                if self.table.item(row, 0).text() == domain:
                    self.table.setItem(row, 3, QTableWidgetItem(status))
                    break

        def get_sites(self):
            return self.sites

    # ── Main Window ───────────────────────────────────────────────────────────
    class MainWindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.threadpool = QThreadPool()
            self.worker = None
            self._build_ui()

        def _build_ui(self):
            self.setWindowTitle("🚀 Advanced Sitemap Extractor Pro — Multi-Site Edition")
            self.setMinimumSize(1300, 950)
            self.setStyleSheet("""
                QMainWindow { background-color: #f0f0f0; }
                QGroupBox { font-weight: bold; border: 2px solid #4CAF50; border-radius: 8px;
                            margin-top: 12px; padding-top: 10px; background-color: white; }
                QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 8px; color: #4CAF50; }
                QPushButton { background-color: #4CAF50; color: white; border: none;
                              padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: bold; }
                QPushButton:hover { background-color: #45a049; }
                QPushButton:disabled { background-color: #cccccc; }
                QPushButton#stopBtn { background-color: #f44336; }
                QPushButton#stopBtn:hover { background-color: #da190b; }
                QLineEdit, QDoubleSpinBox, QTextEdit { border: 1px solid #ccc; border-radius: 4px;
                                                        padding: 6px; font-size: 12px; }
                QProgressBar { border: 1px solid #ccc; border-radius: 5px; text-align: center; }
                QProgressBar::chunk { background-color: #4CAF50; border-radius: 4px; }
                QTableWidget { border: 1px solid #ccc; border-radius: 4px; alternate-background-color: #f9f9f9; }
                QHeaderView::section { background-color: #4CAF50; color: white; padding: 5px; border: none; }
            """)

            central = QWidget()
            self.setCentralWidget(central)
            main_layout = QVBoxLayout(central)
            main_layout.setSpacing(10)

            # Header
            header = QWidget()
            hl = QHBoxLayout(header)
            title_lbl = QLabel("📰 Advanced Sitemap Extractor Pro — Multi-Site Edition")
            title_lbl.setStyleSheet("font-size: 22px; font-weight: bold; color: #4CAF50;")
            hl.addWidget(title_lbl)
            hl.addWidget(QLabel("v5.2"))
            hl.addStretch()
            main_layout.addWidget(header)

            # Sites
            sites_group = QGroupBox("🌐 Websites to Extract")
            sg = QVBoxLayout()
            self.site_list = SiteListWidget()
            sg.addWidget(self.site_list)
            sites_group.setLayout(sg)
            main_layout.addWidget(sites_group)

            # Settings
            settings_group = QGroupBox("⚙️ Extraction Settings")
            settings_layout = QGridLayout()
            self.async_checkbox = QCheckBox("⚡ Enable Ultra-Fast Mode (Recommended for large sites)")
            self.async_checkbox.setChecked(False)
            settings_layout.addWidget(self.async_checkbox, 0, 0, 1, 2)
            note = QLabel("⚠️ Ultra-fast mode uses 50 concurrent connections — much faster but uses more resources")
            note.setStyleSheet("color: #666; font-size: 10px; font-style: italic;")
            settings_layout.addWidget(note, 1, 0, 1, 2)
            settings_layout.addWidget(QLabel("Processing Range:"), 2, 0)
            pct_layout = QHBoxLayout()
            self.percentage_spinbox = QDoubleSpinBox()
            self.percentage_spinbox.setRange(0.01, 100.0)
            self.percentage_spinbox.setValue(100.0)
            self.percentage_spinbox.setSuffix("%")
            self.percentage_spinbox.setDecimals(2)
            pct_layout.addWidget(self.percentage_spinbox)
            self.percentage_slider = QSlider(Qt.Orientation.Horizontal)
            self.percentage_slider.setRange(0, 10000)
            self.percentage_slider.setValue(10000)
            self.percentage_slider.valueChanged.connect(lambda v: self.percentage_spinbox.setValue(v / 100.0))
            pct_layout.addWidget(self.percentage_slider)
            settings_layout.addLayout(pct_layout, 2, 1)
            preset_layout = QHBoxLayout()
            preset_layout.addWidget(QLabel("Presets:"))
            for p in [0.01, 0.1, 0.5, 1, 2, 5, 10, 25, 50, 100]:
                btn = QPushButton(f"{p}%")
                btn.setMaximumWidth(60)
                btn.clicked.connect(lambda checked, pct=p: self.percentage_spinbox.setValue(pct))
                preset_layout.addWidget(btn)
            preset_layout.addStretch()
            settings_layout.addLayout(preset_layout, 3, 0, 1, 2)
            settings_group.setLayout(settings_layout)
            main_layout.addWidget(settings_group)

            # Buttons
            btn_row = QHBoxLayout()
            self.start_btn = QPushButton("▶ START EXTRACTION FOR ALL SITES")
            self.start_btn.clicked.connect(self.start_extraction)
            self.start_btn.setMinimumHeight(40)
            self.stop_btn = QPushButton("⏹ STOP")
            self.stop_btn.setObjectName("stopBtn")
            self.stop_btn.clicked.connect(self.stop_extraction)
            self.stop_btn.setEnabled(False)
            self.stop_btn.setMinimumHeight(40)
            self.clear_btn = QPushButton("🗑 Clear Logs")
            self.clear_btn.clicked.connect(lambda: self.log_text.clear())
            self.clear_btn.setMinimumHeight(40)
            for b in [self.start_btn, self.stop_btn, self.clear_btn]:
                btn_row.addWidget(b)
            btn_row.addStretch()
            main_layout.addLayout(btn_row)

            # Progress
            prog_group = QGroupBox("📊 Overall Progress")
            pl = QVBoxLayout()
            self.progress_bar = QProgressBar()
            self.progress_bar.setMinimumHeight(25)
            pl.addWidget(self.progress_bar)
            self.status_label = QLabel("✅ Ready to start extraction")
            self.status_label.setStyleSheet("color: #666; padding: 5px;")
            pl.addWidget(self.status_label)
            prog_group.setLayout(pl)
            main_layout.addWidget(prog_group)

            # Tabs
            tabs = QTabWidget()
            log_w = QWidget()
            ll = QVBoxLayout(log_w)
            self.log_text = QTextEdit()
            self.log_text.setReadOnly(True)
            self.log_text.setMaximumHeight(280)
            self.log_text.setStyleSheet("font-family: 'Consolas', monospace; font-size: 11px;")
            ll.addWidget(self.log_text)
            tabs.addTab(log_w, "📝 Extraction Log")

            results_w = QWidget()
            rl = QVBoxLayout(results_w)
            self.results_table = QTableWidget()
            self.results_table.setColumnCount(5)
            self.results_table.setHorizontalHeaderLabels(["Site", "Excel File", "DB File", "URLs Extracted", "Status"])
            self.results_table.horizontalHeader().setStretchLastSection(True)
            self.results_table.setAlternatingRowColors(True)
            rl.addWidget(self.results_table)
            tabs.addTab(results_w, "📊 Extraction Summary")
            main_layout.addWidget(tabs)

            self.statusBar().showMessage("Ready")
            self.add_log("🎉 Welcome to Advanced Sitemap Extractor Pro — Multi-Site Edition!", "success")
            self.add_log("ℹ️ Add sites using the ➕ button, then click START.", "info")

        def start_extraction(self):
            sites = self.site_list.get_sites()
            if not sites:
                QMessageBox.warning(self, "Warning", "Please add at least one website.")
                return
            use_async = self.async_checkbox.isChecked()
            pct = self.percentage_spinbox.value()
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.progress_bar.setValue(0)
            self.results_table.setRowCount(0)
            for site in sites:
                row = self.results_table.rowCount()
                self.results_table.insertRow(row)
                for col, key in enumerate(['domain', 'output_file', 'db_file']):
                    self.results_table.setItem(row, col, QTableWidgetItem(site[key]))
                self.results_table.setItem(row, 3, QTableWidgetItem("Pending"))
                self.results_table.setItem(row, 4, QTableWidgetItem("⏳ Queued"))
            self.worker = SitemapWorker(sites, use_async, pct)
            self.worker.signals.finished.connect(self.extraction_finished)
            self.worker.signals.error.connect(lambda m: self.add_log(f"❌ {m}", "error"))
            self.worker.signals.log.connect(self.add_log)
            self.worker.signals.progress_update.connect(self.update_progress)
            self.worker.signals.extraction_complete.connect(self.on_extraction_complete)
            self.worker.signals.site_complete.connect(self.on_site_complete)
            self.threadpool.start(self.worker)
            self.add_log(f"🎯 Starting extraction for {len(sites)} site(s) | {pct}% | {'Async' if use_async else 'Sync'}", "info")

        def on_site_complete(self, domain, success):
            status = "✅ Completed" if success else "❌ Failed"
            for row in range(self.results_table.rowCount()):
                if self.results_table.item(row, 0).text() == domain:
                    self.results_table.setItem(row, 4, QTableWidgetItem(status))
                    break
            self.site_list.update_site_status(domain, status)

        def on_extraction_complete(self, domain, new_urls, total_urls):
            for row in range(self.results_table.rowCount()):
                if self.results_table.item(row, 0).text() == domain:
                    self.results_table.setItem(row, 3, QTableWidgetItem(f"{new_urls}/{total_urls}"))
                    break
            self.add_log(f"✅ {domain}: {new_urls} new URLs (total {total_urls})", "success")

        def stop_extraction(self):
            if self.worker:
                self.worker.is_running = False
                self.stop_btn.setEnabled(False)
                self.add_log("⏸️ Stopping...", "warning")

        def extraction_finished(self):
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.add_log("🎉 All sites extraction completed!", "success")
            QMessageBox.information(self, "Done", "Extraction completed!\nSee the Summary tab for details.")

        def add_log(self, message, level="info"):
            ts = datetime.now().strftime("%H:%M:%S")
            colors = {"info": "#000", "warning": "#FFA500", "error": "#FF0000", "success": "#008000"}
            icons = {"info": "ℹ️", "warning": "⚠️", "error": "❌", "success": "✅"}
            color = colors.get(level, "#000")
            icon = icons.get(level, "ℹ️")
            self.log_text.append(f'<span style="color:{color};">[{ts}] {icon} {message}</span>')
            self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())
            self.statusBar().showMessage(message[:100])

        def update_progress(self, current, total, description):
            if total > 0:
                pct = int((current / total) * 100)
                self.progress_bar.setValue(pct)
                self.status_label.setText(f"{description}: {current}/{total} ({pct}%)")

    # ── Launch ─────────────────────────────────────────────────────────────────
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)
    window = MainWindow()
    window.show()
    with loop:
        sys.exit(loop.run_forever())


# ══════════════════════════════════════════════════════════════════════════════
#  GITHUB ACTIONS WORKFLOW  –  written to disk on first run if missing
# ══════════════════════════════════════════════════════════════════════════════

WORKFLOW_YAML = """\
# .github/workflows/daily_extraction.yml
# Auto-generated by sitemap_extractor.py
# Runs every day at 06:00 AM Bangladesh Standard Time (= 00:00 UTC)

name: Daily Sitemap Extraction

on:
  schedule:
    - cron: '0 0 * * *'   # 00:00 UTC = 06:00 AM BST (UTC+6)
  workflow_dispatch:        # Allow manual trigger from GitHub UI

jobs:
  extract:
    runs-on: ubuntu-latest
    timeout-minutes: 120

    permissions:
      contents: write       # Needed to commit output files back to repo

    steps:
      # ── 1. Checkout the repository ─────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      # ── 2. Set up Python ───────────────────────────────────────────────────
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      # ── 3. Install system dependencies (virtual display for PyQt6) ─────────
      - name: Install system dependencies
        run: |
          sudo apt-get update -qq
          sudo apt-get install -y xvfb libxkbcommon-x11-0 libxcb-icccm4 \\
            libxcb-image0 libxcb-keysyms1 libxcb-randr0 libxcb-render-util0 \\
            libxcb-xinerama0 libxcb-xfixes0 libgl1-mesa-glx libegl1

      # ── 4. Install Python packages ─────────────────────────────────────────
      - name: Install Python dependencies
        run: |
          pip install --upgrade pip
          pip install \\
            aiohttp \\
            pandas \\
            openpyxl \\
            beautifulsoup4 \\
            python-dateutil \\
            PyQt6 \\
            qasync \\
            requests \\
            validators \\
            lxml \\
            pytz \\
            tqdm

      # ── 5. Show Bangladesh time (for log readability) ──────────────────────
      - name: Show Bangladesh time
        run: |
          python -c "
          from datetime import datetime
          import pytz
          bdt = pytz.timezone('Asia/Dhaka')
          print('Run time (BD):', datetime.now(bdt).strftime('%Y-%m-%d %H:%M:%S %Z'))
          "

      # ── 6. Run the extractor (headless, async, 100% URLs) ──────────────────
      - name: Run Sitemap Extractor
        run: |
          xvfb-run --auto-servernum --server-args="-screen 0 1024x768x24" \\
            python sitemap_extractor.py \\
              --headless \\
              --async-mode \\
              --percent 100 \\
              --sites sites.txt
        env:
          DISPLAY: ':99'
          QT_QPA_PLATFORM: offscreen

      # ── 7. Commit output files back to the repository ──────────────────────
      - name: Commit output files
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "GitHub Actions Bot"
          git add *.xlsx *.db || true
          git diff --staged --quiet || git commit -m "chore: auto-update extraction data $(date +'%Y-%m-%d %H:%M UTC')"
          git push
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      # ── 8. Upload as artifact (downloadable for 30 days) ───────────────────
      - name: Upload extraction artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: extraction-results-${{ github.run_number }}
          path: |
            *.xlsx
            *.db
          retention-days: 30
"""

SITES_TXT_TEMPLATE = """\
# sites.txt
# Add one website URL per line.
# Lines starting with # are comments and are ignored.
# The script auto-generates Excel and SQLite filenames from the domain.
#
# Example:
# https://www.prothomalo.com
# https://www.thedailystar.net
# https://www.bdnews24.com

https://www.example.com
"""


def maybe_create_github_files():
    """Create .github/workflows/daily_extraction.yml and sites.txt if they don't exist."""
    workflow_dir = Path(".github/workflows")
    workflow_file = workflow_dir / "daily_extraction.yml"

    if not workflow_file.exists():
        workflow_dir.mkdir(parents=True, exist_ok=True)
        workflow_file.write_text(WORKFLOW_YAML, encoding='utf-8')
        print(f"✅ Created: {workflow_file}", flush=True)

    sites_file = Path("sites.txt")
    if not sites_file.exists():
        sites_file.write_text(SITES_TXT_TEMPLATE, encoding='utf-8')
        print(f"✅ Created: sites.txt  ← Add your website URLs here!", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Sitemap Extractor — GUI or Headless (GitHub Actions) mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  GUI mode (default):
      python sitemap_extractor.py

  Headless / GitHub Actions mode:
      python sitemap_extractor.py --headless
      python sitemap_extractor.py --headless --sites sites.txt --async-mode --percent 50

  Create GitHub Actions workflow files:
      python sitemap_extractor.py --setup
        """
    )
    parser.add_argument('--headless', action='store_true',
                        help='Run without GUI (for GitHub Actions / cron)')
    parser.add_argument('--setup', action='store_true',
                        help='Create .github/workflows/daily_extraction.yml and sites.txt then exit')
    parser.add_argument('--sites', default='sites.txt',
                        help='Path to file containing website URLs (default: sites.txt)')
    parser.add_argument('--async-mode', action='store_true',
                        help='Use ultra-fast async extraction (50 concurrent connections)')
    parser.add_argument('--percent', type=float, default=100.0,
                        help='Percentage of URLs to process per site, e.g. 50 (default: 100)')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.setup:
        maybe_create_github_files()
        print("\n✅ Setup complete!")
        print("📝 Next steps:")
        print("   1. Edit  sites.txt  and add your website URLs")
        print("   2. Push everything to GitHub")
        print("   3. Go to GitHub → Actions tab → Enable workflows")
        print("   4. It will run automatically every day at 6:00 AM Bangladesh time")
        sys.exit(0)

    if args.headless:
        # Always generate workflow files if they're missing (handy on first run)
        maybe_create_github_files()
        run_headless(
            sites_file=args.sites,
            use_async=args.async_mode,
            percent=args.percent
        )
    else:
        run_gui()
