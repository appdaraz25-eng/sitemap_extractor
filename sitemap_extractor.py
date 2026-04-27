#!/usr/bin/env python3
"""
Headless Sitemap Extractor - Automatically runs daily at 6 AM Bangladesh Time
No GUI - For GitHub Actions / Cron jobs
"""

import sys
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import pandas as pd
from datetime import datetime, time
import pytz
from tqdm.asyncio import tqdm_asyncio
from typing import List, Set, Dict
import os
import re
import logging
import requests
import random
import sqlite3
from dateutil import parser as date_parser
import validators
import json
import schedule
import time as time_module
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sitemap_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class UltraFastSitemapExtractor:
    """Ultra-fast asynchronous sitemap extractor - No GUI dependencies"""
    
    def __init__(self, domain: str, output_file: str, db_file: str, process_percentage: float = 100.0, log_callback=None):
        self.domain = domain.rstrip('/')
        self.output_file = output_file
        self.db_file = db_file
        self.process_percentage = process_percentage
        self.existing_urls = self.load_existing_urls()
        self.all_urls = set()
        self.urls_metadata = {}
        self.batch_size = 50
        self.semaphore = None
        self.log_callback = log_callback
        self.total_urls_found = 0
        self.new_urls_extracted = 0
        
    def log(self, message: str, level: str = "info"):
        """Log message to both file and callback"""
        logger.log(
            logging.INFO if level != "error" else logging.ERROR,
            f"[{self.domain}] {message}"
        )
        if self.log_callback:
            self.log_callback(self.domain, message, level)
        
    def load_existing_urls(self) -> Set[str]:
        """Load existing URLs from Excel file"""
        if os.path.exists(self.output_file):
            try:
                df = pd.read_excel(self.output_file)
                existing = set(df['url'].tolist())
                self.log(f"Loaded {len(existing)} existing URLs from Excel", "info")
                return existing
            except Exception as e:
                self.log(f"Error loading existing file: {e}", "error")
                return set()
        return set()
    
    def init_database(self):
        """Initialize SQLite database for news articles"""
        try:
            os.makedirs(os.path.dirname(self.db_file) if os.path.dirname(self.db_file) else '.', exist_ok=True)
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
            self.log(f"Database initialized: {self.db_file}", "info")
        except Exception as e:
            self.log(f"Error initializing database: {e}", "error")
    
    async def fetch_robots_txt(self, session: aiohttp.ClientSession) -> str:
        """Fetch robots.txt from domain"""
        robots_url = f"{self.domain}/robots.txt"
        try:
            async with session.get(robots_url, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            self.log(f"Could not fetch robots.txt: {e}", "warning")
        return ""
    
    def extract_sitemap_from_robots(self, robots_content: str) -> List[str]:
        """Extract sitemap URLs from robots.txt"""
        sitemaps = []
        pattern = r'[Ss]itemap:\s*(https?://\S+)'
        matches = re.findall(pattern, robots_content)
        
        if matches:
            sitemaps.extend(matches)
            self.log(f"Found {len(matches)} sitemap(s) in robots.txt", "info")
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
            self.log("No sitemaps in robots.txt, trying common locations", "info")
            
        return list(set(sitemaps))
    
    async def fetch_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str) -> tuple[str, str]:
        """Fetch sitemap XML content"""
        try:
            async with self.semaphore:
                async with session.get(sitemap_url, timeout=15) as response:
                    if response.status == 200:
                        content = await response.text()
                        return content, sitemap_url
                    else:
                        self.log(f"Failed to fetch {sitemap_url}: HTTP {response.status}", "warning")
        except Exception as e:
            self.log(f"Error fetching {sitemap_url}: {e}", "warning")
        return "", sitemap_url
    
    def parse_sitemap_with_metadata(self, content: str, sitemap_url: str) -> tuple[Set[str], Set[str], Dict]:
        """Parse sitemap XML and extract URLs with metadata"""
        urls = set()
        subsitemaps = set()
        url_metadata = {}
        
        try:
            root = ET.fromstring(content)
            
            namespaces = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9',
                'image': 'http://www.google.com/schemas/sitemap-image/1.1',
                'video': 'http://www.google.com/schemas/sitemap-video/1.1'
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
            self.log(f"Error parsing sitemap XML: {e}", "error")
            
        return urls, subsitemaps, url_metadata
    
    def extract_date_from_url(self, url: str) -> str:
        """Extract date from URL pattern"""
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
                    year = int(match.group(1))
                    month = int(match.group(2))
                    day = int(match.group(3))
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        date_obj = datetime(year, month, day)
                        return date_obj.strftime("%Y-%m-%d")
                except:
                    continue
        return None
    
    def parse_date_from_string(self, date_str: str) -> datetime:
        """Parse date from various string formats"""
        try:
            dt = date_parser.parse(date_str)
            return dt
        except:
            return None
    
    async def crawl_sitemap_tree(self, session: aiohttp.ClientSession, sitemap_url: str):
        """Recursively crawl sitemap tree"""
        visited_sitemaps = set()
        to_visit = [sitemap_url]
        
        while to_visit and len(visited_sitemaps) < 200:
            current = to_visit.pop(0)
            if current in visited_sitemaps:
                continue
                
            visited_sitemaps.add(current)
            self.log(f"Processing sitemap {len(visited_sitemaps)}: {current[:100]}", "info")
            
            content, sitemap_url = await self.fetch_sitemap(session, current)
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
        """Select URLs based on percentage"""
        if self.process_percentage >= 100.0:
            return urls_list
        
        total_urls = len(urls_list)
        urls_to_process = max(1, int((self.process_percentage / 100.0) * total_urls))
        
        if urls_to_process < 1:
            urls_to_process = 1
            
        selected_urls = random.sample(urls_list, urls_to_process)
        
        self.log(f"Processing {self.process_percentage}% of total URLs ({urls_to_process}/{total_urls})", "info")
        
        return selected_urls
    
    def extract_date_from_article_content(self, article_content: str) -> datetime:
        """Extract date from article content using various patterns"""
        if not article_content:
            return None
        
        date_patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{2})\.(\d{2})\.(\d{4})',
            r'(\d{4})/(\d{2})/(\d{2})',
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
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
        """Convert month name to number"""
        months = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
            'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
            'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
            'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
        return months.get(month_str.lower(), 1)
    
    def get_best_published_date(self, url: str, metadata: Dict, article_content: str = "", soup=None) -> str:
        """Get the best available published date from multiple sources"""
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
        
        if not best_date:
            return ""
        
        return best_date.strftime("%d %b %Y, %I:%M %p")
    
    def extract_date_from_html(self, soup) -> datetime:
        """Extract date from HTML meta tags"""
        if soup is None:
            return None
            
        date_patterns = [
            ('meta[property="article:published_time"]', 'content'),
            ('meta[name="article:published_time"]', 'content'),
            ('meta[name="date"]', 'content'),
            ('meta[property="og:article:published_time"]', 'content'),
            ('meta[name="published_date"]', 'content'),
            ('meta[name="publish_date"]', 'content'),
            ('time[datetime]', 'datetime'),
            ('[itemprop="datePublished"]', 'content'),
        ]
        
        for selector, attr in date_patterns:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    if attr:
                        date_str = elem.get(attr, '')
                    else:
                        date_str = elem.get_text(strip=True)
                    
                    if date_str:
                        parsed = self.parse_date_from_string(date_str)
                        if parsed and parsed.year > 2000:
                            return parsed
            except:
                continue
        
        return None
    
    def extract_article_content(self, soup) -> str:
        """Extract article content from <p> tags"""
        if soup is None:
            return ""
            
        article_paragraphs = []
        
        paragraphs = soup.find_all('p')
        
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                article_paragraphs.append(text)
        
        article_selectors = [
            'article', '[class*="article"]', '[class*="content"]',
            '[class*="post"]', '[class*="story"]', '[id*="article"]',
            '[id*="content"]', 'main', '.entry-content', '.post-content'
        ]
        
        for selector in article_selectors:
            try:
                containers = soup.select(selector)
                for container in containers:
                    container_paragraphs = container.find_all('p')
                    for p in container_paragraphs:
                        text = p.get_text(strip=True)
                        if text and len(text) > 20 and text not in article_paragraphs:
                            article_paragraphs.append(text)
            except:
                continue
        
        if article_paragraphs:
            article_text = '\n\n'.join(article_paragraphs)
            if len(article_text) > 10000:
                article_text = article_text[:10000] + "..."
            return article_text
        
        return ""
    
    def extract_title(self, soup, metadata: Dict) -> str:
        """Extract title from H1 tag or title tag"""
        if soup is None:
            return ""
            
        title = ""
        
        if 'news_title' in metadata and metadata['news_title']:
            title = metadata['news_title']
        
        if not title:
            h1_tag = soup.find('h1')
            if h1_tag and h1_tag.get_text(strip=True):
                title = h1_tag.get_text(strip=True)[:500]
        
        if not title:
            title_tag = soup.find('title')
            if title_tag and title_tag.get_text(strip=True):
                title = title_tag.get_text(strip=True)[:500]
        
        return title
    
    async def extract_page_metadata_async(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Extract page metadata"""
        from bs4 import BeautifulSoup
        
        metadata = {
            'url': url,
            'title': '',
            'published_date': '',
            'article_content': ''
        }
        
        try:
            async with self.semaphore:
                async with session.get(url, timeout=10, headers={
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
                        
                        if metadata['article_content']:
                            word_count = len(metadata['article_content'].split())
                            self.log(f"Extracted {word_count} words from {url[:80]}...", "info")
                        
        except Exception as e:
            self.log(f"Error extracting from {url[:80]}: {e}", "warning")
            
        return metadata
    
    async def process_all_urls_ultrafast(self):
        """Process all URLs with ultra-fast concurrent requests"""
        all_new_urls = [url for url in self.all_urls if url not in self.existing_urls]
        
        if not all_new_urls:
            self.log("No new URLs to process", "info")
            return []
        
        urls_to_process = self.select_urls_by_percentage(all_new_urls)
        self.total_urls_found = len(self.all_urls)
        self.new_urls_extracted = len(urls_to_process)
        
        self.log(f"Processing {len(urls_to_process)} new URLs ultra-fast...", "info")
        
        results = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(urls_to_process), self.batch_size):
                batch = urls_to_process[i:i + self.batch_size]
                tasks = [self.extract_page_metadata_async(session, url) for url in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, dict):
                        results.append(result)
                
                self.log(f"Progress: {min(i + len(batch), len(urls_to_process))}/{len(urls_to_process)} URLs processed", "info")
        
        return results
    
    def save_to_database(self, metadata_list: List[Dict]):
        """Save news articles to SQLite database"""
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
                            metadata['published_date'] if metadata['published_date'] else None,
                            metadata['article_content'],
                            datetime.now().isoformat()
                        ))
                        if cursor.rowcount > 0:
                            new_count += 1
                    except Exception as e:
                        self.log(f"Error saving {metadata['url']} to DB: {e}", "warning")
            
            conn.commit()
            conn.close()
            
            if new_count > 0:
                self.log(f"Saved {new_count} news articles to database: {self.db_file}", "success")
            
        except Exception as e:
            self.log(f"Error saving to database: {e}", "error")
    
    def save_to_excel(self, metadata_list: List[Dict]):
        """Save to Excel file"""
        os.makedirs(os.path.dirname(self.output_file) if os.path.dirname(self.output_file) else '.', exist_ok=True)
        
        if os.path.exists(self.output_file):
            existing_df = pd.read_excel(self.output_file)
            all_data = existing_df.to_dict('records')
        else:
            all_data = []
        
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
            df = pd.DataFrame(all_data)
            df = df[['serial', 'url', 'title', 'published_date']]
            df.to_excel(self.output_file, index=False)
            self.log(f"Saved {new_count} new URLs to Excel: {self.output_file}", "success")
        
        return new_count
    
    async def extract_all_sitemaps(self):
        """Main extraction method"""
        self.semaphore = asyncio.Semaphore(50)
        
        self.init_database()
        
        self.log(f"Starting ultra-fast extraction for {self.domain}", "info")
        self.log(f"Processing mode: {self.process_percentage}% of total URLs", "info")
        
        async with aiohttp.ClientSession() as session:
            robots_content = await self.fetch_robots_txt(session)
            sitemap_list = self.extract_sitemap_from_robots(robots_content)
            
            if not sitemap_list:
                self.log("No sitemaps found! Make sure the website has sitemaps.", "error")
                return 0, 0
            
            self.log(f"Processing {len(sitemap_list)} sitemap(s)...", "info")
            
            for sitemap in sitemap_list:
                await self.crawl_sitemap_tree(session, sitemap)
            
            self.log(f"Total unique URLs found: {len(self.all_urls)}", "info")
            
            metadata_results = await self.process_all_urls_ultrafast()
            
            new_excel = self.save_to_excel(metadata_results)
            self.save_to_database(metadata_results)
            
            news_dates_found = sum(1 for m in metadata_results if 'news_date' in self.urls_metadata.get(m['url'], {}))
            url_dates_found = sum(1 for m in metadata_results if 'url_date' in self.urls_metadata.get(m['url'], {}))
            articles_with_content = sum(1 for m in metadata_results if m['article_content'])
            
            self.log(f"Statistics for {self.domain}:", "info")
            self.log(f"   - News dates from sitemap: {news_dates_found}", "info")
            self.log(f"   - Dates from URL patterns: {url_dates_found}", "info")
            self.log(f"   - Articles with full content: {articles_with_content}", "info")
            
            return len(metadata_results), len(self.all_urls)


class SitemapExtractorManager:
    """Manager for handling multiple site extractions"""
    
    def __init__(self, config_file: str = "sites_config.json"):
        self.config_file = config_file
        self.sites = self.load_config()
        self.results = {}
        
    def load_config(self) -> List[Dict]:
        """Load sites configuration from JSON file"""
        default_config = [
            {
                "domain": "https://example.com",
                "output_file": "output/example.xlsx",
                "db_file": "databases/example.db",
                "enabled": True,
                "process_percentage": 100.0
            }
        ]
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    logger.info(f"Loaded {len(config)} sites from {self.config_file}")
                    return config
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                return default_config
        else:
            # Create default config file
            os.makedirs(os.path.dirname(self.config_file) if os.path.dirname(self.config_file) else '.', exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            logger.info(f"Created default config file: {self.config_file}")
            logger.info("Please edit the config file with your sites and run again")
            return []
    
    def save_config(self):
        """Save current configuration"""
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(self.sites, f, indent=2)
    
    def add_site(self, domain: str, output_file: str = None, db_file: str = None, process_percentage: float = 100.0):
        """Add a new site to extract"""
        if not output_file:
            domain_name = re.sub(r'^https?://(www\.)?', '', domain)
            domain_name = re.sub(r'\.[^.]+$', '', domain_name)
            domain_name = re.sub(r'[^a-zA-Z0-9]', '_', domain_name)
            output_file = f"output/{domain_name}.xlsx"
        
        if not db_file:
            domain_name = re.sub(r'^https?://(www\.)?', '', domain)
            domain_name = re.sub(r'\.[^.]+$', '', domain_name)
            domain_name = re.sub(r'[^a-zA-Z0-9]', '_', domain_name)
            db_file = f"databases/{domain_name}.db"
        
        site = {
            "domain": domain,
            "output_file": output_file,
            "db_file": db_file,
            "enabled": True,
            "process_percentage": process_percentage
        }
        
        # Check if site already exists
        for existing in self.sites:
            if existing['domain'] == domain:
                logger.warning(f"Site {domain} already exists in config")
                return False
        
        self.sites.append(site)
        self.save_config()
        logger.info(f"Added site: {domain}")
        return True
    
    def remove_site(self, domain: str):
        """Remove a site from configuration"""
        self.sites = [s for s in self.sites if s['domain'] != domain]
        self.save_config()
        logger.info(f"Removed site: {domain}")
    
    def list_sites(self):
        """List all configured sites"""
        if not self.sites:
            logger.info("No sites configured")
            return
        
        logger.info("\n" + "="*80)
        logger.info("Configured Sites:")
        logger.info("="*80)
        for i, site in enumerate(self.sites, 1):
            status = "ENABLED" if site.get('enabled', True) else "DISABLED"
            logger.info(f"{i}. {site['domain']} - {status}")
            logger.info(f"   Excel: {site['output_file']}")
            logger.info(f"   DB: {site['db_file']}")
            logger.info(f"   Percentage: {site.get('process_percentage', 100.0)}%")
        logger.info("="*80)
    
    async def extract_site_async(self, site: Dict) -> Dict:
        """Extract a single site asynchronously"""
        if not site.get('enabled', True):
            logger.info(f"Skipping disabled site: {site['domain']}")
            return {'domain': site['domain'], 'status': 'skipped', 'new_urls': 0, 'total_urls': 0}
        
        try:
            extractor = UltraFastSitemapExtractor(
                site['domain'],
                site['output_file'],
                site['db_file'],
                site.get('process_percentage', 100.0)
            )
            new_urls, total_urls = await extractor.extract_all_sitemaps()
            return {
                'domain': site['domain'],
                'status': 'success',
                'new_urls': new_urls,
                'total_urls': total_urls,
                'output_file': site['output_file'],
                'db_file': site['db_file']
            }
        except Exception as e:
            logger.error(f"Error extracting {site['domain']}: {e}")
            return {
                'domain': site['domain'],
                'status': 'failed',
                'error': str(e),
                'new_urls': 0,
                'total_urls': 0
            }
    
    async def extract_all_sites(self):
        """Extract all configured sites"""
        logger.info("="*80)
        logger.info("Starting extraction for all sites")
        logger.info("="*80)
        
        results = []
        for site in self.sites:
            result = await self.extract_site_async(site)
            results.append(result)
            self.results[site['domain']] = result
        
        # Print summary
        self.print_summary(results)
        
        return results
    
    def print_summary(self, results: List[Dict]):
        """Print extraction summary"""
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        failed_count = sum(1 for r in results if r['status'] == 'failed')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        total_new_urls = sum(r.get('new_urls', 0) for r in results)
        
        logger.info(f"✅ Successful: {success_count}")
        logger.info(f"❌ Failed: {failed_count}")
        logger.info(f"⏭️ Skipped: {skipped_count}")
        logger.info(f"📊 Total new URLs extracted: {total_new_urls}")
        logger.info("="*80)
        
        for result in results:
            if result['status'] == 'success':
                logger.info(f"✅ {result['domain']}: {result['new_urls']}/{result['total_urls']} new URLs")
            elif result['status'] == 'failed':
                logger.info(f"❌ {result['domain']}: {result.get('error', 'Unknown error')}")
            else:
                logger.info(f"⏭️ {result['domain']}: Skipped")
        
        logger.info("="*80)


def run_extraction():
    """Main extraction function to be called by scheduler"""
    bangladesh_tz = pytz.timezone('Asia/Dhaka')
    current_time = datetime.now(bangladesh_tz)
    
    logger.info("="*80)
    logger.info(f"SITEMAP EXTRACTOR STARTING AT {current_time.strftime('%Y-%m-%d %H:%M:%S')} (Bangladesh Time)")
    logger.info("="*80)
    
    # Create necessary directories
    os.makedirs('output', exist_ok=True)
    os.makedirs('databases', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    # Initialize manager
    manager = SitemapExtractorManager()
    
    # Check if there are configured sites
    if not manager.sites:
        logger.error("No sites configured. Please edit sites_config.json with your websites.")
        logger.info("\nExample configuration:")
        logger.info('''
        [
            {
                "domain": "https://news-site-1.com",
                "output_file": "output/news_site_1.xlsx",
                "db_file": "databases/news_site_1.db",
                "enabled": true,
                "process_percentage": 100.0
            },
            {
                "domain": "https://news-site-2.com",
                "output_file": "output/news_site_2.xlsx",
                "db_file": "databases/news_site_2.db",
                "enabled": true,
                "process_percentage": 50.0
            }
        ]
        ''')
        return
    
    # List configured sites
    manager.list_sites()
    
    # Run extraction
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        results = loop.run_until_complete(manager.extract_all_sites())
        logger.info("Extraction completed successfully!")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
    finally:
        loop.close()
    
    logger.info("="*80)
    logger.info(f"SITEMAP EXTRACTOR FINISHED AT {datetime.now(bangladesh_tz).strftime('%Y-%m-%d %H:%M:%S')} (Bangladesh Time)")
    logger.info("="*80)


def main():
    """Main entry point with scheduling"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Headless Sitemap Extractor')
    parser.add_argument('--run-now', action='store_true', help='Run extraction immediately')
    parser.add_argument('--add-site', type=str, help='Add a new site to extract')
    parser.add_argument('--remove-site', type=str, help='Remove a site from configuration')
    parser.add_argument('--list-sites', action='store_true', help='List all configured sites')
    parser.add_argument('--set-percentage', type=float, help='Set processing percentage for a site (0.01-100)')
    parser.add_argument('--site-domain', type=str, help='Domain for percentage setting')
    parser.add_argument('--disable-schedule', action='store_true', help='Run once and exit (no scheduling)')
    
    args = parser.parse_args()
    
    # Create directories
    os.makedirs('output', exist_ok=True)
    os.makedirs('databases', exist_ok=True)
    
    manager = SitemapExtractorManager()
    
    if args.add_site:
        if validators.url(args.add_site):
            manager.add_site(args.add_site)
            logger.info(f"Site added: {args.add_site}")
        else:
            logger.error(f"Invalid URL: {args.add_site}")
        return
    
    if args.remove_site:
        manager.remove_site(args.remove_site)
        logger.info(f"Site removed: {args.remove_site}")
        return
    
    if args.list_sites:
        manager.list_sites()
        return
    
    if args.set_percentage and args.site_domain:
        for site in manager.sites:
            if site['domain'] == args.site_domain:
                site['process_percentage'] = args.set_percentage
                manager.save_config()
                logger.info(f"Set processing percentage for {args.site_domain} to {args.set_percentage}%")
                break
        else:
            logger.error(f"Site not found: {args.site_domain}")
        return
    
    if args.run_now or args.disable_schedule:
        # Run once immediately
        run_extraction()
        return
    
    # Schedule daily at 6 AM Bangladesh time
    bangladesh_tz = pytz.timezone('Asia/Dhaka')
    
    logger.info("="*80)
    logger.info("SITEMAP EXTRACTOR SCHEDULER STARTING")
    logger.info("="*80)
    logger.info("Will run daily at 6:00 AM Bangladesh Time (Asia/Dhaka)")
    logger.info("Press Ctrl+C to stop the scheduler")
    logger.info("="*80)
    
    # Schedule the job
    schedule.every().day.at("06:00").do(run_extraction)
    
    # Also run once at startup to check if we missed the scheduled time
    current_time = datetime.now(bangladesh_tz)
    scheduled_time = current_time.replace(hour=6, minute=0, second=0, microsecond=0)
    
    # If current time is past 6 AM but before 6:30 AM, run immediately
    if current_time >= scheduled_time and current_time < scheduled_time.replace(hour=6, minute=30):
        logger.info("Missed scheduled run, running immediately...")
        run_extraction()
    
    # Keep the scheduler running
    try:
        while True:
            schedule.run_pending()
            time_module.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("\nScheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")


if __name__ == "__main__":
    main()
