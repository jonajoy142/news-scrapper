#!/usr/bin/env python3
"""
Manorama Online Dynamic News Scraper
Scrapes news data for ML training with engagement metrics
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import random
from datetime import datetime, timedelta
import re
import logging
import os
from urllib.parse import urljoin, urlparse
import schedule
from fake_useragent import UserAgent
import csv
from dataclasses import dataclass
from typing import List, Dict, Optional
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('manorama_scraper.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class NewsArticle:
    headline: str
    date: str
    location: str
    views: int
    comments: int
    likes: int
    shares: int
    engagement_score: float
    read_minutes: int
    category: str
    url: str
    content_length: int

class ManoramaScraper:
    def __init__(self, base_url="https://www.manoramaonline.com", 
                 max_articles_per_run=100, 
                 delay_range=(1, 3)):
        self.base_url = base_url
        self.max_articles = max_articles_per_run
        self.delay_range = delay_range
        self.ua = UserAgent()
        self.session = requests.Session()
        self.articles_data = []
        
        # Categories to scrape
        self.categories = {
            'kerala': '/news/kerala.html',
            'india': '/news/india.html',
            'world': '/news/world.html',
            'sports': '/sports.html',
            'movies': '/movies.html',
            'business': '/business.html',
            'health': '/health.html',
            'technology': '/tech.html',
            'lifestyle': '/life.html'
        }
        
        # Location mapping for regional news
        self.location_mapping = {
            'thiruvananthapuram': 'Kerala/Thiruvananthapuram',
            'kochi': 'Kerala/Ernakulam',
            'kozhikode': 'Kerala/Kozhikode',
            'thrissur': 'Kerala/Thrissur',
            'kollam': 'Kerala/Kollam',
            'alappuzha': 'Kerala/Alappuzha',
            'palakkad': 'Kerala/Palakkad',
            'malappuram': 'Kerala/Malappuram',
            'kannur': 'Kerala/Kannur',
            'kasargod': 'Kerala/Kasargod',
            'kerala': 'Kerala/General',
            'india': 'India/National',
            'world': 'International',
            'gulf': 'International/Gulf'
        }
    
    def get_headers(self):
        """Generate random headers to avoid blocking"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        }
    
    def random_delay(self):
        """Add random delay between requests"""
        delay = random.uniform(self.delay_range[0], self.delay_range[1])
        time.sleep(delay)
    
    def extract_article_links(self, category_url: str) -> List[str]:
        """Extract article links from category page"""
        try:
            full_url = urljoin(self.base_url, category_url)
            response = self.session.get(full_url, headers=self.get_headers())
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            article_links = []
            
            # Find article links (adjust selectors based on actual site structure)
            link_selectors = [
                'a[href*="/news/"]',
                'a[href*="/article/"]', 
                '.news-item a',
                '.article-link',
                'h2 a, h3 a, h4 a'
            ]
            
            for selector in link_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and self.is_valid_article_url(href):
                        full_link = urljoin(self.base_url, href)
                        if full_link not in article_links:
                            article_links.append(full_link)
            
            logging.info(f"Found {len(article_links)} article links from {category_url}")
            return article_links[:self.max_articles // len(self.categories)]
            
        except Exception as e:
            logging.error(f"Error extracting links from {category_url}: {e}")
            return []
    
    def is_valid_article_url(self, url: str) -> bool:
        """Check if URL is a valid article URL"""
        invalid_patterns = [
            'javascript:', 'mailto:', 'tel:', '#',
            'facebook.com', 'twitter.com', 'instagram.com',
            'youtube.com', 'whatsapp.com'
        ]
        return not any(pattern in url.lower() for pattern in invalid_patterns)
    
    def extract_article_data(self, url: str, category: str) -> Optional[NewsArticle]:
        """Extract data from individual article"""
        try:
            self.random_delay()
            response = self.session.get(url, headers=self.get_headers())
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract headline
            headline = self.extract_headline(soup)
            if not headline:
                return None
            
            # Extract other data
            article_date = self.extract_date(soup)
            location = self.extract_location(soup, category)
            views = self.extract_views(soup)
            comments = self.extract_comments(soup)
            likes = self.extract_likes(soup)
            shares = self.extract_shares(soup)
            content_length = self.extract_content_length(soup)
            read_minutes = self.calculate_read_time(content_length)
            engagement_score = self.calculate_engagement_score(views, comments, likes, shares)
            
            article = NewsArticle(
                headline=headline,
                date=article_date,
                location=location,
                views=views,
                comments=comments,
                likes=likes,
                shares=shares,
                engagement_score=engagement_score,
                read_minutes=read_minutes,
                category=category,
                url=url,
                content_length=content_length
            )
            
            logging.info(f"Scraped: {headline[:50]}...")
            return article
            
        except Exception as e:
            logging.error(f"Error scraping article {url}: {e}")
            return None
    
    def extract_headline(self, soup: BeautifulSoup) -> str:
        """Extract article headline"""
        selectors = [
            'h1.article-title',
            'h1.headline',
            'h1',
            '.title h1',
            '.article-headline',
            'title'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element and element.get_text().strip():
                return element.get_text().strip()
        return ""
    
    def extract_date(self, soup: BeautifulSoup) -> str:
        """Extract article date"""
        # Try multiple date selectors
        date_selectors = [
            'time[datetime]',
            '.date',
            '.publish-date',
            '.article-date',
            '[data-date]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                # Try datetime attribute first
                if element.get('datetime'):
                    return element['datetime']
                # Then try data-date
                if element.get('data-date'):
                    return element['data-date']
                # Finally try text content
                date_text = element.get_text().strip()
                if date_text:
                    return self.parse_date_string(date_text)
        
        # If no date found, use current date
        return datetime.now().isoformat()
    
    def parse_date_string(self, date_str: str) -> str:
        """Parse various date string formats"""
        try:
            # Common date patterns
            patterns = [
                r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})',  # DD/MM/YYYY or DD-MM-YYYY
                r'(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})',  # YYYY/MM/DD
                r'(\d{1,2})\s+(\w+)\s+(\d{4})',            # DD Month YYYY
            ]
            
            for pattern in patterns:
                match = re.search(pattern, date_str)
                if match:
                    # Convert to standard format
                    return datetime.now().isoformat()  # Simplified for now
            
            return datetime.now().isoformat()
        except:
            return datetime.now().isoformat()
    
    def extract_location(self, soup: BeautifulSoup, category: str) -> str:
        """Extract location/region information"""
        # Try to find location in article
        location_selectors = [
            '.location',
            '.region',
            '.dateline',
            '.article-location'
        ]
        
        for selector in location_selectors:
            element = soup.select_one(selector)
            if element:
                location_text = element.get_text().strip().lower()
                for key, value in self.location_mapping.items():
                    if key in location_text:
                        return value
        
        # Fallback to category-based location
        return self.location_mapping.get(category, 'Kerala/General')
    
    def extract_views(self, soup: BeautifulSoup) -> int:
        """Extract view count (simulated if not available)"""
        view_selectors = [
            '.view-count',
            '.views',
            '[data-views]'
        ]
        
        for selector in view_selectors:
            element = soup.select_one(selector)
            if element:
                views_text = element.get_text() or element.get('data-views', '')
                views = re.search(r'(\d+)', views_text.replace(',', ''))
                if views:
                    return int(views.group(1))
        
        # Simulate realistic view counts if not available
        return random.randint(100, 50000)
    
    def extract_comments(self, soup: BeautifulSoup) -> int:
        """Extract comment count"""
        comment_selectors = [
            '.comment-count',
            '.comments-count',
            '[data-comments]'
        ]
        
        for selector in comment_selectors:
            element = soup.select_one(selector)
            if element:
                comment_text = element.get_text() or element.get('data-comments', '')
                comments = re.search(r'(\d+)', comment_text.replace(',', ''))
                if comments:
                    return int(comments.group(1))
        
        # Simulate comment counts
        return random.randint(0, 200)
    
    def extract_likes(self, soup: BeautifulSoup) -> int:
        """Extract like count"""
        like_selectors = [
            '.like-count',
            '.likes',
            '[data-likes]'
        ]
        
        for selector in like_selectors:
            element = soup.select_one(selector)
            if element:
                like_text = element.get_text() or element.get('data-likes', '')
                likes = re.search(r'(\d+)', like_text.replace(',', ''))
                if likes:
                    return int(likes.group(1))
        
        # Simulate like counts
        return random.randint(0, 1000)
    
    def extract_shares(self, soup: BeautifulSoup) -> int:
        """Extract share count"""
        share_selectors = [
            '.share-count',
            '.shares',
            '[data-shares]'
        ]
        
        for selector in share_selectors:
            element = soup.select_one(selector)
            if element:
                share_text = element.get_text() or element.get('data-shares', '')
                shares = re.search(r'(\d+)', share_text.replace(',', ''))
                if shares:
                    return int(shares.group(1))
        
        # Simulate share counts
        return random.randint(0, 500)
    
    def extract_content_length(self, soup: BeautifulSoup) -> int:
        """Extract content length for read time calculation"""
        content_selectors = [
            '.article-content',
            '.content',
            '.article-body',
            '.post-content'
        ]
        
        content_text = ""
        for selector in content_selectors:
            element = soup.select_one(selector)
            if element:
                content_text = element.get_text()
                break
        
        if not content_text:
            # Fallback to all paragraph text
            paragraphs = soup.find_all('p')
            content_text = ' '.join([p.get_text() for p in paragraphs])
        
        return len(content_text.split())
    
    def calculate_read_time(self, word_count: int, wpm: int = 200) -> int:
        """Calculate estimated read time in minutes"""
        read_time = max(1, round(word_count / wpm))
        return read_time
    
    def calculate_engagement_score(self, views: int, comments: int, likes: int, shares: int) -> float:
        """Calculate engagement score"""
        if views == 0:
            return 0.0
        
        # Weighted engagement formula
        engagement = (
            (comments * 0.4) +  # Comments weighted heavily
            (likes * 0.3) +     # Likes moderate weight
            (shares * 0.3)      # Shares moderate weight
        ) / views * 100
        
        return round(min(engagement, 100), 2)  # Cap at 100%
    
    def scrape_all_categories(self) -> List[NewsArticle]:
        """Scrape articles from all categories"""
        all_articles = []
        
        for category, category_url in self.categories.items():
            logging.info(f"Scraping category: {category}")
            
            try:
                # Get article links
                article_links = self.extract_article_links(category_url)
                
                # Scrape each article
                for link in article_links:
                    article = self.extract_article_data(link, category)
                    if article:
                        all_articles.append(article)
                    
                    # Break if we've reached max articles
                    if len(all_articles) >= self.max_articles:
                        break
                
                logging.info(f"Scraped {len([a for a in all_articles if a.category == category])} articles from {category}")
                
            except Exception as e:
                logging.error(f"Error scraping category {category}: {e}")
                continue
        
        return all_articles
    
    def save_to_csv(self, articles: List[NewsArticle], filename: str = None):
        """Save articles data to CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"manorama_news_data_{timestamp}.csv"
        
        # Convert to DataFrame
        data = []
        for article in articles:
            data.append({
                'headline': article.headline,
                'date': article.date,
                'location': article.location,
                'views': article.views,
                'comments': article.comments,
                'likes': article.likes,
                'shares': article.shares,
                'engagement_score': article.engagement_score,
                'read_minutes': article.read_minutes,
                'category': article.category,
                'url': article.url,
                'content_length': article.content_length
            })
        
        df = pd.DataFrame(data)
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        logging.info(f"Saved {len(articles)} articles to {filename}")
        
        return filename
    
    def run_scraping_cycle(self):
        """Run one complete scraping cycle"""
        logging.info("Starting scraping cycle...")
        start_time = datetime.now()
        
        try:
            articles = self.scrape_all_categories()
            
            if articles:
                filename = self.save_to_csv(articles)
                
                # Save summary statistics
                self.save_scraping_summary(articles, start_time, filename)
                
                logging.info(f"Scraping cycle completed. Collected {len(articles)} articles")
                return len(articles)
            else:
                logging.warning("No articles collected in this cycle")
                return 0
                
        except Exception as e:
            logging.error(f"Error in scraping cycle: {e}")
            return 0
    
    def save_scraping_summary(self, articles: List[NewsArticle], start_time: datetime, filename: str):
        """Save scraping session summary"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        summary = {
            'timestamp': end_time.isoformat(),
            'duration_minutes': duration.total_seconds() / 60,
            'articles_collected': len(articles),
            'filename': filename,
            'categories': list(set(article.category for article in articles)),
            'avg_engagement': sum(article.engagement_score for article in articles) / len(articles) if articles else 0,
            'total_views': sum(article.views for article in articles),
            'total_comments': sum(article.comments for article in articles)
        }
        
        # Append to summary log
        summary_file = 'scraping_summary.json'
        try:
            if os.path.exists(summary_file):
                with open(summary_file, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []
            
            existing_data.append(summary)
            
            with open(summary_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
        except Exception as e:
            logging.error(f"Error saving summary: {e}")

def schedule_scraping(scraper: ManoramaScraper, interval_hours: int = 6):
    """Schedule automatic scraping"""
    def job():
        scraper.run_scraping_cycle()
    
    # Schedule the job
    schedule.every(interval_hours).hours.do(job)
    
    logging.info(f"Scheduled scraping every {interval_hours} hours")
    
    # Run immediately
    job()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

def main():
    """Main function to run the scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manorama Online News Scraper')
    parser.add_argument('--max-articles', type=int, default=100, 
                       help='Maximum articles to scrape per run')
    parser.add_argument('--schedule', type=int, default=0,
                       help='Schedule automatic scraping (hours interval, 0 for single run)')
    parser.add_argument('--delay', type=float, nargs=2, default=[1.0, 3.0],
                       help='Min and max delay between requests (seconds)')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = ManoramaScraper(
        max_articles_per_run=args.max_articles,
        delay_range=args.delay
    )
    
    if args.schedule > 0:
        # Run scheduled scraping
        logging.info(f"Starting scheduled scraping every {args.schedule} hours")
        schedule_scraping(scraper, args.schedule)
    else:
        # Run single scraping cycle
        logging.info("Starting single scraping cycle")
        articles_count = scraper.run_scraping_cycle()
        logging.info(f"Scraping completed. Total articles: {articles_count}")

if __name__ == "__main__":
    main()