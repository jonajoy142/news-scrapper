#!/usr/bin/env python3
"""
Easy run script for Manorama News Scraper
"""
import sys
import os
import json
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from manorama_scraper import main, ManoramaScraper, schedule_scraping

def load_config():
    try:
        with open('scraper_config.json', 'r', encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def quick_run():
    config = load_config()
    if not config:
        print("❌ Config file not found. Run setup_scraper.py first!")
        return
    settings = config['scraper_settings']
    scraper = ManoramaScraper(
        max_articles_per_run=settings['max_articles_per_run'],
        delay_range=settings['delay_range']
    )
    print("🚀 Starting quick scraping run...")
    count = scraper.run_scraping_cycle()
    print(f"✅ Completed! Scraped {count} articles")

def scheduled_run():
    config = load_config()
    if not config:
        print("❌ Config file not found. Run setup_scraper.py first!")
        return
    settings = config['scraper_settings']
    scraper = ManoramaScraper(
        max_articles_per_run=settings['max_articles_per_run'],
        delay_range=settings['delay_range']
    )
    print(f"📅 Starting scheduled scraping every {settings['schedule_hours']} hours...")
    schedule_scraping(scraper, settings['schedule_hours'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--quick', action='store_true', help='Quick single run')
    parser.add_argument('--schedule', action='store_true', help='Scheduled runs')
    args = parser.parse_args()

    if args.quick:
        quick_run()
    elif args.schedule:
        scheduled_run()
    else:
        print("Usage: python run_scraper.py [--quick | --schedule]")
