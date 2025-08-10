import os
import sys
import subprocess
import json

def install_requirements():
    """Install required packages"""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing requirements: {e}")
        return False

def create_config():
    """Create configuration file"""
    config = {
        "scraper_settings": {
            "max_articles_per_run": 150,
            "delay_range": [1, 3],
            "schedule_hours": 6,
            "base_url": "https://www.manoramaonline.com"
        },
        "categories": {
            "kerala": "/news/kerala.html",
            "india": "/news/india.html", 
            "world": "/news/world.html",
            "sports": "/sports.html",
            "movies": "/movies.html",
            "business": "/business.html",
            "health": "/health.html",
            "technology": "/tech.html"
        },
        "output_settings": {
            "csv_folder": "scraped_data",
            "log_folder": "logs",
            "backup_enabled": True,
            "max_files_to_keep": 30
        }
    }
    
    with open('scraper_config.json', 'w', encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    
    print("✅ Configuration file created!")

def create_directories():
    """Create necessary directories"""
    for dir_name in ['scraped_data', 'logs', 'backups']:
        os.makedirs(dir_name, exist_ok=True)
    print("✅ Directories created!")

def create_run_script():
    """Create run script for easy execution"""
    run_script = '''#!/usr/bin/env python3
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
'''
    
    with open('run_scraper.py', 'w', encoding="utf-8") as f:
        f.write(run_script)
    try:
        os.chmod('run_scraper.py', 0o755)
    except:
        pass
    print("✅ Run script created!")

def create_data_merger():
    """Create script to merge CSV files"""
    merger_script = '''#!/usr/bin/env python3
"""
Merge multiple CSV files from scraping sessions
"""
import pandas as pd
import glob
import os
from datetime import datetime

def merge_csv_files(folder_path="scraped_data", output_name=None):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        print("❌ No CSV files found!")
        return
    print(f"📁 Found {len(csv_files)} CSV files")
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            df['source_file'] = os.path.basename(file)
            dfs.append(df)
            print(f"✅ Loaded {file}: {len(df)} articles")
        except Exception as e:
            print(f"❌ Error loading {file}: {e}")
    if not dfs:
        return
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['headline', 'date'], keep='first')
    if not output_name:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_name = f"merged_manorama_data_{timestamp}.csv"
    merged_df.to_csv(output_name, index=False)
    print(f"💾 Saved to: {output_name}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', default='scraped_data', help='Folder containing CSV files')
    parser.add_argument('--output', help='Output filename')
    args = parser.parse_args()
    merge_csv_files(args.folder, args.output)
'''
    
    with open('merge_data.py', 'w', encoding="utf-8") as f:
        f.write(merger_script)
    try:
        os.chmod('merge_data.py', 0o755)
    except:
        pass
    print("✅ Data merger script created!")

def main():
    print("🔧 Setting up Manorama News Scraper...")
    print("=" * 50)
    if not install_requirements():
        print("❌ Setup failed! Please install requirements manually.")
        return
    create_config()
    create_directories()
    create_run_script()
    create_data_merger()
    print("\n" + "=" * 50)
    print("✅ Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Run: python run_scraper.py --quick  (scrape once)")
    print("2. Run: python run_scraper.py --schedule  (continuous mode)")

if __name__ == "__main__":
    main()
