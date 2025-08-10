# Manorama News Scraper

A Python-based scraper to collect news articles from Manorama Online for data analysis and machine learning projects.

---

## Features

- Scrapes multiple categories like Kerala, India, World, Sports, Movies, Business, Health, and Technology.
- Configurable max articles per run and delay between requests.
- Supports quick single-run scraping and scheduled continuous scraping.
- Outputs scraped data as CSV files.
- Provides a data merger script to combine multiple CSV files and remove duplicates.

---

## Setup

1. **Create and activate a Python virtual environment (recommended):**

```bash
python -m venv venv
# On Windows
venv\Scripts\activate
# On Unix/macOS
source venv/bin/activate
````

2. **Install dependencies: (optional as step will automatically do ths)**

```bash
pip install -r requirements.txt  
```

3. **Run setup script to initialize configs and scripts:**

```bash
python setup_scraper.py
```

---

## Usage

* **Quick single scrape run:**

```bash
python run_scraper.py --quick
```

* **Scheduled continuous scraping (every 6 hours by default):**

```bash
python run_scraper.py --schedule
```

* **Merge all CSV data files:**

```bash
python merge_data.py --folder scraped_data --output merged_data.csv
```

---

## Configuration

Edit `scraper_config.json` to adjust settings such as:

* `max_articles_per_run`: Number of articles to scrape per run.
* `delay_range`: Delay in seconds between requests (to reduce server load).
* `schedule_hours`: Interval between scheduled scraping runs.
* `categories`: Modify or add scraping categories and URLs.

---

## Notes

* Avoid committing the `venv` folder by using the included `.gitignore`.
* The scraper currently supports basic article headline and metadata extraction.
* Logging may produce encoding errors on Windows console for non-ASCII characters; redirect logs to a file if needed.

---

## Contact
GitHub: [Jona](https://github.com/jonajoy142)

For issues or suggestions, please open an issue on the GitHub repository.