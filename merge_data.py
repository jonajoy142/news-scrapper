#!/usr/bin/env python3
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
