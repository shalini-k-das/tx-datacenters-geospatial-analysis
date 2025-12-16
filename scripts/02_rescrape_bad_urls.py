import pandas as pd
import sys
import os

# Import the scraper
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from texas_datacenter_scraper import TexasDataCenterScraper
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def identify_bad_records(csv_file='texas_datacenters_MERGED.csv'):
    """
    Find all records with error messages
    """
    df = pd.read_csv(csv_file)
    
    # Find records with error page content
    error_patterns = ['full capacity', 'right place', "you're in the right"]
    error_mask = df['name'].str.contains('|'.join(error_patterns), case=False, na=False)
    
    bad_records = df[error_mask]
    good_records = df[~error_mask]
    
    return bad_records, good_records


def rescrape_bad_urls(output_dir='.', delay=30.0):
    """
    Re-scrape all URLs that returned error pages
    """
    print("="*60)
    print("RE-SCRAPE BAD URLS")
    print("="*60)
    
    csv_file = os.path.join(output_dir, 'texas_datacenters_complete.csv')
    
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found")
        print("Please run the retry script first to create texas_datacenters_complete.csv")
        return
    
    # Identify bad records
    bad_records, good_records = identify_bad_records(csv_file)
    
    print(f"\nTotal records in CSV: {len(bad_records) + len(good_records)}")
    print(f"Good records: {len(good_records)}")
    print(f"Bad records (error pages): {len(bad_records)}")
    
    if len(bad_records) == 0:
        print("\nNo bad records found! All data looks good.")
        return
    
    # Show sample of bad records
    print(f"\nSample of bad records:")
    print(bad_records[['name', 'city', 'latitude']].head(3))
    
    # Get list of URLs to re-scrape
    bad_urls = bad_records['url'].tolist()
    
    print(f"\nWill re-scrape {len(bad_urls)} URLs")
    print(f"Estimated time: ~{len(bad_urls) * delay / 3600:.1f} hours")
    
    response = input(f"\nProceed with re-scraping? (yes/no): ").strip().lower()
    if response != 'yes':
        print("Cancelled.")
        return
    
    # Initialize scraper with FIXED code
    scraper = TexasDataCenterScraper(delay=delay, output_dir=output_dir)
    
    # Re-scrape bad URLs
    fixed_data = []
    still_bad = []
    
    for idx, url in enumerate(bad_urls, 1):
        logger.info(f"Re-scraping {idx}/{len(bad_urls)}: {url}")
        
        dc_data = scraper.scrape_data_center_page(url)
        
        if dc_data and dc_data['name']:
            # Check if it's still an error page
            if 'full capacity' in dc_data['name'].lower() or 'right place' in dc_data['name'].lower():
                logger.warning(f"Still getting error page for: {url}")
                still_bad.append(url)
            else:
                fixed_data.append(dc_data)
                logger.info(f"Successfully fixed: {dc_data['name']}")
        else:
            logger.warning(f"Failed to scrape: {url}")
            still_bad.append(url)
        
        # Save progress every 50 records
        if idx % 50 == 0:
            temp_df = pd.DataFrame(fixed_data)
            temp_file = os.path.join(output_dir, f'fixed_data_checkpoint_{idx}.csv')
            temp_df.to_csv(temp_file, index=False)
            logger.info(f"Checkpoint saved: {temp_file}")
    
    # Save fixed data
    if fixed_data:
        fixed_df = pd.DataFrame(fixed_data)
        fixed_file = os.path.join(output_dir, 'texas_datacenters_fixed.csv')
        fixed_df.to_csv(fixed_file, index=False)
        print(f"\nSaved {len(fixed_df)} fixed records to {fixed_file}")
        
        # Merge good records + fixed records
        combined = pd.concat([good_records, fixed_df], ignore_index=True)
        
        # Remove duplicates (keep the newly scraped version)
        combined = combined.drop_duplicates(subset=['url'], keep='last')
        
        # Save final complete dataset
        final_file = os.path.join(output_dir, 'texas_datacenters_final_clean.csv')
        combined.to_csv(final_file, index=False)
        print(f"Saved complete clean dataset ({len(combined)} records) to {final_file}")
        
        # Summary
        print("\n" + "="*60)
        print("RE-SCRAPE SUMMARY")
        print("="*60)
        print(f"Bad records identified: {len(bad_urls)}")
        print(f"Successfully fixed: {len(fixed_df)}")
        print(f"Still bad/failed: {len(still_bad)}")
        print(f"Final dataset: {len(combined)} records")
        print(f"Success rate: {len(combined)/392*100:.1f}%")
        
        if still_bad:
            print(f"\n{len(still_bad)} URLs still returning errors:")
            for url in still_bad[:10]:
                print(f"  - {url}")
            if len(still_bad) > 10:
                print(f"  ... and {len(still_bad) - 10} more")
        
        # Display data quality metrics
        print("\n" + "="*60)
        print("FINAL DATA QUALITY")
        print("="*60)
        print(f"Records with coordinates: {combined['latitude'].notna().sum()} ({combined['latitude'].notna().sum()/len(combined)*100:.1f}%)")
        print(f"Records with city: {combined['city'].notna().sum()} ({combined['city'].notna().sum()/len(combined)*100:.1f}%)")
        print(f"Records with operator: {combined['operator'].notna().sum()} ({combined['operator'].notna().sum()/len(combined)*100:.1f}%)")
        
    else:
        print("\nNo records were successfully fixed.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Re-scrape URLs that returned error pages')
    parser.add_argument('--output-dir', default='.', help='Output directory')
    parser.add_argument('--delay', type=float, default=30.0, help='Delay between requests (default: 30s)')
    
    args = parser.parse_args()
    
    rescrape_bad_urls(output_dir=args.output_dir, delay=args.delay)
