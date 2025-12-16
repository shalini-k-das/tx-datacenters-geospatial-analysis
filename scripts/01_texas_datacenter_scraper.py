## Data Center Map Web Scraper
###### Scrapes data center information from datacentermap.com
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
import os
from typing import Dict, List, Optional
from urllib.parse import urljoin

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TexasDataCenterScraper:
    """Scraper for datacentermap.com - Texas data centers only"""
    
    def __init__(self, delay: float = 30.0, output_dir: str = '.'):
        """
        Initialize scraper with robots.txt compliant delay
        
        Args:
            delay: Delay between requests (30s per robots.txt for AI crawlers)
            output_dir: Directory to save output files (default: current directory)
        """
        self.base_url = "https://www.datacentermap.com"
        self.delay = delay  # 30 seconds as per robots.txt
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a webpage with robots.txt compliance
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if error
        """
        # Check robots.txt disallowed paths
        disallowed_paths = ['/ui/', '/api/', '/visit/', '/as/', '/legal/', '/c/']
        for path in disallowed_paths:
            if path in url:
                logger.warning(f"Skipping disallowed URL per robots.txt: {url}")
                return None
        
        try:
            time.sleep(self.delay)  # 30-second delay per robots.txt
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def get_texas_city_urls(self) -> List[str]:
        """
        Get all city/market URLs from Texas page
        
        Returns:
            List of city URLs in Texas
        """
        # Texas main page - shows markets/cities in a table
        texas_url = f"{self.base_url}/usa/texas/"
        
        soup = self.get_page(texas_url)
        if not soup:
            logger.error("Failed to fetch Texas page")
            return []
        
        city_urls = []
        
        # Find the table containing city links (debug showed 1 table with 25 links)
        tables = soup.find_all('table')
        
        if not tables:
            logger.warning("No tables found, trying alternative method")
            # Fallback: find all links matching /usa/texas/city/ pattern
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if '/usa/texas/' in href and href.count('/') == 4 and href.endswith('/'):
                    # Pattern: /usa/texas/city/ (exactly 4 slashes, ends with /)
                    if href not in ['/usa/texas/', '/usa/texas/quote/']:  # Exclude main page and quote page
                        full_url = urljoin(self.base_url, href)
                        if full_url not in city_urls:
                            city_name = href.split('/')[-2]
                            city_urls.append(full_url)
                            logger.info(f"Found city: {city_name}")
        else:
            # Extract links from the table
            for table in tables:
                links = table.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    # City links follow pattern: /usa/texas/city-name/
                    if '/usa/texas/' in href and href.count('/') == 4 and href.endswith('/'):
                        if href not in ['/usa/texas/', '/usa/texas/quote/']:
                            full_url = urljoin(self.base_url, href)
                            if full_url not in city_urls:
                                city_name = href.split('/')[-2]
                                city_urls.append(full_url)
                                logger.info(f"Found city: {city_name}")
        
        logger.info(f"Found {len(city_urls)} cities in Texas")
        return city_urls
    
    def get_datacenters_from_city(self, city_url: str) -> List[str]:
        """
        Get all data center URLs from a city page
        
        Args:
            city_url: URL of city/market page
            
        Returns:
            List of data center URLs in that city
        """
        soup = self.get_page(city_url)
        if not soup:
            return []
        
        dc_urls = []
        
        # Look for individual data center links in tables or lists
        # Individual data centers have URLs like: /usa/texas/dallas/facility-name/
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Data center pages follow pattern: /usa/texas/city/facility-name/
            # They have more path segments than city pages (5+ slashes)
            if '/usa/texas/' in href and href.count('/') >= 5:
                # Skip if it's a quote, visit, or other non-datacenter page
                if not any(skip in href for skip in ['/quote/', '/visit/', '/api/', '/ui/', '/as/', '/legal/']):
                    full_url = urljoin(self.base_url, href)
                    if full_url not in dc_urls:
                        dc_urls.append(full_url)
                        logger.debug(f"Found DC: {text[:50]}")
        
        logger.info(f"Found {len(dc_urls)} data centers in {city_url}")
        return dc_urls
    
    def get_texas_datacenter_urls(self) -> List[str]:
        """
        Get all data center URLs from Texas (via cities)
        
        Returns:
            List of all data center URLs in Texas
        """
        all_dc_urls = []
        
        # First, get all city URLs
        city_urls = self.get_texas_city_urls()
        
        if not city_urls:
            logger.warning("No city URLs found. Trying direct approach...")
            # Fallback: try to find data centers directly from main page
            texas_url = f"{self.base_url}/usa/texas/"
            soup = self.get_page(texas_url)
            if soup:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if '/usa/texas/' in href and href.count('/') >= 5:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in all_dc_urls:
                            all_dc_urls.append(full_url)
            
            logger.info(f"Direct approach found {len(all_dc_urls)} data center URLs")
            return all_dc_urls
        
        # Then, scrape each city for data centers
        for city_url in city_urls:
            city_dcs = self.get_datacenters_from_city(city_url)
            all_dc_urls.extend(city_dcs)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in all_dc_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        logger.info(f"Total unique data center URLs in Texas: {len(unique_urls)}")
        return unique_urls
    
    def scrape_data_center_page(self, url: str) -> Optional[Dict]:
        """
        Scrape individual data center detail page
        
        Args:
            url: URL of data center page
            
        Returns:
            Dictionary of data center information
        """
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {
            'url': url,
            'name': None,
            'operator': None,
            'address': None,
            'city': None,
            'state': 'Texas',
            'country': 'United States',
            'postal_code': None,
            'latitude': None,
            'longitude': None,
            'power_capacity_mw': None,
            'building_size_sqft': None,
            'whitespace_sqft': None,
            'tier_rating': None,
            'year_operational': None,
            'certifications': [],
            'description': None
        }
        
        # PRIORITY: Extract from JSON first (most reliable)
        next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
        if next_data_script and next_data_script.string:
            try:
                import json
                next_data = json.loads(next_data_script.string)
                
                # Navigate to the dc (data center) object in the JSON
                dc_data = next_data.get('props', {}).get('pageProps', {}).get('dc', {})
                
                if dc_data and isinstance(dc_data, dict):
                    # Extract name from JSON (most reliable)
                    if 'name' in dc_data and dc_data['name']:
                        data['name'] = dc_data['name']
                    
                    # Extract coordinates
                    if 'latitude' in dc_data and dc_data['latitude']:
                        data['latitude'] = float(dc_data['latitude'])
                    if 'longitude' in dc_data and dc_data['longitude']:
                        data['longitude'] = float(dc_data['longitude'])
                    
                    # Extract other fields from JSON
                    if 'city' in dc_data and dc_data['city']:
                        data['city'] = dc_data['city']
                    if 'postal' in dc_data and dc_data['postal']:
                        data['postal_code'] = dc_data['postal']
                    if 'address' in dc_data and dc_data['address']:
                        data['address'] = dc_data['address']
                    
                    # Extract power capacity from meta_power
                    meta_power = dc_data.get('meta_power', {})
                    if meta_power and isinstance(meta_power, dict) and 'totalmw' in meta_power:
                        try:
                            data['power_capacity_mw'] = float(meta_power['totalmw'])
                        except (ValueError, TypeError):
                            pass
                    
                    # Extract building info from meta_building
                    meta_building = dc_data.get('meta_building', {})
                    if meta_building and isinstance(meta_building, dict):
                        if 'area_building' in meta_building:
                            try:
                                data['building_size_sqft'] = int(meta_building['area_building'])
                            except (ValueError, TypeError):
                                pass
                        if 'area_whitespace' in meta_building:
                            try:
                                data['whitespace_sqft'] = int(meta_building['area_whitespace'])
                            except (ValueError, TypeError):
                                pass
                        if 'year_operational' in meta_building:
                            try:
                                data['year_operational'] = int(meta_building['year_operational'])
                            except (ValueError, TypeError):
                                pass
                    
                    # Extract tier rating from meta_standards
                    meta_standards = dc_data.get('meta_standards', {})
                    if meta_standards and isinstance(meta_standards, dict) and 'tier_designed' in meta_standards:
                        tier = meta_standards['tier_designed']
                        if tier:
                            data['tier_rating'] = f"TIER {tier}"
                    
                    # Extract operator/company
                    companies = dc_data.get('companies', {})
                    if companies and isinstance(companies, dict) and 'name' in companies:
                        data['operator'] = companies['name']
                    
                    logger.info(f"Extracted data from JSON: lat={data['latitude']}, lng={data['longitude']}")
            except (json.JSONDecodeError, KeyError, AttributeError, TypeError) as e:
                logger.warning(f"Could not parse __NEXT_DATA__: {e}")
        
        # Only use HTML fallback if JSON didn't provide name (indicates error/placeholder page)
        if not data['name']:
            # Extract name from HTML only as fallback
            name_selectors = ['h1.datacenter-name', 'h1', '.facility-name']
            for selector in name_selectors:
                name_elem = soup.select_one(selector)
                if name_elem:
                    name_text = name_elem.get_text(strip=True)
                    # Skip if it's an error message
                    if "full capacity" not in name_text.lower() and "right place" not in name_text.lower():
                        data['name'] = name_text
                        break
        
        # Extract operator from HTML only if not from JSON
        if not data['operator']:
            operator_selectors = ['.provider-name', '.operator', '.company-name', 'a[href*="/company/"]']
            for selector in operator_selectors:
                operator_elem = soup.select_one(selector)
                if operator_elem:
                    operator_text = operator_elem.get_text(strip=True)
                    # Skip generic text
                    if operator_text and operator_text != "Follow on LinkedIn":
                        data['operator'] = operator_text
                        break
        
        # Fallback methods if JSON extraction didn't work
        if not data['latitude']:
            # Method 1: Look for meta tags (backup)
            lat_meta = soup.find('meta', {'name': 'geo.position'})
            if lat_meta:
                coords = lat_meta.get('content', '').split(';')
                if len(coords) == 2:
                    try:
                        data['latitude'] = float(coords[0].strip())
                        data['longitude'] = float(coords[1].strip())
                    except ValueError:
                        pass
        
        # Method 2: Look for separate lat/long meta tags (backup)
        if not data['latitude']:
            lat_meta = soup.find('meta', {'name': 'geo.latitude'})
            lon_meta = soup.find('meta', {'name': 'geo.longitude'})
            if lat_meta and lon_meta:
                try:
                    data['latitude'] = float(lat_meta.get('content', ''))
                    data['longitude'] = float(lon_meta.get('content', ''))
                except ValueError:
                    pass
        
        # Method 3: Look in script tags for coordinate patterns (backup)
        if not data['latitude']:
            scripts = soup.find_all('script')
            for script in scripts:
                script_text = script.string if script.string else ''
                # Look for common patterns like: lat: 32.7767, lng: -96.7970
                lat_match = re.search(r'lat[:\s]*([+-]?\d+\.\d+)', script_text, re.IGNORECASE)
                lng_match = re.search(r'l(?:ng|on)[:\s]*([+-]?\d+\.\d+)', script_text, re.IGNORECASE)
                if lat_match and lng_match:
                    try:
                        potential_lat = float(lat_match.group(1))
                        potential_lng = float(lng_match.group(1))
                        # Sanity check: Texas is roughly lat 25-36, lng -106 to -93
                        if 25 <= potential_lat <= 37 and -107 <= potential_lng <= -93:
                            data['latitude'] = potential_lat
                            data['longitude'] = potential_lng
                            break
                    except ValueError:
                        pass
        
        # Extract all text content for specifications
        page_text = soup.get_text().lower()
        
        # Extract specifications from tables, lists, or text
        spec_elements = soup.select('.spec-item, .specification, tr, li, p, div')
        
        for elem in spec_elements:
            text = elem.get_text().lower()
            
            # Power capacity
            if not data['power_capacity_mw']:
                power_match = re.search(r'(\d+(?:\.\d+)?)\s*mw', text, re.IGNORECASE)
                if power_match:
                    data['power_capacity_mw'] = float(power_match.group(1))
            
            # Building size
            if not data['building_size_sqft']:
                size_match = re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft|square\s*feet)', text, re.IGNORECASE)
                if size_match:
                    data['building_size_sqft'] = int(size_match.group(1).replace(',', ''))
            
            # Whitespace
            if not data['whitespace_sqft']:
                ws_match = re.search(r'whitespace[:\s]*([\d,]+)\s*(?:sq\.?\s*ft|sqft)', text, re.IGNORECASE)
                if ws_match:
                    data['whitespace_sqft'] = int(ws_match.group(1).replace(',', ''))
            
            # Tier rating
            if not data['tier_rating']:
                tier_match = re.search(r'tier\s*([IViv1-4]+)', text, re.IGNORECASE)
                if tier_match:
                    data['tier_rating'] = tier_match.group(1).upper()
            
            # Year operational
            if not data['year_operational']:
                year_match = re.search(r'(?:year|opened|operational|built)[:\s]*(19|20)\d{2}', text, re.IGNORECASE)
                if year_match:
                    data['year_operational'] = int(year_match.group(1))
        
        # Extract certifications
        cert_keywords = ['iso', 'leed', 'tier', 'uptime', 'soc', 'pci', 'hipaa']
        cert_elements = soup.select('.certifications, .certification, .badge, .award')
        for cert in cert_elements:
            cert_text = cert.get_text(strip=True)
            if cert_text and any(keyword in cert_text.lower() for keyword in cert_keywords):
                data['certifications'].append(cert_text)
        
        # Extract description
        desc_selectors = ['.description', '.about', '.overview', 'meta[name="description"]']
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                if desc_elem.name == 'meta':
                    data['description'] = desc_elem.get('content', '').strip()
                else:
                    data['description'] = desc_elem.get_text(strip=True)
                break
        
        return data
    
    def scrape_all_texas(self, max_datacenters: Optional[int] = None, 
                        chunk_size: int = 50,
                        start_index: int = 0,
                        output_prefix: str = 'texas_datacenters') -> pd.DataFrame:
        """
        Scrape all Texas data centers with chunked saving
        
        Args:
            max_datacenters: Maximum number of data centers to scrape (None = all)
            chunk_size: Save progress every N data centers (default: 50)
            start_index: Index to start from (for resuming)
            output_prefix: Prefix for output files
            
        Returns:
            DataFrame with all scraped data
        """
        logger.info("Starting Texas data center scraping...")
        logger.info(f"Using {self.delay}s delay between requests per robots.txt")
        logger.info(f"Chunk size: {chunk_size} (saves every ~{chunk_size * self.delay / 60:.1f} minutes)")
        
        all_data = []
        
        # Get all Texas data center URLs
        dc_urls = self.get_texas_datacenter_urls()
        
        if not dc_urls:
            logger.error("No data center URLs found for Texas")
            return pd.DataFrame()
        
        # Save the URL list for reference
        url_list_file = os.path.join(self.output_dir, f'{output_prefix}_urls.txt')
        with open(url_list_file, 'w') as f:
            for idx, url in enumerate(dc_urls):
                f.write(f"{idx},{url}\n")
        logger.info(f"Saved {len(dc_urls)} URLs to {url_list_file}")
        
        # Limit if specified
        if max_datacenters:
            dc_urls = dc_urls[:max_datacenters]
            logger.info(f"Limited to {max_datacenters} data centers for scraping")
        
        # Apply start_index for resuming
        if start_index > 0:
            logger.info(f"Resuming from index {start_index}")
            dc_urls = dc_urls[start_index:]
        
        # Scrape each data center with chunked saving
        for idx, dc_url in enumerate(dc_urls, start_index):
            logger.info(f"Processing {idx + 1}/{len(dc_urls) + start_index}: {dc_url}")
            
            dc_data = self.scrape_data_center_page(dc_url)
            if dc_data and dc_data['name']:
                all_data.append(dc_data)
                logger.info(f"Successfully scraped: {dc_data['name']}")
            else:
                logger.warning(f"Failed to scrape or no data found for: {dc_url}")
            
            # Save chunk periodically
            if (idx + 1) % chunk_size == 0:
                chunk_df = pd.DataFrame(all_data)
                chunk_file = os.path.join(self.output_dir, f'{output_prefix}_chunk_{idx + 1}.csv')
                chunk_df.to_csv(chunk_file, index=False)
                logger.info(f"✓ Saved checkpoint at {idx + 1} records to {chunk_file}")
                logger.info(f"Progress: {idx + 1}/{len(dc_urls) + start_index} ({(idx + 1)/(len(dc_urls) + start_index)*100:.1f}%)")
        
        # Final save
        df = pd.DataFrame(all_data)
        final_file = os.path.join(self.output_dir, f'{output_prefix}_final.csv')
        df.to_csv(final_file, index=False)
        logger.info(f"\nTotal Texas data centers scraped: {len(df)}")
        logger.info(f"Final data saved to {final_file}")
        return df


def merge_chunks(prefix: str = 'texas_datacenters', output_dir: str = '.') -> pd.DataFrame:
    """
    Merge all chunk files into a single dataset
    
    Args:
        prefix: Prefix of chunk files to merge
        output_dir: Directory containing chunk files
        
    Returns:
        Combined DataFrame
    """
    import glob
    
    # Look for chunk files in the specified directory
    search_pattern = os.path.join(output_dir, f'{prefix}_chunk_*.csv')
    chunk_files = sorted(glob.glob(search_pattern))
    
    if not chunk_files:
        print(f"No chunk files found in '{output_dir}' with prefix '{prefix}'")
        return pd.DataFrame()
    
    print(f"Found {len(chunk_files)} chunk files:")
    for f in chunk_files:
        print(f"  - {f}")
    
    dfs = []
    for file in chunk_files:
        df = pd.read_csv(file)
        dfs.append(df)
        print(f"Loaded {len(df)} records from {file}")
    
    combined = pd.concat(dfs, ignore_index=True)
    
    # Remove duplicates based on URL
    original_len = len(combined)
    combined = combined.drop_duplicates(subset=['url'], keep='last')
    if len(combined) < original_len:
        print(f"Removed {original_len - len(combined)} duplicate entries")
    
    output_file = os.path.join(output_dir, f'{prefix}_merged.csv')
    combined.to_csv(output_file, index=False)
    print(f"\nMerged {len(combined)} unique records to {output_file}")
    
    return combined


def main():
    """Main execution function"""
    import sys
    
    print("=" * 60)
    print("TEXAS DATA CENTER SCRAPER")
    print("=" * 60)
    print(f"Target: 391 data centers in Texas")
    print(f"Delay: 30 seconds between requests (robots.txt compliant)")
    print(f"Chunk size: 50 (saves every ~25 minutes)")
    print(f"Estimated time for 391 centers: ~3.3 hours")
    print("=" * 60)
    
    # Get output directory
    print("\nOutput directory:")
    print("1. Current directory (.)")
    print("2. Specify custom path")
    
    dir_choice = input("Select option (1-2): ").strip()
    
    if dir_choice == '2':
        output_dir = input("Enter output directory path: ").strip()
        # Handle quotes if user copied path with them
        output_dir = output_dir.strip('"').strip("'")
    else:
        output_dir = '.'
    
    print(f"\n✓ Output directory: {os.path.abspath(output_dir)}")
    
    # Initialize scraper with output directory
    scraper = TexasDataCenterScraper(delay=30.0, output_dir=output_dir)
    
    # Mode selection
    print("\nSelect mode:")
    print("1. Test mode (10 centers)")
    print("2. Full scrape (all centers)")
    print("3. Resume from checkpoint (provide start index)")
    print("4. Merge existing chunks")
    
    mode = input("Enter mode (1-4): ").strip()
    
    if mode == '4':
        # Merge existing chunks
        print("\nMerging existing chunk files...")
        df = merge_chunks('texas_datacenters', output_dir)
        if not df.empty:
            print("\n" + "=" * 60)
            print("MERGE COMPLETE")
            print("=" * 60)
            print(f"Total records: {len(df)}")
        return
    
    elif mode == '3':
        # Resume from checkpoint
        start_idx = int(input("Enter starting index (e.g., 50, 100): ").strip())
        print(f"\nResuming from index {start_idx}...")
        df = scraper.scrape_all_texas(
            chunk_size=50,
            start_index=start_idx,
            output_prefix='texas_datacenters'
        )
    
    elif mode == '2':
        # Full scrape
        confirm = input("\nThis will take ~3.3 hours. Continue? (yes/no): ").lower().strip()
        if confirm != 'yes':
            print("Scraping cancelled.")
            return
        
        print("\nRunning FULL MODE...")
        print("Progress will be saved every 50 centers (~25 minutes)")
        print("Press Ctrl+C to stop (progress will be saved)")
        
        df = scraper.scrape_all_texas(
            chunk_size=50,
            output_prefix='texas_datacenters'
        )
    
    else:  # mode == '1' or default
        # Test mode
        print("\nRunning TEST MODE - scraping 10 data centers...")
        df = scraper.scrape_all_texas(
            max_datacenters=10,
            chunk_size=5,
            output_prefix='texas_datacenters_test'
        )
    
    if df.empty:
        print("\nNo data was scraped. Please check the logs for errors.")
        return
    
    # Display summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    print(f"Total records scraped: {len(df)}")
    print(f"Records with name: {df['name'].notna().sum()}")
    print(f"Records with operator: {df['operator'].notna().sum()}")
    print(f"Records with city: {df['city'].notna().sum()}")
    print(f"Records with address: {df['address'].notna().sum()}")
    print(f"Records with coordinates: {df['latitude'].notna().sum()}")
    print(f"Records with power capacity: {df['power_capacity_mw'].notna().sum()}")
    print(f"Records with building size: {df['building_size_sqft'].notna().sum()}")
    print(f"Records with tier rating: {df['tier_rating'].notna().sum()}")
    print(f"Records with year operational: {df['year_operational'].notna().sum()}")
    
    # Display sample data
    print("\n" + "=" * 60)
    print("SAMPLE DATA (First 5 records)")
    print("=" * 60)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df[['name', 'operator', 'city', 'latitude', 'longitude', 'power_capacity_mw']].head())
    
    # City distribution
    if df['city'].notna().sum() > 0:
        print("\n" + "=" * 60)
        print("TOP 10 CITIES BY DATA CENTER COUNT")
        print("=" * 60)
        print(df['city'].value_counts().head(10))


if __name__ == "__main__":
    main()