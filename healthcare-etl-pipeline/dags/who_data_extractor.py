# who_data_extractor.py
import requests
import pandas as pd
import logging
import time
from typing import List, Dict, Optional
from config import GHO_API_BASE_URL, GHO_INDICATORS, RAW_DATA_DIR, ETL_CONFIG, LOG_CONFIG
import os

# Ensure directories exist
os.makedirs(LOG_CONFIG['log_dir'], exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Setup logging - normalize configured log level (name like 'INFO' or numeric)
_level_conf = LOG_CONFIG.get('log_level', 'INFO')
_level = None

if isinstance(_level_conf, str):
    # Try resolve named level (e.g. 'INFO') to its numeric value
    _level = getattr(logging, _level_conf.upper(), None)
    if not isinstance(_level, int):
        # If it's not a named level, try to parse an integer value
        try:
            _level = int(_level_conf)
        except Exception:
            _level = logging.INFO
else:
    # If the config provided a non-string (e.g. already an int), try to use it
    try:
        _level = int(_level_conf)
    except Exception:
        _level = logging.INFO

# Ensure we have a valid numeric logging level
if not isinstance(_level, int):
    _level = logging.INFO

logging.basicConfig(
    level=_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_CONFIG['log_dir'], 'extract.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WHODataExtractor:
    """Extract health data from WHO Global Health Observatory API"""
    
    def __init__(self, base_url: str = GHO_API_BASE_URL, indicators: List[str] = None):
        self.base_url = base_url
        self.indicators = indicators or GHO_INDICATORS
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Healthcare-ETL-System/1.0'
        })
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close session"""
        self.close()
    
    def close(self):
        """Close the requests session"""
        if self.session:
            self.session.close()
            logger.debug("Session closed")
        
    def fetch_indicator_data(self, indicator: str, retries: int = ETL_CONFIG['retries']) -> Optional[Dict]:
        """
        Fetch data for a specific indicator with retry logic
        
        Args:
            indicator: WHO indicator code
            retries: Number of retry attempts
            
        Returns:
            JSON response or None if failed
        """
        url = f"{self.base_url}/{indicator}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching indicator {indicator} (attempt {attempt + 1}/{retries})")
                response = self.session.get(
                    url,
                    timeout=ETL_CONFIG['timeout'],
                    params={'format': 'json'}
                )
                response.raise_for_status()
                logger.info(f"Successfully fetched indicator {indicator}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error fetching {indicator}: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(ETL_CONFIG['retry_delay'])
                else:
                    logger.error(f"Failed to fetch {indicator} after {retries} attempts")
                    return None
                    
    def extract_all_indicators(self) -> Dict[str, pd.DataFrame]:
        """
        Extract data for all configured indicators
        
        Returns:
            Dictionary with indicator codes as keys and DataFrames as values
        """
        all_data = {}
        
        for indicator in self.indicators:
            data = self.fetch_indicator_data(indicator)
            
            # Validate response structure
            if not data:
                logger.warning(f"No response received for indicator {indicator}")
                continue
            
            # WHO GHO API returns data in 'value' key (OData format)
            data_key = 'value' if 'value' in data else 'data'
            
            if data_key not in data:
                logger.warning(f"No '{data_key}' key found in response for indicator {indicator}. Available keys: {list(data.keys())}")
                logger.debug(f"Response structure: {str(data)[:500]}")  # Log first 500 chars for debugging
                continue
            
            if not data[data_key]:
                logger.warning(f"Empty data array for indicator {indicator}")
                continue
            
            try:
                # Convert to DataFrame
                df = pd.json_normalize(data[data_key])
                
                # Add metadata columns
                df['indicator'] = indicator
                df['source'] = 'WHO_GHO'
                df['extracted_at'] = pd.Timestamp.now()
                
                # Clean column names
                df.columns = [col.replace('.', '_') for col in df.columns]
                
                # Add to results
                all_data[indicator] = df
                
                # Save raw data
                raw_file_path = os.path.join(RAW_DATA_DIR, f"{indicator}_raw.csv")
                df.to_csv(raw_file_path, index=False)
                logger.info(f"Saved raw data for {indicator} to {raw_file_path}")
                
            except Exception as e:
                logger.error(f"Error processing data for {indicator}: {str(e)}")
                continue
                
        return all_data
    
    def save_extracted_data(self, data: Dict[str, pd.DataFrame], filename: str = 'extracted_data.csv') -> Optional[str]:
        """
        Save all extracted data to a single CSV file
        
        Args:
            data: Dictionary of DataFrames
            filename: Name of output file
            
        Returns:
            Path to saved file or None if no data
        """
        # Check if there's any data to save
        if not data:
            logger.warning("No data to save - all extractions failed")
            return None
        
        try:
            # Exclude empty or all-NA DataFrames to avoid future concat behavior changes
            filtered_frames = []
            for df in data.values():
                if df is None:
                    continue
                # Skip completely empty DataFrames
                if df.empty:
                    continue
                # Skip DataFrames where every value is NA
                try:
                    if df.isna().all().all():
                        continue
                except Exception:
                    # If checking NA fails for unexpected types, keep the frame
                    pass

                filtered_frames.append(df)

            if not filtered_frames:
                logger.warning("No valid dataframes to concatenate after filtering empty/all-NA frames")
                return None

            # Combine the filtered dataframes
            combined_df = pd.concat(filtered_frames, ignore_index=True)
            
            # Save to file
            output_path = os.path.join(RAW_DATA_DIR, filename)
            combined_df.to_csv(output_path, index=False)
            logger.info(f"Saved combined extracted data to {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving combined data: {str(e)}")
            return None


# Example usage
if __name__ == "__main__":
    # Use context manager to ensure session is closed
    with WHODataExtractor() as extractor:
        # Extract data
        extracted_data = extractor.extract_all_indicators()
        
        # Save combined data
        if extracted_data:
            combined_path = extractor.save_extracted_data(extracted_data)
            
            # Log results
            logger.info(f"Extracted {len(extracted_data)} indicators")
            logger.info(f"Total records: {sum(len(df) for df in extracted_data.values())}")
            
            if combined_path:
                logger.info(f"Combined data saved to: {combined_path}")
        else:
            logger.error("No data was extracted")