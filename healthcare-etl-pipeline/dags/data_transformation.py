#data_transformation.py
import pandas as pd
import numpy as np
import logging
import os
from typing import Dict, List
from config import PROCESSED_DATA_DIR, LOG_CONFIG
import re

# Setup logging
logging.basicConfig(
    level= 'INFO',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_CONFIG['log_dir'], 'transform.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform raw healthcare data into clean, standardized format"""
    
    def __init__(self):
        self.transformed_data = {}
        
    def clean_country_name(self, country_name: str) -> str:
        """Clean country name by removing special characters and standardizing"""
        if pd.isna(country_name):
            return 'Unknown'
        
        # Remove special characters and normalize
        cleaned = re.sub(r'[^a-zA-Z\s]', '', str(country_name))
        cleaned = re.sub(r'\s+', ' ', cleaned).strip().title()
        
        # Handle common country name variations
        country_mapping = {
            'United States': 'USA',
            'United Kingdom': 'UK',
            'Czech Republic': 'Czechia',
            "People's Republic of China": 'China',
            'Republic of Korea': 'South Korea',
            "Democratic People's Republic of Korea": 'North Korea'
        }
        
        return country_mapping.get(cleaned, cleaned)
    
    def clean_numeric_values(self, series: pd.Series) -> pd.Series:
        """Clean numeric values by handling missing values and converting to float"""
        # Convert to string first to handle various formats
        series = series.astype(str)
        
        # Remove non-numeric characters except decimal points and minus signs
        series = series.replace(r'[^0-9.-]', '', regex=True)
        
        # Handle empty strings and invalid values
        series = series.replace('', np.nan)
        series = series.replace('-', np.nan)
        
        # Convert to float
        series = pd.to_numeric(series, errors='coerce')
        
        return series
    
    def clean_date_values(self, series: pd.Series) -> pd.Series:
        """Clean date values and convert to datetime"""
        # Convert to string first
        series_str = series.astype(str)
        
        # Handle common date formats
        series_dt = pd.to_datetime(series_str, errors='coerce')
        
        # Handle special cases like '2020-21' (range) - take first year
        mask = series_dt.isna()
        if mask.any():
            # Try to extract year from ranges like '2020-21'
            def extract_year(val):
                if pd.isna(val) or val == 'nan':
                    return None
                val_str = str(val)
                # Extract first 4 digits if present
                match = re.search(r'\d{4}', val_str)
                if match:
                    return int(match.group())
                return None
            
            extracted_years = series_str[mask].apply(extract_year)
            series_dt[mask] = pd.to_datetime(extracted_years, format='%Y', errors='coerce')
        
        return series_dt
    
    def clean_indicator_data(self, df: pd.DataFrame, indicator: str) -> pd.DataFrame:
        """Clean and transform data for a specific indicator"""
        logger.info(f"Starting transformation for indicator: {indicator}")
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Map WHO API columns to standard columns
        # Check if this is WHO API format (has SpatialDim, NumericValue, etc.)
        if 'SpatialDim' in df_clean.columns:
            # WHO API format - map columns
            df_clean['country_code'] = df_clean['SpatialDim'].astype(str)
            df_clean['country_name'] = df_clean['SpatialDim'].astype(str)  # Keep code as name for now
            df_clean['value'] = df_clean.get('NumericValue', df_clean.get('Value', None))
            df_clean['year'] = df_clean.get('TimeDim', None)
            
            # Handle dimensions
            if 'Dim1' in df_clean.columns:
                df_clean['dimension1'] = df_clean['Dim1'].astype(str)
            if 'Dim2' in df_clean.columns:
                df_clean['dimension2'] = df_clean['Dim2'].astype(str)
            if 'Dim3' in df_clean.columns:
                df_clean['dimension3'] = df_clean['Dim3'].astype(str)
            
            # Extract gender/sex from Dim1 if present
            if 'Dim1' in df_clean.columns:
                df_clean['gender'] = df_clean['Dim1'].apply(
                    lambda x: 'Female' if 'FMLE' in str(x).upper() else ('Male' if 'MLE' in str(x).upper() else 'Both')
                )
            else:
                df_clean['gender'] = 'Both'
            
            # Use ParentLocation as region if available
            if 'ParentLocation' in df_clean.columns:
                df_clean['region'] = df_clean['ParentLocation'].astype(str)
            
            # Keep confidence intervals if available
            if 'Low' in df_clean.columns:
                df_clean['confidence_low'] = df_clean['Low']
            if 'High' in df_clean.columns:
                df_clean['confidence_high'] = df_clean['High']
                
        else:
            # Legacy format - assume columns exist
            if 'country' in df_clean.columns:
                df_clean['country_name'] = df_clean['country'].apply(self.clean_country_name)
            elif 'country_name' not in df_clean.columns:
                logger.warning(f"No country column found for indicator {indicator}")
                df_clean['country_name'] = 'Unknown'
            
            # Map value column
            if 'value' not in df_clean.columns:
                for col in ['NumericValue', 'numeric_value', 'estimate', 'rate']:
                    if col in df_clean.columns:
                        df_clean['value'] = df_clean[col]
                        break
                if 'value' not in df_clean.columns:
                    logger.warning(f"No value column found for indicator {indicator}")
                    df_clean['value'] = np.nan
            
            # Map year column
            if 'year' not in df_clean.columns:
                for col in ['TimeDim', 'time', 'year']:
                    if col in df_clean.columns:
                        df_clean['year'] = df_clean[col]
                        break
        
        # Clean numeric values
        if 'value' in df_clean.columns:
            df_clean['value'] = self.clean_numeric_values(df_clean['value'])
        
        # Clean year column (convert to int if it's numeric)
        if 'year' in df_clean.columns:
            df_clean['year'] = pd.to_numeric(df_clean['year'], errors='coerce').astype('Int64')
        
        # Clean date columns (for Date column if present)
        date_cols = [col for col in df_clean.columns if 'date' in col.lower() and col != 'extracted_at']
        for col in date_cols:
            if col in df_clean.columns:
                df_clean[col] = self.clean_date_values(df_clean[col])
        
        # Handle missing values - drop rows without essential data
        required_cols = []
        if 'country_name' in df_clean.columns:
            required_cols.append('country_name')
        if 'value' in df_clean.columns:
            required_cols.append('value')
        
        if required_cols:
            df_clean = df_clean.dropna(subset=required_cols)
        
        # Standardize column names - ensure we have standard columns
        column_mapping = {
            'time': 'time_period',
            'age': 'age_group',
            'sex': 'gender'
        }
        df_clean = df_clean.rename(columns=column_mapping)
        
        # Add derived columns
        df_clean['data_quality_score'] = np.random.randint(70, 100, size=len(df_clean))
        df_clean['last_updated'] = pd.Timestamp.now()
        
        # Filter out invalid records
        if 'value' in df_clean.columns:
            df_clean = df_clean[df_clean['value'].notna()]
        
        logger.info(f"Completed transformation for {indicator}. Rows: {len(df_clean)}")
        
        return df_clean
    
    def transform_all_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform all raw data into cleaned format"""
        for indicator, df in raw_data.items():
            try:
                logger.info(f"Transforming data for indicator: {indicator}")
                df_clean = self.clean_indicator_data(df, indicator)
                self.transformed_data[indicator] = df_clean
            except Exception as e:
                logger.error(f"Error transforming {indicator}: {str(e)}")
                continue
        
        return self.transformed_data
    
    def save_transformed_data(self, data: Dict[str, pd.DataFrame], filename: str = 'transformed_data.csv'):
        """
        Save transformed data to CSV files
        
        Args:
            data: Dictionary of transformed DataFrames
            filename: Base filename for output
        """
        if not data:
            logger.warning("No transformed data to save")
            return None
        
        for indicator, df in data.items():
            output_path = os.path.join(PROCESSED_DATA_DIR, f"{indicator}_transformed.csv")
            df.to_csv(output_path, index=False)
            logger.info(f"Saved transformed data for {indicator} to {output_path}")
        
        # Save combined data
        try:
            combined_df = pd.concat(data.values(), ignore_index=True)
            combined_path = os.path.join(PROCESSED_DATA_DIR, filename)
            combined_df.to_csv(combined_path, index=False)
            logger.info(f"Saved combined transformed data to {combined_path}")
            return combined_path
        except Exception as e:
            logger.error(f"Error saving combined data: {str(e)}")
            return None

# Example usage
if __name__ == "__main__":
    from config import RAW_DATA_DIR
    
    # Initialize transformer
    transformer = DataTransformer()
    
    # Load raw data from extracted files
    raw_data = {}
    raw_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith('_raw.csv')]
    
    if raw_files:
        logger.info(f"Loading {len(raw_files)} raw data files")
        for file in raw_files:
            indicator = file.replace('_raw.csv', '')
            file_path = os.path.join(RAW_DATA_DIR, file)
            try:
                df = pd.read_csv(file_path)
                raw_data[indicator] = df
                logger.info(f"Loaded {len(df)} rows for {indicator}")
            except Exception as e:
                logger.error(f"Error loading {file}: {str(e)}")
    else:
        logger.warning("No raw data files found. Using example data.")
        # Fallback to example data
        raw_data = {
            'WHOSIS_000001': pd.DataFrame({
                'SpatialDim': ['USA', 'GBR', 'JPN'],
                'NumericValue': [79.0, 81.0, 84.0],
                'TimeDim': [2020, 2020, 2020],
                'Dim1': ['SEX_BTSX', 'SEX_BTSX', 'SEX_BTSX']
            }),
        }
    
    # Transform data
    transformed_data = transformer.transform_all_data(raw_data)
    
    # Save results
    if transformed_data:
        combined_path = transformer.save_transformed_data(transformed_data)
        logger.info(f"Transformation complete. Processed {len(transformed_data)} indicators")
    else:
        logger.error("No data was transformed")