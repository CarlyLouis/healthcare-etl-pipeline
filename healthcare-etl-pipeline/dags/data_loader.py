# data_load.py
import pandas as pd
import numpy as np
import logging
import os
from sqlalchemy import create_engine, text
from typing import Dict, List
import config
import time
from dotenv import load_dotenv  # Load environment variables from .env file if it exists
#from __future__ import annotations

load_dotenv()  # Load environment variables from .env file if it exists  

# Setup logging
# Configure logging with validation of log level (in case env var is mis-set)
_log_cfg = getattr(config, 'LOG_CONFIG', {}) or {}
_raw_level = _log_cfg.get('log_level', 'INFO')
try:
    _level_int = int(_raw_level)
    _level = _level_int if _level_int in (10, 20, 30, 40, 50) else logging.INFO
except Exception:
    _level = _raw_level

logging.basicConfig(
    level=_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(_log_cfg.get('log_dir', '.'), 'load.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Load transformed data into MySQL database"""
    
    def __init__(self, db_config: dict | None = None):
        # Prefer provided config, otherwise use config.DB_CONFIG
        self.db_config = db_config or getattr(config, 'DB_CONFIG', {})
        # Verify configuration is loaded correctly if available
        if hasattr(config, 'verify_db_config'):
            try:
                config.verify_db_config()
            except Exception:
                logger.warning('verify_db_config() raised an exception')
        self.engine = self.create_database_engine()
        self.connection = None
        
    def create_database_engine(self):
        """Create SQLAlchemy engine for database connection"""
        try:
            # Log connection attempt (without password)
            logger.info(f"Attempting to connect to MySQL database: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']} as user '{self.db_config['user']}'")
            
            # Create connection string
            connection_string = f"mysql+pymysql://airflow:KrlificationMySQL$01@mysql:3306/healthcare_etl_db"
            
            # Create engine with connection pooling
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Successfully created database engine")
            return engine
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to create database engine: {error_msg}")
            
            # Provide helpful diagnostic messages
            if "Access denied" in error_msg or "1045" in error_msg:
                logger.error("=" * 60)
                logger.error("MySQL Authentication Error - Possible causes:")
                logger.error("1. Incorrect password for MySQL user")
                logger.error("2. MySQL user doesn't exist or lacks permissions")
                logger.error("3. MySQL server is not running")
                logger.error("")
                logger.error("Troubleshooting steps:")
                logger.error(f"  - Verify MySQL is running: Check MySQL service status")
                logger.error(f"  - Check credentials in .env file or config.py")
                logger.error(f"  - Test connection manually: mysql -u {self.db_config['user']} -p -h {self.db_config['host']}")
                logger.error(f"  - Ensure database '{self.db_config['database']}' exists")
                logger.error(f"  - Create .env file with correct DB_PASSWORD if using environment variables")
                logger.error("=" * 60)
            elif "Can't connect" in error_msg or "2003" in error_msg:
                logger.error("=" * 60)
                logger.error("MySQL Connection Error - Possible causes:")
                logger.error("1. MySQL server is not running")
                logger.error(f"2. Incorrect host ({self.db_config['host']}) or port ({self.db_config['port']})")
                logger.error("3. Firewall blocking connection")
                logger.error("")
                logger.error("Troubleshooting steps:")
                logger.error(f"  - Start MySQL service")
                logger.error(f"  - Verify MySQL is listening on {self.db_config['host']}:{self.db_config['port']}")
                logger.error("=" * 60)
            elif "Unknown database" in error_msg or "1049" in error_msg:
                logger.error("=" * 60)
                logger.error(f"Database '{self.db_config['database']}' does not exist")
                logger.error("")
                logger.error("Troubleshooting steps:")
                logger.error(f"  - Create the database: CREATE DATABASE {self.db_config['database']};")
                logger.error(f"  - Or update DB_NAME in .env file or config.py")
                logger.error("=" * 60)
            
            raise
    
    def create_table_if_not_exists(self, table_name: str, df: pd.DataFrame):
        """Create table if it doesn't exist"""
        try:
            # Check if table exists
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SHOW TABLES LIKE '{table_name}'")
                )
                table_exists = result.fetchone() is not None
                
            if not table_exists:
                # Create table using SQLAlchemy DDL derived from DataFrame dtypes
                cols = []
                # Ensure metadata columns are present in the schema
                working_df = df.copy()
                if 'indicator' not in working_df.columns:
                    working_df['indicator'] = pd.Series(dtype='str')
                if 'loaded_at' not in working_df.columns:
                    working_df['loaded_at'] = pd.Series(dtype='datetime64[ns]')

                for col, dtype in working_df.dtypes.items():
                    if pd.api.types.is_integer_dtype(dtype):
                        sql_type = 'INT'
                    elif pd.api.types.is_float_dtype(dtype):
                        sql_type = 'DOUBLE'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        sql_type = 'DATETIME'
                    else:
                        sql_type = 'VARCHAR(255)'
                    cols.append(f"`{col}` {sql_type}")

                create_sql = f"CREATE TABLE `{table_name}` ({', '.join(cols)})"
                with self.engine.begin() as conn:
                    conn.execute(text(create_sql))
                logger.info(f"Created table {table_name}")
            else:
                logger.info(f"Table {table_name} already exists")
                # Ensure metadata columns exist
                with self.engine.begin() as conn:
                    # indicator
                    res = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}` LIKE 'indicator'"))
                    if res.fetchone() is None:
                        conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `indicator` VARCHAR(255)"))
                    # loaded_at
                    res = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}` LIKE 'loaded_at'"))
                    if res.fetchone() is None:
                        conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `loaded_at` DATETIME"))
                
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def load_data(self, data: Dict[str, pd.DataFrame], table_name: str = "healthcare_data"):
        """
        Load data into database
        
        Args:
            data: Dictionary of DataFrames to load
            table_name: Name of the target table
        """
        try:
            # Create table if it doesn't exist. Build a single DataFrame
            # from the provided dict of DataFrames so `create_table_if_not_exists`
            # receives a DataFrame (it previously received a list and failed).
            if isinstance(data, dict) and len(data) > 0:
                sample_df = pd.concat(list(data.values()), ignore_index=True)
            else:
                sample_df = pd.DataFrame()
            self.create_table_if_not_exists(table_name, sample_df)
            
            # Load data in batches
            for indicator, df in data.items():
                logger.info(f"Loading data for indicator: {indicator}")
                
                # Add metadata columns
                df['indicator'] = indicator
                df['loaded_at'] = pd.Timestamp.now()
                
                # Convert to proper types
                df = df.astype({
                    'country_name': 'str',
                    'value': 'float',
                    'year': 'int',
                    'data_quality_score': 'int',
                    'loaded_at': 'datetime64[ns]'
                })
                
                # Handle missing values
                df = df.fillna({
                    'country_name': 'Unknown',
                    'value': 0,
                    'data_quality_score': 50
                })
                
                # Insert data in batches
                batch_size = 1000
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    # Insert batch
                    try:
                        # Insert batch using executemany with SQLAlchemy
                        cols = list(batch.columns)
                        placeholders = ", ".join(':' + c for c in cols)
                        cols_sql = ", ".join(f'`{c}`' for c in cols)
                        insert_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
                        records = batch.where(pd.notnull(batch), None).to_dict(orient='records')
                        with self.engine.begin() as conn:
                            conn.execute(text(insert_sql), records)
                        logger.info(f"Successfully loaded batch {i//batch_size + 1} for {indicator}")
                        
                    except Exception as e:
                        logger.error(f"Error loading batch for {indicator}: {str(e)}")
                        # Try to insert row by row as fallback
                        for _, row in batch.iterrows():
                            try:
                                cols = list(row.index)
                                placeholders = ", ".join(':' + c for c in cols)
                                cols_sql = ", ".join(f'`{c}`' for c in cols)
                                insert_sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
                                record = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                                with self.engine.begin() as conn:
                                    conn.execute(text(insert_sql), record)
                            except Exception as row_error:
                                logger.error(f"Failed to insert row: {str(row_error)}")
                    
                    # Small delay to avoid overwhelming database
                    time.sleep(0.1)
                
                logger.info(f"Completed loading data for {indicator}")
                
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def validate_load(self, table_name: str = "healthcare_data"):
        """Validate that data was loaded correctly"""
        try:
            with self.engine.connect() as conn:
                # Count total records
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()
                
                # Check for missing data
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE country_name = 'Unknown'"))
                unknown_count = result.fetchone()
                
                logger.info(f"Data validation complete: {count} total records, {unknown_count} with unknown country")
                
                return count, unknown_count
                
        except Exception as e:
            logger.error(f"Error validating load: {str(e)}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
            
    def __del__(self):
        self.close_connection()

# Example usage
if __name__ == "__main__":
    # Initialize loader
    loader = DataLoader()
    
    # Load transformed data (example - in practice this would be loaded from transform.py)
    transformed_data = {
        'WHOSIS_000001': pd.DataFrame({
            'country_name': ['USA', 'UK', 'Japan'],
            'value': [79.0, 81.0, 84.0],
            'year': [2020, 2020, 2020],
            'data_quality_score': [90, 85, 95],
            'loaded_at': pd.Timestamp.now()
        }),
        'WHOSIS_000015': pd.DataFrame({
            'country_name': ['USA', 'UK', 'Japan'],
            'value': [35.0, 28.0, 22.0],
            'year': [2020, 2020, 2020],
            'data_quality_score': [80, 75, 85],
            'loaded_at': pd.Timestamp.now()
        })
    }
    
    # Load data
    loader.load_data(transformed_data)
    
    # Validate load
    count, unknown_count = loader.validate_load()
    
    # Clean up
    loader.close_connection()