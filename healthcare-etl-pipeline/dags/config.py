# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'password'),
    'database': os.getenv('DB_NAME', 'healthcare_etl_db'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# API Configuration
GHO_API_BASE_URL = 'https://ghoapi.azureedge.net/api'
GHO_INDICATORS = [
    'WHOSIS_000001',  # Life expectancy at birth
    'MDG_0000000007',  # Mortality rate, under-5
    'GHED_CHE_pc_US_SHA2011',  # Health expenditure per capita
    'MDG_0000000026',  # Maternal mortality ratio
]

# Data paths
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

# Create directories if they don't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Logging Configuration
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_CONFIG = {
    'log_dir': LOG_DIR,
    'log_level': os.getenv('LOG_LEVEL', 'INFO')
}

# ETL Configuration
ETL_CONFIG = {
    'batch_size': 10000,
    'timeout': 30,
    'retries': 3,
    'retry_delay': 5
}

# Diagnostic function to verify configuration
def verify_db_config():
    """Verify database configuration is loaded correctly"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Database Configuration:")
    logger.info(f"  Host: {DB_CONFIG['host']}")
    logger.info(f"  Port: {DB_CONFIG['port']}")
    logger.info(f"  User: {DB_CONFIG['user']}")
    logger.info(f"  Database: {DB_CONFIG['database']}")
    logger.info(f"  Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else 'NOT SET'}")
    
    # Check if .env file exists
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        logger.info(f"  .env file found at: {env_path}")
    else:
        logger.warning(f"  .env file not found at: {env_path} - using defaults or system environment variables")
    
    return DB_CONFIG