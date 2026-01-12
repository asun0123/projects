import pickle as pkl
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time
from pathlib import Path
import os
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
from collections import deque
from time import sleep
import signal
import sys
import traceback
import pymysql

#configuration
# =============================================================================
DB_CONFIG = {
    'host': 'localhost',             
    'port': 3306,                     
    'user': 'alfredsun',        
    'password': 'Qs-581030',     
    'database': 'all_countries_database', 
    'table_name': 'cleaned_all_countries_results'   
}

USE_DATABASE = True 
DB_CONNECTION = None
DB_LOCK = threading.Lock()

GLOBAL_EXECUTOR = None
EXECUTOR_MAX_WORKERS = 1000  

# Global cookie management
GLOBAL_COOKIES = {}
COOKIE_REFRESH_LOCK = threading.Lock()
_401_ERROR_COUNT = 0
_401_ERROR_THRESHOLD = 5

# Global cookie file tracking to avoid unnecessary reloads
COOKIE_FILE_MTIME = None
LAST_COOKIE_RELOAD_TIME = None 
COOKIE_RELOAD_INTERVAL = 300 

# Run folder for organizing checkpoints and output
RUN_FOLDER_PATH = None

TEST_MODE = False
TEST_DATASETS = ['BRAZIL_2022_TEST.csv',
                  'MEXICO_2022_TEST.csv']

MAX_PARALLEL_TIER1_COMPANIES = 50  # Max companies in parallel
MAX_TIER = 5
RATE_LIMIT_DELAY = 0.01  # Minimal delay between requests (10ms)
BATCH_SAVE_INTERVAL = 100
MAX_COMPANIES_PER_TIER = 1000

ENABLE_CHECKPOINTS = True
CHECKPOINT_FILE = 'checkpoint_5tier_progress.pkl'
CHECKPOINT_FILE_TEST = 'checkpoint_5tier_progress_test.pkl'
MI_KEY_CACHE_FILE = 'mi_key_cache.pkl'
MI_KEY_CACHE_FILE_TEST = 'mi_key_cache_test.pkl'

FORMATTER_CONFIGS = {
    'suppliers': {
        'fields': ['supplier_name', 'primary_industry', 'expense_million_usd',
                   'expense_percent', 'min_percent', 'max_percent', 'min_value',
                   'max_value', 'start_dates', 'end_dates'],
        'date_fields': ['start_dates', 'end_dates'],
        'output_prefix': 'supplier'
    },
    'customers': {
        'fields': ['customer_name', 'start_dates', 'end_dates', 'primary_industry',
                   'expense_million_usd', 'expense_percent', 'min_percent',
                   'max_percent', 'min_value', 'max_value'],
        'date_fields': ['start_dates', 'end_dates'],
        'output_prefix': 'customer'
    },
    'previous_suppliers': {
        'fields': ['previous_supplier_name', 'previous_supplier_start_dates',
                   'previous_supplier_end_dates', 'previous_supplier_institution_keys',
                   'previous_supplier_primary_industry', 'previous_supplier_expense_million_usd',
                   'previous_supplier_expense_percent', 'previous_supplier_min_percent',
                   'previous_supplier_max_percent', 'previous_supplier_min_value',
                   'previous_supplier_max_value', 'previous_supplier_source'],
        'date_fields': ['previous_supplier_start_dates', 'previous_supplier_end_dates'],
        'output_prefix': 'previous_supplier'
    },
    'previous_customers': {
        'fields': ['previous_customer_name', 'previous_customer_start_dates',
                   'previous_customer_end_dates', 'previous_customer_institution_keys',
                   'previous_customer_primary_industry', 'previous_customer_expense_million_usd',
                   'previous_customer_expense_percent', 'previous_customer_min_percent',
                   'previous_customer_max_percent', 'previous_customer_min_value',
                   'previous_customer_max_value', 'previous_customer_source'],
        'date_fields': ['previous_customer_start_dates', 'previous_customer_end_dates'],
        'output_prefix': 'previous_customer'
    },
    'tags': {
        'fields': ['tag_name', 'trust_rank'],
        'output_prefix': 'tag'
    },
    'nace_codes': {
        'fields': ['nace_code', 'industry'],
        'output_prefix': 'nace'
    },
    'naics_codes': {
        'fields': ['naics_code', 'description', 'relationship_level'],
        'output_prefix': 'naics'
    },
    'mining_properties': {
        'fields': ['property_name', 'primary_commodity', 'all_commodities', 'operator',
                   'stage', 'status', 'equity_ownership',
                   'primary_resources', 'unit', 'in_situ_value',
                   'as_of_date', 'risk_score', 'property_id'],
        'use_braces': True,
        'output_prefix': 'property'
    },
    'property_profiles': {
        'fields': ['property_profile_aka', 'property_profile_type', 'property_profile_dev_stage',
                   'property_profile_activity_status', 'property_profile_country',
                   'property_profile_state'],
        'use_braces': True,
        'output_prefix': 'property_profile'
    }
}

csv_files_2022 = ['BRAZIL_2022.csv','CHILE_2022.csv']

if TEST_MODE:
    csv_files_2022 = TEST_DATASETS

DATASET_COLUMN_MAPPING = {
    'BRAZIL_2022_TEST': {'name_col': ['shipper', 'consignee'],
                        'country_col': ['loading_country', 'consignee_country']},
    'BRAZIL_2022': {'name_col': ['shipper', 'consignee'],
                    'country_col': ['loading_country', 'consignee_country']},
    'CHILE_2022': {'name_col': ['shipper', 'consignee'],
                   'country_col': ['loading_country', 'destination_country']},
    'COLOMBIA_2022': {'name_col': 'shipper', 
                      'country_col': 'buyer_seller_country'},
    'COSTARICA_2022': {'name_col': 'shipper',
                       'country_col': 'loading_country'},
    'ECUADOR_2022': {'name_col': 'shipper',
                     'country_col': 'loading_country'},
    'INDIA_2022': {'name_col': 'consignee',
                   'country_col': 'destination_country'},
    'MEXICO_2022': {'name_col': 'shipper',
                    'country_col': 'buyer_seller_country'},
    'MEXICO_2022_TEST': {'name_col': 'shipper',
                         'country_col': 'buyer_seller_country'},
    'PAKISTAN_2022': {'name_col': 'consignee',
                      'country_col': 'destination_country'},
    'PARAGUAY_2022': {'name_col': 'consignee',
                      'country_col': 'place_of_receipt_country'},
    'PERU_2022': {'name_col': 'shipper',
                  'country_col': 'shipper_country'},
    'PHILIPPINES_2022': {'name_col': 'shipper',
                         'country_col': 'place_of_receipt_country'},
    'SRILANKA_2022': {'name_col': 'shipper',
                      'country_col': 'place_of_receipt_country'},
    'TURKEY_2022': {'name_col': ['shipper', 'consignee'], 
                    'country_col': ['manufacture_country', 'destination_country']},
    'US_2022': {'name_col': 'fname',
                'country_col': 'fcountry'}
}

API_URL = "https://www.capitaliq.spglobal.com/apisv3/search-service/v3/OmniSuggest/suggest"
REPORT_API_URL = "https://www.capitaliq.spglobal.com/apisv3/hydra-service/v1/report/render/SPA"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.capitaliq.spglobal.com',
    'Referer': 'https://www.capitaliq.spglobal.com/'}

# Database Helper Functions
# =============================================================================
def get_database_connection():
    """Get or create database connection (thread-safe)"""
    global DB_CONNECTION

    with DB_LOCK:
        if DB_CONNECTION is None or not DB_CONNECTION.open:
            try:
                DB_CONNECTION = pymysql.connect(
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    database=DB_CONFIG['database'],
                    autocommit=False,
                    connect_timeout=60,
                    read_timeout=300,
                    write_timeout=300
                )
                thread_safe_print(f"[DATABASE] Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            except pymysql.Error as err:
                thread_safe_print(f"[DATABASE ERROR] Connection failed: {err}")
                thread_safe_print(f"[DATABASE ERROR] Error code: {err.args[0] if err.args else 'N/A'}")
                thread_safe_print(f"[DATABASE ERROR] Host: {DB_CONFIG['host']}, Port: {DB_CONFIG['port']}, Database: {DB_CONFIG['database']}")
                raise
        else:
            # Ping the connection to ensure it's still alive
            try:
                DB_CONNECTION.ping(reconnect=True)
            except:
                # Connection is dead, create a new one
                DB_CONNECTION = pymysql.connect(
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    database=DB_CONFIG['database'],
                    autocommit=False,
                    connect_timeout=60,
                    read_timeout=300,
                    write_timeout=300
                )
                thread_safe_print(f"[DATABASE] Reconnected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return DB_CONNECTION

def sanitize_column_name(col_name):
    """Ensure column name is MySQL-compatible (global function for consistency across DB and CSV)"""
    # MySQL reserved words that need special handling
    reserved_words = {'key', 'index', 'value', 'order', 'group', 'status', 'type',
                    'date', 'table', 'column', 'desc', 'name', 'primary', 'source'}

    # Replace problematic characters
    sanitized = str(col_name).strip()
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^\w]', '_', sanitized)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized
    # Check for reserved words and add suffix if needed
    if sanitized.lower() in reserved_words:
        sanitized = sanitized + '_col'
    # Limit length to 64 characters (MySQL limit)
    if len(sanitized) > 64:
        sanitized = sanitized[:64]

    return sanitized if sanitized else 'unnamed_column'

def save_dataframe_to_database(df, table_name=None):
    if table_name is None:
        table_name = DB_CONFIG.get('table_name', 'scraper_results')

    connection = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            # Create table from DataFrame schema
            thread_safe_print(f"[DATABASE] Table '{table_name}' does not exist. Creating new table...")

            # Map pandas dtypes to MySQL column types
            column_definitions = []
            column_mapping = {}  # Track original -> sanitized names
            used_names = set()  # Track used sanitized names to prevent duplicates

            for col in df.columns:
                sanitized_col = sanitize_column_name(col)

                # Handle duplicate sanitized names by appending a counter
                if sanitized_col in used_names:
                    counter = 2
                    base_name = sanitized_col
                    while f"{base_name}_{counter}" in used_names:
                        counter += 1
                    sanitized_col = f"{base_name}_{counter}"

                used_names.add(sanitized_col)
                column_mapping[col] = sanitized_col
                dtype = df[col].dtype

                # Determine MySQL column type based on pandas dtype
                if pd.api.types.is_integer_dtype(dtype):
                    mysql_type = 'BIGINT'
                elif pd.api.types.is_float_dtype(dtype):
                    mysql_type = 'DOUBLE'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    mysql_type = 'DATETIME'
                elif pd.api.types.is_bool_dtype(dtype):
                    mysql_type = 'BOOLEAN'
                else:
                    # Use LONGTEXT for all string/object columns to avoid row size limit
                    mysql_type = 'LONGTEXT'

                column_definitions.append(f"`{sanitized_col}` {mysql_type}")

            # Create the table
            create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)})"
            cursor.execute(create_table_sql)
            connection.commit()
            thread_safe_print(f"[DATABASE] Table '{table_name}' created successfully with {len(column_definitions)} columns")

            # Rename DataFrame columns to match sanitized names
            df_aligned = df.rename(columns=column_mapping)
        else:
            # Get database schema to align columns
            cursor.execute(f'DESCRIBE `{table_name}`')
            db_schema = cursor.fetchall()
            db_columns = [row[0] for row in db_schema]

            # Check if any financial columns need to be converted from numeric to TEXT
            # This handles the case where columns were created as DOUBLE/BIGINT but now contain comma-separated values
            columns_to_alter = []
            for col_name, col_type, *_ in db_schema:
                if col_name.startswith('fin_'):
                    col_type_upper = col_type.upper()
                    # Check if it's a numeric type
                    if any(num_type in col_type_upper for num_type in ['BIGINT', 'INT', 'DOUBLE', 'FLOAT', 'DECIMAL']):
                        # Check if DataFrame has comma-separated values for this column
                        if col_name in df.columns:
                            sample_values = df[col_name].dropna().head(10)
                            if any(isinstance(v, str) and ',' in str(v) for v in sample_values):
                                columns_to_alter.append(col_name)

            # Alter columns if needed
            if columns_to_alter:
                thread_safe_print(f"[DATABASE] Converting {len(columns_to_alter)} financial columns from numeric to TEXT to support comma-separated values...")
                for col in columns_to_alter:
                    try:
                        cursor.execute(f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col}` LONGTEXT")
                        thread_safe_print(f"[DATABASE] Converted column '{col}' to LONGTEXT")
                    except pymysql.Error as alter_err:
                        thread_safe_print(f"[DATABASE WARNING] Could not alter column '{col}': {alter_err}")
                connection.commit()

            # Align DataFrame columns to database schema (efficient approach)
            aligned_data = {}
            for col in db_columns:
                if col in df.columns:
                    aligned_data[col] = df[col]
                else:
                    aligned_data[col] = None
            df_aligned = pd.DataFrame(aligned_data)

        # Get column names from aligned DataFrame
        columns = list(df_aligned.columns)

        # Create INSERT query with placeholders
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'`{col}`' for col in columns])
        insert_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        # Convert DataFrame to list of tuples with comprehensive NaN handling
        # This is the most reliable way to ensure no NaN values reach MySQL
        data_tuples = []
        for _, row in df_aligned.iterrows():
            clean_row = []
            for val in row:
                # Check for all types of NaN/null/inf values
                if pd.isna(val) or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                    clean_row.append(None)
                elif val == 'NaN' or val == 'nan' or (isinstance(val, str) and val.strip() == ''):
                    clean_row.append(None)
                else:
                    clean_row.append(val)
            data_tuples.append(tuple(clean_row))

        # Execute batch insert in chunks to avoid connection timeout
        # Insert in batches of 500 rows to prevent MySQL timeout
        batch_size = 500
        total_rows = len(data_tuples)
        rows_inserted = 0

        thread_safe_print(f"[DATABASE] Inserting {total_rows} rows in batches of {batch_size}...")

        for i in range(0, total_rows, batch_size):
            batch = data_tuples[i:i+batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            rows_inserted += len(batch)

            # Print progress for large inserts
            if total_rows > batch_size:
                thread_safe_print(f"[DATABASE] Progress: {rows_inserted}/{total_rows} rows inserted ({int(rows_inserted/total_rows*100)}%)")

        thread_safe_print(f"[DATABASE] Successfully inserted {rows_inserted} rows into {table_name}")

        cursor.close()
        return rows_inserted, df_aligned

    except pymysql.Error as err:
        thread_safe_print(f"[DATABASE ERROR] Insert failed: {err}")
        thread_safe_print(f"[DATABASE ERROR] Error code: {err.args[0] if err.args else 'N/A'}")
        thread_safe_print(f"[DATABASE ERROR] Table: {table_name}, Rows attempted: {len(df_aligned) if 'df_aligned' in locals() else 'Unknown'}")
        if connection:
            connection.rollback()
        raise
    except Exception as e:
        thread_safe_print(f"[DATABASE ERROR] Unexpected error: {e}")
        thread_safe_print(f"[DATABASE ERROR] Error type: {type(e).__name__}")
        thread_safe_print(f"[DATABASE ERROR] Table: {table_name}, Rows attempted: {len(df_aligned) if 'df_aligned' in locals() else 'Unknown'}")
        import traceback
        thread_safe_print(f"[DATABASE ERROR] Traceback: {traceback.format_exc()}")
        if connection:
            connection.rollback()
        raise

def close_database_connection():
    """Close database connection"""
    global DB_CONNECTION

    with DB_LOCK:
        if DB_CONNECTION and DB_CONNECTION.open:
            DB_CONNECTION.close()
            thread_safe_print("[DATABASE] Connection closed")
            DB_CONNECTION = None

def save_dataframe_to_csv(df, run_folder_path, csv_filename='scraper_results.csv'):
    """
    Save DataFrame to CSV file in the run folder.
    Creates new CSV on first run, appends on subsequent runs.

    Args:
        df: DataFrame to save
        run_folder_path: Path to the run folder
        csv_filename: Name of the CSV file (default: 'scraper_results.csv')

    Returns:
        Path to the saved CSV file
    """
    if df is None or df.empty:
        thread_safe_print("[CSV] No data to save")
        return None

    try:
        csv_path = os.path.join(run_folder_path, csv_filename)
        file_exists = os.path.exists(csv_path)

        if file_exists:
            # Append to existing CSV
            df.to_csv(csv_path, mode='a', header=False, index=False, na_rep='NaN')
            thread_safe_print(f"[CSV] Appended {len(df)} rows to {csv_filename}")
        else:
            # Create new CSV with header
            df.to_csv(csv_path, mode='w', header=True, index=False, na_rep='NaN')
            thread_safe_print(f"[CSV] Created new file {csv_filename} with {len(df)} rows")

        thread_safe_print(f"[CSV] File location: {csv_path}")
        return csv_path

    except Exception as e:
        thread_safe_print(f"[CSV ERROR] Failed to save CSV: {e}")
        import traceback
        thread_safe_print(f"[CSV ERROR] Traceback: {traceback.format_exc()}")
        return None

# exclusion lists
# =============================================================================
EXCLUDED_COMPANIES = [
    'not specified', 'not available', 'n/a', 'na', 'unknown', 'none',
    'dhl global forwarding', 'dhl', 'fedex', 'ups', 'maersk',
    'same as consignee', 'same as shipper', 'to order', 'freight forwarder',
]
EXCLUDED_COMPANIES_NORMALIZED = [exc.lower().strip() for exc in EXCLUDED_COMPANIES]

# country match score tracking
# =============================================================================
company_country_stats = defaultdict(lambda: {'match_count': 0, 'total_count': 0})
company_country_stats_lock = threading.Lock()

def update_country_match_stats(company_name, dataset_country, api_country):
    with company_country_stats_lock:
        company_norm = normalize_string(company_name)
        company_country_stats[company_norm]['total_count'] += 1
        
        if dataset_country and api_country:
            if normalize_string(dataset_country) == normalize_string(api_country):
                company_country_stats[company_norm]['match_count'] += 1

def get_country_match_percentage(company_name):
    with company_country_stats_lock:
        company_norm = normalize_string(company_name)
        stats = company_country_stats[company_norm]
        
        if stats['total_count'] == 0:
            return 0.0
        
        return (stats['match_count'] / stats['total_count']) * 100.0

# checkpoint system
# =============================================================================
def save_checkpoint(results):
    """Save checkpoint directly in Runs folder."""
    if not ENABLE_CHECKPOINTS:
        return

    # Filter out any results that might have 401 error markers (safety check)
    # This catches any edge cases where results might have error flags
    filtered_results = [r for r in results if not r.get('_401_error') and not r.get('_401_error_no_retry')]

    if len(filtered_results) < len(results):
        skipped = len(results) - len(filtered_results)
        thread_safe_print(f"Checkpoint: Filtered out {skipped} results with 401 errors")

    checkpoint_data = {
        'results': filtered_results,
        'timestamp': datetime.now().isoformat(),
        'stats': progress_tracker.get_stats()
    }

    # Save checkpoint directly in Runs folder (not in timestamped subfolder)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    runs_dir = os.path.join(script_dir, 'Runs')

    # Create Runs directory if it doesn't exist
    os.makedirs(runs_dir, exist_ok=True)

    # Save checkpoint directly in Runs folder
    target_file = os.path.join(runs_dir, 'checkpoint_progress.pkl')

    with open(target_file, 'wb') as f:
        pkl.dump(checkpoint_data, f)
    thread_safe_print(f"Checkpoint saved: {len(filtered_results)} rows to {os.path.basename(target_file)}")

def load_checkpoint():
    """Load checkpoint from Runs folder, with legacy fallback."""
    if not ENABLE_CHECKPOINTS:
        return None

    # Check if there's a checkpoint directly in the Runs folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    runs_dir = os.path.join(script_dir, 'Runs')
    checkpoint_file = os.path.join(runs_dir, 'checkpoint_progress.pkl')

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'rb') as f:
                checkpoint_data = pkl.load(f)

            thread_safe_print(f"Checkpoint loaded from Runs folder")
            thread_safe_print(f"   Timestamp: {checkpoint_data['timestamp']}")
            thread_safe_print(f"   Resuming with {len(checkpoint_data['results'])} existing rows")
            return checkpoint_data
        except Exception as e:
            thread_safe_print(f"Failed to load checkpoint from Runs folder: {e}")
            return None

    # Legacy fallback: check old checkpoint locations (for backward compatibility)
    legacy_file = CHECKPOINT_FILE_TEST if TEST_MODE else CHECKPOINT_FILE
    if os.path.exists(legacy_file):
        try:
            with open(legacy_file, 'rb') as f:
                checkpoint_data = pkl.load(f)

            thread_safe_print(f"Checkpoint loaded from legacy location: {os.path.basename(legacy_file)}")
            thread_safe_print(f"   Timestamp: {checkpoint_data['timestamp']}")
            thread_safe_print(f"   Resuming with {len(checkpoint_data['results'])} existing rows")
            thread_safe_print(f"   Note: Future checkpoints will be saved to Runs folder")
            return checkpoint_data
        except Exception as e:
            thread_safe_print(f"Failed to load checkpoint: {e}")
            return None

    return None

# MI key checkpoint system
# =============================================================================
def save_mi_key_cache(results, cache_file=MI_KEY_CACHE_FILE, test_cache_file=MI_KEY_CACHE_FILE_TEST):
    """Save MI key extraction results to cache file"""
    if not ENABLE_CHECKPOINTS:
        return

    target_file = test_cache_file if TEST_MODE else cache_file

    cache_data = {
        'results': results,
        'timestamp': datetime.now().isoformat(),
        'total_companies': len(results),
        'mi_keys_found': sum(1 for r in results if r.get('mi_key'))
    }

    with open(target_file, 'wb') as f:
        pkl.dump(cache_data, f)

    thread_safe_print(f"MI key cache saved: {len(results)} companies ({cache_data['mi_keys_found']} with MI keys)")

def load_mi_key_cache(cache_file=MI_KEY_CACHE_FILE, test_cache_file=MI_KEY_CACHE_FILE_TEST):
    """Load MI key extraction results from cache file"""
    if not ENABLE_CHECKPOINTS:
        return None

    target_file = test_cache_file if TEST_MODE else cache_file

    if not os.path.exists(target_file):
        return None

    try:
        with open(target_file, 'rb') as f:
            cache_data = pkl.load(f)

        thread_safe_print(f"MI key cache loaded from {cache_data['timestamp']}")
        thread_safe_print(f"   Cached companies: {cache_data['total_companies']}")
        thread_safe_print(f"   MI keys found: {cache_data['mi_keys_found']}")
        return cache_data
    except Exception as e:
        thread_safe_print(f"Failed to load MI key cache: {e}")
        return None

# shutdown handler
# =============================================================================
def signal_handler(sig, frame):
    if 'current_results' in globals():
        thread_safe_print("Interrupt signal received - saving checkpoint...")
        save_checkpoint(current_results)
        thread_safe_print("Checkpoint saved successfully")
    progress_tracker.print_progress()
    thread_safe_print("Exiting")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

#new classes
# =============================================================================
class ThreadSafeResultCollector:
    def __init__(self):
        self.results = []
        self.lock = threading.Lock()
        self.processed_companies = set()
        self.last_checkpoint_time = time.time()
        self.last_db_save_time = time.time()
        self.checkpoint_interval = 300
        self.db_save_interval = 300  # Save to database every 5 minutes
        self.last_db_saved_count = 0  # Track how many rows were last saved to DB
    
    def add_results(self, new_results):
        with self.lock:
            self.results.extend(new_results)
            
            for result in new_results:
                if 'spciq_key' in result:
                    self.processed_companies.add(result['spciq_key'])
            
            return len(self.results)
    
    def get_all_results(self):
        with self.lock:
            return self.results.copy()
    
    def get_processed_companies(self):
        with self.lock:
            return self.processed_companies.copy()
    
    def should_checkpoint(self):
        with self.lock:
            current_time = time.time()
            if current_time - self.last_checkpoint_time >= self.checkpoint_interval:
                self.last_checkpoint_time = current_time
                return True
            return False

    def should_save_to_database(self):
        """Check if it's time to save to database (every 5 minutes)"""
        with self.lock:
            current_time = time.time()
            if current_time - self.last_db_save_time >= self.db_save_interval:
                self.last_db_save_time = current_time
                return True
            return False

    def get_new_results_since_last_db_save(self):
        """Get only results that haven't been saved to database yet"""
        with self.lock:
            # Return results from last_db_saved_count onwards
            new_results = self.results[self.last_db_saved_count:]
            self.last_db_saved_count = len(self.results)
            return new_results
    
    def get_stats(self):
        with self.lock:
            return {
                'total_results': len(self.results),
                'unique_companies': len(self.processed_companies)
            }

# Cookie utilities for handling authentication
# =============================================================================
def get_essential_cookies(cookies_dict):
    essential_keywords = ['auth', 'session', 'token', 'snl', 'ciq']
    essential_cookies = {}

    for key, value in cookies_dict.items():
        key_lower = key.lower()
        if any(keyword in key_lower for keyword in essential_keywords):
            essential_cookies[key] = value

    return essential_cookies

def check_cookies_valid(cookies_list):
    current_time = time.time()
    critical_expired = []

    for cookie in cookies_list:
        if 'expiry' in cookie:
            name = cookie.get('name', '')
            name_lower = name.lower()

            # Check if this is a critical auth cookie
            is_critical = any(keyword in name_lower
                            for keyword in ['auth', 'session', 'token', 'snl', 'ciq'])

            if is_critical and cookie['expiry'] < current_time:
                critical_expired.append(name)

    return len(critical_expired) == 0, critical_expired

def load_and_validate_cookies(cookie_file=None):
    try:
        # If no cookie_file specified, use absolute path based on script location
        if cookie_file is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cookie_file = os.path.join(script_dir, 'Cookies', 'cookies.pkl')

        with open(cookie_file, 'rb') as f:
            cookies_list = pkl.load(f)

        # Convert to dict
        cookies_dict = {}
        for cookie in cookies_list:
            cookies_dict[cookie['name']] = cookie['value']

        # Check validity
        is_valid, expired = check_cookies_valid(cookies_list)

        if not is_valid:
            thread_safe_print(f"Warning: {len(expired)} critical cookies expired: {expired}")
            thread_safe_print("Please run Cookie.py to refresh your session")

        return cookies_list, cookies_dict, is_valid

    except Exception as e:
        thread_safe_print(f"ERROR loading cookies: {e}")
        return None, None, False

class CookieManager:
    def __init__(self, cookie_file=None, refresh_interval=300, regenerate_interval=1800):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # Always use absolute path based on script location
        if cookie_file is None:
            self.cookie_file = os.path.join(self.script_dir, 'Cookies', 'cookies.pkl')
        elif os.path.isabs(cookie_file):
            self.cookie_file = cookie_file
        else:
            # Convert relative path to absolute based on script directory
            self.cookie_file = os.path.join(self.script_dir, 'Cookies', 'cookies.pkl')
        
        self.refresh_interval = refresh_interval  # How often to reload from disk (5 min)
        self.regenerate_interval = regenerate_interval  # How often to generate fresh cookies (30 min)
        self.last_refresh_time = time.time()
        self.last_regenerate_time = time.time()
        self.lock = threading.Lock()
        self.current_account_index = 0
        self.failed_account_indices = set()  # Track accounts that have failed
        
        print(f"[COOKIE MANAGER] Using cookie file: {self.cookie_file}")

    def refresh_session_cookies(self, session, force_regenerate=False):
        with self.lock:
            try:
                # Use the improved load_fresh_cookies function that checks file modification time
                cookies_list, was_reloaded = load_fresh_cookies(force=force_regenerate)

                if was_reloaded and cookies_list:
                    # Clear existing cookies first
                    session.cookies.clear()
                    # Properly set each cookie with domain and path info
                    for cookie in cookies_list:
                        session.cookies.set(
                            name=cookie['name'],
                            value=cookie['value'],
                            domain=cookie.get('domain', ''),
                            path=cookie.get('path', '/'),
                            secure=cookie.get('secure', False)
                        )
                    self.last_refresh_time = time.time()
                    thread_safe_print(f"[COOKIE REFRESH] Reloaded {len(cookies_list)} cookies from disk (maintained by cron job)")
                    return True
                elif not was_reloaded:
                    # File hasn't been modified, no need to reload
                    return True
                else:
                    thread_safe_print("Failed to load cookies")
                    return False

            except Exception as e:
                thread_safe_print(f"Failed to refresh cookies: {e}")
                return False
    
    def check_and_refresh_cookies(self, session, force=False): #"when to refresh"
        current_time = time.time()
        time_since_refresh = current_time - self.last_refresh_time

        if force or time_since_refresh > self.refresh_interval:
            return self.refresh_session_cookies(session)
        return False
    
    def refresh_on_error(self, session, switch_account=False): #"refresh on API error"
        """
        Refresh cookies on 401 error - just reload from disk (cron job maintains them).
        Note: switch_account parameter is kept for compatibility but has no effect.
        """
        with self.lock:
            try:
                # COMMENTED OUT: Scraper should NOT regenerate cookies - cron job handles this
                # Instead, just reload the latest cookies from disk
                # # Try to regenerate with account switching if requested
                # if self.regenerate_cookies(switch_account=switch_account):
                #     # Small delay to ensure file is written
                #     time.sleep(2)
                #
                #     # Load new cookies into session
                #     with open(self.cookie_file, 'rb') as f:
                #         cookies_list = pkl.load(f)
                #         session.cookies.clear()
                #         for cookie in cookies_list:
                #             session.cookies.set(
                #                 name=cookie['name'],
                #                 value=cookie['value'],
                #                 domain=cookie.get('domain', ''),
                #                 path=cookie.get('path', '/'),
                #                 secure=cookie.get('secure', False)
                #             )
                #     self.last_refresh_time = time.time()
                #
                #     # Get current account name for logging
                #     current_account = CAPITALIQ_ACCOUNTS[self.current_account_index]
                #     thread_safe_print(f"Session updated with {len(cookies_list)} cookies from: {current_account['name']}")
                #     return True
                # else:
                #     thread_safe_print("Failed to regenerate cookies")
                #     return False

                # Force reload cookies from disk (maintained by cron job) on 401 error
                thread_safe_print("[COOKIE REFRESH] 401 error detected - forcing cookie reload from disk")
                cookies_list, _ = load_fresh_cookies(force=True)

                if cookies_list:
                    session.cookies.clear()
                    for cookie in cookies_list:
                        session.cookies.set(
                            name=cookie['name'],
                            value=cookie['value'],
                            domain=cookie.get('domain', ''),
                            path=cookie.get('path', '/'),
                            secure=cookie.get('secure', False)
                        )
                    self.last_refresh_time = time.time()
                    thread_safe_print(f"Reloaded {len(cookies_list)} cookies from disk (maintained by cron job)")
                    return True
                else:
                    thread_safe_print("Failed to load cookies from disk")
                    return False

            except Exception as e:
                thread_safe_print(f"Cookie refresh error: {e}")
                return False

cookie_manager = CookieManager(
    refresh_interval=1800,      # Reload from disk every 30 minutes
    regenerate_interval=1800    # Generate fresh cookies every 30 minutes
)

class RateLimiter:
    def __init__(self, max_calls_per_second=10):
        self.max_calls = max_calls_per_second
        self.min_interval = 1.0 / max_calls_per_second  # Time between calls for smooth distribution
        self.last_call_time = 0
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            import random
            now = time.time()
            time_since_last_call = now - self.last_call_time

            # Calculate base sleep time
            base_sleep = 0
            if time_since_last_call < self.min_interval:
                base_sleep = self.min_interval - time_since_last_call

            # Add random jitter (±30% variation to look more human)
            jitter = base_sleep * random.uniform(-0.3, 0.3)
            sleep_time = max(0, base_sleep + jitter)

            # Add small random delay even if no rate limiting needed (simulate human behavior)
            if sleep_time < 0.1:
                sleep_time += random.uniform(0.05, 0.15)

            sleep(sleep_time)
            self.last_call_time = time.time()

rate_limiter = RateLimiter(max_calls_per_second=100)  # TESTING MODE: 100 calls/sec (essentially no rate limiting)

class ProgressTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.tier_counts = defaultdict(int)
        self.total_api_calls = 0
        self.start_time = time.time()
        self.companies_processed = 0

    def increment_tier(self, tier):
        with self.lock:
            self.tier_counts[tier] += 1
            self.companies_processed += 1

    def increment_api_calls(self, count=1):
        with self.lock:
            self.total_api_calls += count

    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            rate = self.companies_processed / elapsed if elapsed > 0 else 0
            return {
                'elapsed': elapsed,
                'companies': self.companies_processed,
                'api_calls': self.total_api_calls,
                'rate': rate,
                'tier_counts': dict(self.tier_counts)
            }
    
    def print_progress(self):
        stats = self.get_stats()
        thread_safe_print(f"Time elapsed: {stats['elapsed']/60:.1f} minutes")
        thread_safe_print(f"Companies processed: {stats['companies']}")
        thread_safe_print(f"API calls made: {stats['api_calls']}")
        thread_safe_print(f"Processing rate: {stats['rate']:.2f} companies/sec")
        thread_safe_print(f"\nTier breakdown:")
        for tier in sorted(stats['tier_counts'].keys()):
            thread_safe_print(f"  Tier {tier}: {stats['tier_counts'][tier]} companies")

progress_tracker = ProgressTracker()

# helper functions
# ============================================================================
print_lock = threading.Lock()
def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def refresh_global_cookies():
    global GLOBAL_COOKIES, _401_ERROR_COUNT
    
    with COOKIE_REFRESH_LOCK:
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cookie_path = os.path.join(script_dir, 'Cookies', 'cookies.pkl')
            
            thread_safe_print(f"\n{'='*70}")
            thread_safe_print("401 errors detected -> refresh_global_cookies")
            thread_safe_print(f"{'='*70}")
            
            with open(cookie_path, 'rb') as f:
                cookies_list = pkl.load(f)
                new_cookies = {cookie['name']: cookie['value'] for cookie in cookies_list}
            
            GLOBAL_COOKIES.clear()
            GLOBAL_COOKIES.update(new_cookies)
            _401_ERROR_COUNT = 0
            
            thread_safe_print(f"Cookies refreshed successfully ({len(GLOBAL_COOKIES)} cookies loaded)")
            thread_safe_print(f"{'='*70}\n")
            
            # Clear thread-local sessions to force new connections with new cookies
            if hasattr(thread_local, 'session'):
                try:
                    thread_local.session.close()
                    delattr(thread_local, 'session')
                except:
                    pass
            
            return True
            
        except Exception as e:
            thread_safe_print(f"Error refreshing cookies: {e}")
            return False

def handle_401_error():
    global _401_ERROR_COUNT
    
    _401_ERROR_COUNT += 1
    
    if _401_ERROR_COUNT >= _401_ERROR_THRESHOLD:
        thread_safe_print(f"\nWarning: {_401_ERROR_COUNT} consecutive 401 errors detected")
        refresh_global_cookies()
        time.sleep(2)  # Brief pause after refresh

#Global ThreadPool, uses central pool of executors instead of calling new session every time
def initialize_global_executor():
    global GLOBAL_EXECUTOR
    if GLOBAL_EXECUTOR is None:
        GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_MAX_WORKERS)
        thread_safe_print(f"Initialized global ThreadPoolExecutor with {EXECUTOR_MAX_WORKERS} workers")
    return GLOBAL_EXECUTOR

def shutdown_global_executor():
    global GLOBAL_EXECUTOR
    if GLOBAL_EXECUTOR is not None:
        # Request shutdown and wait for all threads to finish
        GLOBAL_EXECUTOR.shutdown(wait=True)
        GLOBAL_EXECUTOR = None
        thread_safe_print("Global ThreadPoolExecutor shutdown complete")
        # Note: Thread-local sessions will be closed when threads terminate naturally

def extract_company_names(csv_path, datasets_to_process, chunk_size=1000):
    all_chunks = []

    for dataset_file in datasets_to_process:
        file_path = csv_path / dataset_file
        if not file_path.exists():
            thread_safe_print(f"Dataset file not found: {file_path}")
            continue
    #file_path = csv_path / dataset_file
    #if not file_path.exists():
    #    return set()
    
        try:
            df = pd.read_csv(file_path, dtype=str, low_memory=False).fillna('')
            name_cols, _ = get_dataset_columns(dataset_file)
            name_cols = [c for c in name_cols if c in df.columns]

            if not name_cols:
                return set()

            total_rows = len(df)

            for i in range(0, total_rows, chunk_size):
                chunk_df = df.iloc[i:i + chunk_size]
                all_chunks.append((dataset_file, chunk_df, name_cols))
        except Exception as e:
            thread_safe_print(f"Error reading {dataset_file}: {e}")
            continue
    return all_chunks

def extract_companies_from_chunk(df_chunk, name_cols):
    companies = set()
    for col in name_cols:
        if col in df_chunk.columns:
            companies_in_col = df_chunk[col].dropna().unique()
            for company in companies_in_col:
                company_clean = str(company).strip()
                if not _is_excluded_company(company_clean):
                    companies.add(company_clean)
    
    return companies

def chunk_extract_MIKey(companies_chunk, cookies_list):
    session = get_thread_session(cookies_list)

    results = []
    for company in companies_chunk:
        try:
            result = search_company(session, company)
            results.append(result)

        except Exception as e:
            results.append({
                'company_name': company, 
                'matched_name': '', 
                'country': '',
                'mi_key': None, 
                'spciq_key': None, 
                'status': f'chunk_error_{type(e).__name__}',
                'error_details': str(e)
            })

    # Don't close session - it's managed by thread-local storage
    return results

thread_local = threading.local()

def load_fresh_cookies(force=False):
    """
    Load cookies from disk only if the file has been modified since last load.

    Args:
        force: If True, reload regardless of modification time

    Returns:
        Tuple of (cookies_list, was_reloaded) where was_reloaded indicates if cookies were actually updated
    """
    global COOKIE_FILE_MTIME, LAST_COOKIE_RELOAD_TIME

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'Cookies', 'cookies.pkl')

        # Check file modification time
        current_mtime = os.path.getmtime(cookie_path)

        # Only reload if file has been modified or force is True
        if not force and COOKIE_FILE_MTIME is not None and current_mtime <= COOKIE_FILE_MTIME:
            return None, False  # File hasn't changed, no need to reload

        # Load cookies
        with open(cookie_path, 'rb') as f:
            cookies = pkl.load(f)

        # Update tracking variables
        COOKIE_FILE_MTIME = current_mtime
        LAST_COOKIE_RELOAD_TIME = time.time()

        return cookies, True

    except Exception as e:
        thread_safe_print(f"Error reloading cookies: {e}")
        return None, False

def get_thread_session(cookies_list):
    """
    Get or create a thread-local session with automatic cookie refresh.

    Note: Sessions are automatically cleaned up when threads exit during executor shutdown.
    The executor.shutdown(wait=True) ensures all threads complete before termination.
    """
    global LAST_COOKIE_RELOAD_TIME

    # Initialize session if this is the first time for this thread
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
        for cookie in cookies_list:
            thread_local.session.cookies.set(
                name=cookie['name'],
                value=cookie['value'],
                domain=cookie.get('domain', ''),
                path=cookie.get('path', '/'),
                secure=cookie.get('secure', False)
            )

    # Check if we should attempt to reload cookies (using global timer, not thread-local)
    current_time = time.time()
    if LAST_COOKIE_RELOAD_TIME is None or (current_time - LAST_COOKIE_RELOAD_TIME > COOKIE_RELOAD_INTERVAL):
        # Only one thread should check for cookie updates at a time
        with COOKIE_REFRESH_LOCK:
            # Double-check after acquiring lock (another thread may have just reloaded)
            # Re-capture current_time inside lock for accurate timing
            current_time = time.time()
            if LAST_COOKIE_RELOAD_TIME is None or (current_time - LAST_COOKIE_RELOAD_TIME > COOKIE_RELOAD_INTERVAL):
                fresh_cookies, was_reloaded = load_fresh_cookies()
                if was_reloaded and fresh_cookies:
                    thread_safe_print("[COOKIE REFRESH] Cookie file updated - reloading cookies from disk")
                    thread_local.session.cookies.clear()
                    for cookie in fresh_cookies:
                        thread_local.session.cookies.set(
                            name=cookie['name'],
                            value=cookie['value'],
                            domain=cookie.get('domain', ''),
                            path=cookie.get('path', '/'),
                            secure=cookie.get('secure', False)
                        )
                    thread_safe_print(f"[COOKIE REFRESH] Successfully reloaded {len(fresh_cookies)} cookies")

    return thread_local.session

def refresh_cookies_and_retry(session, operation_name="API call"):
    """
    Force refresh cookies from disk and update the session.
    Uses proper locking to prevent race conditions with automatic cookie refresh.
    Returns True if cookies were successfully refreshed, False otherwise.
    """
    global COOKIE_FILE_MTIME, LAST_COOKIE_RELOAD_TIME

    try:
        thread_safe_print(f"[COOKIE REFRESH] Timeout on {operation_name} - attempting cookie reload")

        # Use the same lock as automatic refresh to prevent race conditions
        with COOKIE_REFRESH_LOCK:
            # Check if cookies were just reloaded by another thread (within last 10 seconds)
            current_time = time.time()
            if LAST_COOKIE_RELOAD_TIME is not None and (current_time - LAST_COOKIE_RELOAD_TIME < 10):
                thread_safe_print(f"[COOKIE REFRESH] Cookies were just reloaded {int(current_time - LAST_COOKIE_RELOAD_TIME)}s ago - skipping reload, using existing cookies")
                return True  # Return True because we have fresh cookies, just reloaded by another thread

            # Force reload cookies from disk
            fresh_cookies, _ = load_fresh_cookies(force=True)

            if fresh_cookies:
                # Clear and reload session cookies
                session.cookies.clear()
                for cookie in fresh_cookies:
                    session.cookies.set(
                        name=cookie['name'],
                        value=cookie['value'],
                        domain=cookie.get('domain', ''),
                        path=cookie.get('path', '/'),
                        secure=cookie.get('secure', False)
                    )
                thread_safe_print(f"[COOKIE REFRESH] Reloaded {len(fresh_cookies)} cookies - retrying {operation_name}")
                return True
            else:
                thread_safe_print(f"[COOKIE REFRESH] No fresh cookies available for {operation_name}")
                return False

    except Exception as e:
        thread_safe_print(f"[COOKIE REFRESH] Error refreshing cookies: {e}")
        return False

def clean_company_name(name):
    if not name or pd.isna(name):
        return ''
    name = str(name)
    name = re.sub(r'^\([A-Z]+:[A-Z0-9]+\)\s*', '', name)
    name = re.sub(r'\s*‚Ä∫\s*Corporate Profile\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*-\s*Corporate Profile\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*Corporate Profile\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*‚Ä∫.*$', '', name)
    name = re.sub(r'[\u00C2\u00E2\u201A\u201E\u00A0\u00AB\u00BB]+\s*$', '', name)
    name = re.sub(r'[^\w\s\.\-&,\'\(\)]+\s*$', '', name)
    name = ' '.join(name.split())
    return name.strip()

def normalize_string(s):
    if pd.isna(s):
        return ''
    return str(s).lower().strip()

def fuzzy_match_score(str1, str2):
    return fuzz.ratio(normalize_string(str1), normalize_string(str2))

def _is_excluded_company(company_name):
    if not company_name or pd.isna(company_name):
        return True
    company_normalized = normalize_string(company_name)
    if not company_normalized:
        return True
    for excluded in EXCLUDED_COMPANIES_NORMALIZED:
        if excluded in company_normalized:
            return True
    return False

def format_data_for_csv(data, config_key):
    config = FORMATTER_CONFIGS.get(config_key)
    if not config:
        raise ValueError (f"Unknown configuration key: {config_key}")
    
    if not data:
        result = {}
        for field in config['fields']:
            output_name = field.replace(config.get('output_prefix', ''), '')
            if output_name.startswith('_'):
                output_name = output_name[1:]
            # Don't add 's' suffix - use exact column names from database schema
            column_name = f"{config.get('output_prefix', '')}_{output_name}"
            result[column_name] = 'NaN'
        return result
    
    result = {}
    date_fields = config.get('date_fields', [])
    use_braces = config.get('use_braces', False)

    for field in config['fields']:
        values = [item.get(field, '') for item in data]

        output_name = field.replace(config.get('output_prefix', ''), '')
        if output_name.startswith('_'):
            output_name = output_name[1:]

        # Don't add 's' suffix - use exact column names from database schema
        column_name = f"{config.get('output_prefix', '')}_{output_name}"

        if field in date_fields:
            result[column_name] = format_dates_for_csv(values)
        elif use_braces:
            result[column_name] = format_nested_with_braces(values)
        else:
            result[column_name] = format_list_for_csv(values)
    
    return result

def format_nested_with_braces(items):
    if not items:
        return 'NaN'
    formatted = []
    for item in items:
        if item and str(item).strip():
            formatted.append('{' + str(item) + '}')
        else:
            formatted.append('{NaN}')
    return ', '.join(formatted)

def format_list_for_csv(items):
    if not items:
        return ''
    return ', '.join([str(item) for item in items if item])

def format_dates_for_csv(date_items):
    if not date_items:
        return ''
    
    parsed_years = []
    for date_item in date_items:
        year = parse_year_from_date(date_item)
        if year and str(year).strip():
            parsed_years.append(year)
        else:
            parsed_years.append('NaN')
    
    return ', '.join(parsed_years)

def parse_year_from_date(date_string):
    if not date_string or pd.isna(date_string):
        return ''

    date_string = str(date_string).strip()

    if not date_string:
        return ''

    try:
        if date_string.isdigit() and len(date_string) == 4:
            return date_string

        if date_string.isdigit() and len(date_string) > 10:
            timestamp_ms = int(date_string)
            timestamp_s = timestamp_ms / 1000
            dt = datetime.fromtimestamp(timestamp_s)
            return str(dt.year)

        if date_string.isdigit() and len(date_string) == 10:
            timestamp_s = int(date_string)
            dt = datetime.fromtimestamp(timestamp_s)
            return str(dt.year)

        if '-' in date_string or '/' in date_string:
            dt = pd.to_datetime(date_string, errors='coerce')
            if pd.notna(dt):
                return str(dt.year)

        year_match = re.search(r'(19|20)\d{2}', date_string)
        if year_match:
            return year_match.group(0)

        return date_string

    except Exception as e:
        return ''

def format_multi_period_financials(financial_periods_list):
    """
    Format multi-period financial data (list of dicts) into comma-separated format
    Similar to how suppliers/customers are formatted

    Args:
        financial_periods_list: List of dicts, one per period, or single dict for backward compatibility

    Returns:
        Dictionary with comma-separated values for each financial metric
        Example: {'fin_total_revenue_usd000': '1000, 1200, 1500', 'fin_periodended': '2023, 2022, 2021'}
    """
    # Handle empty or None input
    if not financial_periods_list:
        return get_empty_financials()

    # Handle backward compatibility - if it's a single dict (old format), convert to list
    if isinstance(financial_periods_list, dict):
        financial_periods_list = [financial_periods_list]

    # If the list is empty, return empty financials
    if not financial_periods_list:
        return get_empty_financials()

    # Get all unique financial field names across all periods
    all_fields = set()
    for period_data in financial_periods_list:
        all_fields.update(period_data.keys())

    # Create result dictionary with comma-separated values for each field
    result = {}
    for field in all_fields:
        values = []
        for period_data in financial_periods_list:
            value = period_data.get(field)
            # Convert np.nan to 'NaN' string for consistency
            if pd.isna(value):
                values.append('NaN')
            else:
                values.append(str(value))

        # Join with comma and space (same format as suppliers)
        result[field] = ', '.join(values)

    # Ensure minimum fields exist
    if 'fin_periodended' not in result:
        result['fin_periodended'] = 'NaN'
    if 'fin_currency' not in result:
        result['fin_currency'] = 'NaN'
    if 'fin_magnitude' not in result:
        result['fin_magnitude'] = 'NaN'

    return result

def clean_html(text):
    if not text:
        return ""
    clean = re.sub('<[^<]+?>', '', str(text))
    clean = ' '.join(clean.split())
    return clean

def extract_value_from_html(cell_html):
    """
    Extract value from HTML cell content - can be numeric or text
    Extracts text immediately before </a> or </div>
    """
    if not cell_html:
        return np.nan

    cell_str = str(cell_html)

    # Check for NA/NM indicators
    if 'NA' in cell_str or 'NM' in cell_str:
        return np.nan

    # Skip if this looks like CSS classes (common issue)
    if 'hui-' in cell_str and '<' not in cell_str:
        return np.nan

    # Extract text before </a> or </div>
    text = None
    if '</a>' in cell_str:
        # Find text before </a>
        parts = cell_str.split('</a>')
        if len(parts) > 0:
            # Get everything after the last '>'
            before_close = parts[0]
            if '>' in before_close:
                text = before_close.split('>')[-1]
    elif '</div>' in cell_str:
        # Find text before </div>
        parts = cell_str.split('</div>')
        if len(parts) > 0:
            before_close = parts[0]
            if '>' in before_close:
                text = before_close.split('>')[-1]
    else:
        # No HTML tags, use as-is
        text = cell_str

    if not text or text.strip() in ['', 'NA', 'NM', '-']:
        return np.nan

    text = text.strip()

    # Try to convert to float
    try:
        text_clean = text.replace(',', '')

        # Handle negative numbers in parentheses
        if '(' in text_clean and ')' in text_clean:
            text_clean = '-' + text_clean.replace('(', '').replace(')', '')

        return float(text_clean)
    except (ValueError, AttributeError):
        # If not numeric, return text as-is (e.g., dates, currency codes)
        return text if text else np.nan
    
def is_valid_metric_name(metric_key):
    if not metric_key or not isinstance(metric_key, str):
        return False
    
    if len(metric_key) < 2:
        return False
    
    invalid_indicators = ['<', '>', '=', 'class', 'style', 'div', 'span', 'font-', 'hui-']
    metric_lower = metric_key.lower()
    
    if any(indicator in metric_lower for indicator in invalid_indicators):
        return False
    
    if metric_key in ['K_RH_STYLE', 'K_TOOL_TIP', 'K_PIVOT_STYLE']:
        return False
    
    return True

def fetch_with_retry(session, fetch_function, *args, max_retries = 2, **kwargs):
    rate_limiter.wait_if_needed()

    for attempt in range(max_retries + 1):
        try:
            result = fetch_function(session, *args, **kwargs)
            # Note: Empty results are valid - companies may have no suppliers/customers
            # Do NOT retry on empty results - this was causing 7s delays per company
            # Only retry on actual errors (caught in except blocks below)
            return result
        
        except requests.exceptions.HTTPError as e:
            if attempt < max_retries:
                backoff_time = 2 ** attempt
                if '401' in str(e) or 'Unauthorized' in str(e):
                    thread_safe_print(f"HTTP 401 Unauthorized error, refreshing cookies (attempt {attempt + 1}/{max_retries})")
                    cookie_manager.refresh_on_error(session)
                else:
                    thread_safe_print(f"HTTP error: {e}, retrying after {backoff_time} seconds (attempt {attempt + 1}/{max_retries})")
                cookie_manager.refresh_session_cookies(session)
                time.sleep(backoff_time)
            else:
                thread_safe_print(f"Failed after {max_retries} retries: {e}")
                return None
        
        except Exception as e:
            if attempt < max_retries:
                backoff_time = 2 ** attempt
                thread_safe_print(f"Error: {e}, retrying after cookie refresh (attempt {attempt + 1}/{max_retries})")
                cookie_manager.refresh_on_error(session)
                time.sleep(backoff_time)
            else:
                thread_safe_print(f"Failed after {max_retries} retries: {e}")
                return None
    
    return None

def safe_get(data, *keys, default=''):
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, {})
        else:
            return default
    return data if data != {} else default

def safe_get_list(data, *keys, default=None):
    result = safe_get(data, *keys, default=[])
    return result if isinstance(result, list) else default or []

def quick_relationship_check(session, company_id, debug=False):
    has_suppliers = False
    has_customers = False

    try:
        suppliers_params = {
            'mode': 'browser',
            'id': company_id,
            'keypage': '478395',
            'crossSiteFilterCurrency': 'USD',
            '_': str(int(time.time() * 1000))
        }

        rate_limiter.wait_if_needed()
        suppliers_response = session.get(REPORT_API_URL, params=suppliers_params, headers=HEADERS)

        if debug:
            thread_safe_print(f"[DEBUG] Suppliers API status: {suppliers_response.status_code}")

        if suppliers_response.status_code == 200:
            suppliers_data = suppliers_response.json()
            suppliers_controls = suppliers_data.get('controls', {})

            if debug:
                thread_safe_print(f"[DEBUG] Controls keys: {list(suppliers_controls.keys())[:10]}")

            supplier_source = suppliers_controls.get('section_1_control_38', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            previous_suppliers_source = suppliers_controls.get('section_1_control_45', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])

            if debug:
                thread_safe_print(f"[DEBUG] Supplier count: {len(supplier_source)}, Previous: {len(previous_suppliers_source)}")

            has_suppliers = len(supplier_source) > 0 or len(previous_suppliers_source) > 0
        else:
            if debug:
                thread_safe_print(f"[DEBUG] Suppliers API failed: {suppliers_response.text[:200]}")

        customer_params = {
            'mode': 'browser',
            'id': company_id,
            'keypage': '478397',
            'crossSiteFilterCurrency': 'USD',
            '_': str(int(time.time() * 1000))
        }

        rate_limiter.wait_if_needed()
        customers_response = session.get(REPORT_API_URL, params=customer_params, headers=HEADERS)

        if debug:
            thread_safe_print(f"[DEBUG] Customers API status: {customers_response.status_code}")

        if customers_response.status_code == 200:
            customers_data = customers_response.json()
            customers_controls = customers_data.get('controls', {})

            if debug:
                thread_safe_print(f"[DEBUG] Customer controls keys: {list(customers_controls.keys())[:10]}")

            customer_source = customers_controls.get('section_1_control_47', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            previous_customers_source = customers_controls.get('section_1_control_40', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])

            if debug:
                thread_safe_print(f"[DEBUG] Customer count: {len(customer_source)}, Previous: {len(previous_customers_source)}")

            has_customers = len(customer_source) > 0 or len(previous_customers_source) > 0
        else:
            if debug:
                thread_safe_print(f"[DEBUG] Customers API failed: {customers_response.text[:200]}")

    except Exception as e:
        if debug:
            thread_safe_print(f"[DEBUG] Exception in quick_relationship_check: {str(e)}")
        return (True, True)

    return (has_suppliers, has_customers)

def get_dataset_columns(dataset_file):
    dataset_key = dataset_file.replace('.csv', '')
    if dataset_key not in DATASET_COLUMN_MAPPING:
        return None, None
    mapping = DATASET_COLUMN_MAPPING[dataset_key]
    name_col = mapping['name_col']
    country_col = mapping['country_col']
    if not isinstance(name_col, list):
        name_col = [name_col]
    if not isinstance(country_col, list):
        country_col = [country_col]
    return name_col, country_col

def get_alternate_name_columns(primary_col):
    alternate_cols = []
    if primary_col in ['shipper', 'consignee', 'fname']:
        for alt in ['shipper', 'consignee', 'buyer', 'seller', 'fname', 'cname']:
            if alt != primary_col:
                alternate_cols.append(alt)
    return alternate_cols

# Main Execution Functions
#------------------------------------------------------------------------------------------------------------------------
def process_company_recursive(session, company_name, spciq_key, dataset_file, dataset_row_index,
                              dataset_country, found_in_column, parent_company='',
                              tier=1, max_tier=MAX_TIER, processed_companies=None,
                              tier_company_counts=None, api_matched_name='', api_country='',
                              mi_key='', last_cookie_refresh = None):

    cookie_manager.check_and_refresh_cookies(session)

    if processed_companies is None:
        processed_companies = set()
    if tier_company_counts is None:
        tier_company_counts = defaultdict(int)
    
    if spciq_key in processed_companies:
        return []

    # MAX_COMPANIES_PER_TIER restriction removed - process all companies
    # if tier_company_counts[tier] >= MAX_COMPANIES_PER_TIER:
    #     return []

    processed_companies.add(spciq_key)
    tier_company_counts[tier] += 1
    progress_tracker.increment_tier(tier)
    
    results = []

    skip_financials = False
    if tier == 1:
        thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Quick check: {company_name}")
        # Enable debug for first 3 tier-1 companies
        debug_enabled = tier_company_counts[tier] <= 3
        has_suppliers, has_customers = quick_relationship_check(session, spciq_key, debug=debug_enabled)

        if not has_suppliers and not has_customers:
            thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] No suppliers or customers found for {company_name}, skipping detailed fetch.")
            skip_financials = True
    
    thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Processing: {company_name}")
    all_data = fetch_all_company_data_parallel(session, spciq_key, company_name)

    # Check if account switching failed completely
    if all_data and all_data.get('_401_error_failed_switch'):
        thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Warning: Account switching failed for {company_name}, skipping...")
        return []

    # Check if 401 error occurred (no retry mode)
    if all_data and all_data.get('_401_error_no_retry'):
        thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Warning: 401 error for {company_name}, skipping to avoid saving incomplete data...")
        return []

    if not all_data or not isinstance(all_data, dict):
        thread_safe_print(f"Invalid data structure returned for {company_name}, skipping...")
        return []

    relationships = all_data.get('relationships', {'suppliers': [], 'customers': []})
    mining_data = all_data.get('mining', {'properties': []})
    profile_data = all_data.get('profile', get_empty_profile_data())
    financial_data_raw = all_data.get('financials', get_empty_financials())
    property_profiles = all_data.get('property_profiles', [])
    mining_locations = all_data.get('mining_locations', [])

    # Format multi-period financial data (list of dicts) into comma-separated format
    financial_data = format_multi_period_financials(financial_data_raw)

    if not isinstance(relationships, dict):
        relationships = {'suppliers': [], 'customers': []}

    suppliers = relationships.get('suppliers', [])
    customers = relationships.get('customers', [])
    previous_suppliers = relationships.get('previous_suppliers', [])
    previous_customers = relationships.get('previous_customers', [])

    #thread_safe_print(f"\n{'='*60}")
    #thread_safe_print(f"[DEBUG] Tier {tier} - {company_name}")
    #thread_safe_print(f"{'='*60}")
    #thread_safe_print(f"Found {len(suppliers)} suppliers, {len(customers)} customers")
    #
    ## Check if suppliers have key_instn
    #suppliers_with_keys = [s for s in suppliers if s.get('key_instn')]
    #customers_with_keys = [c for c in customers if c.get('key_instn')]
    #
    #thread_safe_print(f"Suppliers with key_instn: {len(suppliers_with_keys)}/{len(suppliers)}")
    #thread_safe_print(f"Customers with key_instn: {len(customers_with_keys)}/{len(customers)}")
    #
    ## Show first few examples
    #if suppliers:
    #    thread_safe_print(f"\nFirst supplier example:")
    #    first_supplier = suppliers[0]
    #    thread_safe_print(f"  Name: {first_supplier.get('supplier_name', 'N/A')}")
    #    thread_safe_print(f"  key_instn: {first_supplier.get('key_instn', 'MISSING')}")
    #    thread_safe_print(f"  All keys: {list(first_supplier.keys())}")
    #
    #thread_safe_print(f"Current tier: {tier}, Max tier: {max_tier}")
    #thread_safe_print(f"Will process next tier: {tier < max_tier}")
    #thread_safe_print(f"{'='*60}\n")

    supplier_data_formatted = format_data_for_csv(suppliers, 'suppliers')
    customer_data_formatted = format_data_for_csv(customers, 'customers')
    previous_supplier_data_formatted = format_data_for_csv(previous_suppliers, 'previous_suppliers')
    previous_customer_data_formatted = format_data_for_csv(previous_customers, 'previous_customers')
    tags_formatted = format_data_for_csv(profile_data.get('tags', []), 'tags')
    nace_formatted = format_data_for_csv(profile_data.get('nace_codes', []), 'nace_codes')
    naics_formatted = format_data_for_csv(profile_data.get('naics_codes', []), 'naics_codes')
    mining_properties_formatted = format_data_for_csv(mining_data.get('properties', []), 'mining_properties')
    property_profiles_formatted = format_data_for_csv(property_profiles, 'property_profiles')
    mining_location_formatted = format_mining_location_for_csv(mining_locations)
    
    update_country_match_stats(company_name, dataset_country, api_country)
    
    fuzzy_score = fuzzy_match_score(company_name, api_matched_name) if api_matched_name else 100.0

# Country Match our own metric calculation
    if dataset_country and dataset_country.strip():
        if api_country:
            country_match = (normalize_string(dataset_country) == normalize_string(api_country))
        else:
            country_match = False

        update_country_match_stats(company_name, dataset_country, api_country)

        country_match_pct = get_country_match_percentage(company_name)
        average_score = (fuzzy_score + country_match_pct) / 2.0

    else:
        country_match = 'NaN'
        country_match_pct = 0.0

        average_score = fuzzy_score

    result = {
       'tier': tier,
       'parent_company': parent_company,
       'dataset_file': dataset_file,
       'dataset_row_index': dataset_row_index,
       'company_name': company_name,
       'found_in_column': found_in_column,
       'dataset_country': dataset_country,
       'api_matched_name': api_matched_name if api_matched_name else company_name,
       'api_country': api_country,
       'spciq_key': spciq_key,
       'mi_key': mi_key,
       'sic_code': profile_data.get('sic_code', ''),
       'lei': profile_data.get('lei', ''),
       'local_registry_id': profile_data.get('local_registry_id', ''),
       'vat_id': profile_data.get('vat_id', ''),
       'fuzzy_match_score': round(fuzzy_score, 2),
       'country_match': country_match,
       'country_match_percentage': round(country_match_pct, 2),
       'average_match_score': round(average_score, 2),
       'tier_1_num_suppliers': len(suppliers),
       'tier_1_num_customers': len(customers),
       'num_mining_properties': len(mining_data.get('properties', [])),
       'num_property_profiles': len(property_profiles),
       'num_properties_with_locations': len(mining_locations),
       'company_description': profile_data.get('description', ''),
       'description_date': profile_data.get('description_date', ''),
       'description_source': profile_data.get('description_source', ''),
       'num_tags': len(profile_data.get('tags', [])),
       'num_nace_codes': len(profile_data.get('nace_codes', [])),
       'num_naics_codes': len(profile_data.get('naics_codes', [])),
       **supplier_data_formatted,
       **previous_supplier_data_formatted,
       **customer_data_formatted,
       **previous_customer_data_formatted,
       **mining_properties_formatted,
       **property_profiles_formatted,
       **mining_location_formatted,
       **tags_formatted,
       **nace_formatted,
       **naics_formatted,
       **financial_data
    }   

    result = clean_financial_columns(result)
    result = convert_empty_financials_to_nan(result)

    results.append(result)

    # Check for financials - now values are comma-separated strings
    has_financials = any(
        k.startswith('fin_') and v and str(v) not in ['NaN', 'nan', '', 'NaN, NaN']
        for k, v in financial_data.items()
    )
    fin_status = "Financials" if has_financials else "No financials"
    prop_status = f"{len(property_profiles)} property profiles" if property_profiles else "No property profiles"
    
    thread_safe_print(f"{'  ' * (tier-1)}  Found {len(suppliers)} suppliers, {len(customers)} customers | {fin_status} | {prop_status} | Country: {api_country}")

    if tier < max_tier:
        next_tier_companies = []


        for supplier in suppliers:
            try:
                # Get supplier info
                supplier_name = supplier.get('supplier_name')
                supplier_id = supplier.get('key_instn')

                # Validate supplier name
                if not supplier_name:
                    continue

                if _is_excluded_company(supplier_name):
                    continue
                
                # Initialize API data variables
                supplier_api_matched_name = supplier_name
                supplier_api_country = supplier.get('country', '')
                supplier_mi_key = ''

                # Handle missing key_instn by searching
                if not supplier_id:
                    time.sleep(RATE_LIMIT_DELAY)

                    try:
                        search_result = search_company(session, supplier_name)
                        supplier_id = search_result.get('spciq_key', '')
                        supplier_api_matched_name = search_result.get('matched_name', supplier_name)
                        supplier_api_country = search_result.get('country', '')
                        supplier_mi_key = search_result.get('mi_key', '')

                        if not supplier_id:
                            continue


                    except Exception as search_error:
                        continue

                # Check if already processed
                if supplier_id in processed_companies:
                    continue

                # Add to next tier processing queue
                next_tier_companies.append({
                    'company_name': supplier_name,
                    'spciq_key': supplier_id,
                    'mi_key': supplier_mi_key,
                    'found_in_column': f'tier_{tier}_supplier',
                    'api_matched_name': supplier_api_matched_name,
                    'api_country': supplier_api_country
                })


            except Exception as e:
                continue

        for customer in customers:
            try:
                # Get customer info
                customer_name = customer.get('customer_name')
                customer_id = customer.get('key_instn')

                # Validate customer name
                if not customer_name:
                    continue

                if _is_excluded_company(customer_name):
                    continue
                
                # Initialize API data variables
                customer_api_matched_name = customer_name
                customer_api_country = customer.get('country', '')
                customer_mi_key = ''

                # Handle missing key_instn by searching
                if not customer_id:
                    time.sleep(RATE_LIMIT_DELAY)

                    try:
                        search_result = search_company(session, customer_name)
                        customer_id = search_result.get('spciq_key', '')
                        customer_api_matched_name = search_result.get('matched_name', customer_name)
                        customer_api_country = search_result.get('country', '')
                        customer_mi_key = search_result.get('mi_key', '')

                        if not customer_id:
                            continue


                    except Exception as search_error:
                        continue

                # Check if already processed
                if customer_id in processed_companies:
                    continue

                # Add to next tier processing queue
                next_tier_companies.append({
                    'company_name': customer_name,
                    'spciq_key': customer_id,
                    'mi_key': customer_mi_key,
                    'found_in_column': f'tier_{tier}_customer',
                    'api_matched_name': customer_api_matched_name,
                    'api_country': customer_api_country
                })


            except Exception as e:
                continue
            

        if next_tier_companies:
            thread_safe_print(f"{'  ' * (tier-1)}[INFO] Processing {len(next_tier_companies)} companies in tier {tier+1} sequentially")

            # Process nested tiers sequentially to avoid deadlock
            for company_info in next_tier_companies:
                try:
                    tier_results = process_company_recursive(
                        session=session,
                        company_name=company_info['company_name'],
                        spciq_key=company_info['spciq_key'],
                        mi_key=company_info.get('mi_key', ''),
                        dataset_file=dataset_file,
                        dataset_row_index=dataset_row_index,
                        dataset_country='',
                        found_in_column=company_info['found_in_column'],
                        parent_company=company_name,
                        tier=tier + 1,
                        max_tier=max_tier,
                        processed_companies=processed_companies,
                        tier_company_counts=tier_company_counts,
                        api_matched_name=company_info['api_matched_name'],
                        api_country=company_info['api_country'],
                        last_cookie_refresh=last_cookie_refresh
                    )
                    if tier_results:
                        results.extend(tier_results)
                        thread_safe_print(f"{'  ' * (tier-1)}[SUCCESS] Collected {len(tier_results)} results from tier {tier+1}")
                except Exception as e:
                    thread_safe_print(f"{'  ' * (tier-1)}[ERROR] Processing next tier company: {e}")
                    import traceback
                    thread_safe_print(f"{'  ' * (tier-1)}[TRACE] {traceback.format_exc()}")

            thread_safe_print(f"{'  ' * (tier-1)}[COMPLETE] Finished processing tier {tier+1}")
        else:
            thread_safe_print(f"{'  ' * (tier-1)}[INFO] No companies to process in tier {tier+1}")

    return results

def process_single_company(company_info, session, max_tier, result_collector, csv_dir, output_file):
    try:
        thread_safe_print(f"\n{'='*70}")
        thread_safe_print(f"[PARALLEL] Processing: {company_info['company_name']}")
        thread_safe_print(f"{'='*70}")

        current_processed = result_collector.get_processed_companies()

        company_results = process_company_recursive(
            session=session,
            company_name=company_info['company_name'],
            spciq_key=company_info['spciq_key'],
            mi_key=company_info.get('mi_key', ''),
            dataset_file=company_info['dataset_file'],
            dataset_row_index=company_info['dataset_row_index'],
            dataset_country=company_info['dataset_country'],
            found_in_column=company_info['found_in_column'],
            parent_company='',
            tier=1,
            max_tier=max_tier,
            processed_companies=current_processed,
            api_matched_name=company_info['api_matched_name'],
            api_country=company_info['api_country']
        )

        if company_results:
            total_results = result_collector.add_results(company_results)
            thread_safe_print(f"[PARALLEL] Completed: {company_info['company_name']} ({len(company_results)} rows, total: {total_results})")

            # Check if we should save checkpoint
            if result_collector.should_checkpoint():
                all_results = result_collector.get_all_results()
                save_checkpoint(all_results)

                stats = result_collector.get_stats()
                thread_safe_print(f"\n{'='*70}")
                thread_safe_print(f"CHECKPOINT SAVED (time-based)")
                thread_safe_print(f"{'='*70}")
                thread_safe_print(f"Total rows: {stats['total_results']}")
                thread_safe_print(f"Unique companies: {stats['unique_companies']}")
                thread_safe_print(f"Checkpoint file: checkpoint_progress.pkl")

            # Check if we should save to database (incremental saves every 5 minutes)
            if result_collector.should_save_to_database():
                new_results = result_collector.get_new_results_since_last_db_save()
                if new_results:
                    thread_safe_print(f"\n{'='*70}")
                    thread_safe_print(f"[DATABASE] Time-based save triggered - {len(new_results)} new rows to save")
                    thread_safe_print(f"{'='*70}")

                    try:
                        # Convert new results to DataFrame
                        df_new = pd.DataFrame(new_results)

                        # Save to database (will create table if it doesn't exist)
                        save_dataframe_to_database(df_new, DB_CONFIG['table_name'])

                        thread_safe_print(f"[DATABASE] Successfully saved {len(new_results)} new rows to database")
                    except Exception as e:
                        thread_safe_print(f"[DATABASE ERROR] Failed to save to database: {e}")
                        import traceback
                        thread_safe_print(f"[DATABASE TRACE] {traceback.format_exc()}")
                thread_safe_print(f"{'='*70}\n")

        return True

    except Exception as e:
        thread_safe_print(f"[PARALLEL] Error processing {company_info['company_name']}: {e}")
        import traceback
        thread_safe_print(f"[TRACE] {traceback.format_exc()}")
        return False

def match_companies_to_datasets(df_api_results, session, csv_dir='csv_files_2022',
                                output_file='all_countries_results.csv', max_tier=MAX_TIER):
    global current_results
    
    df_found = df_api_results[df_api_results['mi_key'].notna()].copy()
    
    if df_found.empty:
        thread_safe_print("No companies with MI keys found")
        return
    
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"Starting {max_tier}-tier PARALLEL processing WITH FINANCIAL DATA...")
    thread_safe_print(f"Tier 1 companies will be processed in parallel batches")
    thread_safe_print(f"{'='*70}\n")
    
    company_to_api = {}
    for _, row in df_found.iterrows():
        company_norm = normalize_string(row['company_name'])
        company_to_api[company_norm] = row.to_dict()
    
    #load checkpoint
    # ========================================================================
    checkpoint_data = load_checkpoint()
    if checkpoint_data:
        existing_results = checkpoint_data['results']

        # Build set of already-processed root companies (tier 1)
        processed_root_companies = set()
        for result in existing_results:
            if result.get('tier') == 1:
                processed_root_companies.add(normalize_string(result.get('company_name', '')))

        # Build set of all processed spciq_keys
        global_processed_companies = {r['spciq_key'] for r in existing_results if 'spciq_key' in r}

        thread_safe_print(f"\n{'='*70}")
        thread_safe_print(f"checkpoint loaded")
        thread_safe_print(f"Existing results: {len(existing_results)} rows")
        thread_safe_print(f"Tier 1 companies already processed: {len(processed_root_companies)}")
        thread_safe_print(f"All unique companies processed: {len(global_processed_companies)}")
        thread_safe_print(f"{'='*70}\n")
    else:
        existing_results = []
        processed_root_companies = set()
        global_processed_companies = set()
        thread_safe_print("No checkpoint found, starting fresh\n")
    
    # Initialize thread-safe result collector
    result_collector = ThreadSafeResultCollector()
    result_collector.results = existing_results
    result_collector.processed_companies = global_processed_companies
    
    current_results = result_collector.get_all_results()

    datasets_to_process = TEST_DATASETS if TEST_MODE else csv_files_2022
    
    companies_to_process = []
    
    for dataset_file in datasets_to_process:
        file_path = Path(csv_dir) / dataset_file
        
        if not file_path.exists():
            thread_safe_print(f"Dataset not found: {dataset_file}")
            continue
        
        thread_safe_print(f"\nScanning dataset: {dataset_file}")
        
        df_dataset = pd.read_csv(file_path, dtype=str, low_memory=False).fillna('')
        
        name_cols, country_cols = get_dataset_columns(dataset_file)
        if not name_cols:
            thread_safe_print(f"No name columns configured for {dataset_file}")
            continue
        
        name_cols = [c for c in name_cols if c in df_dataset.columns]
        country_cols = [c for c in country_cols if c in df_dataset.columns]
        
        if not name_cols:
            thread_safe_print(f"No valid name columns found in {dataset_file}")
            continue
        
        primary_name_col = name_cols[0] if isinstance(name_cols[0], str) else name_cols[0]
        alternate_cols = get_alternate_name_columns(primary_name_col)
        search_cols = name_cols + [col for col in alternate_cols if col in df_dataset.columns]
        
        for idx, row in df_dataset.iterrows():
            for col in search_cols:
                company_in_dataset = str(row.get(col, '')).strip()
                
                if not company_in_dataset or _is_excluded_company(company_in_dataset):
                    continue
                
                company_norm = normalize_string(company_in_dataset)
                
                # Skip if already processed
                if company_norm in processed_root_companies:
                    continue
                
                if company_norm in company_to_api:
                    api_data = company_to_api[company_norm]
                    
                    dataset_country = ''
                    if country_cols:
                        for country_col in country_cols:
                            if country_col in row and row[country_col]:
                                dataset_country = str(row[country_col]).strip()
                                break
                    
                    spciq_key = api_data.get('spciq_key')
                    if not spciq_key or spciq_key in global_processed_companies:
                        continue

                    api_matched_name = api_data.get('matched_name', company_in_dataset)
                    api_country = api_data.get('country', '')
                    mi_key = api_data.get('mi_key', '')

                    companies_to_process.append({
                        'company_name': company_in_dataset,
                        'spciq_key': spciq_key,
                        'mi_key': mi_key,
                        'dataset_file': dataset_file,
                        'dataset_row_index': idx,
                        'dataset_country': dataset_country,
                        'found_in_column': col,
                        'api_matched_name': api_matched_name,
                        'api_country': api_country
                    })
                    
                    # Mark as queued
                    processed_root_companies.add(company_norm)
                    
                    break  # Found company in this column, move to next row
    
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"Companies to process")
    thread_safe_print(f"Total Tier 1 companies queued: {len(companies_to_process)}")
    thread_safe_print(f"Already processed (from checkpoint): {len(existing_results)}")
    thread_safe_print(f"{'='*70}\n")
    
    if not companies_to_process:
        thread_safe_print("All companies already processed")
        return

    #process company in parallel
    # ========================================================================
    max_parallel_tier1 = min(MAX_PARALLEL_TIER1_COMPANIES, len(companies_to_process))  # Start conservative with 5
    
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"STARTING PARALLEL PROCESSING")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Parallel Tier 1 workers: {max_parallel_tier1}")
    thread_safe_print(f"Companies to process: {len(companies_to_process)}")
    thread_safe_print(f"{'='*70}\n")
    
    start_time = time.time()
    completed = 0

    # Process companies in batches to avoid overwhelming the API
    # Batch size limits concurrent API load: batch_size × 4 futures × 2 API calls = total API load
    batch_size = max_parallel_tier1
    total_companies = len(companies_to_process)

    thread_safe_print(f"Processing {total_companies} companies in batches of {batch_size}")

    for batch_start in range(0, total_companies, batch_size):
        batch_end = min(batch_start + batch_size, total_companies)
        batch = companies_to_process[batch_start:batch_end]

        thread_safe_print(f"\nProcessing batch {batch_start//batch_size + 1}: companies {batch_start+1}-{batch_end} of {total_companies}")

        # Cookie reloading is now handled automatically by get_thread_session() based on file modification time
        # No need for explicit batch-level reloads

        # Submit this batch
        futures = {
            GLOBAL_EXECUTOR.submit(
                process_single_company,
                company_info,
                session,
                max_tier,
                result_collector,
                csv_dir,
                output_file
            ): company_info
            for company_info in batch
        }

        # Wait for this batch to complete before starting next batch
        for future in futures:
            try:
                result = future.result()
                completed += 1

                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = len(companies_to_process) - completed
                eta = remaining / rate if rate > 0 else 0

                if completed % 5 == 0:
                    thread_safe_print(f"\n{'='*70}")
                    thread_safe_print(f"PROGRESS UPDATE")
                    thread_safe_print(f"{'='*70}")
                    thread_safe_print(f"Completed: {completed}/{len(companies_to_process)}")
                    thread_safe_print(f"Processing rate: {rate:.2f} companies/sec")
                    thread_safe_print(f"Elapsed: {elapsed/60:.1f} min")
                    thread_safe_print(f"ETA: {eta/60:.1f} min")
                    thread_safe_print(f"{'='*70}\n")

            except Exception as e:
                thread_safe_print(f"Future failed: {e}")
    
    # final save
    # ========================================================================
    all_results = result_collector.get_all_results()

    if all_results:
        df_results = pd.DataFrame(all_results)

        # DATABASE EXPORT
        thread_safe_print(f"\n{'='*70}")
        thread_safe_print("SAVING TO DATABASE")
        thread_safe_print(f"{'='*70}")

        rows_inserted, df_sanitized = save_dataframe_to_database(df_results)

        thread_safe_print(f"Database: {DB_CONFIG['database']}")
        thread_safe_print(f"Table: {DB_CONFIG.get('table_name', 'scraper_results')}")
        thread_safe_print(f"Rows inserted: {rows_inserted}")

        # CSV EXPORT to Runs folder - use sanitized DataFrame for consistency
        thread_safe_print(f"\n{'='*70}")
        thread_safe_print("SAVING TO CSV")
        thread_safe_print(f"{'='*70}")

        csv_path = save_dataframe_to_csv(df_sanitized, RUN_FOLDER_PATH)

        if csv_path:
            thread_safe_print(f"CSV saved successfully")
        else:
            thread_safe_print(f"CSV save failed")

        thread_safe_print(f"\n{'='*70}")
        thread_safe_print("PROCESSING COMPLETE")
        thread_safe_print(f"{'='*70}")
        thread_safe_print(f"Total rows: {len(df_results)}")
        thread_safe_print(f"Unique companies: {len(result_collector.get_processed_companies())}")
        
        if 'tier' in df_results.columns:
            tier_counts = df_results['tier'].value_counts().sort_index()
            thread_safe_print(f"\nTIER BREAKDOWN:")
            for tier, count in tier_counts.items():
                thread_safe_print(f"  Tier {tier}: {count} companies")
        
        total_time = time.time() - start_time
        thread_safe_print(f"\nTotal processing time: {total_time/60:.1f} minutes")
        thread_safe_print(f"Average rate: {len(companies_to_process)/(total_time/60):.2f} companies/minute")
        thread_safe_print(f"{'='*70}\n")
        
        progress_tracker.print_progress()

    
    # Scoring statistics
    if 'fuzzy_match_score' in df_results.columns:
        thread_safe_print(f"FUZZY MATCH SCORE STATISTICS:")
        thread_safe_print(f"  Average fuzzy match score: {df_results['fuzzy_match_score'].mean():.2f}")
        thread_safe_print(f"  Minimum fuzzy match score: {df_results['fuzzy_match_score'].min():.2f}")
        thread_safe_print(f"  Maximum fuzzy match score: {df_results['fuzzy_match_score'].max():.2f}")
        
    if 'country_match' in df_results.columns:
        thread_safe_print(f"\nCOUNTRY MATCH STATISTICS:")

        country_match_count = (df_results['country_match'] == True).sum()
        
        # Count Tier 1 companies (those with actual country comparisons)
        tier_1_count = (df_results['country_match'] != 'NaN').sum()
        
        thread_safe_print(f"  Tier 1 companies (with dataset country): {tier_1_count}")
        thread_safe_print(f"  Tier 1 companies with country match: {country_match_count}")
        
        if tier_1_count > 0:
            match_rate = (country_match_count / tier_1_count * 100)
            thread_safe_print(f"  Tier 1 country match rate: {match_rate:.2f}%")
        
        tier_2_plus_count = (df_results['country_match'] == 'NaN').sum()
        thread_safe_print(f"  Tier 2+ companies (no dataset country): {tier_2_plus_count}")
        
        financial_cols = [col for col in df_results.columns if col.startswith('fin_')]
        if financial_cols:
            thread_safe_print(f"FINANCIAL DATA STATISTICS:")
            thread_safe_print(f"  Total financial columns: {len(financial_cols)}")
            
            companies_with_financials = 0
            for _, row in df_results.iterrows():
                has_data = any(pd.notna(row[col]) for col in financial_cols)
                if has_data:
                    companies_with_financials += 1
            
            thread_safe_print(f"  Companies with financial data: {companies_with_financials}")
            thread_safe_print(f"  Financial data coverage: {(companies_with_financials / len(df_results) * 100):.2f}%")
            
            key_metrics = ['fin_total_revenue_usd000', 'fin_net_income_usd000', 
                          'fin_market_capitalization_usd000', 'fin_total_enterprise_value_tev_usd000']
            for metric in key_metrics:
                if metric in df_results.columns:
                    non_null = df_results[metric].notna().sum()
                    if non_null > 0:
                        thread_safe_print(f"  {metric}: {non_null} companies have data")

        progress_tracker.print_progress()
        thread_safe_print(f"{'='*70}\n")
        
        thread_safe_print("Preview of results:")
        preview_cols = ['company_name', 'tier', 'fuzzy_match_score',
                       'country_match_percentage', 'average_match_score',
                       'tier_1_num_suppliers', 'tier_1_num_customers']
        
        # Add a few financial columns if available
        fin_preview_cols = ['fin_total_revenue_usd000', 'fin_net_income_usd000', 'fin_market_capitalization_usd000']
        for col in fin_preview_cols:
            if col in df_results.columns:
                preview_cols.append(col)
        
        available_cols = [col for col in preview_cols if col in df_results.columns]
        thread_safe_print(df_results[available_cols].head(10).to_string(index=False))

def format_mining_location_for_csv(mining_location_list):
    if not mining_location_list:
        return {
            'mine_decimal_degree': 'NaN',
            'mine_coordinate_accuracy': 'NaN',
            'Claim_ID': 'NaN',
            'Claim_Name': 'NaN',
            'Claim_Owners': 'NaN',
            'Claim_Type': 'NaN',
            'Claim_Status': 'NaN',
            'Date_Granted': 'NaN',
            'Expiry_Date': 'NaN',
            'Source': 'NaN',
            'Source_as_of_date': 'NaN'
        }
    
    def safe_str(value):
        return 'NaN' if value is None or value == '' else str(value)
    
    def format_nested_with_braces(items):
        if not items:
            return 'NaN'
        formatted_items = []
        for item in items:
            if item and str(item).strip():
                formatted_items.append('{' + str(item) + '}')
            else:
                formatted_items.append('{NaN}')
        return ', '.join(formatted_items)

    decimal_degrees = []
    accuracies = []
    claim_ids_per_property = []
    claim_names_per_property = []
    claim_owners_per_property = []
    claim_types_per_property = []
    claim_statuses_per_property = []
    dates_granted_per_property = []
    expiry_dates_per_property = []
    sources_per_property = []
    source_dates_per_property = []

    for location in mining_location_list:
        decimal_degrees.append(location.get('mine_decimal_degree', 'NaN'))
        accuracies.append(location.get('mine_coordinate_accuracy', 'NaN'))
        
        claims = location.get('claims', [])
        
        if not claims:
            claim_ids_per_property.append('NaN')
            claim_names_per_property.append('NaN')
            claim_owners_per_property.append('NaN')
            claim_types_per_property.append('NaN')
            claim_statuses_per_property.append('NaN')
            dates_granted_per_property.append('NaN')
            expiry_dates_per_property.append('NaN')
            sources_per_property.append('NaN')
            source_dates_per_property.append('NaN')
        else:
            claim_ids_per_property.append(', '.join([safe_str(c.get('Claim_ID')) for c in claims]))
            claim_names_per_property.append(', '.join([safe_str(c.get('Claim_Name')) for c in claims]))
            claim_owners_per_property.append(', '.join([safe_str(c.get('Claim_Owners')) for c in claims]))
            claim_types_per_property.append(', '.join([safe_str(c.get('Claim_Type')) for c in claims]))
            claim_statuses_per_property.append(', '.join([safe_str(c.get('Claim_Status')) for c in claims]))
            dates_granted_per_property.append(', '.join([safe_str(c.get('Date_Granted')) for c in claims]))
            expiry_dates_per_property.append(', '.join([safe_str(c.get('Expiry_Date')) for c in claims]))
            sources_per_property.append(', '.join([safe_str(c.get('Source')) for c in claims]))
            source_dates_per_property.append(', '.join([safe_str(c.get('Source_as_of_date')) for c in claims]))
    
    return {
        'mine_decimal_degree': format_nested_with_braces(decimal_degrees),
        'mine_coordinate_accuracy': format_nested_with_braces(accuracies),
        'Claim_ID': format_nested_with_braces(claim_ids_per_property),
        'Claim_Name': format_nested_with_braces(claim_names_per_property),
        'Claim_Owners': format_nested_with_braces(claim_owners_per_property),
        'Claim_Type': format_nested_with_braces(claim_types_per_property),
        'Claim_Status': format_nested_with_braces(claim_statuses_per_property),
        'Date_Granted': format_nested_with_braces(dates_granted_per_property),
        'Expiry_Date': format_nested_with_braces(expiry_dates_per_property),
        'Source': format_nested_with_braces(sources_per_property),
        'Source_as_of_date': format_nested_with_braces(source_dates_per_property)
    }

#API call #1: financial data
# =============================================================================
def fetch_financial_data(session, company_id, key_page="471751", return_all_periods=False):
    """
    Fetch financial data from S&P Capital IQ API

    Args:
        session: requests.Session with valid cookies
        company_id: S&P Capital IQ company ID
        key_page: API key page (default: "471751")
        return_all_periods: If True, returns list of dicts (one per period).
                           If False (default), returns single dict with latest period.

    Returns:
        If return_all_periods=False: Single dict with latest period data
        If return_all_periods=True: List of dicts, one per period
    """
    try:
        # Parameters with slider set to 100% (all periods)
        # Setting crossSiteFilterValue to empty string ensures no filtering (100% range)
        params = {
            'mode': 'browser',
            'id': company_id,  # lowercase 'id' not 'Id'
            'keypage': key_page,
            'crossSiteFilterCurrency': 'USD',
            'crossSiteSource': '1',
            'crossSiteFilterValue': '',  # Empty = no filtering = 100% slider position
            '_': str(int(time.time() * 1000))
        }

        headers = HEADERS.copy()
        csrf_token = session.cookies.get('x-csrf-token')
        if csrf_token:
            headers['x-csrf-token'] = csrf_token
        else:
            thread_safe_print(f"[WARNING] No CSRF token found in session cookies for Financial API")

        rate_limiter.wait_if_needed()
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=headers
        )

        progress_tracker.increment_api_calls()

        if response.status_code == 200:
            data = response.json()

            # DEBUG: Save raw API response for specific companies (for debugging)
            # Uncomment and add company IDs to debug specific companies
            DEBUG_COMPANY_IDS = []  # e.g., ['4985766'] for Axalta
            if company_id in DEBUG_COMPANY_IDS:
                import json
                debug_filename = f'debug_financial_api_{company_id}.json'
                try:
                    with open(debug_filename, 'w') as f:
                        json.dump(data, f, indent=2)
                    thread_safe_print(f"  DEBUG: Saved raw API response to {debug_filename}")
                except Exception as debug_e:
                    thread_safe_print(f"  DEBUG: Failed to save API response: {debug_e}")

            if 'controls' in data:
                return extract_all_financials(data, return_all_periods=return_all_periods)
            else:
                return None
        elif response.status_code == 401:
            thread_safe_print(f"Financial API returned 401 Unauthorized for {company_id}")
            return {'_401_error': True}  # Special flag to trigger cooldown
        else:
            thread_safe_print(f"Financial API returned status {response.status_code} for {company_id}")
            return None

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        thread_safe_print(f"Warning: Timeout fetching financials for {company_id} - attempting cookie refresh and retry")

        if refresh_cookies_and_retry(session, f"financials for {company_id}"):
            try:
                # Retry the request with fresh cookies and increased timeout
                response = session.get(REPORT_API_URL, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    thread_safe_print(f"Retry succeeded for financials {company_id}")
                    if 'controls' in data:
                        return extract_all_financials(data, return_all_periods=return_all_periods)
                    else:
                        return None
            except Exception as retry_e:
                thread_safe_print(f"Retry failed for financials {company_id}: {retry_e}")

        return None
    except Exception as e:
        return None

def extract_period_dates(controls):
    """
    Extract period dates from control_35 to use as column headers
    Returns: List of period values (e.g., ['2024FQ4', '2024FQ3', '2024FQ2', ...])
    """
    try:
        control_35 = controls.get('section_1_control_35', {})
        period_data = control_35.get('dataModel', {}).get('data', [])

        if not period_data:
            thread_safe_print(f"  Period extraction: control_35 has no data")
            return []

        # Extract period values in order
        periods = [period.get('PeriodValue', '').strip() for period in period_data]
        periods = [p for p in periods if p]  # Filter out empty strings

        if periods:
            thread_safe_print(f"  Period extraction: Found {len(periods)} periods (latest: {periods[0]})")
        else:
            thread_safe_print(f"  Period extraction: No valid periods found in {len(period_data)} period entries")

        return periods

    except Exception as e:
        thread_safe_print(f"  Error extracting period dates: {e}")
        import traceback
        traceback.print_exc()
        return []

#Control-specific parsing functions with universal pattern
def parse_control_50_key_financials(grid_model, period_dates=None):
    try:
        data_source = grid_model.get('dataSource', [])
        if not data_source or len(data_source) < 2:
            thread_safe_print(f"    Control 50: dataSource has insufficient rows ({len(data_source)})")
            return {}

        # First row = headers
        header_row = data_source[0]

        # All other rows = data (process all rows after header)
        data_rows = data_source[1:]

        thread_safe_print(f"    Control 50: Found {len(data_source)} total rows, processing {len(data_rows)} data rows")

        # Get all column keys from header row (sorted by key number)
        all_keys = sorted([k for k in header_row.keys() if k.startswith('_')],
                         key=lambda x: int(x[1:]) if x[1:].isdigit() else 999)

        thread_safe_print(f"    Control 50: Column keys: {all_keys}")

        # Extract column names from header row
        column_names = {}
        for key in all_keys:
            header_value = extract_value_from_html(header_row.get(key, ''))
            if header_value and not pd.isna(header_value):
                column_names[key] = header_value

        thread_safe_print(f"    Control 50: Found {len(column_names)} column headers")
        if column_names:
            sample_cols = list(column_names.items())[:3]
            thread_safe_print(f"    Control 50: Sample headers: {sample_cols}")

        # Extract data from middle rows
        financial_data = {}
        processed_count = 0

        for row in data_rows:
            # Find metric name: it's the key with text (not pure numbers, not dates)
            # It's usually the lowest or highest numbered key
            metric_name = None
            metric_key = None

            # Check first key and last 3 keys for metric name
            check_keys = [all_keys[0]] + all_keys[-3:]
            for key in check_keys:
                val = extract_value_from_html(row.get(key, ''))
                if val and not pd.isna(val):
                    val_str = str(val).strip()
                    # Check if it's text (not a pure number or date)
                    try:
                        float(val_str.replace(',', ''))
                        # It's a number, not a metric name
                        continue
                    except ValueError:
                        # Not a number - check if it's a date
                        if '/' in val_str and len(val_str) < 15:
                            # Likely a date like "12/31/2023"
                            continue
                        # This looks like a metric name!
                        if len(val_str) > 2:
                            metric_name = val
                            metric_key = key
                            break

            if not metric_name:
                continue

            # Clean metric name
            clean_metric = (str(metric_name)
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('%', 'pct')
                .replace('$', 'usd')
                .replace(',', '')
                .replace('-', '_')
                .replace('/', '_')
                .lower()
            )

            if len(clean_metric) < 3:
                continue

            # Data keys: all keys except the one with metric name and last 2 keys
            # Exclude metric_key and last 2 keys
            data_keys = [k for k in all_keys if k != metric_key and k not in all_keys[-2:]]

            if period_dates and len(period_dates) > 0:
                period_values = {}
                for idx, key in enumerate(data_keys):
                    if idx < len(period_dates) and key in row:
                        period = period_dates[idx]
                        value = extract_value_from_html(row.get(key, ''))
                        if not pd.isna(value):
                            period_values[period] = value

                if period_values:
                    financial_data[f'fin_{clean_metric}'] = period_values
                    processed_count += 1
            else:
                # Get latest value from last data column
                if data_keys:
                    value = extract_value_from_html(row.get(data_keys[-1], ''))
                    if not pd.isna(value):
                        financial_data[f'fin_{clean_metric}'] = value
                        processed_count += 1

        thread_safe_print(f"    Control 50: Extracted {len(financial_data)} metrics")

        if financial_data:
            sample_metrics = list(financial_data.keys())[:5]
            thread_safe_print(f"    Control 50: Sample metrics: {sample_metrics}")

        return financial_data

    except Exception as e:
        thread_safe_print(f"    Error parsing control_50: {e}")
        import traceback
        traceback.print_exc()
        return {}

def parse_control_56_valuation(grid_model, period_dates=None):
    """
    Parse control_56 (Valuation Multiples)
    Universal pattern:
    - First row: headers/column names
    - All other rows: data values (extract text before </a> or </div>)
    - Last 2 keys in each row: metadata (ignore)
    """
    try:
        data_source = grid_model.get('dataSource', [])
        if not data_source or len(data_source) < 2:
            thread_safe_print(f"    Control 56: dataSource has insufficient rows ({len(data_source)})")
            return {}

        # First row = headers
        header_row = data_source[0]

        # All other rows = data (process all rows after header)
        data_rows = data_source[1:]

        thread_safe_print(f"    Control 56: Found {len(data_source)} total rows, processing {len(data_rows)} data rows")

        # Get all column keys from header row (sorted by key number)
        all_keys = sorted([k for k in header_row.keys() if k.startswith('_')],
                         key=lambda x: int(x[1:]) if x[1:].isdigit() else 999)

        thread_safe_print(f"    Control 56: Column keys: {all_keys}")

        # Extract data from middle rows
        financial_data = {}

        for row in data_rows:
            # Find metric name: it's the key with text (not pure numbers, not dates)
            # It's usually the lowest or highest numbered key
            metric_name = None
            metric_key = None

            # Check first key and last 3 keys for metric name
            check_keys = [all_keys[0]] + all_keys[-3:]
            for key in check_keys:
                val = extract_value_from_html(row.get(key, ''))
                if val and not pd.isna(val):
                    val_str = str(val).strip()
                    # Check if it's text (not a pure number or date)
                    try:
                        float(val_str.replace(',', ''))
                        # It's a number, not a metric name
                        continue
                    except ValueError:
                        # Not a number - check if it's a date
                        if '/' in val_str and len(val_str) < 15:
                            # Likely a date like "12/31/2023"
                            continue
                        # This looks like a metric name!
                        if len(val_str) > 2:
                            metric_name = val
                            metric_key = key
                            break

            if not metric_name:
                continue

            # Clean metric name
            clean_metric = (str(metric_name)
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('%', 'pct')
                .replace('$', 'usd')
                .replace(',', '')
                .replace('-', '_')
                .replace('/', '_')
                .lower()
            )

            if len(clean_metric) < 3:
                continue

            # Data keys: all keys except the one with metric name and last 2 keys
            # Exclude metric_key and last 2 keys
            data_keys = [k for k in all_keys if k != metric_key and k not in all_keys[-2:]]

            if period_dates and len(period_dates) > 0:
                period_values = {}
                for idx, key in enumerate(data_keys):
                    if idx < len(period_dates) and key in row:
                        period = period_dates[idx]
                        value = extract_value_from_html(row.get(key, ''))
                        if not pd.isna(value):
                            period_values[period] = value

                if period_values:
                    financial_data[f'fin_{clean_metric}'] = period_values
            else:
                if data_keys:
                    value = extract_value_from_html(row.get(data_keys[-1], ''))
                    if not pd.isna(value):
                        financial_data[f'fin_{clean_metric}'] = value

        thread_safe_print(f"    Control 56: Extracted {len(financial_data)} metrics")

        if financial_data:
            sample_metrics = list(financial_data.keys())[:3]
            thread_safe_print(f"    Control 56: Sample metrics: {sample_metrics}")

        return financial_data

    except Exception as e:
        thread_safe_print(f"    Error parsing control_56: {e}")
        import traceback
        traceback.print_exc()
        return {}

def parse_control_60_capitalization(grid_model, period_dates=None):
    """
    Parse control_60 (Latest Capitalization)
    Structure: _16 = column name, _15 = value (extract text before </div> or </a>)
    Note: Process all rows, but skip last 2 keys in each row (metadata)
    """
    try:
        data_source = grid_model.get('dataSource', [])
        if not data_source:
            thread_safe_print(f"    Control 60: dataSource is empty")
            return {}

        # Process all rows (no row skipping)
        financial_data = {}

        thread_safe_print(f"    Control 60: Found {len(data_source)} rows")

        processed_count = 0
        skipped_count = 0

        for row in data_source:
            # Column name is in _16
            metric_key = row.get('_16', '')

            if not metric_key or metric_key.strip() == '':
                skipped_count += 1
                continue

            # Clean metric name
            clean_metric = (metric_key
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('%', 'pct')
                .replace('$', 'usd')
                .replace(',', '')
                .replace('-', '_')
                .replace('/', '_')
                .lower()
            )

            if len(clean_metric) < 3:
                skipped_count += 1
                continue

            # Value is in _15 - extract text before </div> or </a>
            value_html = row.get('_15', '')
            value = extract_value_from_html(value_html)

            # Store as single value (control_60 typically has latest data only)
            financial_data[f'fin_{clean_metric}'] = value
            processed_count += 1

        thread_safe_print(f"    Control 60: Processed {processed_count} metrics, skipped {skipped_count} rows")
        thread_safe_print(f"    Control 60: Extracted {len(financial_data)} metrics")

        if financial_data:
            sample_metrics = list(financial_data.keys())[:3]
            thread_safe_print(f"    Control 60: Sample metrics: {sample_metrics}")

        return financial_data

    except Exception as e:
        thread_safe_print(f"    Error parsing control_60: {e}")
        import traceback
        traceback.print_exc()
        return {}

#financial grid a bit messy, needs own parsing model (LEGACY - kept for backward compatibility)
def parse_financial_grid(grid_model, period_dates=None):
    """
    Parse financial grid and structure data by dates if period_dates provided

    Args:
        grid_model: The dataModel.model from control 50, 56, or 60
        period_dates: List of period values from control_35 (optional)

    Returns:
        If period_dates provided: {metric_name: {period: value, ...}, ...}
        Otherwise: {metric_name: latest_value} (backward compatible)
    """
    try:
        data_source = grid_model.get('dataSource', [])

        if not data_source:
            return {}

        financial_data = {}

        # Identify column keys (e.g., _19, _20, _21 for control_50 or _28, _29, _30 for others)
        # Skip only the label/key columns, not the data columns
        first_row = data_source[0] if data_source else {}
        skip_keys = ['_26', '_16', '_14', '_27', '_15', '_17', '_18']  # Removed _28 and _19 from skip list
        column_keys = sorted([k for k in first_row.keys() if k.startswith('_') and k not in skip_keys])

        for row in data_source:
            metric_key = row.get('_27') or row.get('_17') or row.get('_15', '')

            if not metric_key or metric_key.strip() == '':
                continue

            metric_key_lower = str(metric_key).lower()

            invalid_indicators = [
                '<div', '</div', 'class=', 'style=', '<span', '</span',
                'font-', 'hui-', 'hui_', 'k_rh_', 'k_tool_', 'k_pivot_',
                'padding', 'margin', 'border', 'align', 'text-', 'color:',
                'width', 'height', 'display', 'position', 'float'
            ]

            if any(indicator in metric_key_lower for indicator in invalid_indicators):
                continue

            if metric_key in ['K_RH_STYLE', 'K_TOOL_TIP', 'K_PIVOT_STYLE']:
                continue

            if '<' in str(metric_key) or '>' in str(metric_key) or '=' in str(metric_key):
                continue

            if len(metric_key.strip()) < 3:
                continue

            clean_metric = (metric_key
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('%', 'pct')
                .replace('$', 'usd')
                .replace(',', '')
                .replace('-', '_')
                .replace('/', '_')
                .lower()
            )

            if len(clean_metric) <= 2:
                continue
            if any(c in clean_metric for c in ['<', '>', '=']):
                continue

            parts = clean_metric.split('_')
            if all(len(part) <= 2 for part in parts if part):
                continue

            # If period_dates provided, extract all period values
            if period_dates and len(period_dates) > 0:
                period_values = {}

                for idx, col_key in enumerate(column_keys):
                    if idx < len(period_dates):
                        period = period_dates[idx]
                        value = extract_value_from_html(row.get(col_key, ''))
                        period_values[period] = value

                financial_data[f'fin_{clean_metric}'] = period_values
            else:
                # Backward compatible: just get latest value
                value_keys = [k for k in row.keys() if k.startswith('_') and k not in skip_keys]
                if value_keys:
                    latest_key = sorted(value_keys)[-1]
                    value = extract_value_from_html(row.get(latest_key, ''))
                    financial_data[f'fin_{clean_metric}'] = value

        return financial_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}

    # Duplicate exception handler removed (was unreachable dead code)
    # except Exception as e:
    #     import traceback
    #     traceback.print_exc()
    #     return {}

def extract_all_financials(api_response, return_all_periods=False):
    """
    Extract all financial data with proper date structure

    Args:
        api_response: Raw API response from S&P Capital IQ
        return_all_periods: If True, returns data for all periods. If False, returns latest only.

    Returns dictionary with either:
    - Structured data: {'periods': [...], 'key_financials': {...}, 'valuation_multiples': {...},
                       'capitalization': {...}, 'metadata': {...}}
    - Or flat data for backward compatibility (if no periods found)
    """
    if not api_response:
        return get_empty_financials()

    try:
        controls = api_response.get('controls', {})

        if not controls:
            return get_empty_financials()

        control_keys = list(controls.keys())
        thread_safe_print(f"  Found {len(control_keys)} controls in financial response")

        # Extract period dates first
        period_dates = extract_period_dates(controls)

        if period_dates and len(period_dates) > 0:
            # Structured extraction with dates
            thread_safe_print(f"  Found {len(period_dates)} periods, extracting structured data")

            result = {
                'periods': period_dates,
                'key_financials': {},
                'valuation_multiples': {},
                'capitalization': {},
                'metadata': {}
            }

            # Extract Key Financials (control 50) with dates - USES SPECIFIC PARSER
            key_financials_control = controls.get('section_1_control_50', {})
            if key_financials_control:
                key_financials_model = key_financials_control.get('dataModel', {}).get('model', {})
                if key_financials_model:
                    result['key_financials'] = parse_control_50_key_financials(key_financials_model, period_dates)

            # Extract Valuation Multiples (control 56) with dates - USES SPECIFIC PARSER
            valuation_control = controls.get('section_1_control_56', {})
            if valuation_control:
                valuation_model = valuation_control.get('dataModel', {}).get('model', {})
                if valuation_model:
                    result['valuation_multiples'] = parse_control_56_valuation(valuation_model, period_dates)

            # Extract Capitalization (control 60) with dates - USES SPECIFIC PARSER
            capitalization_control = controls.get('section_1_control_60', {})
            if capitalization_control:
                capitalization_model = capitalization_control.get('dataModel', {}).get('model', {})
                if capitalization_model:
                    result['capitalization'] = parse_control_60_capitalization(capitalization_model, period_dates)

            # Extract metadata
            result['metadata'] = extract_financial_metadata(controls)

            # Check if we got any data
            total_metrics = len(result['key_financials']) + len(result['valuation_multiples']) + len(result['capitalization'])

            if total_metrics == 0:
                thread_safe_print(f"  No financial metrics extracted")
                return get_empty_financials()

            thread_safe_print(f"  Total metrics extracted: {total_metrics}")

            # Flatten for database storage
            return flatten_financial_data_for_db(result, return_all_periods=return_all_periods)

        else:
            # Fallback: no periods found, use backward compatible extraction
            thread_safe_print(f"  No periods found, using backward compatible extraction")

            # If return_all_periods=True but no periods found, return empty list
            if return_all_periods:
                thread_safe_print(f"  No periods available, returning empty list")
                return []

            all_financials = {}

            key_financials_control = controls.get('section_1_control_50', {})
            if key_financials_control:
                key_financials_model = key_financials_control.get('dataModel', {}).get('model', {})
                if key_financials_model:
                    key_financials = parse_financial_grid(key_financials_model)
                    all_financials.update(key_financials)
                    thread_safe_print(f"  Extracted {len(key_financials)} key financial metrics")

            valuation_control = controls.get('section_1_control_56', {})
            if valuation_control:
                valuation_model = valuation_control.get('dataModel', {}).get('model', {})
                if valuation_model:
                    valuation_data = parse_financial_grid(valuation_model)
                    all_financials.update(valuation_data)
                    thread_safe_print(f"  Extracted {len(valuation_data)} valuation metrics")

            capitalization_control = controls.get('section_1_control_60', {})
            if capitalization_control:
                capitalization_model = capitalization_control.get('dataModel', {}).get('model', {})
                if capitalization_model:
                    capitalization_data = parse_financial_grid(capitalization_model)
                    all_financials.update(capitalization_data)
                    thread_safe_print(f"  Extracted {len(capitalization_data)} capitalization metrics")

            metadata = extract_financial_metadata(controls)
            all_financials.update(metadata)

            if not all_financials or all(pd.isna(v) or v == '' for v in all_financials.values()):
                thread_safe_print(f"  No financial metrics extracted")
                return get_empty_financials()

            thread_safe_print(f"  Total financial fields extracted: {len(all_financials)}")
            return all_financials

    except Exception as e:
        thread_safe_print(f"  Error extracting financials: {e}")
        import traceback
        traceback.print_exc()
        return get_empty_financials()

def flatten_financial_data_for_db(structured_data, return_all_periods=False):
    """
    Flatten the structured financial data for database storage

    Args:
        structured_data: Output from extract_all_financials with structure:
                        {'periods': [...], 'key_financials': {...}, 'valuation_multiples': {...},
                         'capitalization': {...}, 'metadata': {...}}
        return_all_periods: If True, returns list of dicts (one per period).
                           If False (default), returns single dict with latest period only.

    Returns:
        If return_all_periods=False: Flat dictionary with latest period (backward compatible)
        If return_all_periods=True: List of flat dictionaries, one per period
    """
    if not structured_data or not structured_data.get('periods'):
        # Return empty financials
        return get_empty_financials() if not return_all_periods else []

    periods = structured_data['periods']

    if not periods:
        return get_empty_financials() if not return_all_periods else []

    metadata = structured_data.get('metadata', {})

    if return_all_periods:
        # Return ALL periods as a list of flat dictionaries
        all_periods_data = []

        for period in periods:
            flat_data = {}

            # Flatten all metrics for this period
            for category in ['key_financials', 'valuation_multiples', 'capitalization']:
                metrics = structured_data.get(category, {})
                for metric_name, period_values in metrics.items():
                    if isinstance(period_values, dict) and period in period_values:
                        flat_data[metric_name] = period_values[period]
                    else:
                        flat_data[metric_name] = np.nan

            # Add metadata (same for all periods)
            flat_data['fin_currency'] = metadata.get('currency', np.nan)
            flat_data['fin_magnitude'] = metadata.get('magnitude', np.nan)

            # Add period identifier
            flat_data['fin_periodended'] = period

            all_periods_data.append(flat_data)

        thread_safe_print(f"  Extracted data for {len(all_periods_data)} periods")
        return all_periods_data

    else:
        # Return only LATEST period (backward compatible)
        latest_period = periods[0]
        flat_data = {}

        thread_safe_print(f"  Flattening data using latest period: {latest_period}")

        # Flatten all metrics using latest period
        for category in ['key_financials', 'valuation_multiples', 'capitalization']:
            metrics = structured_data.get(category, {})
            for metric_name, period_values in metrics.items():
                if isinstance(period_values, dict) and latest_period in period_values:
                    flat_data[metric_name] = period_values[latest_period]
                else:
                    flat_data[metric_name] = np.nan

        # Add metadata
        flat_data['fin_currency'] = metadata.get('currency', np.nan)
        flat_data['fin_magnitude'] = metadata.get('magnitude', np.nan)

        # Add period ended
        flat_data['fin_periodended'] = latest_period

        # Report metrics extracted (no filtering - dynamic schema)
        populated_fields = [f for f in flat_data.keys() if not pd.isna(flat_data[f])]
        thread_safe_print(f"  Metrics extracted for database: {len(populated_fields)} total")

        # Show sample of metrics
        if populated_fields:
            sample = list(populated_fields)[:5]
            thread_safe_print(f"  Sample metrics: {sample}")

        return flat_data

def extract_financial_metadata(controls):
    try:
        currency_control = controls.get('section_1_control_39', {})
        currency_data = currency_control.get('dataModel', {})
        selected_currency = currency_data.get('options', {}).get('Selected', '')
        magnitude_control = controls.get('section_1_control_45', {})
        magnitude_data = magnitude_control.get('dataModel', {})
        selected_magnitude = magnitude_data.get('options', {}).get('Selected', '')
        
        magnitude_map = {
            '3': 'Thousands',
            '6': 'Millions',
            '9': 'Billions'
        }
        
        currency = selected_currency if selected_currency and selected_currency.strip() else np.nan
        magnitude = magnitude_map.get(str(selected_magnitude), np.nan) if selected_magnitude else np.nan
        
        return {
            'fin_currency': currency,
            'fin_magnitude': magnitude
        }
    except:
        return {
            'fin_currency': np.nan,
            'fin_magnitude': np.nan
        }

def get_empty_financials():
    """
    Return empty financial structure for companies without financials
    Uses np.nan which is MySQL-compatible (stored as NULL)

    For dynamic schema: just return empty dict or minimal fields
    """
    # Return minimal structure for companies with no financial data
    return {
        'fin_periodended': np.nan,
        'fin_currency': np.nan,
        'fin_magnitude': np.nan
    }

def clean_financial_columns(result_dict):
    invalid_patterns = [
        'hui_cr',
        'hui-cr',
        'hui_align',
        'hui-align',
        'hui_bold',
        'hui-bold',
        'hui_indent',
        'hui-indent',
        'hui_',          
        'hui-',       
        'k_rh_style',
        'k_tool_tip',
        'k_pivot_style',
        'k_rh_',          
        'k_tool_',    
        'k_pivot_',       
        '<div',
        '</div',
        'class=',
        'style=',
        '<span',
        '</span',
        'font-',
        'font_',
        'color:',
        'color_',
        'padding',
        'margin',
        'border',
        'width',
        'height',
        'display',
        'position',
        'float',
        'align',
        'text_',
        'background',
    ]
    
    def is_likely_css_artifact(key):
        key_lower = key.lower()
        
        for pattern in invalid_patterns:
            if pattern in key_lower:
                return True
        
        if key.startswith('fin_'):
            metric_part = key[4:]  
            if len(metric_part) <= 2:
                return True

            if '_' in metric_part:
                parts = metric_part.split('_')
                if all(len(part) <= 2 for part in parts if part):
                    return True

        if '<' in key or '>' in key or '=' in key:
            return True
        
        return False
    
    keys_to_remove = []
    
    for key in list(result_dict.keys()):
        if key.startswith('fin_'):
            key_lower = key.lower()

            if is_likely_css_artifact(key):
                keys_to_remove.append(key)
                continue

            if len(key) < 8:
                keys_to_remove.append(key)
                continue

    for key in keys_to_remove:
        del result_dict[key]

    return result_dict

def convert_empty_financials_to_nan(result_dict):
    for key in list(result_dict.keys()):
        if key.startswith('fin_'):
            value = result_dict[key]
            
            if value is None:
                result_dict[key] = np.nan
            elif isinstance(value, str):
                if value.strip() == '' or value.strip().upper() in ['NA', 'NM', 'NAN', 'NONE', '-']:
                    result_dict[key] = np.nan
            elif pd.isna(value):
                result_dict[key] = np.nan
    
    return result_dict

#API call #2: company profile data
# =============================================================================
def fetch_company_profile_data(session, company_id):
    profile_url = "https://www.capitaliq.spglobal.com/apisv3/hydra-service/v1/data/render/399557"
    params = {
        'id': company_id, 'premiumPreqin': '0', 'CurrencyOverride': 'USD',
        'InstnProfile': '1', 'AccessGeneral': '1', 'ComputedCurrency': 'USD',
        'IsReportedCurrency': 'false', 'InstnCurrent': '1', 'StatEntityType': '-1',
        'corporatedata': '1', 'topigTag': 'true', 'skipInstnTexts': 'false',
        'IncludeIndustryDetail': 'true', 'IncludeIndustryNAICSDetail': 'true',
        'isGICS': '1', 'showCIQFinl': '1', 'showSNLFinl': '0',
        'showThirdPartyFinl': '0', 'IncludeCrunchbase': 'false',
        'FinlCurrency': 'USD', 'userTimeZone': 'Eastern Standard Time',
        'SectionExpanded': '1', 'IsDocumentSectionExpanded': 'true',
        'disableDocsQuickLinks': '0', 'IsMnM': '1', 'showSPACDataWidget': '1'
    }
    
    try:
        # Add CSRF token to headers if available
        headers = HEADERS.copy()
        csrf_token = session.cookies.get('x-csrf-token')
        if csrf_token:
            headers['x-csrf-token'] = csrf_token

        rate_limiter.wait_if_needed()
        response = session.get(profile_url, params=params, headers=headers)
        progress_tracker.increment_api_calls()

        if response.status_code == 401:
            thread_safe_print(f"Profile API returned 401 Unauthorized for {company_id}")
            profile_data = get_empty_profile_data()
            profile_data['_401_error'] = True  # Special flag to trigger cooldown
            return profile_data
        elif response.status_code != 200:
            thread_safe_print(f"Profile API returned status {response.status_code} for {company_id}")
            return get_empty_profile_data()

        data = response.json()
            
            #corp_data = data.get('Data', {}).get('CorpData_CommonBatch', {}).get('data', {})
            #
            #if not corp_data:
            #    thread_safe_print(f"No CorpData_CommonBatch found for {company_id}")
            #    return get_empty_profile_data()
            #
            #sic_code = ''
            #activity_code = corp_data.get('ActivityCode', {})
            #if isinstance(activity_code, dict):
            #    sic_code = activity_code.get('SIC', '')
            #    if sic_code:
            #        thread_safe_print(f"SIC Code: {sic_code}")
            #    else:
            #        thread_safe_print(f"No SIC Code found")
            #else:
            #    thread_safe_print(f"ActivityCode has unexpected type: {type(activity_code)}")

        #get sic codes
        sic_code = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ActivityCode', 'SIC')
        #get nace codes
        nace_codes = []
        isntn_classes = safe_get_list(data, 'Data', 'CorpData_CommonBatch', 'data', 'NaceCode', 'InstnIndustryClasss')

        for industry_class in isntn_classes:
            nace_tree = safe_get(industry_class, 'NACEIndustryTree', default=None)
            if nace_tree:
                nace_codes.append({
                    'nace_code': safe_get(nace_tree, 'NACECode'),
                    'industry': safe_get(nace_tree, 'NACEIndustry')
                })
            
            #if isinstance(nace_data, dict):
            #    instn_industry_class = nace_data.get('InstnIndustryClasss', [])
            #    
            #    if isinstance(instn_industry_class, list):
            #        thread_safe_print(f"  [Profile] Found {len(instn_industry_class)} NACE industry classes")
            #        
            #        for industry_class in instn_industry_class:
            #            if not isinstance(industry_class, dict):
            #                continue
            #            
            #            nace_tree = industry_class.get('NACEIndustryTree')
            #            
            #            if not nace_tree or not isinstance(nace_tree, dict):
            #                continue
            #            
            #            nace_code = nace_tree.get('NACECode', '')
            #            nace_industry = nace_tree.get('NACEIndustry', '')
            #            
            #            if nace_code or nace_industry:
            #                nace_codes.append({
            #                    'nace_code': nace_code,
            #                    'nace_industry': nace_industry
            #                })
            #                thread_safe_print(f"  [Profile] NACE: {nace_code} - {nace_industry}")
            #    
            #    if not nace_codes:
            #        thread_safe_print(f"No NACE codes found")
            #else:
            #    thread_safe_print(f"NaceCode has unexpected type: {type(nace_data)}")
            
            #get lei, local registry id, vat id
            lei_values = safe_get_list(data, 'Data', 'CorpData_CommonBatch', 'data', 'LEIQuery', 'value')
            lei_data = lei_values[0] if lei_values else {}
            lei = safe_get(lei_data, 'LegalEntityID')
            local_registry_id = safe_get(lei_data, 'LocalRegistryID')
            vat_id = safe_get(lei_data, 'VATID')
            
            #if isinstance(lei_query, dict):
            #    lei_values = lei_query.get('value', [])
            #    
            #    if isinstance(lei_values, list) and len(lei_values) > 0:
            #        first_lei = lei_values[0]
            #        
            #        if isinstance(first_lei, dict):
            #            lei = first_lei.get('LegalEntityID', '')
            #            local_registry_id = first_lei.get('LocalRegistryID', '')
            #            vat_id = first_lei.get('VATID', '')
            #            
            #            thread_safe_print(f"LEI: {lei}")
            #            thread_safe_print(f"Local Registry ID: {local_registry_id}")
            #            thread_safe_print(f"VAT ID: {vat_id}")
            #        else:
            #            thread_safe_print(f"LEI value has unexpected type: {type(first_lei)}")
            #    else:
            #        thread_safe_print(f"No LEI data found")
            #else:
            #    thread_safe_print(f"LEIQuery has unexpected type: {type(lei_query)}")
            
            #get naics codes
            naics_codes = []
            naics_data = safe_get_list(data, 'Data', 'CorpData_CommonBatch', 'data', 'IndustryNAICS', 'value')
            
            for naics_item in naics_data:
                naics_tree = safe_get(naics_item, 'NAICSTree', default=None)
                if naics_tree:
                    naics_codes.append({
                        'naics_code': safe_get(naics_tree, 'NAICS'),
                        'description': safe_get(naics_tree, 'NAICSDesc'),
                        'relationship_level': safe_get(naics_item, 'KeyIndustryRelationshipLevel')
                    })
        
            #if isinstance(naics_data, list):
            #    thread_safe_print(f"Found {len(naics_data)} NAICS codes")
            #    
            #    for naics_item in naics_data:
            #        if not isinstance(naics_item, dict):
            #            continue
            #        
            #        naics_tree = naics_item.get('NAICSTree', {})
            #        
            #        if isinstance(naics_tree, dict):
            #            naics_code = naics_tree.get('NAICS', '')
            #            naics_desc = naics_tree.get('NAICSDesc', '')
            #            key_relationship = naics_item.get('KeyIndustryRelationshipLevel', '')
            #            
            #            if naics_code:
            #                naics_codes.append({
            #                    'naics_code': naics_code,
            #                    'naics_desc': naics_desc,
            #                    'relationship_level': key_relationship
            #                })
            #                thread_safe_print(f"NAICS: {naics_code} - {naics_desc} (Level: {key_relationship})")
            #
            #if not naics_codes:
            #    thread_safe_print(f"No NAICS codes found")

            #extract tags
            tags = []
            topic_tag_data = safe_get_list(data, 'Data', 'TopicTag', 'data', 'value')

            for item in topic_tag_data:
                tag_name = safe_get(item, 'IndustrySegmentTagSourced', 'IndustryTopicTag', 'IndustryTopicTag')
                trust_rank = safe_get(item, 'IndustrySegmentTagSourced', 'TopicTagTrustRank')
                
                if tag_name:
                    tags.append({'tag_name': tag_name, 'trust_rank': trust_rank})
            
            #for item in topic_tag_data:
            #    industry_segment = item.get('IndustrySegmentTagSourced', {})
            #    industry_topic = industry_segment.get('IndustryTopicTag', {})
            #    tag_name = industry_topic.get('IndustryTopicTag', '')
            #    trust_rank = industry_segment.get('TopicTagTrustRank', '')
            #    if tag_name:
            #        tags.append({'tag_name': tag_name, 'trust_rank': trust_rank})
            #
            #extended_info = corp_data.get('ExtendedInfo', {})
            #instn_texts = extended_info.get('InstnTexts', {})
            #
            #if instn_texts is None:
            #    instn_texts = {}
            #
            #country = ''
            #core_data = corp_data.get('CoreDataBatch', {})
            #if core_data:
            #    country = core_data.get('CountryName', '') or core_data.get('Country', '')
            #
            #if not country:
            #    basic_info = corp_data.get('BasicInfo', {})
            #    country = basic_info.get('CountryName', '') or basic_info.get('Country', '')
            #
            #if not country:
            #    address_info = extended_info.get('AddressInfo', {})
            #    country = address_info.get('CountryName', '') or address_info.get('Country', '')
            #
            #if not country:
            #    if isinstance(activity_code, dict):
            #        country = activity_code.get('KeyCountry', '')
            
            #extract description
            description = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnDescription')
            description_date = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnTextAsOf')
            description_source = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnTextSource')

            #extract country
            country = (
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'CoreDataBatch', 'CountryName') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'CoreDataBatch', 'Country') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'BasicInfo', 'CountryName') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'BasicInfo', 'Country') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'AddressInfo', 'CountryName') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'AddressInfo', 'Country') or
                safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ActivityCode', 'KeyCountry')
            )

            result = {
                'tags': tags,
                'description': description,
                'description_date': description_date,
                'description_source': description_source,
                'country': country,
                'nace_codes': nace_codes,
                'sic_code': sic_code,
                'lei': lei,
                'local_registry_id': local_registry_id,
                'vat_id': vat_id,
                'naics_codes': naics_codes
            }
            
            return result

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        thread_safe_print(f"Warning: Timeout fetching profile for {company_id} - attempting cookie refresh and retry")

        if refresh_cookies_and_retry(session, f"profile for {company_id}"):
            try:
                # Retry with fresh cookies and increased timeout
                response = session.get(profile_url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    thread_safe_print(f"Retry succeeded for profile {company_id}")
                    data = response.json()

                    # Parse the profile data using the same logic as above
                    nace_codes = []
                    nace_data = safe_get_list(data, 'Data', 'CorpData_CommonBatch', 'data', 'IndustryNACE', 'value')
                    for nace_item in nace_data:
                        nace_tree = safe_get(nace_item, 'NACETree', default=None)
                        if nace_tree:
                            nace_codes.append({
                                'nace_code': safe_get(nace_tree, 'NACECode'),
                                'nace_desc': safe_get(nace_tree, 'NACECodeDesc'),
                                'relationship_level': safe_get(nace_item, 'KeyIndustryRelationshipLevel')
                            })

                    sic_code = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'IndustrySIC', 'value', 0, 'SICTree', 'SICCode')
                    lei = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'LEIQuery', 'value', 0, 'LegalEntityID')
                    local_registry_id = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'LEIQuery', 'value', 0, 'LocalRegistryID')
                    vat_id = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'LEIQuery', 'value', 0, 'VATID')

                    naics_codes = []
                    naics_data = safe_get_list(data, 'Data', 'CorpData_CommonBatch', 'data', 'IndustryNAICS', 'value')
                    for naics_item in naics_data:
                        naics_tree = safe_get(naics_item, 'NAICSTree', default=None)
                        if naics_tree:
                            naics_codes.append({
                                'naics_code': safe_get(naics_tree, 'NAICS'),
                                'description': safe_get(naics_tree, 'NAICSDesc'),
                                'relationship_level': safe_get(naics_item, 'KeyIndustryRelationshipLevel')
                            })

                    tags = []
                    topic_tag_data = safe_get_list(data, 'Data', 'TopicTag', 'data', 'value')
                    for item in topic_tag_data:
                        tag_name = safe_get(item, 'IndustrySegmentTagSourced', 'IndustryTopicTag', 'IndustryTopicTag')
                        trust_rank = safe_get(item, 'IndustrySegmentTagSourced', 'TopicTagTrustRank')
                        if tag_name:
                            tags.append({'tag_name': tag_name, 'trust_rank': trust_rank})

                    description = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnDescription')
                    description_date = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnTextAsOf')
                    description_source = safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'InstnTexts', 'InstnTextSource')

                    country = (
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'CoreDataBatch', 'CountryName') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'CoreDataBatch', 'Country') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'BasicInfo', 'CountryName') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'BasicInfo', 'Country') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'AddressInfo', 'CountryName') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ExtendedInfo', 'AddressInfo', 'Country') or
                        safe_get(data, 'Data', 'CorpData_CommonBatch', 'data', 'ActivityCode', 'KeyCountry')
                    )

                    result = {
                        'tags': tags,
                        'description': description,
                        'description_date': description_date,
                        'description_source': description_source,
                        'country': country,
                        'nace_codes': nace_codes,
                        'sic_code': sic_code,
                        'lei': lei,
                        'local_registry_id': local_registry_id,
                        'vat_id': vat_id,
                        'naics_codes': naics_codes
                    }
                    return result
            except Exception as retry_e:
                thread_safe_print(f"Warning: Retry failed for profile {company_id}: {retry_e}")

        return get_empty_profile_data()
    except Exception as e:
        thread_safe_print(f"Exception fetching profile for {company_id}: {e}")
        import traceback
        thread_safe_print(f"  Traceback: {traceback.format_exc()}")
        return get_empty_profile_data()


def get_empty_profile_data():
    return {
        'tags': [], 
        'description': 'NaN',
        'description_date': 'NaN',
        'description_source': 'NaN',  
        'country': 'NaN', 
        'nace_codes': [],
        'sic_code': 'NaN', 
        'lei': 'NaN',  
        'local_registry_id': 'NaN',  
        'vat_id': 'NaN',  
        'naics_codes': []
    }

def format_naics_codes_for_csv(naics_codes):
    if not naics_codes:
        return {
            'naics_codes': 'NaN',
            'naics_descriptions': 'NaN',
            'naics_relationship_levels': 'NaN'
        }
    
    return {
        'naics_codes': format_list_for_csv([n['naics_code'] for n in naics_codes]),
        'naics_descriptions': format_list_for_csv([n['description'] for n in naics_codes]),
        'naics_relationship_levels': format_list_for_csv([n['relationship_level'] for n in naics_codes])
    }

#API call #3: mining properties data (assets and properties)
# =============================================================================
def fetch_mining_properties_data(session, company_id):
    params = {
        'mode': 'browser',
        'id': company_id,
        'keypage': '309246',
        'crossSiteFilterCurrency': 'USD',
        'crossSiteSource': '1',
        '_': str(int(time.time() * 1000))
    }

    try:
        rate_limiter.wait_if_needed()
        response = session.get(REPORT_API_URL, params=params, headers=HEADERS)
        progress_tracker.increment_api_calls()

        if response.status_code == 401:
            thread_safe_print(f"Mining Properties API returned 401 Unauthorized for {company_id}")
            return {'properties': [], '_401_error': True}  # Special flag to trigger cooldown
        elif response.status_code == 200:
            data = response.json()
            if 'controls' not in data:
                return {'properties': []}

            controls = data.get('controls', {})
            property_control = controls.get('section_1_control_51', {})
            if not property_control:
                return {'properties': []}

            data_source = property_control.get('dataModel', {}).get('model', {}).get('dataSource', [])
            if not data_source:
                return {'properties': []}

            properties = []
            for record in data_source:
                properties.append({
                    'property_name': record.get('_PropertyName', ''),
                    'country_region': record.get('_CountryShortName', ''),
                    'primary_commodity': record.get('_PrimaryCommodity', ''),
                    'all_commodities': record.get('_Commodity', ''),
                    'operator': record.get('_Operator', ''),
                    'stage': record.get('_Stage', ''),
                    'status': record.get('_Status', ''),
                    'equity_ownership': record.get('_EquityOwnership', ''),
                    'controlling_ownership': record.get('_ControllingOwnership', ''),
                    'royalty_ownership': record.get('_RoyaltyOwnership', ''),
                    'primary_resources': record.get('_PrimaryContained', ''),
                    'unit': record.get('_Magnitude', '').strip() if record.get('_Magnitude') else '',
                    'as_of_date': record.get('_AsOfDate', ''),
                    'property_type': record.get('_PropertyType', ''),
                    'in_situ_value': record.get('_InSitu', ''),
                    'risk_score': record.get('_ECRScore', ''),
                    'property_id': record.get('_KeyMineProject', '')
                })
            return {'properties': properties}
        else:
            return {'properties': []}
    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        thread_safe_print(f"Warning: Timeout fetching mining properties for {company_id} - attempting cookie refresh and retry")

        if refresh_cookies_and_retry(session, f"mining properties for {company_id}"):
            try:
                response = session.get(REPORT_API_URL, params=params, headers=HEADERS, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    thread_safe_print(f"Retry succeeded for mining properties {company_id}")
                    # Extract properties from retry response
                    controls = data.get('controls', {})
                    properties = []
                    for key, value in controls.items():
                        if isinstance(value, dict) and value.get('type') == 'table':
                            for row in value.get('data', []):
                                properties.append({
                                    'property_id': row.get('_PropertyId', ''),
                                    'property_name': row.get('_PropertyName', '')
                                })
                    return {'properties': properties}
            except Exception as retry_e:
                thread_safe_print(f"Warning: Retry failed for mining properties {company_id}: {retry_e}")

        return {'properties': []}
    except Exception as e:
        return {'properties': []}
    

#API call #4: mining property profile data， separate page for mines themselves
# =============================================================================    
def fetch_mining_property_profile_data(session, property_id):
    if not property_id or pd.isna(property_id):
        return get_empty_mining_property_profile_data()
    
    property_id = str(property_id).strip()
    
    if not property_id or property_id.lower() in ['nan', 'none', '']:
        return get_empty_mining_property_profile_data()
    
    params = {
        'mode': 'browser',
        'id': property_id,
        'keypage': '326866',
        'crossSiteFilterCurrency': 'USD',
        'crossSiteSource': '1',
        '_': str(int(time.time() * 1000))
    }
    
    try:
        thread_safe_print(f"Fetching data for property ID: {property_id}")

        # Add CSRF token to headers if available
        headers = HEADERS.copy()
        csrf_token = session.cookies.get('x-csrf-token')
        if csrf_token:
            headers['x-csrf-token'] = csrf_token

        rate_limiter.wait_if_needed()
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=headers
        )

        progress_tracker.increment_api_calls()
        
        if response.status_code == 200:
            data = response.json()

            controls = data.get('controls', {})
            general_info_control = controls.get('section_1_control_26', {})
            
            if not general_info_control:
                return get_empty_mining_property_profile_data()

            data_model = general_info_control.get('dataModel', {})
            model = data_model.get('model', {})
            data_source = model.get('dataSource', [])
            
            if not data_source:
                return get_empty_mining_property_profile_data()

            # Map by position - fields appear in consistent order
            # Position mapping based on observed API structure
            # Handles both full (13 rows) and short (8 rows for refineries) responses
            position_mapping_full = {
                0: 'property_profile_id',      # Property ID
                1: 'property_profile_aka',     # Also Known As
                2: 'property_profile_type',    # Property Type
                3: 'property_profile_commodities',  # Commodity(s)
                4: None,  # Total In-Situ Value - skip
                5: 'property_profile_dev_stage',    # Development Stage
                6: 'property_profile_activity_status',  # Activity Status
                7: None,  # Start Up - skip
                8: None,  # Projected Closure - skip
                9: None,  # Mine Type - skip
                10: 'property_profile_country',  # Country/Region
                11: 'property_profile_state',    # State or Province
                12: None,  # Country/Region Risk Score - skip
            }

            # Short format (refineries/facilities without full details)
            position_mapping_short = {
                0: 'property_profile_id',      # Property ID
                1: 'property_profile_aka',     # Also Known As
                2: 'property_profile_type',    # Property Type
                3: 'property_profile_dev_stage',    # Development Stage
                4: 'property_profile_activity_status',  # Activity Status
                5: 'property_profile_country',  # Country/Region
                6: 'property_profile_state',    # State or Province
                7: None,  # Country/Region Risk Score - skip
            }

            # Choose mapping based on data source length
            position_mapping = position_mapping_short if len(data_source) <= 8 else position_mapping_full

            property_info = {}

            for idx, row in enumerate(data_source):
                # API uses different key pairs depending on response structure
                # Try _14/_13 first (4-column layout), then _9/_8 (4-column alt), then _11/_10 (2-column)
                field_name = row.get('_14', '') or row.get('_9', '') or row.get('_11', '')
                field_value_html = row.get('_13', '') or row.get('_8', '') or row.get('_10', '')

                if not field_value_html:
                    continue

                field_value = clean_html(field_value_html)

                # Map by position
                dict_key = position_mapping.get(idx)
                if dict_key:
                    property_info[dict_key] = field_value
                    thread_safe_print(f"  [{idx}] {field_name}: {field_value[:80]}...")

            if not property_info:
                thread_safe_print(f"No fields extracted for {property_id}")
                return get_empty_mining_property_profile_data()

            thread_safe_print(f"Extracted {len(property_info)} fields for property {property_id}")

            # Fill in any missing fields with 'NaN'
            for key in ['property_profile_id', 'property_profile_aka', 'property_profile_type',
                       'property_profile_commodities', 'property_profile_dev_stage',
                       'property_profile_activity_status', 'property_profile_country',
                       'property_profile_state']:
                if key not in property_info:
                    property_info[key] = 'NaN'

            return property_info
            
        else:
            thread_safe_print(f"API returned status {response.status_code} for {property_id}")
            return get_empty_mining_property_profile_data()

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        if refresh_cookies_and_retry(session, f"property profile for {property_id}"):
            try:
                response = session.get(REPORT_API_URL, params=params, headers=HEADERS, timeout=30)
                if response.status_code == 200:
                    thread_safe_print(f"Retry succeeded for property profile {property_id}")
                    # Parse retry response (simplified - just return empty for now, full parsing too complex)
                    return get_empty_mining_property_profile_data()
            except Exception:
                pass
        return get_empty_mining_property_profile_data()
    except Exception as e:
        import traceback
        thread_safe_print(f"Traceback: {traceback.format_exc()}")
        return get_empty_mining_property_profile_data()

def get_empty_mining_property_profile_data():
    return {
        'property_profile_id': 'NaN',
        'property_profile_aka': 'NaN',
        'property_profile_type': 'NaN',
        'property_profile_commodities': 'NaN',
        'property_profile_dev_stage': 'NaN',
        'property_profile_activity_status': 'NaN',
        'property_profile_country': 'NaN',
        'property_profile_state': 'NaN',
    }

#API call #5: mining property location data
# =============================================================================
def fetch_mining_property_location(session, property_id):
    params = {
        'mode': 'browser',
        'id': property_id,
        'keypage': '326809',
        'crossSiteFilterCurrency': 'USD',
        'crossSiteSource': '1',
        '_': str(int(time.time() * 1000))
    }

    try:
        rate_limiter.wait_if_needed()
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=HEADERS
        )

        if response.status_code != 200:
            return get_empty_mine_location_data()
        
        data = response.json()

        return parse_mine_location_data(data)

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        if refresh_cookies_and_retry(session, f"mine location for {property_id}"):
            try:
                response = session.get(REPORT_API_URL, params=params, headers=HEADERS, timeout=30)
                if response.status_code == 200:
                    thread_safe_print(f"Retry succeeded for mine location {property_id}")
                    data = response.json()
                    return parse_mine_location_data(data)
            except Exception:
                pass
        return get_empty_mine_location_data()
    except Exception as e:
        return get_empty_mine_location_data()
        
def get_empty_mine_location_data():
    return {
        'mine_decimal_degree': 'NaN',
        'mine_coordinate_accuracy': 'NaN',
        'claims': []
    }

def parse_mine_location_data(api_response):
    result = get_empty_mine_location_data()

    try:
        controls = api_response.get('controls', {})
        coord_control = controls.get('section_1_control_31', {})
        coord_model = coord_control.get('dataModel', {}).get('model', {})
        coord_data = coord_model.get('dataSource', [])

        for row in coord_data:
            field_name = row.get('_4', '')
            field_value_html = row.get('_3', '')
            field_value = clean_html(field_value_html)

            if field_name == 'Decimal Degrees':
                latitude, longitude = parse_coordinates(field_value)
                # Store as combined decimal_degree for consistency with CSV formatting
                result['mine_decimal_degree'] = field_value if field_value else 'NaN'
                result['latitude'] = latitude
                result['longitude'] = longitude

            elif field_name == 'Coordinate Accuracy':
                result['mine_coordinate_accuracy'] = field_value

        claim_control = controls.get('section_1_control_49', {})
        claim_model = claim_control.get('dataModel', {}).get('model', {})
        claim_data = claim_model.get('dataSource', [])
        claims = []
        for claim_row in claim_data:
            claim = {
                'Claim_ID': claim_row.get('_MiningClaimID', ''),
                'Claim_Name': claim_row.get('_MiningClaimIDClaimName', ''),
                'Claim_Owners': claim_row.get('_MiningClaimOwners', ''),
                'Claim_Type': claim_row.get ('_MiningClaimType', ''),
                'Claim_Status': claim_row.get('_MiningClaimStatus', ''),
                'Date_Granted': claim_row.get('_MiningClaimActivityAsOf_Type1', ''),
                'Expiry_Date': claim_row.get('_MiningClaimActivityAsOf_Type2', ''),
                'Source': claim_row.get('_Source', ''),
                'Source_as_of_date': claim_row.get('_MiningJurisdictionFilingAsOf', '')
            }
            claims.append(claim)

        result['claims'] = claims

    except Exception as e:
        thread_safe_print(f"Traceback: {traceback.format_exc()}")

    return result

def parse_coordinates(coord_string):
    try:
        if not coord_string or coord_string == 'NaN':
            return 'NaN', 'NaN'
        
        parts = coord_string.split(',')
        if len(parts) != 2:
            return 'NaN', 'NaN'
        
        latitude = parts[0].strip()
        longitude = parts[1].strip()

        return latitude, longitude
    
    except Exception as e:
        return 'NaN', 'NaN'

#API call #6 & #7: supplier and customer data
# =============================================================================
def fetch_suppliers_only(session, company_id):
    """Fetch only supplier data for a company"""
    suppliers = []
    previous_suppliers = []

    suppliers_params = {
        'mode': 'browser',
        'id': company_id,
        'keypage': '478395',
        'crossSiteFilterCurrency': 'USD',
        '_': str(int(time.time() * 1000))
    }

    try:
        rate_limiter.wait_if_needed()
        suppliers_response = session.get(REPORT_API_URL, params=suppliers_params, headers=HEADERS)
        progress_tracker.increment_api_calls()

        if suppliers_response.status_code == 401:
            thread_safe_print(f"Suppliers API returned 401 Unauthorized for {company_id}")
            return {'suppliers': [], 'previous_suppliers': [], '_401_error': True}  # Special flag to trigger cooldown
        elif suppliers_response.status_code == 200:
            suppliers_data = suppliers_response.json()
            suppliers_controls = suppliers_data.get('controls', {})

            supplier_source = suppliers_controls.get('section_1_control_38', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in supplier_source:
                suppliers.append({
                    'supplier_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'start_dates': item.get('_UpdDateSmall', ''),
                    'end_dates': item.get('_BestAnnouncedDate', ''),
                    'key_instn': item.get('_KeyInstn', ''),
                    'primary_industry': item.get('_ShipmentDescription', ''),
                    'expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'min_value': item.get('_CIQBRRevenueMin', ''),
                    'max_value': item.get('_CIQBRRevenueMax', '')
                })

            previous_suppliers_source = suppliers_controls.get('section_1_control_45', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in previous_suppliers_source:
                previous_suppliers.append({
                    'previous_supplier_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'previous_supplier_start_dates': item.get('_UpdDateSmall', ''),
                    'previous_supplier_end_dates': item.get('_BestAnnouncedDate', ''),
                    'previous_supplier_institution_keys': item.get('_KeyInstn', ''),
                    'previous_supplier_primary_industry': item.get('_ShipmentDescription', ''),
                    'previous_supplier_expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'previous_supplier_expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'previous_supplier_min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'previous_supplier_max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'previous_supplier_min_value': item.get('_CIQBRRevenueMin', ''),
                    'previous_supplier_max_value': item.get('_CIQBRRevenueMax', ''),
                    'previous_supplier_source': item.get('_SourceExport', '')
                })

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        if refresh_cookies_and_retry(session, f"suppliers for {company_id}"):
            try:
                response = session.get(REPORT_API_URL, params=suppliers_params, headers=HEADERS, timeout=30)
                if response.status_code == 200:
                    thread_safe_print(f"Retry succeeded for suppliers {company_id}")
                    # Return existing partial data on retry success (full parsing too complex)
            except Exception:
                pass
        return {'suppliers': suppliers, 'previous_suppliers': previous_suppliers}
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"RequestException fetching suppliers for {company_id}: {type(e).__name__}: {str(e)}")
    except Exception as e:
        thread_safe_print(f"Unexpected error fetching suppliers for {company_id}: {type(e).__name__}: {str(e)}")
        import traceback
        thread_safe_print(f"Traceback: {traceback.format_exc()}")

    return {'suppliers': suppliers, 'previous_suppliers': previous_suppliers}

def fetch_customers_only(session, company_id):
    customers = []
    previous_customers = []

    customer_params = {
        'mode': 'browser',
        'id': company_id,
        'keypage': '478397',
        'crossSiteFilterCurrency': 'USD',
        '_': str(int(time.time() * 1000))
    }

    try:
        rate_limiter.wait_if_needed()
        customers_response = session.get(REPORT_API_URL, params=customer_params, headers=HEADERS)
        progress_tracker.increment_api_calls()

        if customers_response.status_code == 401:
            thread_safe_print(f"Customers API returned 401 Unauthorized for {company_id}")
            return {'customers': [], 'previous_customers': [], '_401_error': True}  # Special flag to trigger cooldown
        elif customers_response.status_code == 200:
            customers_data = customers_response.json()
            customers_controls = customers_data.get('controls', {})

            customer_source = customers_controls.get('section_1_control_47', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in customer_source:
                customers.append({
                    'customer_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'start_dates': item.get('_UpdDateSmall', ''),
                    'end_dates': item.get('_BestAnnouncedDate', ''),
                    'key_instn': item.get('_KeyInstn', ''),
                    'primary_industry': item.get('_ShipmentDescription', ''),
                    'expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'min_value': item.get('_CIQBRRevenueMin', ''),
                    'max_value': item.get('_CIQBRRevenueMax', '')
                })

            previous_customers_source = customers_controls.get('section_1_control_40', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in previous_customers_source:
                previous_customers.append({
                    'previous_customer_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'previous_customer_start_dates': item.get('_UpdDateSmall', ''),
                    'previous_customer_end_dates': item.get('_BestAnnouncedDate', ''),
                    'previous_customer_institution_keys': item.get('_KeyInstn', ''),
                    'previous_customer_primary_industry': item.get('_ShipmentDescription', ''),
                    'previous_customer_expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'previous_customer_expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'previous_customer_min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'previous_customer_max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'previous_customer_min_value': item.get('_CIQBRRevenueMin', ''),
                    'previous_customer_max_value': item.get('_CIQBRRevenueMax', ''),
                    'previous_customer_source': item.get('_SourceExport', '')
                })

    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        if refresh_cookies_and_retry(session, f"customers for {company_id}"):
            try:
                response = session.get(REPORT_API_URL, params=customer_params, headers=HEADERS, timeout=30)
                if response.status_code == 200:
                    thread_safe_print(f"Retry succeeded for customers {company_id}")
                    # Return existing partial data on retry success (full parsing too complex)
            except Exception:
                pass
        return {'customers': customers, 'previous_customers': previous_customers}
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"RequestException fetching customers for {company_id}: {type(e).__name__}: {str(e)}")
    except Exception as e:
        thread_safe_print(f"Unexpected error fetching customers for {company_id}: {type(e).__name__}: {str(e)}")
        import traceback
        thread_safe_print(f"Traceback: {traceback.format_exc()}")

    return {'customers': customers, 'previous_customers': previous_customers}

def fetch_supplier_customer_data(session, company_id):
    # Note: Do not submit to GLOBAL_EXECUTOR here - we're already running in a thread
    # Submitting nested futures causes deadlock with limited thread pool
    # Run sequentially instead

    result = {
        'suppliers': [],
        'previous_suppliers': [],
        'customers': [],
        'previous_customers': []
    }

    try:
        suppliers_data = fetch_suppliers_only(session, company_id)  # Call directly, not via executor
        result['suppliers'] = suppliers_data.get('suppliers', [])
        result['previous_suppliers'] = suppliers_data.get('previous_suppliers', [])
        # Propagate 401 error flag
        if suppliers_data.get('_401_error'):
            result['_401_error'] = True
    except Exception as e:
        thread_safe_print(f"Error collecting supplier data for {company_id}: {type(e).__name__}: {str(e)}")

    try:
        customers_data = fetch_customers_only(session, company_id)  # Call directly, not via executor
        result['customers'] = customers_data.get('customers', [])
        result['previous_customers'] = customers_data.get('previous_customers', [])
        # Propagate 401 error flag
        if customers_data.get('_401_error'):
            result['_401_error'] = True
    except Exception as e:
        thread_safe_print(f"Error collecting customer data for {company_id}: {type(e).__name__}: {str(e)}")

    return result
    
def fetch_all_company_data_parallel(session, company_id, company_name):
    results = {
        'relationships': {'suppliers': [], 'previous_suppliers': [], 'customers': [], 'previous_customers': []},
        'mining': {'properties': []},
        'profile': get_empty_profile_data(),
        'financials': get_empty_financials(),
        'property_profiles': [],
        'mining_locations': [],
        'had_timeout': False  # Track if any section timed out
    }

    if not company_id:
        thread_safe_print(f"No company_id for {company_name}")
        return results

    try:
        # 1: Fetch Base Company Data using GLOBAL_EXECUTOR
        future_relationships = GLOBAL_EXECUTOR.submit(
            fetch_with_retry,
            session,
            fetch_supplier_customer_data,
            company_id
        )
        future_mining = GLOBAL_EXECUTOR.submit(
            fetch_with_retry,
            session,
            fetch_mining_properties_data,
            company_id
        )
        future_profile = GLOBAL_EXECUTOR.submit(
            fetch_with_retry,
            session,
            fetch_company_profile_data,
            company_id
        )
        future_financials = GLOBAL_EXECUTOR.submit(
            fetch_with_retry,
            session,
            fetch_financial_data,
            company_id,
            "471751",  # key_page parameter
            True  # return_all_periods=True to get all years
        )

        try:
            relationships_result = future_relationships.result(timeout=180)  # 3 minute timeout
            if relationships_result and isinstance(relationships_result, dict):
                results['relationships'] = relationships_result
        except TimeoutError:
            thread_safe_print(f"Warning: Timeout fetching relationships for {company_name} (exceeded 180s)")
            results['had_timeout'] = True  # Mark as having timeout
        except Exception as e:
            thread_safe_print(f"ERROR fetching relationships for {company_name}: {type(e).__name__}: {str(e)}")

        try:
            mining_result = future_mining.result(timeout=120)  # 2 minute timeout
            if mining_result and isinstance(mining_result, dict):
                results['mining'] = mining_result
        except TimeoutError:
            thread_safe_print(f"Warning: Timeout fetching mining data for {company_name} (exceeded 120s)")
            results['had_timeout'] = True  # Mark as having timeout
        except Exception as e:
            thread_safe_print(f"ERROR fetching mining data for {company_name}: {type(e).__name__}: {str(e)}")

        try:
            profile_result = future_profile.result(timeout=120)  # 2 minute timeout
            if profile_result and isinstance(profile_result, dict):
                results['profile'] = profile_result
        except TimeoutError:
            thread_safe_print(f"Warning: Timeout fetching profile for {company_name} (exceeded 120s)")
            results['had_timeout'] = True  # Mark as having timeout
        except Exception as e:
            thread_safe_print(f"ERROR fetching profile for {company_name}: {type(e).__name__}: {str(e)}")

        try:
            financials_result = future_financials.result(timeout=120)  # 2 minute timeout
            # Financial result can be dict (backward compat) or list (multi-period)
            if financials_result and (isinstance(financials_result, dict) or isinstance(financials_result, list)):
                results['financials'] = financials_result
        except TimeoutError:
            thread_safe_print(f"Warning: Timeout fetching financials for {company_name} (exceeded 120s)")
            results['had_timeout'] = True  # Mark as having timeout
        except Exception as e:
            thread_safe_print(f"ERROR fetching financials for {company_name}: {type(e).__name__}: {str(e)}")

        # Check if any API returned 401 - if so, trigger 5-minute cooldown
        got_401 = False
        if results.get('relationships', {}).get('_401_error'):
            got_401 = True
        if results.get('mining', {}).get('_401_error'):
            got_401 = True
        if results.get('profile', {}).get('_401_error'):
            got_401 = True
        # Check financials - can be dict or list, so handle both
        financials = results.get('financials', {})
        if isinstance(financials, dict) and financials.get('_401_error'):
            got_401 = True

        # COMMENTED OUT FOR TESTING - NO ACCOUNT SWITCHING
        if got_401:
            thread_safe_print(f"\n{'='*80}")
            thread_safe_print(f"Warning: 401 UNAUTHORIZED detected for {company_name} - no retry (testing mode)")
            thread_safe_print(f"{'='*80}\n")
            # Just return what we have without retrying
            results['_401_error_no_retry'] = True
            return results
            
            # # Try switching account if multiple accounts are configured
            # switch_account = len(CAPITALIQ_ACCOUNTS) > 1
            # if switch_account:
            #     thread_safe_print(f"Warning: Multiple accounts configured - switching to next account...")
            # else:
            #     thread_safe_print(f"Warning: Regenerating cookies with same account...")
            # 
            # # Force fresh cookie generation with optional account switching
            # if cookie_manager.refresh_on_error(session, switch_account=switch_account):
            #     thread_safe_print(f"Account switched successfully - retrying {company_name} immediately...")
            #     thread_safe_print(f"{'='*80}\n")
            #     return fetch_all_company_data_parallel(session, company_id, company_name)
            # else:
            #     thread_safe_print(f"Account switch failed")
            #     thread_safe_print(f"{'='*80}\n")
            #     results['_401_error_failed_switch'] = True
            #     return results

        # 2: Process property profiles
        mining_properties = results['mining'].get('properties', [])

        if not mining_properties:
            return results

        mining_property_ids = []
        for prop in mining_properties:
            property_id = prop.get('property_id', '')
            if property_id:
                property_ids_str = str(property_id).strip()
                if property_ids_str and property_ids_str.lower() not in ['nan', 'none', '']:
                    mining_property_ids.append(property_ids_str)

        if not mining_property_ids:
            thread_safe_print(f"No valid mining property IDs for {company_name}")
            return results

        # Use global executor for property fetching
        property_futures = {}

        for prop_id in mining_property_ids:
            future_property_profiles = GLOBAL_EXECUTOR.submit(
                fetch_mining_property_profile_data,
                session,
                prop_id
            )
            future_property_locations = GLOBAL_EXECUTOR.submit(
                fetch_mining_property_location,
                session,
                prop_id
            )

            property_futures[prop_id] = {
                'profile': future_property_profiles,
                'location': future_property_locations
            }

        for prop_id, futures_dict in property_futures.items():
            try:
                property_profile_result = futures_dict['profile'].result()
                if property_profile_result and isinstance(property_profile_result, dict):
                    results['property_profiles'].append(property_profile_result)
            except Exception as e:
                thread_safe_print(f"error fetching property profile for {company_name}, property {prop_id}: {e}")

            try:
                property_location_result = futures_dict['location'].result()
                if property_location_result and isinstance(property_location_result, dict):
                    results['mining_locations'].append(property_location_result)
            except Exception as e:
                thread_safe_print(f"error fetching property location for {company_name}, property {prop_id}: {e}")

    except Exception as e:
        thread_safe_print(f"Error for company {company_name}: {e}")
        thread_safe_print(f"Traceback: {traceback.format_exc()}")

    return results

def search_company(session, company_name):
    """
    Search for company MI Key using essential cookies only.

    Note: The MI Key extraction API has an 8KB header limit.
    We use get_essential_cookies() to filter out non-auth cookies
    and prevent "metadata size exceeds soft limit" errors.
    """
    payload = {
        "query": company_name,
        "max_results": 15,
        "typeahead_card_names": [
            "suggested_searches-gss-typeahead",
            "countries-gss-typeahead",
            "combined_companies_mi-gss-typeahead",
            "insurance_stats_group-gss-typeahead"
        ],
        "typeahead_contexts": {"currentPageId": 10027, "keyCandidate": 0}
    }

    try:
        # Use ALL cookies - filtering was causing 401 errors by excluding critical auth cookies
        # TESTING MODE - Direct API call without random delays
        response = session.post(API_URL, json=payload, headers=HEADERS, timeout=10)
        progress_tracker.increment_api_calls()

        if response.status_code != 200:
            # Handle 401 Unauthorized - cookies expired
            if response.status_code == 401:
                thread_safe_print(f"Warning: 401 Unauthorized for '{company_name}' - tracking for cookie refresh")
                handle_401_error()
            return {
                'company_name': company_name, 'matched_name': '', 'country': '',
                'mi_key': None, 'spciq_key': None, 'status': f'error_{response.status_code}'
            }

        data = response.json()
        destinations = data.get('destinations', [])

        # DEBUG: Print first few responses to understand structure
        if not hasattr(search_company, '_debug_count'):
            search_company._debug_count = 0

        if search_company._debug_count < 5:
            thread_safe_print(f"\n[DEBUG {search_company._debug_count + 1}] API response for '{company_name}':")
            thread_safe_print(f"  Status code: {response.status_code}")
            thread_safe_print(f"  Destinations count: {len(destinations)}")
            if destinations:
                thread_safe_print(f"  First result keys: {list(destinations[0].keys())}")
                thread_safe_print(f"  Name: {destinations[0].get('name', 'N/A')}")
                if 'additionalFields' in destinations[0]:
                    thread_safe_print(f"  Additional fields: {destinations[0]['additionalFields']}")
            else:
                thread_safe_print(f"  No results found for this company")
            search_company._debug_count += 1
        
        if not destinations:
            return {
                'company_name': company_name, 'matched_name': '', 'country': '',
                'mi_key': None, 'spciq_key': None, 'status': 'not_found'
            }
        
        first_result = destinations[0]
        matched_name = clean_company_name(first_result.get('name', ''))

        result = {
            'company_name': company_name, 'matched_name': matched_name,
            'country': '', 'mi_key': None, 'spciq_key': None, 'status': 'found'
        }

        for field in first_result.get('additionalFields', []):
            field_name = field.get('fieldName', '')
            field_value = field.get('fieldValue', '')
            if field_name == 'mi_id':
                result['mi_key'] = field_value
            elif field_name == 'id':
                result['spciq_key'] = field_value
            elif field_name == 'l_country':
                result['country'] = field_value

        if not result['mi_key']:
            result['status'] = 'no_mi_key'

        return result
    except requests.exceptions.Timeout as e:
        # Timeout - refresh cookies and retry once
        thread_safe_print(f"Warning: Timeout searching for '{company_name}' - attempting cookie refresh and retry")

        if refresh_cookies_and_retry(session, f"search for '{company_name}'"):
            # Retry the request with fresh cookies
            try:
                response = session.post(API_URL, json=payload, headers=HEADERS, timeout=15)
                progress_tracker.increment_api_calls()

                if response.status_code == 200:
                    data = response.json()
                    destinations = data.get('destinations', [])

                    if not destinations:
                        return {
                            'company_name': company_name, 'matched_name': '', 'country': '',
                            'mi_key': None, 'spciq_key': None, 'status': 'not_found'
                        }

                    first_result = destinations[0]
                    matched_name = clean_company_name(first_result.get('name', ''))
                    result = {
                        'company_name': company_name, 'matched_name': matched_name,
                        'country': '', 'mi_key': None, 'spciq_key': None, 'status': 'found'
                    }

                    for field in first_result.get('additionalFields', []):
                        field_name = field.get('fieldName', '')
                        field_value = field.get('fieldValue', '')
                        if field_name == 'mi_id':
                            result['mi_key'] = field_value
                        elif field_name == 'id':
                            result['spciq_key'] = field_value
                        elif field_name == 'l_country':
                            result['country'] = field_value

                    if not result['mi_key']:
                        result['status'] = 'no_mi_key'

                    thread_safe_print(f"Retry succeeded for '{company_name}'")
                    return result

            except Exception as retry_e:
                thread_safe_print(f"Warning: Retry failed for '{company_name}': {retry_e}")

        # If retry failed or cookies couldn't refresh, return timeout status
        return {
            'company_name': company_name, 'matched_name': '', 'country': '',
            'mi_key': None, 'spciq_key': None, 'status': 'timeout'
        }
    except Exception as e:
        return {
            'company_name': company_name, 'matched_name': '', 'country': '',
            'mi_key': None, 'spciq_key': None, 'status': f'exception_{type(e).__name__}'
        }

cookies_dict = {}

if __name__ == "__main__":
    thread_safe_print("Loading and validating cookies")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(script_dir, 'Cookies', 'cookies.pkl')

    # Check if cookies file exists, if not generate it
    if not os.path.exists(cookie_path):
        thread_safe_print("No cookies.pkl found. Generating initial cookies...")
        cookie_headless_path = os.path.join(script_dir, 'Cookie_headless.py')
        cookie_script_path = os.path.join(script_dir, 'Cookie.py')
        
        # Prefer headless script
        if os.path.exists(cookie_headless_path):
            script_to_run = cookie_headless_path
            thread_safe_print("Running Cookie_headless.py to generate cookies...")
        elif os.path.exists(cookie_script_path):
            script_to_run = cookie_script_path
            thread_safe_print("Running Cookie.py to generate cookies...")
        else:
            thread_safe_print("Error: No cookie generation script found")
            exit(1)
        
        import subprocess
        try:
            # Use first account credentials for initial cookie generation
            env = os.environ.copy()
            if CAPITALIQ_ACCOUNTS:
                env['CAPITALIQ_USERNAME'] = CAPITALIQ_ACCOUNTS[0]['username']
                env['CAPITALIQ_PASSWORD'] = CAPITALIQ_ACCOUNTS[0]['password']
            
            result = subprocess.run(
                ['/home/alfred/anaconda3/envs/supply-chain/bin/python', script_to_run],
                capture_output=True,
                text=True,
                timeout=90,
                cwd=script_dir,
                env=env
            )
            
            if result.returncode != 0 or not os.path.exists(cookie_path):
                thread_safe_print(f"Error: Failed to generate cookies")
                thread_safe_print(f"Error: {result.stderr[:500]}")
                exit(1)
            
            thread_safe_print("Successfully generated initial cookies")
        except Exception as e:
            thread_safe_print(f"Error: Error generating cookies: {e}")
            exit(1)
    
    cookies_list, cookies_dict, is_valid = load_and_validate_cookies(cookie_path)

    if not cookies_list:
        thread_safe_print("Error: Could not load cookies")
        exit(1)

    if not is_valid:
        thread_safe_print("Warning: Some cookies are expired. Attempting automatic refresh...")

        # Try to refresh cookies automatically using Cookie_headless.py
        cookie_headless_path = os.path.join(script_dir, 'Cookie_headless.py')
        if os.path.exists(cookie_headless_path):
            thread_safe_print("Running automatic cookie refresh...")
            import subprocess
            try:
                result = subprocess.run(['python', cookie_headless_path],
                                      capture_output=True,
                                      text=True,
                                      timeout=60)

                if result.returncode == 0:
                    thread_safe_print("Cookies refreshed successfully")
                    # Reload cookies
                    cookies_list, cookies_dict, is_valid = load_and_validate_cookies(cookie_path)
                    if not is_valid:
                        thread_safe_print("Warning: Cookies still invalid after refresh. Continuing anyway...")
                else:
                    thread_safe_print(f"Warning: Cookie refresh failed: {result.stderr}")
                    thread_safe_print("Continuing with existing cookies...")
            except subprocess.TimeoutExpired:
                thread_safe_print("Warning: Cookie refresh timed out. Continuing with existing cookies...")
            except Exception as e:
                thread_safe_print(f"Warning: Could not refresh cookies: {e}")
                thread_safe_print("Continuing with existing cookies...")
        else:
            thread_safe_print("Warning: Cookie_headless.py not found. Continuing with existing cookies...")

    thread_safe_print("Cookies loaded and validated\n")

    script_start_time = time.time()
    script_start_datetime = datetime.now()

    initialize_global_executor()

    # Use absolute path for CSV directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    CSV_DIRECTORY = os.path.join(project_root, 'csv_files_2022')
    OUTPUT_FILE = 'all_countries_results.csv'

    # Initialize database connection (required - no CSV fallback)
    if USE_DATABASE:
        try:
            thread_safe_print(f"\n{'='*70}")
            thread_safe_print("DATABASE INITIALIZATION")
            thread_safe_print(f"{'='*70}")
            get_database_connection()
            thread_safe_print(f"Target table: {DB_CONFIG.get('table_name', 'scraper_results')}")
            thread_safe_print(f"{'='*70}\n")
        except Exception as e:
            thread_safe_print(f"[DATABASE ERROR] Failed to connect: {e}")
            thread_safe_print("DATABASE CONNECTION REQUIRED - EXITING")
            exit(1)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"main execution parameters:")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Export mode: DATABASE (MySQL only)")
    thread_safe_print(f"Max tiers: {MAX_TIER}")
    thread_safe_print(f"Rate limit delay: {RATE_LIMIT_DELAY}s")
    #thread_safe_print(f"Max companies per tier: {MAX_COMPANIES_PER_TIER}")
    thread_safe_print(f"Batch save interval: {BATCH_SAVE_INTERVAL}")
    thread_safe_print(f"Checkpoints enabled: {ENABLE_CHECKPOINTS}")
    thread_safe_print(f"{'='*70}\n")
    
    csv_path = Path(CSV_DIRECTORY)
    if not csv_path.exists():
        thread_safe_print(f"\n Directory not found: {CSV_DIRECTORY}")
        exit(1)
    
    datasets_to_process = TEST_DATASETS if TEST_MODE else csv_files_2022

    # Use Runs folder directly (no timestamped subfolders)
    RUN_FOLDER_PATH = os.path.join(script_dir, 'Runs')
    os.makedirs(RUN_FOLDER_PATH, exist_ok=True)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"OUTPUT FOLDER: {RUN_FOLDER_PATH}")
    thread_safe_print(f"{'='*70}\n")

    ## Step 1: Extract company names
    #thread_safe_print(f"\n{'='*70}")
    #thread_safe_print("STEP 1: EXTRACTING COMPANY NAMES")
    #thread_safe_print(f"{'='*70}\n")
    #
    #all_companies = set()
    #for dataset_file in datasets_to_process:
    #    file_path = csv_path / dataset_file
    #    if not file_path.exists():
    #        print('file path does not exist')
    #        break
    #    
    #    thread_safe_print(f"Reading {dataset_file}...")
    #    try:
    #        df = pd.read_csv(file_path, dtype=str, low_memory=False).fillna('')
    #    except Exception as e:
    #        thread_safe_print(f"Error reading {dataset_file}: {e}")
    #        continue
    #    
    #    name_cols, _ = get_dataset_columns(dataset_file)
    #    name_cols = [c for c in name_cols if c in df.columns]
    #    
    #    if not name_cols:
    #        continue
    #    
    #    for col in name_cols:
    #        companies_in_col = df[col].dropna().unique()
    #        for company in companies_in_col:
    #            company_clean = str(company).strip()
    #            if not _is_excluded_company(company_clean):
    #                all_companies.add(company_clean)
    #
    #if not all_companies:
    #    thread_safe_print("\n No companies found")
    #    exit(1)
    #
    #thread_safe_print(f"\nTotal unique companies: {len(all_companies)}\n")

    #Step 1: Extract company names (parallel version parallelized at # of companies level)
    all_chunks = extract_company_names(csv_path, datasets_to_process, chunk_size=500)
    thread_safe_print(f"\n total chunks to process {len(all_chunks)}")

    all_companies = set()

    # Use global executor for chunk extraction
    futures = {GLOBAL_EXECUTOR.submit(extract_companies_from_chunk, chunk[1], chunk[2]): i
               for i, chunk in enumerate(all_chunks)}

    completed = 0
    for future in futures:
        chunk_companies = future.result()
        all_companies.update(chunk_companies)
        completed += 1

        if completed % 20 == 0:
            thread_safe_print(f"Chunks completed: {completed}/{len(all_chunks)}")

    thread_safe_print(f"\nTotal unique companies extracted: {len(all_companies)}\n")
    
    # # Step 2: Search for MI keys (parallel processing with 15 cores)
    # thread_safe_print(f"\n{'='*70}")
    # # thread_safe_print("step 2: MI key extraction")
    # # thread_safe_print(f"{'='*70}\n")

    # # companies_list = sorted(list(all_companies))
    # # total_companies = len(companies_list)
    # thread_safe_print(f"Total companies to process: {total_companies}")

    # Step 2: Search for MI keys (single-pass parallel processing with cache)
    companies_list = sorted(list(all_companies))
    total_companies = len(companies_list)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print("STEP 2: MI KEY EXTRACTION")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Total companies extracted: {total_companies}")

    # Load MI key cache
    mi_cache_data = load_mi_key_cache()
    cached_results = {}
    if mi_cache_data:
        for result in mi_cache_data['results']:
            cached_results[result['company_name']] = result
        thread_safe_print(f"Using cached MI keys where available")

    # Identify companies that need processing (not in cache)
    companies_to_process = [c for c in companies_list if c not in cached_results]
    companies_cached = len(companies_list) - len(companies_to_process)

    thread_safe_print(f"Companies already cached: {companies_cached}")
    thread_safe_print(f"Companies to process: {len(companies_to_process)}")
    thread_safe_print(f"{'='*70}\n")

    # Process only new companies
    results = []
    if companies_to_process:
        # Split companies into chunks for parallel processing
        chunk_size = 75  # Balanced chunk size for optimal throughput
        company_chunks = [companies_to_process[i:i + chunk_size]
                         for i in range(0, len(companies_to_process), chunk_size)]

        max_workers = 15  # Optimized for available CPU cores
        thread_safe_print(f"Split into {len(company_chunks)} chunks (chunk size: ~{chunk_size})")
        thread_safe_print(f"Processing with {max_workers} workers\n")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(chunk_extract_MIKey, chunk, cookies_list): i
                for i, chunk in enumerate(company_chunks)
            }

            completed = 0
            for future in futures:
                try:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    completed += 1

                    companies_processed = len(results)
                    thread_safe_print(f"Chunks completed: {completed}/{len(company_chunks)} "
                                    f"| Companies processed: {companies_processed}/{len(companies_to_process)}")
                except Exception as e:
                    thread_safe_print(f"ERROR: Chunk failed with exception: {e}")
                    import traceback
                    thread_safe_print(traceback.format_exc())

    # Merge cached results with new results
    all_results = list(cached_results.values()) + results

    # Save updated cache with all results
    if results:  # Only save if we processed new companies
        save_mi_key_cache(all_results)

    # Check results
    thread_safe_print(f"\nProcessing complete. Analyzing results...")
    mi_keys_found = sum(1 for r in all_results if r.get('mi_key'))
    thread_safe_print(f"MI keys found: {mi_keys_found}/{total_companies}")

    # Show status breakdown
    status_counts = {}
    for r in all_results:
        status = r.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    thread_safe_print(f"\nStatus breakdown:")
    for status, count in sorted(status_counts.items()):
        thread_safe_print(f"  {status}: {count}")

    df_mi_results = pd.DataFrame(all_results)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"MI keys found: {len([r for r in all_results if r['mi_key']])}/{total_companies}")
    thread_safe_print(f"{'='*70}\n")

    # Step 3: Recursive processing
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print("step 3: five tier recursive process")
    thread_safe_print(f"{'='*70}\n")

    # Create session for recursive processing
    session = requests.Session()
    # Properly set cookies with domain information
    for cookie in cookies_list:
        session.cookies.set(
            name=cookie['name'],
            value=cookie['value'],
            domain=cookie.get('domain', ''),
            path=cookie.get('path', '/'),
            secure=cookie.get('secure', False)
        )
    thread_safe_print(f"Session initialized with {len(cookies_list)} cookies")

    if not df_mi_results.empty:
        match_companies_to_datasets(df_mi_results, session, csv_dir=CSV_DIRECTORY,
                                   output_file=OUTPUT_FILE, max_tier=MAX_TIER)

    #Summary statistics
    #----------------------------------------------------------
    script_end_time = time.time()
    script_end_datetime = datetime.now()
    total_elapsed_seconds = script_end_time - script_start_time

    # Calculate time components
    hours = int(total_elapsed_seconds // 3600)
    minutes = int((total_elapsed_seconds % 3600) // 60)
    seconds = int(total_elapsed_seconds % 60)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print("summary statistics")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Start time:    {script_start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    thread_safe_print(f"End time:      {script_end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Total elapsed: {hours}h {minutes}m {seconds}s")
    thread_safe_print(f"Total seconds: {total_elapsed_seconds:.2f}s")
    thread_safe_print(f"{'='*70}")

    # Performance metrics
    if 'current_results' in globals() and current_results:
        companies_processed = len(set(r.get('spciq_key') for r in current_results if r.get('spciq_key')))
        total_rows = len(current_results)
    
        if total_elapsed_seconds > 0:
            companies_per_second = companies_processed / total_elapsed_seconds
            rows_per_second = total_rows / total_elapsed_seconds
            
            thread_safe_print(f"  Total companies processed: {companies_processed}")
            thread_safe_print(f"  Total rows created: {total_rows}")
            thread_safe_print(f"  Processing rate: {companies_per_second:.2f} companies/second")
            thread_safe_print(f"  Row creation rate: {rows_per_second:.2f} rows/second")
            thread_safe_print(f"  Average time per company: {total_elapsed_seconds/companies_processed:.2f}s")

    # Save final results to run folder
    if 'current_results' in globals() and current_results and RUN_FOLDER_PATH:
        final_data_path = os.path.join(RUN_FOLDER_PATH, 'final_results.pkl')
        try:
            with open(final_data_path, 'wb') as f:
                pkl.dump(current_results, f)
            thread_safe_print(f"\n{'='*70}")
            thread_safe_print(f"FINAL RESULTS SAVED")
            thread_safe_print(f"Location: {final_data_path}")
            thread_safe_print(f"Total rows: {len(current_results)}")
            thread_safe_print(f"{'='*70}\n")
        except Exception as e:
            thread_safe_print(f"Warning: Failed to save final results to run folder: {e}")

    # Shutdown global thread pool executor
    shutdown_global_executor()

    # Close database connection if it was used
    if USE_DATABASE:
        close_database_connection()

    thread_safe_print(f"\ncomplete")