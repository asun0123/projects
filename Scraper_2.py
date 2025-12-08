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
from datetime import datetime
import traceback

print_lock = threading.Lock()
def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

#configuration
# =============================================================================
GLOBAL_EXECUTOR = None
EXECUTOR_MAX_WORKERS = 50

TEST_MODE = False
TEST_DATASETS = ['BRAZIL_2022_TEST.csv', 
                  'MEXICO_2022_TEST.csv']

MAX_PARALLEL_TIER1_COMPANIES = 10
MAX_TIER = 3
MAX_CALLS_PER_SECOND = 10
MAX_WORKERS_MAIN = 10
MAX_WORKERS_PROPERTIES = 15
MAX_WORKERS_TIER = 10
RATE_LIMIT_DELAY = 1.0
BATCH_SAVE_INTERVAL = 100
MAX_COMPANIES_PER_TIER = 1000

ENABLE_CHECKPOINTS = True
CHECKPOINT_FILE = 'checkpoint_5tier_progress.pkl'
ENABLE_PROGRESS_ESTIMATION = True

FORMATTER_CONFIGS = {
    'suppliers': {
        'fields': ['supplier_name', 'primary_industry', 'expense_million_usd', 
                   'expense_percent', 'min_percent', 'max_percent', 'min_value', 
                   'max_value', 'start_date', 'end_date'],
        'date_fields': ['start_date', 'end_date'],
        'output_prefix': 'supplier'
    },
    'customers': {
        'fields': ['customer_name', 'start_date', 'end_date', 'primary_industry',
                   'expense_million_usd', 'expense_percent', 'min_percent', 
                   'max_percent', 'min_value', 'max_value'],
        'date_fields': ['start_date', 'end_date'],
        'output_prefix': 'customer'
    },
    'previous_suppliers': {
        'fields': ['previous_supplier_name', 'previous_supplier_start_date', 
                   'previous_supplier_end_date', 'previous_supplier_institution_key',
                   'previous_supplier_primary_industry', 'previous_supplier_expense_million_usd',
                   'previous_supplier_expense_percent', 'previous_supplier_min_percent',
                   'previous_supplier_max_percent', 'previous_supplier_min_value',
                   'previous_supplier_max_value', 'previous_supplier_source'],
        'date_fields': ['previous_supplier_start_date', 'previous_supplier_end_date'],
        'output_prefix': 'previous_supplier'
    },
    'previous_customers': {
        'fields': ['previous_customer_name', 'previous_customer_start_date',
                   'previous_customer_end_date', 'previous_customer_institution_key',
                   'previous_customer_primary_industry', 'previous_customer_expense_million_usd',
                   'previous_customer_expense_percent', 'previous_customer_min_percent',
                   'previous_customer_max_percent', 'previous_customer_min_value',
                   'previous_customer_max_value', 'previous_customer_source'],
        'date_fields': ['previous_customer_start_date', 'previous_customer_end_date'],
        'output_prefix': 'previous_customer'
    },
    'tags': {
        'fields': ['tag_name', 'trust_rank'],
        'output_prefix': 'tag'
    },
    'nace_codes': {
        'fields': ['nace_code', 'nace_industry'],
        'output_prefix': 'nace'
    },
    'naics_codes': {
        'fields': ['naics_code', 'naics_desc', 'relationship_level'],
        'output_prefix': 'naics'
    },
    'mining_properties': {
        'fields': ['property_name', 'primary_commodity', 'all_commodities', 'operator',
                   'development_stage', 'activity_status', 'equity_ownership', 
                   'primary_resources', 'unit', 'total_in_situ_value_usd_m',
                   'reserves_resources_as_of_date', 'country_risk_score', 'property_id'],
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

# exclusion lists
# =============================================================================
EXCLUDED_COMPANIES = [
    'not specified', 'not available', 'n/a', 'na', 'unknown', 'none',
    'dhl global forwarding', 'dhl', 'fedex', 'ups', 'maersk',
    'same as consignee', 'same as shipper', 'to order', 'freight forwarder',
]
EXCLUDED_COMPANIES_NORMALIZED = [exc.lower().strip() for exc in EXCLUDED_COMPANIES]

csv_files_2022 = ['BRAZIL_2022.csv', 'MEXICO_2022.csv']

if TEST_MODE:
    csv_files_2022 = TEST_DATASETS

DATASET_COLUMN_MAPPING = {
    'BRAZIL_2022_TEST': {'name_col': ['shipper', 'consignee'],
                        'country_col': ['loading_country', 'consignee_country']},
    'BRAZIL_2022': {'name_col': ['shipper', 'consignee'],
                    'country_col': ['loading_country', 'consignee_country']},
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

thread_safe_print("Loading cookies")
cookies_dict = {}
with open('./cookies.pkl', 'rb') as f:
    cookies_list = pkl.load(f)
    for cookie in cookies_list:
        cookies_dict[cookie['name']] = cookie['value']
thread_safe_print("Cookies loaded\n")

API_URL = "https://www.capitaliq.spglobal.com/apisv3/search-service/v3/OmniSuggest/suggest"
REPORT_API_URL = "https://www.capitaliq.spglobal.com/apisv3/hydra-service/v1/report/render/SPA"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.capitaliq.spglobal.com',
    'Referer': 'https://www.capitaliq.spglobal.com/'}

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
def save_checkpoint(results, checkpoint_file=CHECKPOINT_FILE):
    if not ENABLE_CHECKPOINTS:
        return
    
    checkpoint_data = {
        'results': results,
        'timestamp': datetime.now().isoformat(),
        'stats': progress_tracker.get_stats()
    }
    
    with open(checkpoint_file, 'wb') as f:
        pkl.dump(checkpoint_data, f)
    
    thread_safe_print(f"Checkpoint saved: {len(results)} rows")

def load_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    if not ENABLE_CHECKPOINTS or not os.path.exists(checkpoint_file):
        return None
    
    try:
        with open(checkpoint_file, 'rb') as f:
            checkpoint_data = pkl.load(f)
        
        thread_safe_print(f"Checkpoint loaded from {checkpoint_data['timestamp']}")
        thread_safe_print(f"   Resuming with {len(checkpoint_data['results'])} existing rows")
        return checkpoint_data
    except Exception as e:
        thread_safe_print(f"Failed to load checkpoint: {e}")
        return None

# shutdown handler
# =============================================================================
def signal_handler(_sig, _frame):
    thread_safe_print("\n\n Interrupt received! Saving progress...")
    if 'current_results' in globals():
        save_checkpoint(current_results, f'emergency_checkpoint_{int(time.time())}.pkl')
        thread_safe_print("Emergency checkpoint saved")
    progress_tracker.print_progress()
    thread_safe_print("Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# helper functions
# ============================================================================
# Global ThreadPool - shared across all operations to eliminate overhead
def initialize_global_executor():
    global GLOBAL_EXECUTOR
    if GLOBAL_EXECUTOR is None:
        GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_MAX_WORKERS)
        thread_safe_print(f"Initialized global ThreadPoolExecutor with {EXECUTOR_MAX_WORKERS} workers")
    return GLOBAL_EXECUTOR

def shutdown_global_executor():
    global GLOBAL_EXECUTOR
    if GLOBAL_EXECUTOR is not None:
        GLOBAL_EXECUTOR.shutdown(wait=True)
        GLOBAL_EXECUTOR = None
        thread_safe_print("Global ThreadPoolExecutor shutdown complete")

def extract_company_names(csv_path, dataset_file, chunk_size=500):
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

def extract_companies_from_chunk(dataset_file, df_chunk, name_cols):
    companies = set()
    for col in name_cols:
        if col in df_chunk.columns:
            companies_in_col = df_chunk[col].dropna().unique()
            for company in companies_in_col:
                company_clean = str(company).strip()
                if not _is_excluded_company(company_clean):
                    companies.add(company_clean)
    
    return companies

def chunk_extract_MIKey(companies_chunk): #Amir
    """Process a chunk of companies - multiprocessing compatible"""
    chunk_session = requests.Session()
    chunk_session.cookies.update(cookies_dict)

    results = []
    for company in companies_chunk:
        try:
            result = search_company(chunk_session, company)
            results.append(result)
        except Exception as e:
            # If search_company itself fails (shouldn't happen, but safety net)
            results.append({
                'company_name': company, 'matched_name': '', 'country': '',
                'mi_key': None, 'spciq_key': None, 'status': f'chunk_error_{type(e).__name__}'
            })

    chunk_session.close()
    return results
        
    #    companies = set()
    #    for col in name_cols:
    #        companies_in_cols = df[col].dropna().unique()
    #        for company in companies_in_cols:
    #            company_clean = str(company).strip()
    #            if not _is_excluded_company(company_clean):
    #                companies.add(company_clean)
    #
    #    thread_safe_print(f"Extracted {len(companies)} unique company names from {dataset_file}")
    #    return companies
    #except Exception as e:
    #    thread_safe_print(f"Error extracting company names from {dataset_file}: {e}")
    #    return set()

thread_local = threading.local()
def get_thread_session(cookies_dict):
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
        thread_local.session.cookies.update(cookies_dict)
        thread_local.last_refresh = time.time()
    
    if time.time() - thread_local.last_refresh > 1800:
        thread_local.session.cookies.update(cookies_dict)
        thread_local.last_refresh = time.time()
    
    return thread_local.session
    
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
            plural_name = f"{config.get('output_prefix', '')}_{output_name}s" if not output_name.endswith('s') else f"{config.get('output_prefix', '')}_{output_name}"
            result[plural_name] = 'NaN'
        return result
    
    result = {}
    date_fields = config.get('date_fields', [])
    use_braces = config.get('use_braces', False)

    for field in config['fields']:
        values = [item.get(field, '') for item in data]

        output_name = field.replace(config.get('output_prefix', ''), '')
        if output_name.startswith('_'):
            output_name = output_name[1:]
        plural_name = f"{config.get('output_prefix', '')}_{output_name}s" if not output_name.endswith('s') else f"{config.get('output_prefix', '')}_{output_name}"
        
        if field in date_fields:
            result[plural_name] = format_dates_for_csv(values)
        elif use_braces:
            result[plural_name] = format_nested_with_braces(values)
        else:
            result[plural_name] = format_list_for_csv(values)
    
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
#    
#    try:
#        if date_string.isdigit() and len(date_string) == 4:
#            return date_string
#        
#        if date_string.isdigit() and len(date_string) > 10:
#            timestamp_ms = int(date_string)
#            timestamp_s = timestamp_ms / 1000
#            dt = datetime.fromtimestamp(timestamp_s)
#            return str(dt.year)
#        
#        if date_string.isdigit() and len(date_string) == 10:
#            timestamp_s = int(date_string)
#            dt = datetime.fromtimestamp(timestamp_s)
#            return str(dt.year)
#        
#        if '-' in date_string or '/' in date_string:
#            dt = pd.to_datetime(date_string, errors='coerce')
#            if pd.notna(dt):
#                return str(dt.year)
#        
#        year_match = re.search(r'(19|20)\d{2}', date_string)
#        if year_match:
#            return year_match.group(0)
#        
#        return date_string
#        
#    except Exception as e:
#        return ''

def clean_html(text):
    if not text:
        return ""
    clean = re.sub('<[^<]+?>', '', str(text))
    clean = ' '.join(clean.split())
    return clean

def extract_value_from_html(cell_html):
    if not cell_html:
        return np.nan
    
    if 'NA' in str(cell_html) or 'NM' in str(cell_html):
        return np.nan
    
    text = clean_html(cell_html)
    
    if not text or text in ['NA', 'NM', '-', '']:
        return np.nan
    
    try:
        text = text.replace(',', '')

        if '(' in text and ')' in text:
            text = '-' + text.replace('(', '').replace(')', '')
        
        return float(text)
    except (ValueError, AttributeError):
        return np.nan
    
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
            if result and isinstance(result, dict):
                if all(not v for v in result.values()):
                    if attempt < max_retries:
                        backoff_time = 2 ** attempt
                        thread_safe_print("refreshing cookies")
                        cookie_manager.refresh_session_cookies(session)
                        time.sleep(backoff_time)
                        continue
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

def quick_relationship_check(session, company_id):
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

        suppliers_response = session.get(REPORT_API_URL, params=suppliers_params, headers=HEADERS, timeout=15)
        if suppliers_response.status_code == 200:
            suppliers_data = suppliers_response.json()
            suppliers_controls = suppliers_data.get('controls', {})
            supplier_source = suppliers_controls.get('section_1_control_38', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            previous_suppliers_source = suppliers_controls.get('section_1_control_45', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            has_suppliers = len(supplier_source) > 0 or len(previous_suppliers_source) > 0

        customer_params = {
            'mode': 'browser',
            'id': company_id,
            'keypage': '478397',
            'crossSiteFilterCurrency': 'USD',
            '_': str(int(time.time() * 1000))
        }
        
        customers_response = session.get(REPORT_API_URL, params=customer_params, headers=HEADERS, timeout=15)
        if customers_response.status_code == 200:
            customers_data = customers_response.json()
            customers_controls = customers_data.get('controls', {})
            customer_source = customers_controls.get('section_1_control_45', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            previous_customers_source = customers_controls.get('section_1_control_49', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            has_customers = len(customer_source) > 0 or len(previous_customers_source) > 0
            
    except Exception as e:
        return (True, True)
    
    return (has_suppliers, has_customers)

# Recursive processing of company and its relationships
def process_company_recursive(session, company_name, spciq_key, dataset_file, dataset_row_index,
                              dataset_country, found_in_column, parent_company='',
                              tier=1, max_tier=MAX_TIER, processed_companies=None,
                              tier_company_counts=None, api_matched_name='', api_country='', last_cookie_refresh = None):

    cookie_manager.check_and_refresh_cookies(session)

    if processed_companies is None:
        processed_companies = set()
    if tier_company_counts is None:
        tier_company_counts = defaultdict(int)
    
    if spciq_key in processed_companies:
        return []
    
    #if tier_company_counts[tier] >= MAX_COMPANIES_PER_TIER:
    #    thread_safe_print(f"Tier {tier} reached safety limit of {MAX_COMPANIES_PER_TIER} companies. Stopping expansion.")
    #    return []
    
    processed_companies.add(spciq_key)
    tier_company_counts[tier] += 1
    progress_tracker.increment_tier(tier)
    
    results = []

    skip_financials = False
    if tier == 1:
        thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Quick check: {company_name}")
        has_suppliers, has_customers = quick_relationship_check(session, spciq_key)

        if not has_suppliers and not has_customers:
            thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] No suppliers or customers found for {company_name}, skipping detailed fetch.")
            skip_financials = True
    
    thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Processing: {company_name}")
    all_data = fetch_all_company_data_parallel(session, spciq_key, company_name, skip_financials=skip_financials)    
    
    #thread_safe_print(f"{'  ' * (tier-1)}[Tier {tier}] Processing: {company_name}")
    #all_data = fetch_all_company_data_parallel(session, spciq_key, company_name)

    if not all_data or not isinstance(all_data, dict):
        thread_safe_print(f"Invalid data structure returned for {company_name}, skipping...")
        return []

    relationships = all_data.get('relationships', {'suppliers': [], 'customers': []})
    mining_data = all_data.get('mining', {'properties': []})
    profile_data = all_data.get('profile', get_empty_profile_data())
    financial_data = all_data.get('financials', get_empty_financials())
    property_profiles = all_data.get('property_profiles', [])
    mining_locations = all_data.get('mining_locations', [])

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
    
    format_supplier_data_for_csv = lambda data: format_data_for_csv(data, 'suppliers')
    format_customer_data_for_csv = lambda data: format_data_for_csv(data, 'customers')
    format_previous_supplier_data_for_csv = lambda data: format_data_for_csv(data, 'previous_suppliers')
    format_previous_customer_data_for_csv = lambda data: format_data_for_csv(data, 'previous_customers')
    format_tags_for_csv = lambda data: format_data_for_csv(data, 'tags')
    format_nace_codes_for_csv = lambda data: format_data_for_csv(data, 'nace_codes')
    format_naics_codes_for_csv = lambda data: format_data_for_csv(data, 'naics_codes')
    format_mining_properties_for_csv = lambda data: format_data_for_csv(data, 'mining_properties')
    format_property_profile_data_for_csv = lambda data: format_data_for_csv(data, 'property_profiles')

    supplier_data_formatted = format_supplier_data_for_csv(suppliers)
    customer_data_formatted = format_customer_data_for_csv(customers)
    previous_supplier_data_formatted = format_previous_supplier_data_for_csv(previous_suppliers)
    previous_customer_data_formatted = format_previous_customer_data_for_csv(previous_customers)
    tags_formatted = format_tags_for_csv(profile_data.get('tags', []))
    nace_formatted = format_nace_codes_for_csv(profile_data.get('nace_codes', []))
    naics_formatted = format_naics_codes_for_csv(profile_data.get('naics_codes', []))
    mining_properties_formatted = format_mining_properties_for_csv(mining_data.get('properties', []))
    property_profiles_formatted = format_property_profile_data_for_csv(property_profiles)
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
       'original_company_name': company_name,
       'dataset_company_name': company_name,
       'found_in_column': found_in_column,
       'dataset_country': dataset_country,
       'api_matched_name': api_matched_name if api_matched_name else company_name,
       'api_country': api_country,
       'spciq_key': spciq_key,
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
    
    has_financials = any(v != np.nan and pd.notna(v) for k, v in financial_data.items() if k.startswith('fin_'))
    fin_status = "Financials" if has_financials else "No financials"
    prop_status = f"{len(property_profiles)} property profiles" if property_profiles else "No property profiles"
    
    thread_safe_print(f"{'  ' * (tier-1)}  Found {len(suppliers)} suppliers, {len(customers)} customers | {fin_status} | {prop_status} | Country: {api_country}")

    if tier < max_tier:
        next_tier_companies = []

        thread_safe_print(f"{'  ' * (tier-1)}[INFO] Processing tier {tier} → tier {tier+1}")
        thread_safe_print(f"{'  ' * (tier-1)}[INFO] Found {len(suppliers)} suppliers, {len(customers)} customers")

        for supplier in suppliers:
            try:
                # Get supplier info
                supplier_name = supplier.get('supplier_name')
                supplier_id = supplier.get('key_instn')

                # Validate supplier name
                if not supplier_name:
                    thread_safe_print(f"{'  ' * (tier-1)}[SKIP] Empty supplier name")
                    continue

                if _is_excluded_company(supplier_name):
                    thread_safe_print(f"{'  ' * (tier-1)}[SKIP] Excluded supplier: {supplier_name}")
                    continue
                
                # Initialize API data variables
                supplier_api_matched_name = supplier_name
                supplier_api_country = supplier.get('country', '')

                # Handle missing key_instn by searching
                if not supplier_id:
                    thread_safe_print(f"{'  ' * (tier-1)}⚠️  No ID for supplier '{supplier_name}', searching API...")
                    time.sleep(RATE_LIMIT_DELAY)

                    try:
                        search_result = search_company(session, supplier_name)
                        supplier_id = search_result.get('spciq_key', '')
                        supplier_api_matched_name = search_result.get('matched_name', supplier_name)
                        supplier_api_country = search_result.get('country', '')

                        if not supplier_id:
                            thread_safe_print(f"{'  ' * (tier-1)}Could not find supplier '{supplier_name}'")
                            continue

                        thread_safe_print(f"{'  ' * (tier-1)}Found ID for supplier '{supplier_name}': {supplier_id}")

                    except Exception as search_error:
                        thread_safe_print(f"{'  ' * (tier-1)}Search failed for supplier '{supplier_name}': {search_error}")
                        continue
                    
                # Check if already processed
                if supplier_id in processed_companies:
                    thread_safe_print(f"{'  ' * (tier-1)}Supplier '{supplier_name}' already processed")
                    continue
                
                # Add to next tier processing queue
                next_tier_companies.append({
                    'company_name': supplier_name,
                    'spciq_key': supplier_id,
                    'found_in_column': f'tier_{tier}_supplier',
                    'api_matched_name': supplier_api_matched_name,
                    'api_country': supplier_api_country
                })

                thread_safe_print(f"{'  ' * (tier-1)}[QUEUE] Added supplier: {supplier_name} (ID: {supplier_id})")

            except Exception as e:
                thread_safe_print(f"{'  ' * (tier-1)}[ERROR] Processing supplier: {e}")
                continue

        # ========================================================================
        # PROCESS CUSTOMERS WITH SEARCH FALLBACK
        # ========================================================================
        for customer in customers:
            try:
                # Get customer info
                customer_name = customer.get('customer_name')
                customer_id = customer.get('key_instn')

                # Validate customer name
                if not customer_name:
                    thread_safe_print(f"{'  ' * (tier-1)}[SKIP] Empty customer name")
                    continue

                if _is_excluded_company(customer_name):
                    thread_safe_print(f"{'  ' * (tier-1)}[SKIP] Excluded customer: {customer_name}")
                    continue
                
                # Initialize API data variables
                customer_api_matched_name = customer_name
                customer_api_country = customer.get('country', '')

                # Handle missing key_instn by searching
                if not customer_id:
                    thread_safe_print(f"{'  ' * (tier-1)}⚠️  No ID for customer '{customer_name}', searching API...")
                    time.sleep(RATE_LIMIT_DELAY)

                    try:
                        search_result = search_company(session, customer_name)
                        customer_id = search_result.get('spciq_key', '')
                        customer_api_matched_name = search_result.get('matched_name', customer_name)
                        customer_api_country = search_result.get('country', '')

                        if not customer_id:
                            thread_safe_print(f"{'  ' * (tier-1)}Could not find customer '{customer_name}'")
                            continue

                        thread_safe_print(f"{'  ' * (tier-1)}Found ID for customer '{customer_name}': {customer_id}")

                    except Exception as search_error:
                        thread_safe_print(f"{'  ' * (tier-1)}Search failed for customer '{customer_name}': {search_error}")
                        continue
                    
                # Check if already processed
                if customer_id in processed_companies:
                    thread_safe_print(f"{'  ' * (tier-1)}Customer '{customer_name}' already processed")
                    continue
                
                # Add to next tier processing queue
                next_tier_companies.append({
                    'company_name': customer_name,
                    'spciq_key': customer_id,
                    'found_in_column': f'tier_{tier}_customer',
                    'api_matched_name': customer_api_matched_name,
                    'api_country': customer_api_country
                })

                thread_safe_print(f"{'  ' * (tier-1)}[QUEUE] Added customer: {customer_name} (ID: {customer_id})")

            except Exception as e:
                thread_safe_print(f"{'  ' * (tier-1)}[ERROR] Processing customer: {e}")
                continue

        # ========================================================================
        # PROCESS ALL NEXT TIER COMPANIES IN PARALLEL (using global executor)
        # ========================================================================
        thread_safe_print(f"{'  ' * (tier-1)}Total companies queued for tier {tier+1}: {len(next_tier_companies)}")

        if next_tier_companies:
            thread_safe_print(f"{'  ' * (tier-1)}Parallel processing of {len(next_tier_companies)} companies")

            # Use global executor instead of creating a new one
            tier_futures = []

            for company_info in next_tier_companies:
                future = GLOBAL_EXECUTOR.submit(
                    process_company_recursive,
                    session=session,
                    company_name=company_info['company_name'],
                    spciq_key=company_info['spciq_key'],
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
                tier_futures.append(future)

            # Collect results
            for future in tier_futures:
                try:
                    tier_results = future.result(timeout=120)
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

def match_companies_to_datasets(df_api_results, session, csv_dir='csv_files_2022',
                                output_file='all_countries_results.csv', max_tier=MAX_TIER):
    global current_results
    
    df_found = df_api_results[df_api_results['mi_key'].notna()].copy()
    
    if df_found.empty:
        thread_safe_print("❌ No companies with MI keys found!")
        return
    
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"Starting {max_tier}-tier PARALLEL processing WITH FINANCIAL DATA...")
    thread_safe_print(f"Tier 1 companies will be processed in parallel batches")
    thread_safe_print(f"{'='*70}\n")
    
    company_to_api = {}
    for _, row in df_found.iterrows():
        company_norm = normalize_string(row['company_name'])
        company_to_api[company_norm] = row.to_dict()
    
    # ========================================================================
    # LOAD CHECKPOINT
    # ========================================================================
    checkpoint_data = load_checkpoint()
    if checkpoint_data:
        existing_results = checkpoint_data['results']
        
        # Build set of already-processed root companies (tier 1)
        processed_root_companies = set()
        for result in existing_results:
            if result.get('tier') == 1:
                processed_root_companies.add(normalize_string(result.get('original_company_name', '')))
        
        # Build set of all processed spciq_keys
        global_processed_companies = {r['spciq_key'] for r in existing_results if 'spciq_key' in r}
        
        thread_safe_print(f"\n{'='*70}")
        thread_safe_print(f"📂 CHECKPOINT LOADED")
        thread_safe_print(f"{'='*70}")
        thread_safe_print(f"Existing results: {len(existing_results)} rows")
        thread_safe_print(f"Tier 1 companies already processed: {len(processed_root_companies)}")
        thread_safe_print(f"All unique companies processed: {len(global_processed_companies)}")
        thread_safe_print(f"{'='*70}\n")
    else:
        existing_results = []
        processed_root_companies = set()
        global_processed_companies = set()
        thread_safe_print("ℹ️  No checkpoint found, starting fresh\n")
    
    # Initialize thread-safe result collector
    result_collector = ThreadSafeResultCollector()
    result_collector.results = existing_results
    result_collector.processed_companies = global_processed_companies
    
    current_results = result_collector.get_all_results()
    
    datasets_to_process = TEST_DATASETS if TEST_MODE else csv_files_2022
    
    # ========================================================================
    # COLLECT ALL COMPANIES TO PROCESS
    # ========================================================================
    companies_to_process = []
    
    for dataset_file in datasets_to_process:
        file_path = Path(csv_dir) / dataset_file
        
        if not file_path.exists():
            thread_safe_print(f"⚠️  Dataset not found: {dataset_file}")
            continue
        
        thread_safe_print(f"\n📊 Scanning dataset: {dataset_file}")
        
        df_dataset = pd.read_csv(file_path, dtype=str, low_memory=False).fillna('')
        
        name_cols, country_cols = get_dataset_columns(dataset_file)
        if not name_cols:
            thread_safe_print(f"⚠️  No name columns configured for {dataset_file}")
            continue
        
        name_cols = [c for c in name_cols if c in df_dataset.columns]
        country_cols = [c for c in country_cols if c in df_dataset.columns]
        
        if not name_cols:
            thread_safe_print(f"⚠️  No valid name columns found in {dataset_file}")
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
                    
                    companies_to_process.append({
                        'company_name': company_in_dataset,
                        'spciq_key': spciq_key,
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
    thread_safe_print(f"📋 COMPANIES TO PROCESS")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Total Tier 1 companies queued: {len(companies_to_process)}")
    thread_safe_print(f"Already processed (from checkpoint): {len(existing_results)}")
    thread_safe_print(f"{'='*70}\n")
    
    if not companies_to_process:
        thread_safe_print("✅ All companies already processed!")
        return
    
    # ========================================================================
    # PROCESS COMPANIES IN PARALLEL
    # ========================================================================
    def process_single_company(company_info):
        """Wrapper function for parallel processing"""
        try:
            thread_safe_print(f"\n{'='*70}")
            thread_safe_print(f"🏢 [PARALLEL] Processing: {company_info['company_name']}")
            thread_safe_print(f"{'='*70}")
            
            # Get current processed companies from collector
            current_processed = result_collector.get_processed_companies()
            
            company_results = process_company_recursive(
                session=session,
                company_name=company_info['company_name'],
                spciq_key=company_info['spciq_key'],
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
                thread_safe_print(f"✅ [PARALLEL] Completed: {company_info['company_name']} ({len(company_results)} rows, total: {total_results})")
                
                # Check if we should save checkpoint
                if result_collector.should_checkpoint():
                    all_results = result_collector.get_all_results()
                    save_checkpoint(all_results)
                    
                    # Save intermediate CSV
                    df_temp = pd.DataFrame(all_results)
                    temp_output = Path(csv_dir) / f'temp_{output_file}'
                    df_temp.to_csv(temp_output, index=False, na_rep='NaN')
                    
                    stats = result_collector.get_stats()
                    thread_safe_print(f"\n{'='*70}")
                    thread_safe_print(f"CHECKPOINT SAVED (time-based)")
                    thread_safe_print(f"{'='*70}")
                    thread_safe_print(f"Total rows: {stats['total_results']}")
                    thread_safe_print(f"Unique companies: {stats['unique_companies']}")
                    thread_safe_print(f"{'='*70}\n")
            
            return True
            
        except Exception as e:
            thread_safe_print(f"[PARALLEL] Error processing {company_info['company_name']}: {e}")
            import traceback
            thread_safe_print(f"[TRACE] {traceback.format_exc()}")
            return False
    
    # Determine number of parallel workers
    max_parallel_tier1 = min(MAX_PARALLEL_TIER1_COMPANIES, len(companies_to_process))  # Start conservative with 5
    
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"🚀 STARTING PARALLEL PROCESSING")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Parallel Tier 1 workers: {max_parallel_tier1}")
    thread_safe_print(f"Companies to process: {len(companies_to_process)}")
    thread_safe_print(f"{'='*70}\n")
    
    start_time = time.time()
    completed = 0

    # Use global executor instead of creating a new one
    # Submit all companies for parallel processing
    futures = {
        GLOBAL_EXECUTOR.submit(process_single_company, company_info): company_info
        for company_info in companies_to_process
    }

    # Wait for completion and track progress
    for future in futures:
        try:
            result = future.result(timeout=600)  # 10 minute timeout per company
            completed += 1

            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            remaining = len(companies_to_process) - completed
            eta = remaining / rate if rate > 0 else 0

            if completed % 5 == 0:  # Progress update every 5 companies
                thread_safe_print(f"\n{'='*70}")
                thread_safe_print(f"📊 PROGRESS UPDATE")
                thread_safe_print(f"{'='*70}")
                thread_safe_print(f"Completed: {completed}/{len(companies_to_process)}")
                thread_safe_print(f"Processing rate: {rate:.2f} companies/sec")
                thread_safe_print(f"Elapsed: {elapsed/60:.1f} min")
                thread_safe_print(f"ETA: {eta/60:.1f} min")
                thread_safe_print(f"{'='*70}\n")

        except Exception as e:
            thread_safe_print(f"❌ Future failed: {e}")
    
    # ========================================================================
    # FINAL SAVE
    # ========================================================================
    all_results = result_collector.get_all_results()
    
    if all_results:
        df_results = pd.DataFrame(all_results)
        output_path = Path(csv_dir) / output_file
        df_results.to_csv(output_path, index=False, na_rep='NaN')
        
        thread_safe_print(f"\n{'='*70}")
        thread_safe_print("✅ PROCESSING COMPLETE")
        thread_safe_print(f"{'='*70}")
        thread_safe_print(f"Total rows: {len(df_results)}")
        thread_safe_print(f"Unique companies: {len(result_collector.get_processed_companies())}")
        
        if 'tier' in df_results.columns:
            tier_counts = df_results['tier'].value_counts().sort_index()
            thread_safe_print(f"\n📊 TIER BREAKDOWN:")
            for tier, count in tier_counts.items():
                thread_safe_print(f"  Tier {tier}: {count} companies")
        
        total_time = time.time() - start_time
        thread_safe_print(f"\n⏱️  Total processing time: {total_time/60:.1f} minutes")
        thread_safe_print(f"📈 Average rate: {len(companies_to_process)/(total_time/60):.2f} companies/minute")
        
        thread_safe_print(f"\n✅ Results saved to: {output_path}")
        thread_safe_print(f"{'='*70}\n")
        
        progress_tracker.print_progress()

#def match_companies_to_datasets(df_api_results, session, csv_dir='csv_files_2022',
#                                output_file='all_countries_results.csv', max_tier=MAX_TIER):
#    global current_results
#    
#    df_found = df_api_results[df_api_results['mi_key'].notna()].copy()
#    
#    if df_found.empty:
#        thread_safe_print("No companies with MI keys found!")
#        return
#    
#    thread_safe_print(f"\n{'='*70}")
#    thread_safe_print(f"Starting {max_tier}-tier recursive processing WITH FINANCIAL DATA...")
#    thread_safe_print(f"WARNING: This may take a VERY long time for {max_tier} tiers!")
#    thread_safe_print(f"{'='*70}\n")
#    
#    company_to_api = {}
#    for _, row in df_found.iterrows():
#        company_norm = normalize_string(row['company_name'])
#        company_to_api[company_norm] = row.to_dict()
#    
#    # ========================================================================
#    # IMPROVED CHECKPOINT LOADING
#    # ========================================================================
#    checkpoint_data = load_checkpoint()
#    if checkpoint_data:
#        all_results = checkpoint_data['results']
#        
#        # Build set of already-processed root companies (tier 1)
#        processed_root_companies = set()
#        for result in all_results:
#            if result.get('tier') == 1:  # Only track tier 1 companies
#                # Store normalized company name for matching
#                processed_root_companies.add(normalize_string(result.get('original_company_name', '')))
#        
#        # Build set of all processed spciq_keys (all tiers)
#        global_processed_companies = {r['spciq_key'] for r in all_results if 'spciq_key' in r}
#        
#        thread_safe_print(f"\n{'='*70}")
#        thread_safe_print(f"CHECKPOINT LOADED")
#        thread_safe_print(f"{'='*70}")
#        thread_safe_print(f"Existing results: {len(all_results)} rows")
#        thread_safe_print(f"Tier 1 companies already processed: {len(processed_root_companies)}")
#        thread_safe_print(f"All unique companies processed: {len(global_processed_companies)}")
#        thread_safe_print(f"{'='*70}\n")
#    else:
#        all_results = []
#        processed_root_companies = set()  # NEW: Track processed root companies
#        global_processed_companies = set()
#        thread_safe_print("ℹNo checkpoint found, starting fresh\n")
#    
#    current_results = all_results
#    
#    datasets_to_process = TEST_DATASETS if TEST_MODE else csv_files_2022
#    
#    # ========================================================================
#    # PROCESS DATASETS WITH SKIP LOGIC
#    # ========================================================================
#    for dataset_file in datasets_to_process:
#        file_path = Path(csv_dir) / dataset_file
#        
#        if not file_path.exists():
#            thread_safe_print(f"⚠️  Dataset not found: {dataset_file}")
#            continue
#        
#        thread_safe_print(f"\n{'='*70}")
#        thread_safe_print(f"Processing dataset: {dataset_file}")
#        thread_safe_print(f"{'='*70}\n")
#        
#        df_dataset = pd.read_csv(file_path, dtype=str, low_memory=False).fillna('')
#        
#        name_cols, country_cols = get_dataset_columns(dataset_file)
#        if not name_cols:
#            thread_safe_print(f"No name columns configured for {dataset_file}")
#            continue
#        
#        name_cols = [c for c in name_cols if c in df_dataset.columns]
#        country_cols = [c for c in country_cols if c in df_dataset.columns]
#        
#        if not name_cols:
#            thread_safe_print(f"No valid name columns found in {dataset_file}")
#            continue
#        
#        primary_name_col = name_cols[0] if isinstance(name_cols[0], str) else name_cols[0]
#        alternate_cols = get_alternate_name_columns(primary_name_col)
#        search_cols = name_cols + [col for col in alternate_cols if col in df_dataset.columns]
#        
#        companies_found = 0
#        companies_skipped = 0
#        companies_processed = 0
#        
#        for idx, row in df_dataset.iterrows():
#            for col in search_cols:
#                company_in_dataset = str(row.get(col, '')).strip()
#                
#                if not company_in_dataset or _is_excluded_company(company_in_dataset):
#                    continue
#                
#                company_norm = normalize_string(company_in_dataset)
#                
#                # ========================================================================
#                # NEW: SKIP IF ALREADY PROCESSED IN CHECKPOINT
#                # ========================================================================
#                if company_norm in processed_root_companies:
#                    companies_skipped += 1
#                    if companies_skipped % 10 == 0:  # Log every 10 skips
#                        thread_safe_print(f"⏭️  Skipped {companies_skipped} already-processed companies...")
#                    continue
#                
#                if company_norm in company_to_api:
#                    companies_found += 1
#                    api_data = company_to_api[company_norm]
#                    
#                    dataset_country = ''
#                    if country_cols:
#                        for country_col in country_cols:
#                            if country_col in row and row[country_col]:
#                                dataset_country = str(row[country_col]).strip()
#                                break
#                    
#                    spciq_key = api_data.get('spciq_key')
#                    if not spciq_key:
#                        continue
#                    
#                    # Double-check against global processed set
#                    if spciq_key in global_processed_companies:
#                        companies_skipped += 1
#                        continue
#                    
#                    api_matched_name = api_data.get('matched_name', company_in_dataset)
#                    api_country = api_data.get('country', '')
#                    
#                    thread_safe_print(f"\n{'='*70}")
#                    thread_safe_print(f"🏢 NEW COMPANY: {company_in_dataset}")
#                    thread_safe_print(f"{'='*70}")
#                    
#                    # Process recursively
#                    company_results = process_company_recursive(
#                        session=session,
#                        company_name=company_in_dataset,
#                        spciq_key=spciq_key,
#                        dataset_file=dataset_file,
#                        dataset_row_index=idx,
#                        dataset_country=dataset_country,
#                        found_in_column=col,
#                        parent_company='',
#                        tier=1,
#                        max_tier=max_tier,
#                        processed_companies=global_processed_companies,
#                        api_matched_name=api_matched_name,
#                        api_country=api_country
#                    )
#                    
#                    if company_results:
#                        all_results.extend(company_results)
#                        current_results = all_results
#                        companies_processed += 1
#                        
#                        # Add to processed root companies
#                        processed_root_companies.add(company_norm)
#                        
#                        # Save checkpoint periodically
#                        if len(all_results) % BATCH_SAVE_INTERVAL == 0:
#                            save_checkpoint(all_results)
#                            
#                            # Save intermediate CSV
#                            df_temp = pd.DataFrame(all_results)
#                            temp_output = Path(csv_dir) / f'temp_{output_file}'
#                            df_temp.to_csv(temp_output, index=False, na_rep='NaN')
#                            
#                            thread_safe_print(f"\n{'='*70}")
#                            thread_safe_print(f"💾 CHECKPOINT SAVED")
#                            thread_safe_print(f"{'='*70}")
#                            thread_safe_print(f"Total rows: {len(all_results)}")
#                            thread_safe_print(f"Companies processed this session: {companies_processed}")
#                            thread_safe_print(f"Companies skipped (already done): {companies_skipped}")
#                            thread_safe_print(f"{'='*70}\n")
#                    
#                    break  # Found company in this column, move to next row
#        
#        thread_safe_print(f"\n{'='*70}")
#        thread_safe_print(f"📊 DATASET SUMMARY: {dataset_file}")
#        thread_safe_print(f"{'='*70}")
#        thread_safe_print(f"Companies found in API results: {companies_found}")
#        thread_safe_print(f"Companies skipped (checkpoint): {companies_skipped}")
#        thread_safe_print(f"Companies processed this run: {companies_processed}")
#        thread_safe_print(f"{'='*70}\n")
#    
#    # ========================================================================
#    # FINAL SAVE
#    # ========================================================================
#    if all_results:
#        df_results = pd.DataFrame(all_results)
#        output_path = Path(csv_dir) / output_file
#        df_results.to_csv(output_path, index=False, na_rep='NaN')
#        
#        thread_safe_print(f"\n{'='*70}")
#        thread_safe_print("PROCESSING COMPLETE")
#        thread_safe_print(f"{'='*70}")
#        thread_safe_print(f"Total rows: {len(df_results)}")
#        thread_safe_print(f"Unique companies: {len(global_processed_companies)}")
#        
#        if 'tier' in df_results.columns:
#            tier_counts = df_results['tier'].value_counts().sort_index()
#            thread_safe_print(f"\nTIER BREAKDOWN:")
#            for tier, count in tier_counts.items():
#                thread_safe_print(f"  Tier {tier}: {count} companies")
#        
#        thread_safe_print(f"\nResults saved to: {output_path}")
#        thread_safe_print(f"{'='*70}\n")
    
    # Scoring statistics
    if 'fuzzy_match_score' in df_results.columns:
        thread_safe_print(f"FUZZY MATCH SCORE STATISTICS:")
        thread_safe_print(f"  Average fuzzy match score: {df_results['fuzzy_match_score'].mean():.2f}")
        thread_safe_print(f"  Minimum fuzzy match score: {df_results['fuzzy_match_score'].min():.2f}")
        thread_safe_print(f"  Maximum fuzzy match score: {df_results['fuzzy_match_score'].max():.2f}")
        
    if 'country_match' in df_results.columns:
        thread_safe_print(f"\n🌍 COUNTRY MATCH STATISTICS:")

        country_match_count = (df_results['country_match'] == True).sum()
        
        # Count Tier 1 companies (those with actual country comparisons)
        tier_1_count = (df_results['country_match'] != 'NaN').sum()
        
        thread_safe_print(f"  Tier 1 companies (with dataset country): {tier_1_count}")
        thread_safe_print(f"  Tier 1 companies with country match: {country_match_count}")
        
        if tier_1_count > 0:
            match_rate = (country_match_count / tier_1_count * 100)
            thread_safe_print(f"  Tier 1 country match rate: {match_rate:.2f}%")
        
        # Count Tier 2+ companies (those with 'NaN')
        tier_2_plus_count = (df_results['country_match'] == 'NaN').sum()
        thread_safe_print(f"  Tier 2+ companies (no dataset country): {tier_2_plus_count}")
        
        # ✨ NEW: Financial data statistics
        financial_cols = [col for col in df_results.columns if col.startswith('fin_')]
        if financial_cols:
            thread_safe_print(f"FINANCIAL DATA STATISTICS:")
            thread_safe_print(f"  Total financial columns: {len(financial_cols)}")
            
            # Count companies with financial data
            companies_with_financials = 0
            for _, row in df_results.iterrows():
                has_data = any(pd.notna(row[col]) and row[col] != np.nan for col in financial_cols)
                if has_data:
                    companies_with_financials += 1
            
            thread_safe_print(f"  Companies with financial data: {companies_with_financials}")
            thread_safe_print(f"  Financial data coverage: {(companies_with_financials / len(df_results) * 100):.2f}%")
            
            # Sample some key metrics
            key_metrics = ['fin_total_revenue_usd000', 'fin_net_income_usd000', 
                          'fin_market_capitalization_usd000', 'fin_total_enterprise_value_tev_usd000']
            for metric in key_metrics:
                if metric in df_results.columns:
                    non_null = df_results[metric].notna().sum()
                    if non_null > 0:
                        thread_safe_print(f"  {metric}: {non_null} companies have data")
        
        progress_tracker.print_progress()
        thread_safe_print(f"\n✓ Results saved to: {output_path}")
        thread_safe_print(f"{'='*70}\n")
        
        # Preview with financial columns
        thread_safe_print("Preview of results:")
        preview_cols = ['original_company_name', 'tier', 'fuzzy_match_score', 
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

#new classes
# =============================================================================
class ThreadSafeResultCollector:
    def __init__(self):
        self.results = []
        self.lock = threading.Lock()
        self.processed_companies = set()
        self.last_checkpoint_time = time.time()
        self.checkpoint_interval = 300
    
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
    
    def get_stats(self):
        with self.lock:
            return {
                'total_results': len(self.results),
                'unique_companies': len(self.processed_companies)
            }

class CookieManager:
    def __init__(self, cookie_file='./cookies.pkl', refresh_interval=1800):
        self.cookie_file = cookie_file
        self.refresh_interval = refresh_interval
        self.last_refresh_time = time.time()
        self.lock = threading.Lock()

    def refresh_session_cookies(self, session):
        with self.lock:
            try:
                with open(self.cookie_file, 'rb') as f:
                    cookies_list = pkl.load(f)
                    new_cookies = {}
                    for cookie in cookies_list:
                        new_cookies[cookie['name']] = cookie['value']
                    session.cookies.update(new_cookies)
                self.last_refresh_time = time.time()
                return True
            
            except Exception as e:
                thread_safe_print(f"Failed to refresh cookies: {e}")
                return False
    
    def check_and_refresh_cookies(self, session, force=False): #"when to refresh"
        current_time = time.time()
        time_since_refresh = current_time - self.last_refresh_time

        if force or time_since_refresh > self.refresh_interval:
            return self.refresh_session_cookies(session)
        return False
    
    def refresh_on_error(self, session): #"refresh on API error"
        return self.refresh_cookies(session)
        #if current_time - last_refresh_time > refresh_interval:
        #    thread_safe_print(f"\nRefreshing session cookies")
        #    if refresh_session_cookies(session):
        #        return current_time
        #    else:
        #        thread_safe_print("Cookie refresh failed, continue with current cookes")
        #        return last_refresh_time
        #
        #return last_refresh_time

cookie_manager = CookieManager(refresh_interval=1800)

class RateLimiter:
    def __init__(self, max_calls_per_second=10):
        self.max_calls = max_calls_per_second
        self.calls = deque()
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()

            while self.calls and self.calls[0] < now - 1.0:
                self.calls.popleft()

            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    sleep(sleep_time)
                    self.calls.popleft()

            self.calls.append(time.time())

rate_limiter = RateLimiter(max_calls_per_second=10)

class ProgressTracker:
    def __init__(self):
        self.lock = threading.Lock()
        #self.tier_counts = defaultdict(int)
        #self.total_api_calls = 0
        self.start_time = time.time()
        self.companies_processed = 0
        
    def increment_tier(self, tier):
        with self.lock:
            self.tier_counts[tier] += 1
            self.companies_processed += 1
    
    #def increment_api_calls(self, count=1):
    #    with self.lock:
    #        self.total_api_calls += count
    
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

#API call #1: financial data
# =============================================================================
def fetch_financial_data(session, company_id, key_page="471751"):
    try:
        params = {
            'mode': 'browser',
            'id': company_id,  # lowercase 'id' not 'Id'
            'keypage': key_page,
            'crossSiteFilterCurrency': 'USD',
            'crossSiteSource': '1',
            '_': str(int(time.time() * 1000))
        }
        
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=HEADERS,
            timeout=30
        )
        
        progress_tracker.increment_api_calls()
        
        if response.status_code == 200:
            data = response.json()
            
            if 'controls' in data:
                return extract_all_financials(data)
            else:
                return None
        else:
            thread_safe_print(f"Financial API returned status {response.status_code} for {company_id}")
            return None
            
    except Exception as e:
        return None

#financial grid a bit messy, needs own parsing model
def parse_financial_grid(grid_model):
    try:
        data_source = grid_model.get('dataSource', [])
        
        if not data_source:
            return {}
        
        financial_data = {}
        
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
            
            skip_keys = ['_26', '_16', '_14', '_27', '_28', '_15', '_17', '_18', '_19']
            value_keys = [k for k in row.keys() if k.startswith('_') and k not in skip_keys]
            
            if value_keys:
                latest_key = sorted(value_keys)[-1]
                value = extract_value_from_html(row.get(latest_key, ''))
                
                clean_metric = (metric_key
                    .replace(' ', '_')
                    .replace('(', '')
                    .replace(')', '')
                    .replace('%', 'pct')
                    .replace('$', 'usd')
                    .replace(',', '')
                    .replace('-', '_')
                    .lower()
                )

                if len(clean_metric) > 2 and not any(c in clean_metric for c in ['<', '>', '=']):
                    parts = clean_metric.split('_')
                    if not all(len(part) <= 2 for part in parts if part):
                        financial_data[f'fin_{clean_metric}'] = value
        
        return financial_data
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}

def extract_all_financials(api_response):
    if not api_response:
        return get_empty_financials()
    
    try:
        controls = api_response.get('controls', {})
        
        if not controls:
            return get_empty_financials()
        
        all_financials = {}
        control_keys = list(controls.keys())
        thread_safe_print(f"  Found {len(control_keys)} controls in financial response")
        
        key_financials_control = controls.get('section_1_control_50', {})
        if key_financials_control:
            key_financials_model = key_financials_control.get('dataModel', {}).get('model', {})
            if key_financials_model:
                key_financials = parse_financial_grid(key_financials_model)
                all_financials.update(key_financials)
                thread_safe_print(f"Extracted {len(key_financials)} key financial metrics")
        
        valuation_control = controls.get('section_1_control_56', {})
        if valuation_control:
            valuation_model = valuation_control.get('dataModel', {}).get('model', {})
            if valuation_model:
                valuation_data = parse_financial_grid(valuation_model)
                all_financials.update(valuation_data)
                thread_safe_print(f"Extracted {len(valuation_data)} valuation metrics")
        
        capitalization_control = controls.get('section_1_control_60', {})
        if capitalization_control:
            capitalization_model = capitalization_control.get('dataModel', {}).get('model', {})
            if capitalization_model:
                capitalization_data = parse_financial_grid(capitalization_model)
                all_financials.update(capitalization_data)
                thread_safe_print(f"Extracted {len(capitalization_data)} capitalization metrics")
        
        metadata = extract_financial_metadata(controls)
        all_financials.update(metadata)
        
        if not all_financials or all(pd.isna(v) or v == '' for v in all_financials.values()):
            thread_safe_print(f"No financial metrics extracted")
            return get_empty_financials()
        
        thread_safe_print(f"Total financial fields extracted: {len(all_financials)}")
        return all_financials
        
    except Exception as e:
        thread_safe_print(f"Error extracting financials: {e}")
        import traceback
        traceback.print_exc()
        return get_empty_financials()

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
    financial_fields = [
        'fin_periodended',
        'fin_spotexchangerate',
        'fin_averageexchangerate',
        'fin_reportedcurrencycode',
        'fin_totalrevenue',
        'fin_totalrevenues1yeargrowth',
        'fin_grossprofit',
        'fin_grossprofitmargin',
        'fin_ebitda',
        'fin_ebitdamargin',
        'fin_ebit',
        'fin_ebitmargin',
        'fin_earningsfromcontops',
        'fin_earningsfromcontopsmargin',
        'fin_netincome',
        'fin_netincomemargin',
        'fin_dilutedepsexclextraitems',
        'fin_dilutedepsbeforeextra1yeargrowth',
        'fin_mitevtorevenueoperating',
        'fin_mitevtoebitda',
        'fin_mitevtoebitre',
        'fin_mipricetoearningsbeforeextra',
        'fin_mipricetobookmultiple',
        'fin_mipricetotangiblebookmultiple',
        'fin_keycurrency',
        'fin_fndgprice',
        'fin_sharesoutstanding',
        'fin_marketcapfinl',
        'fin_cashandshortinvestment',
        'fin_currency',
        'fin_magnitude'
    ]
    
    return {field: np.nan for field in financial_fields}

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
        response = session.get(profile_url, params=params, headers=HEADERS, timeout=30)
        progress_tracker.increment_api_calls()
        
        if response.status_code == 200:
            return get_empty_profile_data
        
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
                    'nace_industry': safe_get(nace_tree, 'NACEIndustry')
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
            #                thread_safe_print(f"  [Profile] ✓ NACE: {nace_code} - {nace_industry}")
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
                        'naics_desc': safe_get(naics_tree, 'NAICSDesc'),
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
        'naics_descriptions': format_list_for_csv([n['naics_desc'] for n in naics_codes]),
        'naics_relationship_levels': format_list_for_csv([n['relationship_level'] for n in naics_codes])
    }

#API call #3: mining properties data
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
        response = session.get(REPORT_API_URL, params=params, headers=HEADERS, timeout=30)
        progress_tracker.increment_api_calls()
        
        if response.status_code == 200:
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
                    'development_stage': record.get('_Stage', ''),
                    'activity_status': record.get('_Status', ''),
                    'equity_ownership': record.get('_EquityOwnership', ''),
                    'controlling_ownership': record.get('_ControllingOwnership', ''),
                    'royalty_ownership': record.get('_RoyaltyOwnership', ''),
                    'primary_resources': record.get('_PrimaryContained', ''),
                    'unit': record.get('_Magnitude', '').strip() if record.get('_Magnitude') else '',
                    'reserves_resources_as_of_date': record.get('_AsOfDate', ''),
                    'property_type': record.get('_PropertyType', ''),
                    'total_in_situ_value_usd_m': record.get('_InSitu', ''),  
                    'country_risk_score': record.get('_ECRScore', ''),  
                    'property_id': record.get('_KeyMineProject', '') 
                })
            return {'properties': properties}
        else:
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
        
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=HEADERS,
            timeout=30
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
            
            property_info = {}
            
            for row in data_source:
                field_name = row.get('_11', '') 
                field_value_html = row.get('_10', '') 
                
                if not field_name:
                    continue

                field_value = clean_html(field_value_html)

                field_mapping = {
                    'Property ID': 'property_profile_id',
                    'Also Known As': 'property_profile_aka',
                    'Property Type': 'property_profile_type',
                    'Commodity(s)': 'property_profile_commodities',
                    'Development Stage': 'property_profile_dev_stage',
                    'Activity Status': 'property_profile_activity_status',
                    'Country/Region': 'property_profile_country',
                    'State or Province': 'property_profile_state',
                }
                
                dict_key = field_mapping.get(field_name)
                if dict_key:
                    property_info[dict_key] = field_value
                    thread_safe_print(f"{field_name}: {field_value[:50]}...")
            
            if not property_info:
                thread_safe_print(f"No fields extracted for {property_id}")
                return get_empty_mining_property_profile_data()
            
            thread_safe_print(f"Extracted {len(property_info)} fields for property {property_id}")
            return property_info
            
        else:
            thread_safe_print(f"API returned status {response.status_code} for {property_id}")
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

def format_mining_property_profile_data_for_csv(property_profiles):
    if not property_profiles:
        return {
            'property_profile_akas': 'NaN',
            'property_profile_types': 'NaN',
            'property_profile_dev_stages': 'NaN',
            'property_profile_activity_statuses': 'NaN',
            'property_profile_countries': 'NaN',
            'property_profile_states': 'NaN',
        }

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
    
    return {
        'property_profile_akas': format_nested_with_braces([p.get('property_profile_aka', '') for p in property_profiles]),
        'property_profile_types': format_nested_with_braces([p.get('property_profile_type', '') for p in property_profiles]),
        'property_profile_dev_stages': format_nested_with_braces([p.get('property_profile_dev_stage', '') for p in property_profiles]),
        'property_profile_activity_statuses': format_nested_with_braces([p.get('property_profile_activity_status', '') for p in property_profiles]),
        'property_profile_countries': format_nested_with_braces([p.get('property_profile_country', '') for p in property_profiles]),
        'property_profile_states': format_nested_with_braces([p.get('property_profile_state', '') for p in property_profiles]),
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
        response = session.get(
            REPORT_API_URL,
            params=params,
            headers=HEADERS,
            timeout=30
        )
    
        if response.status_code != 200:
            return get_empty_mine_location_data()
        
        data = response.json()

        return parse_mine_location_data(data)
    
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
def fetch_supplier_customer_data(session, company_id):
    result = {
        'suppliers': [],
        'previous_suppliers': [],
        'customers': [],
        'previous_customers': []
    }

    #Suppliers Page, requires two separate keycalls
    suppliers_params = {
        'mode': 'browser',
        'id': company_id,
        'keypage': '478395',
        'crossSiteFilterCurrency': 'USD',
        '_': str(int(time.time() * 1000))
    }
    
    try:
        suppliers_response = session.get(REPORT_API_URL, params=suppliers_params, headers=HEADERS, timeout=30)
        progress_tracker.increment_api_calls()

        if suppliers_response.status_code == 200:
            suppliers_data = suppliers_response.json()
            suppliers_controls = suppliers_data.get('controls', {})

            supplier_source = suppliers_controls.get('section_1_control_38', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in supplier_source:
                result['suppliers'].append({
                    'supplier_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'start_date': item.get('_UpdDateSmall', ''),
                    'end_date': item.get('_BestAnnouncedDate', ''),
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
                result['previous_suppliers'].append({
                    'previous_supplier_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'previous_supplier_start_date': item.get('_UpdDateSmall', ''),
                    'previous_supplier_end_date': item.get('_BestAnnouncedDate', ''),
                    'previous_supplier_institution_key': item.get('_KeyInstn', ''),
                    'previous_supplier_primary_industry': item.get('_ShipmentDescription', ''),
                    'previous_supplier_expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'previous_supplier_expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'previous_supplier_min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'previous_supplier_max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'previous_supplier_min_value': item.get('_CIQBRRevenueMin', ''),
                    'previous_supplier_max_value': item.get('_CIQBRRevenueMax', ''),
                    'previous_supplier_source': item.get('_SourceExport', '')
                })

    except requests.exceptions.Timeout:
        thread_safe_print(f"Timeout fetching suppliers for company {company_id}")
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"Error fetching suppliers: {e}")
    except Exception as e:
        thread_safe_print(f"Unexpected error fetching suppliers: {e}")

    #Customers Page, 2/2 Keycalls
    customer_params = {
        'mode': 'browser',
        'id': company_id,
        'keypage': '478397',
        'crossSiteFilterCurrency': 'USD',
        '_': str(int(time.time() * 1000))
    }

    try:
        customers_response = session.get(REPORT_API_URL, params=customer_params, headers=HEADERS, timeout=30)
        progress_tracker.increment_api_calls()

        if customers_response.status_code == 200:
            customers_data = customers_response.json()
            customers_controls = customers_data.get('controls', {})

            customer_source = customers_controls.get('section_1_control_45', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in customer_source:
                result['customers'].append({
                    'customer_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'start_date': item.get('_UpdDateSmall', ''),
                    'end_date': item.get('_BestAnnouncedDate', ''),
                    'key_instn': item.get('_KeyInstn', ''),                      
                    'primary_industry': item.get('_ShipmentDescription', ''),
                    'expense_million_usd': item.get('_CIQBRRevenue', ''),         
                    'expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'min_value': item.get('_CIQBRRevenueMin', ''),
                    'max_value': item.get('_CIQBRRevenueMax', '')
                })

            previous_customers_source = customers_controls.get('section_1_control_47', {}).get('dataModel', {}).get('model', {}).get('dataSource', [])
            for item in previous_customers_source:
                result['previous_customers'].append({
                    'previous_customer_name': clean_company_name(item.get('_CompanyProfile', '')),
                    'previous_customer_start_date': item.get('_UpdDateSmall', ''),
                    'previous_customer_end_date': item.get('_BestAnnouncedDate', ''),
                    'previous_customer_institution_key': item.get('_KeyInstn', ''),
                    'previous_customer_primary_industry': item.get('_ShipmentDescription', ''),
                    'previous_customer_expense_million_usd': item.get('_CIQBRRevenue', ''),
                    'previous_customer_expense_percent': item.get('_CIQBRRevenuePercentage', ''),
                    'previous_customer_min_percent': item.get('_CIQBRRevenueMinPercentage', ''),
                    'previous_customer_max_percent': item.get('_CIQBRRevenueMaxPercentage', ''),
                    'previous_customer_min_value': item.get('_CIQBRRevenueMin', ''),
                    'previous_customer_max_value': item.get('_CIQBRRevenueMax', ''),
                    'previous_customer_source': item.get('_SourceExport', '')
                })

    except requests.exceptions.Timeout:
        thread_safe_print(f"Timeout fetching customers for company {company_id}")
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"Error fetching customers: {e}")
    except Exception as e:
        thread_safe_print(f"Unexpected error fetching customers: {e}")

    return result
    
def fetch_all_company_data_parallel(session, company_id, company_name):
    results = {
        'relationships': {'suppliers': [], 'previous_suppliers': [], 'customers': [], 'previous_customers': []},
        'mining': {'properties': []},
        'profile': get_empty_profile_data(),
        'financials': get_empty_financials(),
        'property_profiles': [],
        'mining_locations': []
    }

    if not company_id:
        thread_safe_print(f"No company_id for {company_name}")
        return results

    try:
        # 1: Fetch Base Company Data (using global executor)
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
            company_id
        )

        try:
            relationships_result = future_relationships.result(timeout=60)
            if relationships_result and isinstance(relationships_result, dict):
                results['relationships'] = relationships_result
        except Exception as e:
            thread_safe_print(f"ERROR fetching relationships for {company_name}: {e}")

        try:
            mining_result = future_mining.result(timeout=60)
            if mining_result and isinstance(mining_result, dict):
                results['mining'] = mining_result
        except Exception as e:
            thread_safe_print(f"ERROR fetching mining data for {company_name}: {e}")

        try:
            profile_result = future_profile.result(timeout=60)
            if profile_result and isinstance(profile_result, dict):
                results['profile'] = profile_result
        except Exception as e:
            thread_safe_print(f"ERROR fetching profile for {company_name}: {e}")

        try:
            financials_result = future_financials.result(timeout=60)
            if financials_result and isinstance(financials_result, dict):
                results['financials'] = financials_result
        except Exception as e:
            thread_safe_print(f"ERROR fetching financials for {company_name}: {e}")

        # 2: Process property profiles using global executor
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
                property_profile_result = futures_dict['profile'].result(timeout=60)
                if property_profile_result and isinstance(property_profile_result, dict):
                    results['property_profiles'].append(property_profile_result)
            except Exception as e:
                thread_safe_print(f"ERROR fetching property profile for {company_name}, property {prop_id}: {e}")

            try:
                property_location_result = futures_dict['location'].result(timeout=60)
                if property_location_result and isinstance(property_location_result, dict):
                    results['mining_locations'].append(property_location_result)
            except Exception as e:
                thread_safe_print(f"ERROR fetching property location for {company_name}, property {prop_id}: {e}")

    except Exception as e:
        thread_safe_print(f"Error for company {company_name}: {e}")
        thread_safe_print(f"Traceback: {traceback.format_exc()}")

    return results

def search_company(session, company_name):
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
        response = session.post(API_URL, json=payload, headers=HEADERS, timeout=10)
        progress_tracker.increment_api_calls()
        
        if response.status_code != 200:
            return {
                'company_name': company_name, 'matched_name': '', 'country': '',
                'mi_key': None, 'spciq_key': None, 'status': f'error_{response.status_code}'
            }
        
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
        
        return result
    except Exception as e:
        return {
            'company_name': company_name, 'matched_name': '', 'country': '',
            'mi_key': None, 'spciq_key': None, 'status': f'exception_{type(e).__name__}'
        }

#def format_tags_for_csv(tags):
#    if not tags:
#        return {'tag_names': '', 'tag_trust_ranks': ''}
#    return {
#        'tag_names': format_list_for_csv([t['tag_name'] for t in tags]),
#        'tag_trust_ranks': format_list_for_csv([t['trust_rank'] for t in tags])
#    }

#def format_mining_properties_for_csv(properties):
#    if not properties:
#        return {
#            'property_names': 'NaN',
#            'property_primary_commodities': 'NaN',
#            'property_all_commodities': 'NaN',
#            'property_operators': 'NaN',
#            'property_stages': 'NaN',
#            'property_statuses': 'NaN',
#            'property_equity_ownerships': 'NaN',
#            'property_primary_resources': 'NaN',
#            'property_units': 'NaN',
#            'property_in_situ_values': 'NaN',
#            'property_as_of_dates': 'NaN',
#            'property_risk_scores': 'NaN',
#            'property_ids': 'NaN'
#        }

    #def format_nested_with_braces(items):
    #    if not items:
    #        return 'NaN'
    #    formatted_items = []
    #    for item in items:
    #        if item and str(item).strip():
    #            formatted_items.append('{' + str(item) + '}')
    #        else:
    #            formatted_items.append('{NaN}')
    #    return ', '.join(formatted_items)
    #
    #return {
    #    'property_names': format_nested_with_braces([p['property_name'] for p in properties]),
    #    'property_primary_commodities': format_nested_with_braces([p['primary_commodity'] for p in properties]),
    #    'property_all_commodities': format_nested_with_braces([p['all_commodities'] for p in properties]),
    #    'property_operators': format_nested_with_braces([p['operator'] for p in properties]),
    #    'property_stages': format_nested_with_braces([p['development_stage'] for p in properties]),
    #    'property_statuses': format_nested_with_braces([p['activity_status'] for p in properties]),
    #    'property_equity_ownerships': format_nested_with_braces([p['equity_ownership'] for p in properties]),
    #    'property_primary_resources': format_nested_with_braces([p['primary_resources'] for p in properties]),
    #    'property_units': format_nested_with_braces([p['unit'] for p in properties]),
    #    'property_in_situ_values': format_nested_with_braces([p['total_in_situ_value_usd_m'] for p in properties]),
    #    'property_as_of_dates': format_nested_with_braces([p['reserves_resources_as_of_date'] for p in properties]),
    #    'property_risk_scores': format_nested_with_braces([p['country_risk_score'] for p in properties]),
    #    'property_ids': format_nested_with_braces([p['property_id'] for p in properties])
    #}
#__________________________________________________________
#def format_supplier_data_for_csv(suppliers):
#    if not suppliers:
#        return {
#            'supplier_names': 'NaN',
#            'supplier_primary_industries': 'NaN',
#            'supplier_expense_million_usd': 'NaN',
#            'supplier_expense_percent': 'NaN',
#            'supplier_min_percent': 'NaN',
#            'supplier_max_percent': 'NaN',
#            'supplier_min_value': 'NaN',
#            'supplier_max_value': 'NaN',
#            'supplier_start_dates': 'NaN',
#            'supplier_end_dates': 'NaN'
#        }
#    
#    return {
#        'supplier_names': format_list_for_csv([s['supplier_name'] for s in suppliers]),
#        'supplier_primary_industries': format_list_for_csv([s['primary_industry'] for s in suppliers]),
#        'supplier_expense_million_usd': format_list_for_csv([s['expense_million_usd'] for s in suppliers]),
#        'supplier_expense_percent': format_list_for_csv([s['expense_percent'] for s in suppliers]),
#        'supplier_min_percent': format_list_for_csv([s['min_percent'] for s in suppliers]),
#        'supplier_max_percent': format_list_for_csv([s['max_percent'] for s in suppliers]),
#        'supplier_min_value': format_list_for_csv([s['min_value'] for s in suppliers]),
#        'supplier_max_value': format_list_for_csv([s['max_value'] for s in suppliers]),
#        'supplier_start_dates': format_dates_for_csv([s['start_date'] for s in suppliers]), 
#        'supplier_end_dates': format_dates_for_csv([s['end_date'] for s in suppliers])      
#    }
#
#def format_previous_supplier_data_for_csv(previous_suppliers):
#    if not previous_suppliers:
#        return {
#            'previous_supplier_names': 'NaN',
#            'previous_supplier_start_dates': 'NaN',
#            'previous_supplier_end_dates': 'NaN',
#            'previous_supplier_institution_keys': 'NaN',
#            'previous_supplier_primary_industries': 'NaN',
#            'previous_supplier_expense_million_usd': 'NaN',
#            'previous_supplier_expense_percent': 'NaN',
#            'previous_supplier_min_percent': 'NaN',
#            'previous_supplier_max_percent': 'NaN',
#            'previous_supplier_min_value': 'NaN',
#            'previous_supplier_max_value': 'NaN',
#            'previous_supplier_source': 'NaN'
#        }
#    
#    return {
#        'previous_supplier_names': format_list_for_csv([ps['previous_supplier_name'] for ps in previous_suppliers]),
#        'previous_supplier_start_dates': format_dates_for_csv([ps['previous_supplier_start_date'] for ps in previous_suppliers]),
#        'previous_supplier_end_dates': format_dates_for_csv([ps['previous_supplier_end_date'] for ps in previous_suppliers]),
#        'previous_supplier_institution_keys': format_list_for_csv([ps['previous_supplier_institution_key'] for ps in previous_suppliers]),
#        'previous_supplier_primary_industries': format_list_for_csv([ps['previous_supplier_primary_industry'] for ps in previous_suppliers]),
#        'previous_supplier_expense_million_usd': format_list_for_csv([ps['previous_supplier_expense_million_usd'] for ps in previous_suppliers]),
#        'previous_supplier_expense_percent': format_list_for_csv([ps['previous_supplier_expense_percent'] for ps in previous_suppliers]),
#        'previous_supplier_min_percent': format_list_for_csv([ps['previous_supplier_min_percent'] for ps in previous_suppliers]),
#        'previous_supplier_max_percent': format_list_for_csv([ps['previous_supplier_max_percent'] for ps in previous_suppliers]),
#        'previous_supplier_min_value': format_list_for_csv([ps['previous_supplier_min_value'] for ps in previous_suppliers]),
#        'previous_supplier_max_value': format_list_for_csv([ps['previous_supplier_max_value'] for ps in previous_suppliers]),
#        'previous_supplier_source': format_list_for_csv([ps['previous_supplier_source'] for ps in previous_suppliers])
#    }
#
#def format_customer_data_for_csv(customers):
#    if not customers:
#        return {
#            'customer_names': 'NaN',
#            'customer_start_dates': 'NaN',
#            'customer_end_dates': 'NaN',
#            'customer_primary_industries': 'NaN',
#            'customer_expense_million_usd': 'NaN',
#            'customer_expense_percent': 'NaN',
#            'customer_min_percent': 'NaN',
#            'customer_max_percent': 'NaN',
#            'customer_min_value': 'NaN',
#            'customer_max_value': 'NaN',
#            'customer_start_dates': 'NaN',
#            'customer_end_dates': 'NaN'
#        }
#    
#    return {
#        'customer_names': format_list_for_csv([c['customer_name'] for c in customers]),
#        'customer_start_dates': format_dates_for_csv([c['start_date'] for c in customers]),
#        'customer_end_dates': format_dates_for_csv([c['end_date'] for c in customers]),
#        'customer_primary_industries': format_list_for_csv([c['primary_industry'] for c in customers]),
#        'customer_expense_million_usd': format_list_for_csv([c['expense_million_usd'] for c in customers]),
#        'customer_expense_percent': format_list_for_csv([c['expense_percent'] for c in customers]),
#        'customer_min_percent': format_list_for_csv([c['min_percent'] for c in customers]),
#        'customer_max_percent': format_list_for_csv([c['max_percent'] for c in customers]),
#        'customer_min_value': format_list_for_csv([c['min_value'] for c in customers]),
#        'customer_max_value': format_list_for_csv([c['max_value'] for c in customers]),
#        'customer_start_dates': format_dates_for_csv([c['start_date'] for c in customers]),  
#        'customer_end_dates': format_dates_for_csv([c['end_date'] for c in customers])        
#    }
#
#def format_previous_customer_data_for_csv(previous_customers):
#    if not previous_customers:
#        return {
#            'previous_customer_names': 'NaN',
#            'previous_customer_start_dates': 'NaN',
#            'previous_customer_end_dates': 'NaN',
#            'previous_customer_institution_keys': 'NaN',
#            'previous_customer_primary_industries': 'NaN',
#            'previous_customer_expense_million_usd': 'NaN',
#            'previous_customer_expense_percent': 'NaN',
#            'previous_customer_min_percent': 'NaN',
#            'previous_customer_max_percent': 'NaN',
#            'previous_customer_min_value': 'NaN',
#            'previous_customer_max_value': 'NaN',
#            'previous_customer_source': 'NaN'
#        }
#    
#    return {
#        'previous_customer_names': format_list_for_csv([pc['previous_customer_name'] for pc in previous_customers]),
#        'previous_customer_start_dates': format_dates_for_csv([pc['previous_customer_start_date'] for pc in previous_customers]),
#        'previous_customer_end_dates': format_dates_for_csv([pc['previous_customer_end_date'] for pc in previous_customers]),
#        'previous_customer_institution_keys': format_list_for_csv([pc['previous_customer_institution_key'] for pc in previous_customers]),
#        'previous_customer_primary_industries': format_list_for_csv([pc['previous_customer_primary_industry'] for pc in previous_customers]),
#        'previous_customer_expense_million_usd': format_list_for_csv([pc['previous_customer_expense_million_usd'] for pc in previous_customers]),
#        'previous_customer_expense_percent': format_list_for_csv([pc['previous_customer_expense_percent'] for pc in previous_customers]),
#        'previous_customer_min_percent': format_list_for_csv([pc['previous_customer_min_percent'] for pc in previous_customers]),
#        'previous_customer_max_percent': format_list_for_csv([pc['previous_customer_max_percent'] for pc in previous_customers]),
#        'previous_customer_min_value': format_list_for_csv([pc['previous_customer_min_value'] for pc in previous_customers]),
#        'previous_customer_max_value': format_list_for_csv([pc['previous_customer_max_value'] for pc in previous_customers]),
#        'previous_customer_source': format_list_for_csv([pc['previous_customer_source'] for pc in previous_customers])
#    }

#def format_nace_codes_for_csv(nace_codes):
#    if not nace_codes:
#        return {
#            'nace_codes': 'NaN',
#            'nace_industries': 'NaN'
#        }
#    
#    return {
#        'nace_codes': format_list_for_csv([n['nace_code'] for n in nace_codes]),
#        'nace_industries': format_list_for_csv([n['nace_industry'] for n in nace_codes])
#    }
#
#def format_naics_codes_for_csv(naics_codes):
#    if not naics_codes:
#        return {
#            'naics_codes': 'NaN',
#            'naics_descriptions': 'NaN',
#            'naics_relationship_levels': 'NaN'
#        }
#    
#    return {
#        'naics_codes': format_list_for_csv([n['naics_code'] for n in naics_codes]),
#        'naics_descriptions': format_list_for_csv([n['naics_desc'] for n in naics_codes]),
#        'naics_relationship_levels': format_list_for_csv([n['relationship_level'] for n in naics_codes])
#    }


            #time.sleep(RATE_LIMIT_DELAY)
            #
            #thread_safe_print(f"{'  ' * tier}  → Searching supplier: {supplier_name}")
            #supplier_search_result = search_company(session, supplier_name)
            #supplier_api_matched_name = supplier_search_result.get('matched_name', supplier_name)
            #supplier_api_country = supplier_search_result.get('country', '')
            #
            #time.sleep(RATE_LIMIT_DELAY)
            
            #supplier_results = process_company_recursive(
            #    session=session,
            #    company_name=supplier_name,
            #    spciq_key=supplier_id,
            #    dataset_file=dataset_file,
            #    dataset_row_index=dataset_row_index,
            #    dataset_country='',
            #    found_in_column=f'tier_{tier}_supplier',
            #    parent_company=company_name,
            #    tier=tier + 1,
            #    max_tier=max_tier,
            #    processed_companies=processed_companies,
            #    tier_company_counts=tier_company_counts,
            #    api_matched_name=supplier_name
            #    api_country='',
            #    last_cookie_refresh=last_cookie_refresh
            #)
            #results.extend(supplier_results)
        

            
            #time.sleep(RATE_LIMIT_DELAY)
            #
            #thread_safe_print(f"{'  ' * tier}  → Searching customer: {customer_name}")
            #customer_search_result = search_company(session, customer_name)
            #customer_api_matched_name = customer_search_result.get('matched_name', customer_name)
            #customer_api_country = customer_search_result.get('country', '')
            #
            #time.sleep(RATE_LIMIT_DELAY)
            
            #customer_results = process_company_recursive(
            #    session=session,
            #    company_name=customer_name,
            #    spciq_key=customer_id,
            #    dataset_file=dataset_file,
            #    dataset_row_index=dataset_row_index,
            #    dataset_country='',
            #    found_in_column=f'tier_{tier}_customer',
            #    parent_company=company_name,
            #    tier=tier + 1,
            #    max_tier=max_tier,
            #    processed_companies=processed_companies,
            #    tier_company_counts=tier_company_counts,
            #    api_matched_name=customer_name,
            #    api_country='',
            #    last_cookie_refresh=last_cookie_refresh
            #)
            #results.extend(customer_results)

    #if progress_tracker.companies_processed % 50 == 0:
    #    progress_tracker.print_progress()
    #
    #return results


if __name__ == "__main__":
    script_start_time = time.time()
    script_start_datetime = datetime.now()

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"SCRIPT STARTED: {script_start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    thread_safe_print(f"{'='*70}\n")

    # Initialize global thread pool executor
    initialize_global_executor()

    CSV_DIRECTORY = 'csv_files_2022'
    OUTPUT_FILE = 'all_countries_results.csv'

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"5-TIER PROCESSING MODE WITH FINANCIAL DATA")
    thread_safe_print(f"{'='*70}")
    thread_safe_print(f"Max tiers: {MAX_TIER}")
    thread_safe_print(f"Rate limit delay: {RATE_LIMIT_DELAY}s")
    #thread_safe_print(f"Max companies per tier: {MAX_COMPANIES_PER_TIER}")
    thread_safe_print(f"Batch save interval: {BATCH_SAVE_INTERVAL}")
    thread_safe_print(f"Checkpoints enabled: {ENABLE_CHECKPOINTS}")
    thread_safe_print(f"Financial data: ENABLED ✓")
    thread_safe_print(f"{'='*70}\n")
    
    csv_path = Path(CSV_DIRECTORY)
    if not csv_path.exists():
        thread_safe_print(f"\n Directory not found: {CSV_DIRECTORY}")
        exit(1)
    
    datasets_to_process = TEST_DATASETS if TEST_MODE else csv_files_2022
    
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
    #    thread_safe_print("\n No companies found!")
    #    exit(1)
    #
    #thread_safe_print(f"\nTotal unique companies: {len(all_companies)}\n")

    #Step 1: Extract company names (parallel version parallelized at # of companies level)
    all_chunks = extract_company_names(csv_path, datasets_to_process, chunk_size=500)
    thread_safe_print(f"\n total chunks to process {len(all_chunks)}")

    all_companies = set()

    # Use global executor for chunk extraction
    futures = {GLOBAL_EXECUTOR.submit(extract_companies_from_chunk, *chunk): i
               for i, chunk in enumerate(all_chunks)}

    completed = 0
    for future in futures:
        chunk_companies = future.result(timeout=120)
        all_companies.update(chunk_companies)
        completed += 1

        if completed % 20 == 0:
            thread_safe_print(f"Chunks completed: {completed}/{len(all_chunks)}")

    thread_safe_print(f"\nTotal unique companies extracted: {len(all_companies)}\n")
    
    # Step 2: Search for MI keys (parallel processing with 15 cores)
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print("STEP 2: EXTRACTING MI KEYS (15 CORES)")
    thread_safe_print(f"{'='*70}\n")

    companies_list = sorted(list(all_companies))
    total_companies = len(companies_list)
    thread_safe_print(f"Total companies to process: {total_companies}")

    # Split companies into chunks for parallel processing
    chunk_size = max(1, total_companies // (15 * 4))  # 4 chunks per core for load balancing
    company_chunks = [companies_list[i:i + chunk_size]
                     for i in range(0, total_companies, chunk_size)]

    thread_safe_print(f"Split into {len(company_chunks)} chunks (chunk size: ~{chunk_size})")
    thread_safe_print("Processing with 15 cores...\n")

    # Process chunks in parallel using ProcessPoolExecutor
    results = []
    with ProcessPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(chunk_extract_MIKey, chunk): i
                  for i, chunk in enumerate(company_chunks)}

        completed = 0
        for future in futures:
            chunk_results = future.result()
            results.extend(chunk_results)
            completed += 1

            # Progress update
            companies_processed = len(results)
            thread_safe_print(f"Chunks completed: {completed}/{len(company_chunks)} "
                            f"| Companies processed: {companies_processed}/{total_companies}")

    df_mi_results = pd.DataFrame(results)

    thread_safe_print(f"\n{'='*70}")
    thread_safe_print(f"MI keys found: {len([r for r in results if r['mi_key']])}/{total_companies}")
    thread_safe_print(f"{'='*70}\n")

    # Step 3: Recursive processing
    thread_safe_print(f"\n{'='*70}")
    thread_safe_print("step 3: five tier recursive process")
    thread_safe_print(f"{'='*70}\n")

    # Create session for recursive processing
    session = requests.Session()
    session.cookies.update(cookies_dict)

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
    thread_safe_print("EXECUTION TIME SUMMARY")
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
            
            thread_safe_print(f"\nPERFORMANCE METRICS:")
            thread_safe_print(f"  Total companies processed: {companies_processed}")
            thread_safe_print(f"  Total rows created: {total_rows}")
            thread_safe_print(f"  Processing rate: {companies_per_second:.2f} companies/second")
            thread_safe_print(f"  Row creation rate: {rows_per_second:.2f} rows/second")
            thread_safe_print(f"  Average time per company: {total_elapsed_seconds/companies_processed:.2f}s")

    # Shutdown global thread pool executor
    shutdown_global_executor()

    thread_safe_print(f"\nCOMPLETE!")
    thread_safe_print(f"{'='*70}\n")