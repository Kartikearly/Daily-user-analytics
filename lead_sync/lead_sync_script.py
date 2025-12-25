"""
Daily Lead Sync Script
Fetches new signups from EarlyFit API and creates leads in CRM
Runs daily at 10 PM IST
"""

import requests
import json
import os
import sys
import logging
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
# Try to load from parent directory (.env file location)
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()  # Fallback to default location

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging(verbose: bool = False, debug: bool = False):
    """Setup logging configuration"""
    log_level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Log file with timestamp
    log_file = log_dir / f"lead_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Remove all existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # Force reconfiguration
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    logger.info(f"Log level: {logging.getLevelName(log_level)}")
    
    return logger

# Initialize logger (will be reconfigured in main)
# Set up a basic logger that will be reconfigured in main()
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# External API Configuration (Source API)
EXTERNAL_API_CONFIG = {
    'base_url': "https://earlyfit-api.saurabhsakhuja.com/api/v1",  # Hardcoded
    'api_key': os.getenv("API_KEY"),  # Still from environment variable
    'endpoint': '/analytics'
}

# CRM Lead Ingestion API Configuration (Target API)
CRM_API_CONFIG = {
    'base_url': os.getenv("CRM_API_BASE_URL", "http://127.0.0.1:8000/api/v1"),
    'api_key': os.getenv("CRM_WEBHOOK_API_KEY"),
    'endpoint': '/webhook/lead-ingestion'
}

# SQL Query to fetch new signups from yesterday
NEW_SIGNUPS_QUERY = """
SELECT
    p.phone,
    TRIM(
        COALESCE(p.firstname, '') ||
        CASE WHEN p.middlename IS NOT NULL AND p.middlename <> '' THEN ' ' || p.middlename ELSE '' END ||
        CASE WHEN p.lastname IS NOT NULL AND p.lastname <> '' THEN ' ' || p.lastname ELSE '' END
    ) AS patient_name,
    p."createdAt" + INTERVAL '5 hours 30 minutes' AS created_at_ist,
    p.email
FROM
    public.patients AS p
WHERE
    p."createdAt" >= ((CURRENT_DATE AT TIME ZONE 'IST') - INTERVAL '1 day') AT TIME ZONE 'UTC'
    AND p."createdAt" < (CURRENT_DATE AT TIME ZONE 'IST') AT TIME ZONE 'UTC';
"""

# SQL Query to fetch active customers (to exclude from lead creation)
ACTIVE_CUSTOMERS_QUERY = """
SELECT p.phone
FROM "public"."patients" p
JOIN "public"."subscriptions" s ON p.active_subscription_id = s.id
WHERE p."isActive" = true 
  AND p.status = 'ACTIVE_SUBSCRIPTION';
"""

# Lead mapping configuration
LEAD_CONFIG = {
    'source': 'MDT App Signup',
    'sub_source': 'Daily Sync',
    'status': 'New'
}


# ============================================================================
# EXTERNAL API CLIENT
# ============================================================================

class ExternalAPIClient:
    """Client to query external EarlyFit API"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.analytics_endpoint = f"{self.base_url}{EXTERNAL_API_CONFIG['endpoint']}"
    
    def query_analytics(self, sql_query: str) -> Dict[str, Any]:
        """Execute a SQL query through the Analytics API"""
        logger.debug(f"Querying analytics API: {self.analytics_endpoint}")
        logger.debug(f"SQL Query: {sql_query[:200]}..." if len(sql_query) > 200 else f"SQL Query: {sql_query}")
        
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key if self.api_key else "NOT_SET"
        }
        
        payload = {"query": sql_query}
        
        try:
            logger.info(f"Making POST request to {self.analytics_endpoint}")
            response = requests.post(
                self.analytics_endpoint,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            result = response.json()
            
            logger.debug(f"Response JSON: {json.dumps(result, indent=2)[:500]}...")
            
            if isinstance(result, dict) and result.get("success"):
                logger.info(f"Query successful. Records returned: {len(result.get('data', []))}")
                return result
            else:
                logger.warning(f"Query returned non-success result: {result}")
                return result
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout when querying analytics API: {e}")
            logger.error(f"Endpoint: {self.analytics_endpoint}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error when querying analytics API: {e}")
            logger.error(f"Status code: {e.response.status_code if e.response else 'N/A'}")
            logger.error(f"Response: {e.response.text if e.response else 'N/A'}")
            logger.error(f"Endpoint: {self.analytics_endpoint}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error when querying analytics API: {e}")
            logger.error(f"Endpoint: {self.analytics_endpoint}")
            logger.error(f"Please check if the API is accessible")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when querying analytics API: {e}")
            logger.error(f"Endpoint: {self.analytics_endpoint}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Response text: {response.text[:500] if 'response' in locals() else 'N/A'}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in query_analytics: {type(e).__name__}: {e}")
            raise


# ============================================================================
# CRM API CLIENT
# ============================================================================

class CRMAPIClient:
    """Client to ingest leads into CRM"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.ingest_endpoint = f"{self.base_url}{CRM_API_CONFIG['endpoint']}"
    
    def ingest_lead(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest a single lead into the CRM
        
        Expected lead_data format (LeadIngestionRequest):
        {
            "phone_number": "9876543210",
            "name": "John Doe",
            "email": "john@example.com",  # optional
            "source": "MDT App Signup",
            "sub_source": "Daily Sync",
            "status": "New",
            "age": 30,  # optional
            "gender": "Male",  # optional
            "height": "175cm",  # optional
            "weight": "70kg",  # optional
            "offering": "Weight Loss",  # optional
            "notes": "Auto-synced from MDT"  # optional
        }
        """
        phone = lead_data.get('phone_number', 'N/A')
        logger.debug(f"Ingesting lead with phone: {phone}")
        logger.debug(f"Lead data: {json.dumps(lead_data, indent=2)}")
        
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key if self.api_key else "NOT_SET"
        }
        
        try:
            logger.info(f"Making POST request to {self.ingest_endpoint} for phone: {phone}")
            response = requests.post(
                self.ingest_endpoint,
                headers=headers,
                json=lead_data,
                timeout=30
            )
            
            logger.debug(f"Response status code: {response.status_code}")
            
            response.raise_for_status()
            result = response.json()
            
            logger.debug(f"Response JSON: {json.dumps(result, indent=2)[:500]}...")
            logger.info(f"Successfully ingested lead: {phone}")
            
            return result
                
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 'N/A'
            logger.warning(f"HTTP error when ingesting lead {phone}: Status {status_code}")
            
            if status_code == 409:
                # Lead already exists
                logger.info(f"Lead {phone} already exists in CRM")
                return {"status": "exists", "message": "Lead already exists"}
            else:
                logger.error(f"HTTP error details: {e}")
                logger.error(f"Response: {e.response.text if e.response else 'N/A'}")
                logger.error(f"Endpoint: {self.ingest_endpoint}")
                raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout when ingesting lead {phone}: {e}")
            logger.error(f"Endpoint: {self.ingest_endpoint}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error when ingesting lead {phone}: {e}")
            logger.error(f"Endpoint: {self.ingest_endpoint}")
            logger.error(f"Please check if the CRM API is accessible")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when ingesting lead {phone}: {e}")
            logger.error(f"Endpoint: {self.ingest_endpoint}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for lead {phone}: {e}")
            logger.error(f"Response text: {response.text[:500] if 'response' in locals() else 'N/A'}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error ingesting lead {phone}: {type(e).__name__}: {e}")
            raise


# ============================================================================
# PHONE NUMBER UTILITIES
# ============================================================================

def normalize_phone_number(phone: str) -> str:
    """
    Normalize phone number for comparison.
    Handles formats: 9876543210, +919876543210, +91 9876543210, etc.
    Returns: 10-digit phone number
    """
    if not phone:
        return ""
    
    # Convert to string and remove spaces, dashes, parentheses
    phone = str(phone).strip()
    phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    
    # Remove country code if present
    if phone.startswith("+91"):
        phone = phone[3:]
    elif phone.startswith("91") and len(phone) > 10:
        phone = phone[2:]
    
    # Keep only digits
    phone = ''.join(filter(str.isdigit, phone))
    
    # Return last 10 digits (in case of any prefix remaining)
    return phone[-10:] if len(phone) >= 10 else phone


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def transform_external_data_to_lead(external_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform external API record to CRM lead format
    
    Input format (from external API):
    {
        "phone": "9876543210",
        "patient_name": "John Doe",
        "created_at_ist": "2024-12-24T10:30:00",
        "email": "john@example.com"
    }
    
    Output format (for CRM ingestion):
    LeadIngestionRequest schema
    """
    # Clean phone number (remove spaces, dashes, etc.)
    phone = external_record.get('phone') or ''
    phone = str(phone).strip() if phone is not None else ''
    phone = ''.join(filter(str.isdigit, phone))
    
    # Get name
    name = external_record.get('patient_name') or ''
    name = str(name).strip() if name is not None else ''
    if not name:
        name = None
    
    # Get email
    email = external_record.get('email') or ''
    email = str(email).strip() if email is not None else ''
    if not email or email.lower() == 'null' or email == '':
        email = None
    
    # Get created timestamp
    created_at = external_record.get('created_at_ist') or ''
    created_at = str(created_at) if created_at is not None else ''
    
    # Build lead data
    lead_data = {
        "phone_number": phone,
        "name": name,
        "email": email,
        "source": LEAD_CONFIG['source'],
        "sub_source": LEAD_CONFIG['sub_source'],
        "status": LEAD_CONFIG['status'],
        "notes": f"Auto-synced from MDT on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nOriginal signup time: {created_at}"
    }
    
    # Remove None values (optional fields)
    lead_data = {k: v for k, v in lead_data.items() if v is not None}
    
    return lead_data


# ============================================================================
# MAIN SYNC FUNCTION
# ============================================================================

def sync_leads() -> Dict[str, Any]:
    """
    Main function to sync leads from external API to CRM
    Returns summary statistics
    """
    logger.info("=" * 80)
    logger.info("Starting lead sync process")
    logger.info("=" * 80)
    
    stats = {
        'total_fetched': 0,
        'active_customers_filtered': 0,
        'total_ingested': 0,
        'already_exists': 0,
        'failed': 0,
        'errors': []
    }
    
    try:
        # Step 1: Fetch new signups from external API
        logger.info("Step 1: Fetching new signups from external API")
        logger.info(f"External API Base URL: {EXTERNAL_API_CONFIG['base_url']}")
        logger.info(f"API Key present: {'Yes' if EXTERNAL_API_CONFIG['api_key'] else 'No'}")
        
        external_client = ExternalAPIClient(
            base_url=EXTERNAL_API_CONFIG['base_url'],
            api_key=EXTERNAL_API_CONFIG['api_key']
        )
        
        result = external_client.query_analytics(NEW_SIGNUPS_QUERY)
        
        if not result.get('success'):
            error_msg = f"External API query failed: {result}"
            logger.error(error_msg)
            stats['errors'].append(error_msg)
            return stats
        
        records = result.get('data', [])
        stats['total_fetched'] = len(records)
        logger.info(f"Fetched {stats['total_fetched']} new signup records")
        
        if not records:
            logger.info("No new signups found. Exiting.")
            return stats
        
        # Step 2: Fetch active customers (to exclude from lead creation)
        logger.info("Step 2: Fetching active customers to filter out")
        try:
            active_result = external_client.query_analytics(ACTIVE_CUSTOMERS_QUERY)
            
            if active_result.get('success'):
                active_customer_records = active_result.get('data', [])
                logger.info(f"Found {len(active_customer_records)} active customers")
                # Normalize all active customer phone numbers
                active_customer_phones = {
                    normalize_phone_number(record['phone']) 
                    for record in active_customer_records 
                    if record.get('phone')
                }
                logger.info(f"Normalized {len(active_customer_phones)} unique active customer phone numbers")
            else:
                logger.warning(f"Failed to fetch active customers: {active_result}")
                active_customer_phones = set()
        except Exception as e:
            logger.error(f"Exception while fetching active customers: {type(e).__name__}: {e}")
            logger.exception(e)
            active_customer_phones = set()
        
        # Filter out active customers from new signups
        logger.info("Step 3: Filtering out active customers from new signups")
        filtered_records = []
        for record in records:
            normalized_phone = normalize_phone_number(record.get('phone', ''))
            if normalized_phone in active_customer_phones:
                stats['active_customers_filtered'] += 1
                logger.debug(f"Filtered out active customer: {normalized_phone}")
            else:
                filtered_records.append(record)
        
        logger.info(f"Filtered out {stats['active_customers_filtered']} active customers")
        logger.info(f"Remaining records to process: {len(filtered_records)}")
        
        if not filtered_records:
            logger.info("No records to process after filtering. Exiting.")
            return stats
        
        # Step 4: Transform and validate data
        logger.info("Step 4: Transforming and validating data")
        leads_to_ingest = []
        
        for idx, record in enumerate(filtered_records, 1):
            try:
                logger.debug(f"Transforming record {idx}/{len(filtered_records)}: {record.get('phone', 'N/A')}")
                lead_data = transform_external_data_to_lead(record)
                
                # Validate phone number
                if not lead_data.get('phone_number') or len(lead_data['phone_number']) < 10:
                    error_msg = f"Invalid phone number for record {idx}: {record.get('phone')}"
                    logger.warning(error_msg)
                    stats['failed'] += 1
                    stats['errors'].append(error_msg)
                    continue
                
                leads_to_ingest.append(lead_data)
                logger.debug(f"Successfully transformed record {idx}")
                
            except Exception as e:
                error_msg = f"Error transforming record {idx}: {type(e).__name__}: {e}"
                logger.error(error_msg)
                logger.exception(e)
                stats['failed'] += 1
                stats['errors'].append(error_msg)
        
        logger.info(f"Successfully transformed {len(leads_to_ingest)} leads")
        
        # Step 5: Ingest leads into CRM
        logger.info("Step 5: Ingesting leads into CRM")
        logger.info(f"CRM API Base URL: {CRM_API_CONFIG['base_url']}")
        logger.info(f"CRM API Key present: {'Yes' if CRM_API_CONFIG['api_key'] else 'No'}")
        
        crm_client = CRMAPIClient(
            base_url=CRM_API_CONFIG['base_url'],
            api_key=CRM_API_CONFIG['api_key']
        )
        
        for idx, lead_data in enumerate(leads_to_ingest, 1):
            try:
                logger.info(f"Ingesting lead {idx}/{len(leads_to_ingest)}: {lead_data.get('phone_number', 'N/A')}")
                result = crm_client.ingest_lead(lead_data)
                
                if result.get('status') == 'exists':
                    stats['already_exists'] += 1
                    logger.info(f"Lead {lead_data['phone_number']} already exists")
                else:
                    stats['total_ingested'] += 1
                    logger.info(f"Successfully ingested lead {lead_data['phone_number']}")
                
            except Exception as e:
                error_msg = f"Failed to ingest lead {lead_data.get('phone_number', 'N/A')}: {type(e).__name__}: {e}"
                logger.error(error_msg)
                logger.exception(e)
                stats['failed'] += 1
                stats['errors'].append(error_msg)
        
        logger.info("=" * 80)
        logger.info("Lead sync process completed")
        logger.info("=" * 80)
        
        return stats
        
    except Exception as e:
        error_msg = f"Unexpected error during sync: {type(e).__name__}: {e}"
        logger.critical(error_msg)
        logger.exception(e)
        stats['errors'].append(error_msg)
        return stats


# ============================================================================
# VALIDATION
# ============================================================================

def validate_config() -> List[str]:
    """Validate configuration"""
    logger.info("Validating configuration...")
    errors = []
    
    if not EXTERNAL_API_CONFIG['api_key'] or EXTERNAL_API_CONFIG['api_key'].strip() == '':
        error_msg = "API_KEY is not set in .env file"
        logger.error(error_msg)
        errors.append(error_msg)
    else:
        logger.debug("API_KEY is set")
    
    # EXTERNAL_API_BASE_URL is hardcoded, no need to validate
    logger.debug(f"External API Base URL: {EXTERNAL_API_CONFIG['base_url']}")
    
    if not CRM_API_CONFIG['api_key'] or CRM_API_CONFIG['api_key'].strip() == '':
        error_msg = "CRM_WEBHOOK_API_KEY is not set in .env file"
        logger.error(error_msg)
        errors.append(error_msg)
    else:
        logger.debug("CRM_WEBHOOK_API_KEY is set")
    
    logger.debug(f"CRM API Base URL: {CRM_API_CONFIG['base_url']}")
    
    if errors:
        logger.error(f"Configuration validation failed with {len(errors)} error(s)")
    else:
        logger.info("Configuration validation passed")
    
    return errors


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def print_stats(stats: Dict[str, Any]):
    """Print statistics in a readable format"""
    print("\n" + "=" * 80)
    print("SYNC STATISTICS")
    print("=" * 80)
    print(f"Total Records Fetched:        {stats['total_fetched']}")
    print(f"Active Customers Filtered:     {stats['active_customers_filtered']}")
    print(f"Leads Successfully Ingested:  {stats['total_ingested']}")
    print(f"Leads Already Exists:         {stats['already_exists']}")
    print(f"Failed:                       {stats['failed']}")
    print("=" * 80)
    
    if stats['errors']:
        print(f"\nERRORS ({len(stats['errors'])}):")
        print("-" * 80)
        for idx, error in enumerate(stats['errors'], 1):
            print(f"{idx}. {error}")
        print("-" * 80)
    else:
        print("\nNo errors encountered.")
    
    print("=" * 80 + "\n")

def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Daily Lead Sync Script - Syncs new signups from EarlyFit API to CRM',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging (INFO level)'
    )
    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        help='Enable debug logging (DEBUG level, includes verbose)'
    )
    parser.add_argument(
        '--no-stats',
        action='store_true',
        help='Do not print statistics summary'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    global logger
    logger = setup_logging(verbose=args.verbose, debug=args.debug)
    
    logger.info("Starting lead sync script")
    logger.info(f"Arguments: verbose={args.verbose}, debug={args.debug}")
    
    # Validate configuration
    config_errors = validate_config()
    if config_errors:
        logger.error("Configuration validation failed. Please fix the following errors:")
        for error in config_errors:
            logger.error(f"  - {error}")
        print("\nERROR: Configuration validation failed!")
        for error in config_errors:
            print(f"  - {error}")
        return False
    
    # Run sync
    try:
        stats = sync_leads()
        
        # Print statistics
        if not args.no_stats:
            print_stats(stats)
        
        # Log statistics
        logger.info("Final Statistics:")
        logger.info(f"  Total Fetched: {stats['total_fetched']}")
        logger.info(f"  Active Customers Filtered: {stats['active_customers_filtered']}")
        logger.info(f"  Successfully Ingested: {stats['total_ingested']}")
        logger.info(f"  Already Exists: {stats['already_exists']}")
        logger.info(f"  Failed: {stats['failed']}")
        logger.info(f"  Errors: {len(stats['errors'])}")
        
        if stats['errors']:
            logger.warning("Errors encountered during sync:")
            for error in stats['errors']:
                logger.warning(f"  - {error}")
        
        # Determine success
        # Consider it successful if we processed records and either ingested or they already existed
        success = (
            stats['failed'] == 0 and 
            (stats['total_ingested'] > 0 or stats['already_exists'] > 0 or stats['total_fetched'] == 0)
        )
        
        if success:
            logger.info("Script completed successfully")
        else:
            logger.warning("Script completed with errors or no records processed")
        
        return success
        
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        print("\nScript interrupted by user")
        return False
    except Exception as e:
        logger.critical(f"Unexpected error in main: {type(e).__name__}: {e}")
        logger.exception(e)
        print(f"\nFATAL ERROR: {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: {type(e).__name__}: {e}")
        sys.exit(1)

