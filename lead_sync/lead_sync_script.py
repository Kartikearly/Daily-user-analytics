"""
Daily Lead Sync Script
Fetches new signups from EarlyFit API and creates leads in CRM
Runs daily at 10 PM IST
"""

import requests
import json
import os
import sys
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
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key
        }
        
        payload = {"query": sql_query}
        
        try:
            response = requests.post(
                self.analytics_endpoint,
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if isinstance(result, dict) and result.get("success"):
                return result
            else:
                return result
                
        except requests.exceptions.RequestException as e:
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
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        }
        
        try:
            response = requests.post(
                self.ingest_endpoint,
                headers=headers,
                json=lead_data,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            return result
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                # Lead already exists
                return {"status": "exists", "message": "Lead already exists"}
            else:
                raise
        except requests.exceptions.RequestException as e:
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
    phone = str(external_record.get('phone', '')).strip()
    phone = ''.join(filter(str.isdigit, phone))
    
    # Get name
    name = external_record.get('patient_name', '').strip()
    if not name:
        name = None
    
    # Get email
    email = external_record.get('email', '').strip()
    if not email or email.lower() == 'null' or email == '':
        email = None
    
    # Get created timestamp
    created_at = external_record.get('created_at_ist', '')
    
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
        external_client = ExternalAPIClient(
            base_url=EXTERNAL_API_CONFIG['base_url'],
            api_key=EXTERNAL_API_CONFIG['api_key']
        )
        
        result = external_client.query_analytics(NEW_SIGNUPS_QUERY)
        
        if not result.get('success'):
            error_msg = f"External API query failed: {result}"
            stats['errors'].append(error_msg)
            return stats
        
        records = result.get('data', [])
        stats['total_fetched'] = len(records)
        
        if not records:
            return stats
        
        # Step 2: Fetch active customers (to exclude from lead creation)
        try:
            active_result = external_client.query_analytics(ACTIVE_CUSTOMERS_QUERY)
            
            if active_result.get('success'):
                active_customer_records = active_result.get('data', [])
                # Normalize all active customer phone numbers
                active_customer_phones = {
                    normalize_phone_number(record['phone']) 
                    for record in active_customer_records 
                    if record.get('phone')
                }
            else:
                active_customer_phones = set()
        except Exception as e:
            active_customer_phones = set()
        
        # Filter out active customers from new signups
        filtered_records = []
        for record in records:
            normalized_phone = normalize_phone_number(record.get('phone', ''))
            if normalized_phone in active_customer_phones:
                stats['active_customers_filtered'] += 1
            else:
                filtered_records.append(record)
        
        if not filtered_records:
            return stats
        
        # Step 3: Transform and validate data
        leads_to_ingest = []
        
        for idx, record in enumerate(filtered_records, 1):
            try:
                lead_data = transform_external_data_to_lead(record)
                
                # Validate phone number
                if not lead_data.get('phone_number') or len(lead_data['phone_number']) < 10:
                    error_msg = f"Invalid phone number for record {idx}: {record.get('phone')}"
                    stats['failed'] += 1
                    stats['errors'].append(error_msg)
                    continue
                
                leads_to_ingest.append(lead_data)
                
            except Exception as e:
                error_msg = f"Error transforming record {idx}: {e}"
                stats['failed'] += 1
                stats['errors'].append(error_msg)
        
        # Step 4: Ingest leads into CRM
        crm_client = CRMAPIClient(
            base_url=CRM_API_CONFIG['base_url'],
            api_key=CRM_API_CONFIG['api_key']
        )
        
        for idx, lead_data in enumerate(leads_to_ingest, 1):
            try:
                result = crm_client.ingest_lead(lead_data)
                
                if result.get('status') == 'exists':
                    stats['already_exists'] += 1
                else:
                    stats['total_ingested'] += 1
                
            except Exception as e:
                error_msg = f"Failed to ingest lead {lead_data['phone_number']}: {e}"
                stats['failed'] += 1
                stats['errors'].append(error_msg)
        
        return stats
        
    except Exception as e:
        error_msg = f"Unexpected error during sync: {e}"
        stats['errors'].append(error_msg)
        return stats


# ============================================================================
# VALIDATION
# ============================================================================

def validate_config() -> List[str]:
    """Validate configuration"""
    errors = []
    
    if not EXTERNAL_API_CONFIG['api_key'] or EXTERNAL_API_CONFIG['api_key'].strip() == '':
        errors.append("API_KEY is not set in .env file")
    
    # EXTERNAL_API_BASE_URL is hardcoded, no need to validate
    
    if not CRM_API_CONFIG['api_key'] or CRM_API_CONFIG['api_key'].strip() == '':
        errors.append("CRM_WEBHOOK_API_KEY is not set in .env file")
    
    return errors


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    # Validate configuration
    config_errors = validate_config()
    if config_errors:
        return False
    
    # Run sync
    try:
        stats = sync_leads()
        
        # Determine success
        success = stats['failed'] == 0 and stats['total_fetched'] > 0
        
        return success
        
    except Exception as e:
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        sys.exit(1)

