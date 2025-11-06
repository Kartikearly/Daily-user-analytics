"""
EarlyFit Sales Email Report System
Complete solution for querying database and sending email reports

Usage: python sales_mail.py
"""

# ============================================================================
# IMPORTS
# ============================================================================

import requests
import json
import smtplib
import csv
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import StringIO

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Configuration
BASE_URL = "https://earlyfit-api.saurabhsakhuja.com/api/v1"
API_KEY = "eyJhbGciOi"  # Update with the API key value from your tech team's env variable

# SQL Queries to execute - Each tuple is (heading, query)
SQL_QUERIES = [
    ("Contact-Me Form leads", """
SELECT
  *
FROM
  "public"."assistant"
WHERE
  "createdAt"::date = (CURRENT_DATE - INTERVAL '1 day');
    """),
    
    ("New Signups(MDT/App)", """
SELECT
    p.phone,
    TRIM(
        COALESCE(p.firstname, '') ||
        CASE WHEN p.middlename IS NOT NULL AND p.middlename <> '' THEN ' ' || p.middlename ELSE '' END ||
        CASE WHEN p.lastname IS NOT NULL AND p.lastname <> '' THEN ' ' || p.lastname ELSE '' END
    ) AS patient_name,
    p."createdAt" + INTERVAL '5 hours 30 minutes' AS created_at_ist
FROM
    public.patients AS p
WHERE
    p."createdAt" >= ((CURRENT_DATE AT TIME ZONE 'IST') - INTERVAL '1 day') AT TIME ZONE 'UTC'
    AND p."createdAt" < (CURRENT_DATE AT TIME ZONE 'IST') AT TIME ZONE 'UTC';
    """)
]

# Email Configuration
EMAIL_CONFIG = {
    # SMTP Server Settings
    'smtp_host': 'smtp.gmail.com',
    'smtp_port': 587,
    'smtp_username': 'kartik@Early.fit',
    'smtp_password': 'gsbpbfxpabmwjhkc',
    'from_email': 'kartik@early.fit',
    'from_name': 'EarlyFit Leads Report',
    
    # Email Content
    'subject': f'New Leads (Channel Product) - {datetime.now().strftime("%Y-%m-%d")}',
    'title': 'EarlyFit Leads',
    'greeting': 'Dear Team,<br><br>Please find yesterday\'s sales data below.',
    'closing': 'Regards,<br><br>EarlyFit Product Team'
}

# Recipients List
RECIPIENTS = [
    'productearly@early.fit', 
    'naman@early.fit',
    'priyansh@early.fit',
]

# ============================================================================
# EMAIL UTILITIES
# ============================================================================

def format_data_as_table(data: List[Dict[Any, Any]], title: str = None) -> str:
    """Format data as HTML table"""
    if not data:
        return f'<p style="color: #666; font-family: Arial, sans-serif;">No data available for {title}</p>'
    
    columns = list(data[0].keys())
    
    html = []
    
    if title:
        html.append(f'''
        <div style="margin-bottom: 15px;">
            <h2 style="color: #333; font-family: Arial, sans-serif; font-size: 18px; margin: 0 0 10px 0; font-weight: bold;">
                {title}
            </h2>
            <p style="color: #666; font-family: Arial, sans-serif; font-size: 12px; margin: 0;">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Total Records: {len(data)}
            </p>
        </div>
        ''')
    
    html.append('<table style="border-collapse: collapse; width: 100%; max-width: 100%; font-family: Arial, sans-serif; font-size: 12px; background-color: #ffffff; border: 1px solid #ddd;">')
    
    html.append('<thead>')
    html.append('<tr style="background-color: #4CAF50;">')
    for col in columns:
        html.append(f'''
        <th style="padding: 12px 10px; text-align: left; color: #ffffff; font-weight: bold; border: 1px solid #45a049; white-space: nowrap;">
            {col}
        </th>''')
    html.append('</tr>')
    html.append('</thead>')
    
    html.append('<tbody>')
    for idx, row in enumerate(data):
        bg_color = '#f9f9f9' if idx % 2 == 0 else '#ffffff'
        html.append(f'<tr style="background-color: {bg_color};">')
        
        for col in columns:
            value = row.get(col, "")
            
            if value is None:
                value = ""
            elif isinstance(value, (dict, list)):
                value = json.dumps(value)
            else:
                value = str(value)
            
            # Escape HTML
            value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
            html.append(f'''
            <td style="padding: 10px; border: 1px solid #ddd; color: #333;">
                {value}
            </td>''')
        
        html.append('</tr>')
    
    html.append('</tbody>')
    html.append('</table>')
    
    return '\n'.join(html)


def generate_email_body(tables: List[tuple], title: str = "Sales Report", 
                       greeting: str = None, closing: str = None) -> str:
    """Generate a complete email body with formatted data in tables"""
    html_parts = []
    
    html_parts.append('''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f5f5f5;">
        <div style="max-width: 800px; margin: 0 auto; padding: 20px; background-color: #ffffff;">
    ''')
    
    html_parts.append(f'''
            <div style="border-bottom: 2px solid #4CAF50; padding-bottom: 15px; margin-bottom: 20px;">
                <h1 style="color: #333; font-size: 24px; margin: 0; font-weight: bold;">
                    {title}
                </h1>
            </div>
    ''')
    
    if greeting:
        html_parts.append(f'''
            <p style="color: #333; font-size: 14px; line-height: 1.6; margin-bottom: 20px;">
                {greeting}
            </p>
        ''')
    
    for idx, (heading, data) in enumerate(tables):
        if data and len(data) > 0:
            html_parts.append(format_data_as_table(data, title=heading))
            if idx < len(tables) - 1:
                html_parts.append('<div style="margin: 30px 0; border-top: 1px solid #ddd;"></div>')
        else:
            html_parts.append(f'<p style="color: #666; font-family: Arial, sans-serif;">No data available for {heading}</p>')
    
    if closing:
        html_parts.append(f'''
            <p style="color: #333; font-size: 14px; line-height: 1.6; margin-top: 30px;">
                {closing}
            </p>
        ''')
    
    html_parts.append(f'''
            <div style="margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #666; font-size: 11px;">
                <p style="margin: 0;">
                    This is an automated report generated by EarlyFit Sales System.<br>
                    Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </div>
        </div>
    </body>
    </html>
    ''')
    
    return '\n'.join(html_parts)


def print_data_preview(data: List[Dict[Any, Any]]):
    """Print the data to console"""
    if not data:
        print("No data to display")
        return
    
    print(f"\nTotal records: {len(data)}")
    print("-" * 60)
    
    for idx, row in enumerate(data, 1):
        print(f"\nRecord {idx}:")
        for key, value in row.items():
            print(f"  {key}: {value}")


# ============================================================================
# API CLIENT
# ============================================================================

class EarlyFitAPIClient:
    """Client to query EarlyFit database through Analytics API"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.analytics_endpoint = f"{self.base_url}/analytics"
    
    def query_analytics(self, sql_query: str) -> Dict[Any, Any]:
        """
        Execute a SQL query through the Analytics API
        Only SELECT, SHOW, and EXPLAIN queries are allowed for security reasons.
        """
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
            print(f"Error making API request: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            raise


# ============================================================================
# MAIN EMAIL FUNCTION
# ============================================================================

def send_report_email():
    """Main function to query database and send email report"""
    print("="*60)
    print("EarlyFit Sales Report Email")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Step 1: Initialize API client
    print("[1/4] Initializing API client...")
    try:
        client = EarlyFitAPIClient(base_url=BASE_URL, api_key=API_KEY)
        print("    [OK] API client initialized")
    except Exception as e:
        print(f"    [ERROR] Failed to initialize API client: {e}")
        return False
    
    # Step 2: Query database and get email HTML for all queries
    print(f"\n[2/4] Querying database...")
    print(f"    Executing {len(SQL_QUERIES)} queries...")
    
    tables_data = []
    all_successful = True
    
    for idx, (heading, query) in enumerate(SQL_QUERIES):
        try:
            print(f"\n    Query {idx + 1}/{len(SQL_QUERIES)}: {heading}")
            result = client.query_analytics(query)
            
            if isinstance(result, dict) and result.get("success"):
                data = result.get("data", [])
                if len(data) > 0:
                    tables_data.append((heading, data))
                    print(f"        [OK] Retrieved {len(data)} record(s)")
                    if idx == 0:
                        print_data_preview(data)
                else:
                    print(f"        [WARNING] No data returned for {heading}")
                    tables_data.append((heading, []))
            else:
                print(f"        [ERROR] Query failed for {heading}")
                all_successful = False
                
        except Exception as e:
            print(f"        [ERROR] Query failed: {e}")
            all_successful = False
    
    if not tables_data:
        print("    [ERROR] No data returned from any query")
        return False
    
    # Generate combined email HTML
    try:
        email_html = generate_email_body(
            tables=tables_data,
            title=EMAIL_CONFIG['title'],
            greeting=EMAIL_CONFIG['greeting'],
            closing=EMAIL_CONFIG['closing']
        )
        print(f"\n    [OK] HTML email generated with {len(tables_data)} section(s)")
        
    except Exception as e:
        print(f"    [ERROR] Failed to generate email HTML: {e}")
        return False
    
    # Step 3: Prepare email
    print(f"\n[3/4] Preparing email...")
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = EMAIL_CONFIG['subject']
        msg['From'] = f"{EMAIL_CONFIG['from_name']} <{EMAIL_CONFIG['from_email']}>"
        
        html_part = MIMEText(email_html, 'html')
        msg.attach(html_part)
        
        print(f"    [OK] Email prepared for {len(RECIPIENTS)} recipient(s)")
        
    except Exception as e:
        print(f"    [ERROR] Failed to prepare email: {e}")
        return False
    
    # Step 4: Send email
    print(f"\n[4/4] Sending email...")
    try:
        print(f"    Connecting to {EMAIL_CONFIG['smtp_host']}...")
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_host'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        
        print(f"    Logging in as {EMAIL_CONFIG['smtp_username']}...")
        server.login(EMAIL_CONFIG['smtp_username'], EMAIL_CONFIG['smtp_password'])
        
        failed_recipients = []
        for recipient in RECIPIENTS:
            try:
                recipient_msg = MIMEMultipart('alternative')
                recipient_msg['Subject'] = EMAIL_CONFIG['subject']
                recipient_msg['From'] = f"{EMAIL_CONFIG['from_name']} <{EMAIL_CONFIG['from_email']}>"
                recipient_msg['To'] = recipient
                
                html_part = MIMEText(email_html, 'html')
                recipient_msg.attach(html_part)
                
                server.send_message(recipient_msg)
                print(f"    [OK] Sent to: {recipient}")
            except Exception as e:
                print(f"    [ERROR] Failed to send to {recipient}: {e}")
                failed_recipients.append(recipient)
        
        server.quit()
        
        if failed_recipients:
            print(f"\n[WARNING] Failed to send to {len(failed_recipients)} recipient(s)")
            return False
        else:
            print(f"\n[SUCCESS] Email sent successfully to all {len(RECIPIENTS)} recipient(s)!")
            return True
            
    except smtplib.SMTPAuthenticationError:
        print(f"    [ERROR] Authentication failed. Check your email and password.")
        print(f"      For Gmail, use an App Password instead of your regular password.")
        return False
    except Exception as e:
        print(f"    [ERROR] Failed to send email: {e}")
        return False


# ============================================================================
# VALIDATION
# ============================================================================

def validate_config():
    """Validate that configuration is set up correctly"""
    errors = []
    
    if EMAIL_CONFIG['smtp_username'] == 'your-email@gmail.com':
        errors.append("  - Update EMAIL_CONFIG['smtp_username'] with your email")
    
    if EMAIL_CONFIG['smtp_password'] == 'your-app-password':
        errors.append("  - Update EMAIL_CONFIG['smtp_password'] with your password/app password")
    
    if EMAIL_CONFIG['from_email'] == 'your-email@gmail.com':
        errors.append("  - Update EMAIL_CONFIG['from_email'] with your email")
    
    if not RECIPIENTS or RECIPIENTS[0] == 'recipient1@example.com':
        errors.append("  - Add recipient email addresses to RECIPIENTS list")
    
    if not SQL_QUERIES or len(SQL_QUERIES) == 0:
        errors.append("  - Add SQL queries to SQL_QUERIES list")
    
    return errors


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    print("\n")
    
    config_errors = validate_config()
    if config_errors:
        print("="*60)
        print("CONFIGURATION REQUIRED")
        print("="*60)
        print("Please update the following settings in sales_mail.py:\n")
        for error in config_errors:
            print(error)
        print("\nOnce configured, run the script again.")
        return
    
    success = send_report_email()
    
    print("\n" + "="*60)
    if success:
        print("REPORT EMAIL SENT SUCCESSFULLY!")
    else:
        print("REPORT EMAIL FAILED!")
    print("="*60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return success


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[INFO] Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

