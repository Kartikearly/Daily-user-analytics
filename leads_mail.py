"""
EarlyFit Sales Email Report System
Complete solution for querying database, updating Google Sheets, and sending email reports

Usage: python sales_mail.py
"""

# ============================================================================
# IMPORTS
# ============================================================================

import requests
import json
import os
import smtplib
import csv
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import StringIO
from dotenv import load_dotenv

# === [NEW] Added Google Sheets Imports ===
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
# =========================================

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Configuration
BASE_URL = os.getenv("BASE_URL", "https://earlyfit-api.saurabhsakhuja.com/api/v1")
API_KEY = os.getenv("API_KEY") 
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
    'smtp_host': os.getenv("SMTP_HOST", "smtp.gmail.com"),
    'smtp_port': int(os.getenv("SMTP_PORT", "587")),
    'smtp_username': os.getenv("SMTP_USERNAME"),
    'smtp_password': os.getenv("SMTP_PASSWORD"),
    'from_email': os.getenv("FROM_EMAIL"),
    'from_name': 'EarlyFit Leads Report',
    
    # Email Content
    'subject': f'New Leads (Channel Product) - {datetime.now().strftime("%Y-%m-%d")}',
    'title': 'EarlyFit Leads',
    'greeting': 'Dear Team,<br><br>Please find yesterday\'s sales data below.',
    'closing': 'Regards,<br><br>EarlyFit Product Team'
}

# Recipients List
RECIPIENTS = [
    # 'sales_ops@early.fit', 
    # 'parth@early.fit',
    'kabirgupta609@gmail.com'
]

# Error notification recipient hi
ERROR_NOTIFICATION_EMAIL = 'kartik@early.fit'

# === [MODIFIED] Google Sheets Configuration ===
GOOGLE_SHEETS_CONFIG = {
    # This file MUST be in the same directory as your script
    'SERVICE_ACCOUNT_FILE': 'landingpageconnections-e3325175d396.json',
    'SCOPES': ['https://www.googleapis.com/auth/spreadsheets'],
    
    # Get this from your .env file
    'SPREADSHEET_ID': os.getenv("SPREADSHEET_ID"),
    
    # Set the names of the tabs (sheets) you want to write to.
    'SHEET_NAMES': ['Website leads', 'New Sign ups MDT'],
}
# =========================================


# ============================================================================
# GOOGLE SHEETS UTILITIES (MODIFIED)
# ============================================================================

def get_google_sheets_service():
    """Authenticates and returns a Google Sheets service object."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['SERVICE_ACCOUNT_FILE'], 
            scopes=GOOGLE_SHEETS_CONFIG['SCOPES']
        )
        service = build('sheets', 'v4', credentials=creds)
        print("    [OK] Google Sheets service authenticated")
        return service
    except Exception as e:
        print(f"    [ERROR] Failed to authenticate with Google Sheets: {e}")
        return None

def append_to_google_sheet(service, spreadsheet_id: str, sheet_name: str, data: List[Dict[Any, Any]]):
    """
    (SMART APPEND) Appends data to a sheet.
    Checks if headers are needed and adds them if the sheet is empty.
    """
    
    if not data:
        print(f"           No data to append for sheet '{sheet_name}'.")
        return True # Not an error, just nothing to do

    values_to_append = []
    
    try:
        # 1. Check if the sheet is empty by checking A1
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1"
        ).execute()
        
        sheet_is_empty = not result.get('values')
        print(f"           Sheet '{sheet_name}' is empty: {sheet_is_empty}")

        # 2. Get headers from the first data row
        headers = list(data[0].keys())

        # 3. If the sheet is empty, add headers to our list
        if sheet_is_empty:
            values_to_append.append(headers)
        
        # 4. Add all the data rows
        for row in data:
            row_values = []
            for col in headers:
                value = row.get(col)
                
                if value is None:
                    row_values.append("")
                elif isinstance(value, (dict, list)):
                    row_values.append(json.dumps(value))
                else:
                    row_values.append(str(value)) # Convert all values to string
            values_to_append.append(row_values)

        # 5. Perform the append operation
        body = {'values': values_to_append}
        
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1", # Appends after the last row
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        if sheet_is_empty:
             print(f"    [OK] Wrote headers and {len(data)} records to sheet '{sheet_name}'")
        else:
             print(f"    [OK] Appended {len(data)} records to sheet '{sheet_name}'")
        return True

    except HttpError as e:
        print(f"    [ERROR] HTTP error appending to sheet '{sheet_name}': {e}")
        if "UNABLE_TO_PARSE" in str(e):
             print("           This error often means the sheet name is incorrect or does not exist.")
        return False
    except Exception as e:
        print(f"    [ERROR] General error appending to sheet '{sheet_name}': {e}")
        return False


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


def generate_error_email_body(error_details: str) -> str:
    """Generate error notification email body"""
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
    
    html_parts.append('''
            <div style="border-bottom: 2px solid #f44336; padding-bottom: 15px; margin-bottom: 20px;">
                <h1 style="color: #f44336; font-size: 24px; margin: 0; font-weight: bold;">
                    ⚠️ EarlyFit Sales Report - Script Failure
                </h1>
            </div>
    ''')
    
    html_parts.append('''
            <p style="color: #333; font-size: 14px; line-height: 1.6; margin-bottom: 20px;">
                Dear Kartik,<br><br>
                The EarlyFit Sales Report script has encountered an error and failed to complete successfully.
            </p>
    ''')
    
    html_parts.append(f'''
            <div style="background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0;">
                <h3 style="color: #856404; margin: 0 0 10px 0;">Error Details:</h3>
                <pre style="color: #856404; font-family: 'Courier New', monospace; font-size: 12px; white-space: pre-wrap; word-wrap: break-word; margin: 0;">{error_details}</pre>
            </div>
    ''')
    
    html_parts.append('''
            <p style="color: #333; font-size: 14px; line-height: 1.6; margin-top: 20px;">
                Please investigate and resolve the issue as soon as possible.
            </p>
    ''')
    
    html_parts.append(f'''
            <div style="margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #666; font-size: 11px;">
                <p style="margin: 0;">
                    This is an automated error notification from EarlyFit Sales System.<br>
                    Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </div>
        </div>
    </body>
    </html>
    ''')
    
    return '\n'.join(html_parts)


def send_error_notification(error_message: str):
    """Send error notification email to kartik@early.fit"""
    try:
        print(f"\n[ERROR NOTIFICATION] Sending error notification to {ERROR_NOTIFICATION_EMAIL}...")
        
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_host'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['smtp_username'], EMAIL_CONFIG['smtp_password'])
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'⚠️ EarlyFit Sales Report Failed - {datetime.now().strftime("%Y-%m-%d")}'
        msg['From'] = f"{EMAIL_CONFIG['from_name']} <{EMAIL_CONFIG['from_email']}>"
        msg['To'] = ERROR_NOTIFICATION_EMAIL
        
        html_body = generate_error_email_body(error_message)
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        server.send_message(msg)
        server.quit()
        
        print(f"    [OK] Error notification sent to {ERROR_NOTIFICATION_EMAIL}")
        
    except Exception as e:
        print(f"    [ERROR] Failed to send error notification: {e}")


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
# MAIN REPORT FUNCTION (FIXED)
# ============================================================================

def send_report_email():
    """Main function to query database, update Google Sheets, and send email report"""
    print("="*60)
    print("EarlyFit Sales Report")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    error_log = []  # Collect all errors for notification
    
    # Step 1: Initialize API client
    print("[1/5] Initializing API client...")
    try:
        client = EarlyFitAPIClient(base_url=BASE_URL, api_key=API_KEY)
        print("    [OK] API client initialized")
    except Exception as e:
        error_msg = f"Failed to initialize API client: {e}"
        print(f"    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False
    
    # Step 2: Query database and get email HTML for all queries
    print(f"\n[2/5] Querying database...")
    print(f"    Executing {len(SQL_QUERIES)} queries...")
    
    tables_data = []
    all_queries_successful = True
    
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
                error_msg = f"Query failed for {heading}: {result}"
                print(f"        [ERROR] {error_msg}")
                error_log.append(error_msg)
                all_queries_successful = False
                
        except Exception as e:
            error_msg = f"Query exception for {heading}: {e}"
            print(f"        [ERROR] {error_msg}")
            error_log.append(error_msg)
            all_queries_successful = False
    
    # Check if queries were successful
    if not all_queries_successful:
        error_msg = "One or more database queries failed"
        print(f"\n    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False
    
    if not tables_data:
        error_msg = "No data returned from any query"
        print(f"    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False
    
    # === [FIXED] Step 3: Write data to Google Sheets ===
    print(f"\n[3/5] Writing to Google Sheets...")
    sheets_success = True
    sheets_errors = []
    
    try:
        sheets_service = get_google_sheets_service()
        if sheets_service and GOOGLE_SHEETS_CONFIG['SPREADSHEET_ID'] != "YOUR_SPREADSHEET_ID_HERE":
            for i, (heading, data) in enumerate(tables_data):
                # Skip Google Sheets update for "Contact-Me Form leads" (index 0)
                # Only update for "New Signups(MDT/App)" (index 1)
                if i == 0:
                    print(f"    Skipping Google Sheets update for '{heading}' (Contact-Me Form leads)")
                    continue
                
                # For index 1 (New Signups), use sheet name at index 1
                if i == 1 and i < len(GOOGLE_SHEETS_CONFIG['SHEET_NAMES']):
                    sheet_name = GOOGLE_SHEETS_CONFIG['SHEET_NAMES'][i]
                    print(f"    Appending '{heading}' to sheet '{sheet_name}'...")
                    
                    if not append_to_google_sheet(sheets_service, GOOGLE_SHEETS_CONFIG['SPREADSHEET_ID'], sheet_name, data):
                        sheets_success = False
                        error_msg = f"Failed to append data to sheet '{sheet_name}'"
                        sheets_errors.append(error_msg)
                        error_log.append(error_msg)
                elif i >= len(GOOGLE_SHEETS_CONFIG['SHEET_NAMES']):
                    error_msg = f"No sheet name defined in config for query {i+1}"
                    print(f"    [WARNING] {error_msg}")
                    sheets_errors.append(error_msg)
                    error_log.append(error_msg)
                    sheets_success = False
        elif not sheets_service:
            error_msg = "Could not authenticate with Google Sheets"
            print(f"    [ERROR] {error_msg}")
            sheets_errors.append(error_msg)
            error_log.append(error_msg)
            sheets_success = False
        else:
            error_msg = "SPREADSHEET_ID is not set in .env file"
            print(f"    [ERROR] {error_msg}")
            sheets_errors.append(error_msg)
            error_log.append(error_msg)
            sheets_success = False
            
    except Exception as e:
        error_msg = f"Failed to write to Google Sheets: {e}"
        print(f"    [ERROR] {error_msg}")
        sheets_errors.append(error_msg)
        error_log.append(error_msg)
        sheets_success = False
    
    # Note: We continue even if sheets fail, but will notify kartik
    # We only stop if queries themselves failed (checked earlier)
    # =================================================
    
    # Step 4: Generate Email HTML
    print(f"\n[4/5] Generating email HTML...")
    try:
        email_html = generate_email_body(
            tables=tables_data,
            title=EMAIL_CONFIG['title'],
            greeting=EMAIL_CONFIG['greeting'],
            closing=EMAIL_CONFIG['closing']
        )
        print("    [OK] HTML email generated")
        
    except Exception as e:
        error_msg = f"Failed to generate email HTML: {e}"
        print(f"    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False
    
    # Step 5: Prepare and Send email
    print(f"\n[5/5] Sending email...")
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
                error_msg = f"Failed to send to {recipient}: {e}"
                print(f"    [ERROR] {error_msg}")
                failed_recipients.append(recipient)
                error_log.append(error_msg)
        
        server.quit()
        
        # Determine overall success
        email_success = len(failed_recipients) == 0
        overall_success = email_success and sheets_success
        
        if failed_recipients:
            error_msg = f"Failed to send to {len(failed_recipients)} recipient(s): {', '.join(failed_recipients)}"
            print(f"\n[WARNING] {error_msg}")
            error_log.append(error_msg)
            send_error_notification("\n".join(error_log))
            return False
        elif not sheets_success:
            # Emails sent successfully but sheets failed
            print(f"\n[PARTIAL SUCCESS] Email sent to all {len(RECIPIENTS)} recipient(s), but Google Sheets update failed!")
            send_error_notification("\n".join(error_log))
            return True  # Return True because emails were sent
        else:
            # Everything succeeded
            print(f"\n[SUCCESS] Email sent successfully to all {len(RECIPIENTS)} recipient(s)!")
            return True
            
    except smtplib.SMTPAuthenticationError:
        error_msg = "SMTP Authentication failed. Check your email and password. For Gmail, use an App Password."
        print(f"    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False
    except Exception as e:
        error_msg = f"Failed to send email: {e}"
        print(f"    [ERROR] {error_msg}")
        error_log.append(error_msg)
        send_error_notification("\n".join(error_log))
        return False


# ============================================================================
# VALIDATION
# ============================================================================

def validate_config():
    """Validate that configuration is set up correctly"""
    errors = []
    
    # Validate API Configuration
    if not API_KEY or API_KEY.strip() == '':
        errors.append("  - API_KEY is not set in .env file")
    
    if not BASE_URL or BASE_URL.strip() == '':
        errors.append("  - BASE_URL is not set in .env file")
    
    # Validate Email Configuration
    if not EMAIL_CONFIG['smtp_username'] or EMAIL_CONFIG['smtp_username'].strip() == '':
        errors.append("  - SMTP_USERNAME is not set in .env file")
    
    if not EMAIL_CONFIG['smtp_password'] or EMAIL_CONFIG['smtp_password'].strip() == '':
        errors.append("  - SMTP_PASSWORD is not set in .env file")
    
    if not EMAIL_CONFIG['from_email'] or EMAIL_CONFIG['from_email'].strip() == '':
        errors.append("  - FROM_EMAIL is not set in .env file")
    
    # Validate Recipients
    if not RECIPIENTS or len(RECIPIENTS) == 0:
        errors.append("  - Add recipient email addresses to RECIPIENTS list")
    
    # Validate SQL Queries
    if not SQL_QUERIES or len(SQL_QUERIES) == 0:
        errors.append("  - Add SQL queries to SQL_QUERIES list")
        
    # Validate Google Sheets Config
    if not GOOGLE_SHEETS_CONFIG['SPREADSHEET_ID'] or GOOGLE_SHEETS_CONFIG['SPREADSHEET_ID'] == "YOUR_SPREADSHEET_ID_HERE":
        errors.append("  - SPREADSHEET_ID is not set in .env file")
    
    if not os.path.exists(GOOGLE_SHEETS_CONFIG['SERVICE_ACCOUNT_FILE']):
        errors.append(f"  - Service account file '{GOOGLE_SHEETS_CONFIG['SERVICE_ACCOUNT_FILE']}' not found.")
        
    if len(GOOGLE_SHEETS_CONFIG['SHEET_NAMES']) < len(SQL_QUERIES):
        errors.append("  - Not enough sheet names in GOOGLE_SHEETS_CONFIG for the number of SQL_QUERIES.")
    
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
        print("Please fix the following configuration issues:\n")
        for error in config_errors:
            print(error)
        print("\nOnce configured, run the script again.")
        
        # Send error notification for configuration issues
        error_message = "Configuration validation failed:\n" + "\n".join(config_errors)
        send_error_notification(error_message)
        return False
    
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
        error_message = "Script was interrupted by user (KeyboardInterrupt)"
        send_error_notification(error_message)
        sys.exit(1)
    except Exception as e:
        error_message = f"Unexpected error: {e}\n\nTraceback:\n"
        import traceback
        error_message += traceback.format_exc()
        print(f"\n[ERROR] {error_message}")
        send_error_notification(error_message)
        sys.exit(1)
