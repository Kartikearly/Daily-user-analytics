"""
EarlyFit Automated Email Report System
Complete solution for querying database and sending email reports

Usage: python dogfooding_mail.py
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
BASE_URL = "https://earlyfit-api-staging.saurabhsakhuja.com/api/v1"
API_KEY = "eyJhbGciOi"  # Update with the API key value from your tech team's env variable

# SQL Queries to execute - Each tuple is (heading, query)
SQL_QUERIES = [
    ("Dogfooding Analytics", """
WITH target_patients AS (
    -- CTE 1: Select the patient ID and concatenate first and last names for the specified phone numbers.
    SELECT
        id AS patient_id,
        (COALESCE(firstname, '') || ' ' || COALESCE(lastname, '')) AS patient_name,
        phone
    FROM public.patients
    WHERE phone IN ('8373957475', '9815813691', '7042210925', '9582175147', '8287458802', '7889371195', '7007377818', '9991666648', '9560998230', '6290499500', '8572820094', '9910236709', '9953099458', '9999879762', '9560998500')
),
yesterday_date AS (
    -- CTE 2: Define yesterday's date dynamically.
    SELECT (CURRENT_DATE - INTERVAL '1 day')::date AS d -- Dynamic Date Calculation
),
daily_log_presence AS (
    -- CTE 3: Check for the presence of each of the three required logs for all target patients and for all dates in the lookback window.
    SELECT
        tp.patient_id,
        dates.check_date,
        -- Check 1: Meal Log Presence
        EXISTS (
            SELECT 1 FROM public.patientfoodlogs pfl
            WHERE pfl.patient_id = tp.patient_id AND pfl.date::date = dates.check_date
        ) AS has_food_log,
        -- Check 2: Habit Log Presence (regardless of status)
        EXISTS (
            SELECT 1 FROM public.patienthabitlogs phl
            WHERE phl.patient_id = tp.patient_id AND phl.date::date = dates.check_date
        ) AS has_habit_log,
        -- Check 3: Activity Data Presence
        EXISTS (
            SELECT 1 FROM public.activity a
            WHERE a.patient_id = tp.patient_id AND a.date::date = dates.check_date
        ) AS has_activity_entry
    FROM target_patients tp
    -- Generate a series of dates for streak calculation (30 days up to yesterday)
    CROSS JOIN LATERAL (
        SELECT generate_series((SELECT d FROM yesterday_date) - interval '30 days', (SELECT d FROM yesterday_date), '1 day'::interval)::date AS check_date
    ) dates
),
date_series_with_activity AS (
    -- CTE 4: Apply the STRICT definition of 'Active' (must have ALL 3 logs).
    SELECT
        patient_id,
        check_date,
        -- STRICT LOGIC: is_active_day is TRUE only if ALL three logs are present.
        (has_food_log AND has_habit_log AND has_activity_entry) AS is_active_day,
        has_food_log AS logged_meal,
        has_habit_log AS logged_habit,
        has_activity_entry AS logged_activity
    FROM daily_log_presence
),
non_active_streak AS (
    -- CTE 5: Calculate the length of the consecutive inactive streak *ending* on yesterday.
    SELECT
        patient_id,
        COUNT(*) AS non_active_days_streak -- Simplified streak calculation for the group ending yesterday
    FROM (
        SELECT
            patient_id,
            is_active_day,
            check_date,
            -- Group consecutive days with the same activity status
            check_date - (ROW_NUMBER() OVER (PARTITION BY patient_id, is_active_day ORDER BY check_date))::int * INTERVAL '1 day' AS date_group
        FROM date_series_with_activity
    ) AS streak_groups
    -- Filter for the specific group that includes yesterday and is inactive
    WHERE NOT is_active_day
      AND date_group = (
            SELECT check_date - (ROW_NUMBER() OVER (PARTITION BY patient_id, is_active_day ORDER BY check_date))::int * INTERVAL '1 day'
            FROM date_series_with_activity
            WHERE check_date = (SELECT d FROM yesterday_date) AND NOT is_active_day AND patient_id = streak_groups.patient_id
          )
    GROUP BY patient_id
),
yesterday_activity_status AS (
    -- CTE 6: Pull yesterday's specific activity status and logs for the final output.
    SELECT
        patient_id,
        is_active_day,
        logged_meal,
        logged_habit,
        logged_activity
    FROM date_series_with_activity
    WHERE check_date = (SELECT d FROM yesterday_date)
),
last_7_days_weight_log AS (
    -- CTE 7: Check if the user has logged a weight metric in the last 7 days (up to and including yesterday).
    SELECT
        patient_id,
        COUNT(*) > 0 AS has_logged_weight_in_7_days
    FROM public.metrics
    WHERE name = 'BODY_WEIGHT'
    -- Date range uses the dynamic yesterday date
    AND date::date BETWEEN (SELECT d FROM yesterday_date) - INTERVAL '6 days' AND (SELECT d FROM yesterday_date)
    GROUP BY patient_id
)
-- Final SELECT statement to combine all computed metrics.
SELECT
    tp.patient_name AS "Names",
    CASE WHEN yas.is_active_day THEN 'Yes' ELSE 'No' END AS "Active Yesterday",
    -- Get the calculated streak length, defaulting to 0 if the user was active yesterday.
    COALESCE(nas.non_active_days_streak, 0) AS "No of Non Active Days",
    -- Show 'Yes' if any weight log exists in the last 7 days, 'No' otherwise.
    CASE WHEN COALESCE(l7dwl.has_logged_weight_in_7_days, FALSE) THEN 'Yes' ELSE 'No' END AS "Weight Log(last 7 days )",
    -- REASON LOGIC: Only list the *missing* activities, or show 'Active'.
    CASE
        -- UPDATED: If user was active, just show 'Active'
        WHEN yas.is_active_day THEN 'Active'
        ELSE TRIM(TRAILING ', ' FROM
             CASE WHEN NOT yas.logged_meal THEN 'Missing Meal Log, ' ELSE '' END ||
             CASE WHEN NOT yas.logged_habit THEN 'Missing Habit Log, ' ELSE '' END ||
             CASE WHEN NOT yas.logged_activity THEN 'Missing Steps/Activity Data, ' ELSE '' END
        )
    END AS "Reason of Inactivity Yesterday"
FROM target_patients tp
JOIN yesterday_activity_status yas ON tp.patient_id = yas.patient_id
LEFT JOIN non_active_streak nas ON tp.patient_id = nas.patient_id
LEFT JOIN last_7_days_weight_log l7dwl ON tp.patient_id = l7dwl.patient_id
ORDER BY "Names";
    """)
]

# Email Configuration
EMAIL_CONFIG = {
    # SMTP Server Settings
    'smtp_host': 'smtp.gmail.com',
    'smtp_port': 587,
    'smtp_username': 'kartikgupta0043@gmail.com',
    'smtp_password': 'atdfvirejgizafaw',
    'from_email': 'kartikgupta0043@gmail.com',
    'from_name': 'Dogfooding Analytics',
    
    # Email Content
    'subject': f'Dogfooding Analytics - {datetime.now().strftime("%Y-%m-%d")}',
    'title': 'Dogfooding Analytics',
    'greeting': 'Dear Team,<br><br>Please find below the dogfooding analytics for our team members.',
    'closing': 'Regards,<br><br>EarlyFit Product Team'
}

# Recipients List
RECIPIENTS = [
    'kartik@early.fit',
]

# ============================================================================
# TABLE UTILITIES
# ============================================================================

def print_table_preview(data: List[Dict[Any, Any]]):
    """Print the complete table data to console"""
    if not data:
        print("No data to display")
        return
    
    columns = list(data[0].keys())
    col_widths = {}
    for col in columns:
        col_widths[col] = max(
            len(str(col)),
            max((len(str(row.get(col, ""))) for row in data), default=0)
        )
        # Don't limit column width - show full content
    
    header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
    print("=" * len(header))
    print(header)
    print("=" * len(header))
    
    for i, row in enumerate(data):
        values = []
        for col in columns:
            value = str(row.get(col, ""))
            # Don't truncate values - show full content
            values.append(value.ljust(col_widths[col]))
        print(" | ".join(values))
    
    print(f"\nTotal rows: {len(data)}")


def generate_email_table(data: List[Dict[Any, Any]], title: str = None, conditional_formatting: bool = True) -> str:
    """Generate an email-compatible HTML table from JSON data"""
    if not data:
        return '<p style="color: #666; font-family: Arial, sans-serif;">No data available</p>'
    
    columns = list(data[0].keys())
    
    def get_cell_color(column_name: str, value: Any) -> str:
        """Determine cell background color based on column name and value"""
        if not conditional_formatting:
            return ""
        
        value_str = str(value).strip() if value is not None else ""
        
        # User Onboarded = "No" → Bright Red
        if column_name == "User Onboarded" and value_str.lower() == "no":
            return "#ff6666"
        
        # Goals Set, Smart Scale Logged, Meal Logged = "No" → Bright Yellow
        if column_name in ["Goals Set", "Smart Scale Logged", "Meal Logged"]:
            if value_str.lower() == "no":
                return "#ffff00"
        
        # Interaction (5 Days), Meal Log (3 days), Weight Log (7 days), Weight Log(last 7 days ) = "No" → Bright Yellow
        if column_name in ["Interaction (5 Days)", "Meal Log (3 days)", "Weight Log (7 days)", "Weight Log(last 7 days )"]:
            if value_str.lower() == "no":
                return "#ffff00"
        
        # Active Yesterday = "No" → Bright Red
        if column_name == "Active Yesterday" and value_str.lower() == "no":
            return "#ff6666"
        
        # On/Off Track contains "off track" → Bright Orange
        if column_name == "On/Off Track" and "off track" in value_str.lower():
            return "#ff9900"
        
        # Current Weight Lose is negative → Bright Red
        if column_name == "Current Weight Lose":
            try:
                numeric_value = float(value_str)
                if numeric_value < 0:
                    return "#ff6666"
            except (ValueError, TypeError):
                pass
        
        return ""
    
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
            
            # No truncation - show full content
            value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
            cell_bg_color = get_cell_color(col, row.get(col, ""))
            cell_style = f"padding: 10px; border: 1px solid #ddd; color: #333;"
            if cell_bg_color:
                cell_style += f" background-color: {cell_bg_color};"
            
            html.append(f'''
            <td style="{cell_style}">
                {value}
            </td>''')
        
        html.append('</tr>')
    
    html.append('</tbody>')
    html.append('</table>')
    
    return '\n'.join(html)


def generate_multiple_tables_email(tables: List[tuple], title: str = "Data Report", 
                                   greeting: str = None, closing: str = None) -> str:
    """Generate a complete email body with multiple tables"""
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
            html_parts.append(f'''
            <div style="margin-top: {'30px' if idx > 0 else '0'}; margin-bottom: 15px;">
                <h2 style="color: #4CAF50; font-size: 18px; margin: 0 0 10px 0; font-weight: bold; border-bottom: 1px solid #ddd; padding-bottom: 5px;">
                    {heading}
                </h2>
            </div>
            ''')
            
            # Apply conditional formatting for Dogfooding Analytics
            use_formatting = heading in ["Dogfooding Analytics"]
            html_parts.append(generate_email_table(data, title=None, conditional_formatting=use_formatting))
    
    if closing:
        html_parts.append(f'''
            <p style="color: #333; font-size: 14px; line-height: 1.6; margin-top: 30px;">
                {closing}
            </p>
        ''')
    
    html_parts.append(f'''
            <div style="margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #666; font-size: 11px;">
                <p style="margin: 0;">
                    This is an automated report generated by EarlyFit Analytics System.<br>
                    Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </div>
        </div>
    </body>
    </html>
    ''')
    
    return '\n'.join(html_parts)


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
    print("EarlyFit Automated Report Email")
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
                        print_table_preview(data)
                else:
                    print(f"        [WARNING] No data returned for {heading}")
                    tables_data.append((heading, []))
            else:
                print(f"        [ERROR] Query failed for {heading}")
                all_successful = False
                
        except Exception as e:
            print(f"        [ERROR] Query failed: {e}")
            all_successful = False
    
    if not tables_data or not any(data for _, data in tables_data):
        print("    [ERROR] No data returned from any query")
        return False
    
    # Generate combined email HTML
    try:
        email_html = generate_multiple_tables_email(
            tables=tables_data,
            title=EMAIL_CONFIG['title'],
            greeting=EMAIL_CONFIG['greeting'],
            closing=EMAIL_CONFIG['closing']
        )
        print(f"\n    [OK] HTML email generated with {len(tables_data)} table(s)")
        
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
        print("Please update the following settings in dogfooding_mail.py:\n")
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

