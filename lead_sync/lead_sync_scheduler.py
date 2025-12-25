"""
Lead Sync Scheduler
Schedules the lead sync script to run daily at 10 PM IST
"""

import os
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from pathlib import Path

# Import the sync function
# Add parent directory to path to import from same folder
sys.path.insert(0, str(Path(__file__).parent))

from lead_sync_script import sync_leads, validate_config

# Indian Standard Time
IST = pytz.timezone('Asia/Kolkata')


def scheduled_sync_job():
    """Wrapper function for the scheduled sync job"""
    try:
        stats = sync_leads()
        
        return True
        
    except Exception as e:
        return False


def main():
    """Main scheduler function"""
    # Validate configuration
    config_errors = validate_config()
    if config_errors:
        return False
    
    # Create scheduler
    scheduler = BlockingScheduler(timezone=IST)
    
    # Schedule the job to run daily at 11:25 AM IST
    scheduler.add_job(
        scheduled_sync_job,
        trigger=CronTrigger(hour=11, minute=25, timezone=IST),
        id='daily_lead_sync',
        name='Daily Lead Sync (11:25 AM IST)',
        replace_existing=True
    )
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        return True
    except Exception as e:
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        sys.exit(1)

