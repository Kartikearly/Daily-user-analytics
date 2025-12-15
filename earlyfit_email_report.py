"""
EarlyFit Automated Email Report System
Complete solution for querying database and sending email reports

Usage: python earlyfit_email_report.py
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

# Google Sheets imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Configuration
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")

# SQL Queries to execute - Each tuple is (heading, query)
SQL_QUERIES = [
    ("Summary Comparison", """
WITH
    -- 1. COACH + APP REPORT (TODAY)
    coach_app_report_today AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true 
                    AND p.status = 'ACTIVE_SUBSCRIPTION' 
                    AND p.subscription_end_date >= CURRENT_DATE 
                    AND s.type = 'Coach+App'
                    -- Removed 7-day filter: AND (CURRENT_DATE - p.subscription_start_date::date) >= 7
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 22, 24)
            ),
            onboarding_status AS (
                SELECT pb.id AS patient_id,
                    CASE
                        WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                        AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT')
                        AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id)
                        THEN 'Yes' ELSE 'No'
                    END AS user_onboarded
                FROM patient_base pb
            ),
            last_consultant_interaction AS (
                SELECT patient_id, (CURRENT_DATE - MAX(interaction_date)) AS days_since_last_interaction
                FROM (
                    SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
                    UNION ALL
                    SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED'
                    UNION ALL
                    SELECT c.patient_id, m."messagedAt"::date AS interaction_date
                    FROM "public"."chats" c
                    JOIN "public"."messages" m ON c.id = m.chat_id
                    WHERE m.sender = c.consultant_id
                ) all_interactions
                GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT') sub
                WHERE rn = 1
            ),
            weight_log_last_7_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_7_days
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days') sub
                WHERE rn = 1
            )
        SELECT
            os.user_onboarded,
            lci.days_since_last_interaction,
            COALESCE(mll3d.logged_in_last_3_days, 'No') AS last_3_days_meal_log,
            CASE
                WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
                WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
                ELSE
                    CASE
                        WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date))) THEN 'On Track'
                        ELSE 'Off Track'
                    END
            END AS on_track_status,
            COALESCE(wll7d.logged_in_last_7_days, 'No') AS logged_weight_last_7_days
        FROM patient_base pb
        LEFT JOIN onboarding_status os ON pb.id = os.patient_id
        LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
        LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
        LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
        LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
    ),

    -- 2. GLP REPORT (TODAY)
    glp_report_today AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true 
                    AND p.status = 'ACTIVE_SUBSCRIPTION' 
                    AND p.subscription_end_date >= CURRENT_DATE 
                    AND s.type = 'GLP'
                    -- Removed 7-day filter: AND (CURRENT_DATE - p.subscription_start_date::date) >= 7
                    AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 22, 24)
            ),
            onboarding_status AS (
                SELECT pb.id AS patient_id,
                    CASE
                        WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                        AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT')
                        AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id)
                        THEN 'Yes' ELSE 'No'
                    END AS user_onboarded
                FROM patient_base pb
            ),
            last_consultant_interaction AS (
                SELECT patient_id, (CURRENT_DATE - MAX(interaction_date)) AS days_since_last_interaction
                FROM (
                    SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
                    UNION ALL
                    SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED'
                    UNION ALL
                    SELECT c.patient_id, m."messagedAt"::date AS interaction_date
                    FROM "public"."chats" c
                    JOIN "public"."messages" m ON c.id = m.chat_id
                    WHERE m.sender = c.consultant_id
                ) all_interactions
                GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT') sub
                WHERE rn = 1
            ),
            weight_log_last_7_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_7_days
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days') sub
                WHERE rn = 1
            )
        SELECT
            os.user_onboarded,
            lci.days_since_last_interaction,
            COALESCE(mll3d.logged_in_last_3_days, 'No') AS last_3_days_meal_log,
            CASE
                WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
                WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
                ELSE
                    CASE
                        WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date))) THEN 'On Track'
                        ELSE 'Off Track'
                    END
            END AS on_track_status,
            COALESCE(wll7d.logged_in_last_7_days, 'No') AS logged_weight_last_7_days
        FROM patient_base pb
        LEFT JOIN onboarding_status os ON pb.id = os.patient_id
        LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
        LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
        LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
        LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
    ),

    -- 3. COACH + APP REPORT (YESTERDAY)
    coach_app_report_yesterday AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true 
                    AND p.status = 'ACTIVE_SUBSCRIPTION' 
                    AND p.subscription_end_date >= CURRENT_DATE 
                    AND s.type = 'Coach+App'
                    -- Changed 7-day filter to 0-day (ensure start date <= yesterday)
                    AND ( (CURRENT_DATE - INTERVAL '1 day')::date - p.subscription_start_date::date ) >= 0
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 22, 24)
            ),
            onboarding_status AS (
                SELECT pb.id AS patient_id,
                    CASE
                        WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                        AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT' AND m.date::date < CURRENT_DATE)
                        AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id AND pfl.date::date < CURRENT_DATE)
                        THEN 'Yes' ELSE 'No'
                    END AS user_onboarded
                FROM patient_base pb
            ),
            last_consultant_interaction AS (
                SELECT patient_id, ((CURRENT_DATE - INTERVAL '1 day')::date - MAX(interaction_date)) AS days_since_last_interaction
                FROM (
                    SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE date::date < CURRENT_DATE
                    UNION ALL
                    SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED' AND date::date < CURRENT_DATE
                    UNION ALL
                    SELECT c.patient_id, m."messagedAt"::date AS interaction_date
                    FROM "public"."chats" c
                    JOIN "public"."messages" m ON c.id = m.chat_id
                    WHERE m.sender = c.consultant_id AND m."messagedAt"::date < CURRENT_DATE
                ) all_interactions
                GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= (CURRENT_DATE - INTERVAL '1 day')::date - INTERVAL '3 days' AND date::date < CURRENT_DATE GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date < CURRENT_DATE) sub
                WHERE rn = 1
            ),
            weight_log_last_7_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_7_days
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= (CURRENT_DATE - INTERVAL '1 day')::date - INTERVAL '7 days' AND date::date < CURRENT_DATE) sub
                WHERE rn = 1
            )
        SELECT
            os.user_onboarded,
            lci.days_since_last_interaction,
            COALESCE(mll3d.logged_in_last_3_days, 'No') AS last_3_days_meal_log,
            CASE
                WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
                WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
                ELSE
                    CASE
                        WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, ( (CURRENT_DATE - INTERVAL '1 day')::date - pb.subscription_start_date::date ))) THEN 'On Track'
                        ELSE 'Off Track'
                    END
            END AS on_track_status,
            COALESCE(wll7d.logged_in_last_7_days, 'No') AS logged_weight_last_7_days
        FROM patient_base pb
        LEFT JOIN onboarding_status os ON pb.id = os.patient_id
        LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
        LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
        LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
        LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
    ),

    -- 4. GLP REPORT (YESTERDAY)
    glp_report_yesterday AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true 
                    AND p.status = 'ACTIVE_SUBSCRIPTION' 
                    AND p.subscription_end_date >= CURRENT_DATE 
                    AND s.type = 'GLP'
                    -- Changed 7-day filter to 0-day
                    AND ( (CURRENT_DATE - INTERVAL '1 day')::date - p.subscription_start_date::date ) >= 0
                    AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 22, 24)
            ),
            onboarding_status AS (
                SELECT pb.id AS patient_id,
                    CASE
                        WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                        AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT' AND m.date::date < CURRENT_DATE)
                        AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id AND pfl.date::date < CURRENT_DATE)
                        THEN 'Yes' ELSE 'No'
                    END AS user_onboarded
                FROM patient_base pb
            ),
            last_consultant_interaction AS (
                SELECT patient_id, ((CURRENT_DATE - INTERVAL '1 day')::date - MAX(interaction_date)) AS days_since_last_interaction
                FROM (
                    SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL AND date::date < CURRENT_DATE
                    UNION ALL
                    SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED' AND date::date < CURRENT_DATE
                    UNION ALL
                    SELECT c.patient_id, m."messagedAt"::date AS interaction_date
                    FROM "public"."chats" c
                    JOIN "public"."messages" m ON c.id = m.chat_id
                    WHERE m.sender = c.consultant_id AND m."messagedAt"::date < CURRENT_DATE
                ) all_interactions
                GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= (CURRENT_DATE - INTERVAL '1 day')::date - INTERVAL '3 days' AND date::date < CURRENT_DATE GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date < CURRENT_DATE) sub
                WHERE rn = 1
            ),
            weight_log_last_7_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_7_days
                FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= (CURRENT_DATE - INTERVAL '1 day')::date - INTERVAL '7 days' AND date::date < CURRENT_DATE) sub
                WHERE rn = 1
            )
        SELECT
            os.user_onboarded,
            lci.days_since_last_interaction,
            COALESCE(mll3d.logged_in_last_3_days, 'No') AS last_3_days_meal_log,
            CASE
                WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
                WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
                ELSE
                    CASE
                        WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, ( (CURRENT_DATE - INTERVAL '1 day')::date - pb.subscription_start_date::date ))) THEN 'On Track'
                        ELSE 'Off Track'
                    END
            END AS on_track_status,
            COALESCE(wll7d.logged_in_last_7_days, 'No') AS logged_weight_last_7_days
        FROM patient_base pb
        LEFT JOIN onboarding_status os ON pb.id = os.patient_id
        LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
        LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
        LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
        LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
    ),
    final_counts AS (
        SELECT
            1 AS sort_order,
            'Number of Paid Users' AS "Metric",
            (SELECT COUNT(*) FROM coach_app_report_today) AS coach_today,
            (SELECT COUNT(*) FROM coach_app_report_yesterday) AS coach_yesterday,
            (SELECT COUNT(*) FROM glp_report_today) AS glp_today,
            (SELECT COUNT(*) FROM glp_report_yesterday) AS glp_yesterday
        UNION ALL
        SELECT
            2 AS sort_order,
            'Num Completely not onboarded' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM glp_report_yesterday)
        UNION ALL
        SELECT
            3 AS sort_order,
            'Num On Track Users' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM glp_report_yesterday)
        UNION ALL
        SELECT
            4 AS sort_order,
            'Num Off Track Users' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM glp_report_yesterday)
        UNION ALL
        SELECT
            5 AS sort_order,
            'Num users with no interaction in last 2 days' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 2 OR days_since_last_interaction IS NULL) FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 2 OR days_since_last_interaction IS NULL) FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 2 OR days_since_last_interaction IS NULL) FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 2 OR days_since_last_interaction IS NULL) FROM glp_report_yesterday)
        UNION ALL
        SELECT
            6 AS sort_order,
            'Num users with no meal log in last 3 days' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM glp_report_yesterday)
        UNION ALL
        SELECT
            7 AS sort_order,
            'Num users with no weight log in last 7 days' AS "Metric",
            (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM coach_app_report_today),
            (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM coach_app_report_yesterday),
            (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM glp_report_today),
            (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM glp_report_yesterday)
    )
SELECT
    fc."Metric",
    (
        fc.coach_today::text ||
        ' (' ||
        CASE WHEN (fc.coach_today - fc.coach_yesterday) >= 0 THEN '+' ELSE '' END ||
        (fc.coach_today - fc.coach_yesterday)::text ||
        ')'
    ) AS "Coach + App (vs Yesterday)",
    
    (
        fc.glp_today::text ||
        ' (' ||
        CASE WHEN (fc.glp_today - fc.glp_yesterday) >= 0 THEN '+' ELSE '' END ||
        (fc.glp_today - fc.glp_yesterday)::text ||
        ')'
    ) AS "GLP (vs Yesterday)"
FROM
    final_counts fc
ORDER BY
    fc.sort_order;
    """),
    
    ("Coach +App User analytics", """
WITH
    patient_base AS (
        SELECT
            p.id, 
            p.firstname, 
            p.start_weight, 
            p.goal_weight, 
            p.target_duration, 
            p.subscription_start_date,
            s.name AS subscription_name,
            -- Added Consultant Details
            p.nutritionist_id AS consultant_id,
            c.name AS consultant_name
        FROM "public"."patients" AS p
        JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
        LEFT JOIN "public"."consultants" AS c ON p.nutritionist_id = c.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND p.subscription_end_date >= CURRENT_DATE
            AND s.type = 'Coach+App'
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 22, 24)
    ),
    onboarding_status AS (
        SELECT
            pb.id AS patient_id,
            CASE
                WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT')
                AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id)
                THEN 'Yes' ELSE 'No'
            END AS user_onboarded
        FROM patient_base pb
    ),
    last_consultant_interaction AS (
        SELECT patient_id, MAX(interaction_date) AS last_date
        FROM (
            SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
            UNION ALL
            SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED'
            UNION ALL
            SELECT c.patient_id, m."messagedAt"::date AS interaction_date 
            FROM "public"."chats" c
            JOIN "public"."messages" m ON c.id = m.chat_id
            WHERE m.sender = c.consultant_id
        ) all_interactions
        GROUP BY patient_id
    ),
    severe_side_effects AS (
        SELECT patient_id, STRING_AGG(type, ', ') AS side_effects
        FROM "public"."patientsideeffects"
        WHERE creator = 'PATIENT' 
          AND date >= (CURRENT_DATE - INTERVAL '7 days')
          AND LOWER(type) NOT IN ('good', 'okay', 'great')
        GROUP BY patient_id
    ),
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs"
        WHERE date::date >= CURRENT_DATE - INTERVAL '3 days'
        GROUP BY patient_id
    ),
    latest_weight AS (
        SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
        FROM (
            SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn
            FROM "public"."metrics" WHERE name = 'BODY_WEIGHT'
        ) sub
        WHERE rn = 1
    ),
    weight_log_last_7_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."metrics"
        WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY patient_id
    ),
    all_patient_interactions AS (
        SELECT patient_id, date::date AS interaction_date FROM "public"."activity"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."patientfoodlogs"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."metrics"
    ),
    activity_summary AS (
        SELECT
            patient_id,
            MAX(interaction_date) AS last_active_day,
            COUNT(DISTINCT CASE
                WHEN interaction_date >= CURRENT_DATE - INTERVAL '7 days' THEN interaction_date
                ELSE NULL
            END) AS active_days_last_7
        FROM all_patient_interactions
        GROUP BY patient_id
    )

SELECT
    TRIM(pb.firstname) AS "Patient Name",
    pb.consultant_id AS "Consultant ID",
    pb.consultant_name AS "Consultant Name",
    pb.subscription_name AS "Subscription Name",
    os.user_onboarded AS "User Onboarded",
    ROUND(pb.goal_weight::numeric, 2) AS "OnTrack/OffTrack",
    (CURRENT_DATE - lci.last_date) AS "Days Since Last Interaction",
    COALESCE(sse.side_effects, 'None') AS "Recent Severe Side Effects",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    COALESCE(act.active_days_last_7, 0) AS "Num Active days (in last 7 days)",
    act.last_active_day AS "Last Active Day"
FROM
    patient_base pb
LEFT JOIN onboarding_status os ON pb.id = os.patient_id
LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
LEFT JOIN severe_side_effects sse ON pb.id = sse.patient_id
LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN activity_summary act ON pb.id = act.patient_id;"""),
    
    ("GLP User analytics", """
WITH
    patient_base AS (
        SELECT
            p.id, 
            p.firstname, 
            p.start_weight, 
            p.goal_weight, 
            p.target_duration, 
            p.subscription_start_date,
            s.name AS subscription_name,
            -- Added Consultant Details
            p.nutritionist_id AS consultant_id,
            c.name AS consultant_name
        FROM "public"."patients" AS p
        JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
        LEFT JOIN "public"."consultants" AS c ON p.nutritionist_id = c.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND p.subscription_end_date >= CURRENT_DATE
            AND s.type = 'GLP'
            AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 22, 24)
    ),
    onboarding_status AS (
        SELECT
            pb.id AS patient_id,
            CASE
                WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL)
                AND EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT')
                AND EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id)
                THEN 'Yes' ELSE 'No'
            END AS user_onboarded
        FROM patient_base pb
    ),
    last_consultant_interaction AS (
        SELECT patient_id, MAX(interaction_date) AS last_date
        FROM (
            SELECT patient_id, date::date AS interaction_date FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
            UNION ALL
            SELECT patient_id, date::date AS interaction_date FROM "public"."appointment" WHERE status = 'COMPLETED'
            UNION ALL
            SELECT c.patient_id, m."messagedAt"::date AS interaction_date 
            FROM "public"."chats" c
            JOIN "public"."messages" m ON c.id = m.chat_id
            WHERE m.sender = c.consultant_id
        ) all_interactions
        GROUP BY patient_id
    ),
    severe_side_effects AS (
        SELECT patient_id, STRING_AGG(type, ', ') AS side_effects
        FROM "public"."patientsideeffects"
        WHERE creator = 'PATIENT' 
          AND date >= (CURRENT_DATE - INTERVAL '7 days')
          AND LOWER(type) NOT IN ('good', 'okay', 'great')
        GROUP BY patient_id
    ),
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs"
        WHERE date::date >= CURRENT_DATE - INTERVAL '3 days'
        GROUP BY patient_id
    ),
    latest_weight AS (
        SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
        FROM (
            SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn
            FROM "public"."metrics" WHERE name = 'BODY_WEIGHT'
        ) sub
        WHERE rn = 1
    ),
    weight_log_last_7_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."metrics"
        WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY patient_id
    ),
    all_patient_interactions AS (
        SELECT patient_id, date::date AS interaction_date FROM "public"."activity"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."patientfoodlogs"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."metrics"
    ),
    activity_summary AS (
        SELECT
            patient_id,
            MAX(interaction_date) AS last_active_day,
            COUNT(DISTINCT CASE
                WHEN interaction_date >= CURRENT_DATE - INTERVAL '7 days' THEN interaction_date
                ELSE NULL
            END) AS active_days_last_7
        FROM all_patient_interactions
        GROUP BY patient_id
    )

SELECT
    TRIM(pb.firstname) AS "Patient Name",
    pb.consultant_id AS "Consultant ID",
    pb.consultant_name AS "Consultant Name",
    pb.subscription_name AS "Subscription Name",
    os.user_onboarded AS "User Onboarded",
    ROUND(pb.goal_weight::numeric, 2) AS "OnTrack/OffTrack",
    (CURRENT_DATE - lci.last_date) AS "Days Since Last Interaction",
    COALESCE(sse.side_effects, 'None') AS "Recent Severe Side Effects",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    COALESCE(act.active_days_last_7, 0) AS "Num Active days (in last 7 days)",
    act.last_active_day AS "Last Active Day"
FROM
    patient_base pb
LEFT JOIN onboarding_status os ON pb.id = os.patient_id
LEFT JOIN last_consultant_interaction lci ON pb.id = lci.patient_id
LEFT JOIN severe_side_effects sse ON pb.id = sse.patient_id
LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN activity_summary act ON pb.id = act.patient_id;"""),
    
    ("Full Analytics", """
WITH
    coach_app_base AS (
        SELECT
            p.id, p.firstname, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date,
            s.type AS subscription_type
        FROM "public"."patients" AS p
        JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND p.subscription_end_date >= CURRENT_DATE
            AND s.type = 'Coach+App'
            -- Removed 7-day filter logic
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 22, 24)
    ),
    glp_base AS (
        SELECT
            p.id, p.firstname, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date,
            s.type AS subscription_type
        FROM "public"."patients" AS p
        JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND p.subscription_end_date >= CURRENT_DATE
            AND s.type = 'GLP'
            AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
            -- Removed 7-day filter logic
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 22, 24)
    ),
    -- CTE 3: patient_base (UNION)
    patient_base AS (
        SELECT * FROM coach_app_base
        UNION ALL
        SELECT * FROM glp_base
    ),
    -- CTE 4: onboarding_metrics
    onboarding_metrics AS (
        SELECT
            pb.id AS patient_id,
            CASE WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL) THEN 'Yes' ELSE 'No' END AS "Goals Set",
            CASE WHEN EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT') THEN 'Yes' ELSE 'No' END AS "Smart Scale Logged",
            CASE WHEN EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id) THEN 'Yes' ELSE 'No' END AS "Meal Logged"
        FROM patient_base pb
    ),
    -- CTE 5: last_consultant_note_details
    last_consultant_note_details AS (
        SELECT patient_id, last_note_date, last_note_description
        FROM (
            SELECT patient_id, date AS last_note_date, description AS last_note_description, ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY date DESC) as rn
            FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
        ) ranked_notes
        WHERE rn = 1
    ),
    -- CTE 6: last_completed_appointment
    last_completed_appointment AS (
        SELECT patient_id, MAX(date::date) AS last_appt_date
        FROM "public"."appointment"
        WHERE status = 'COMPLETED'
        GROUP BY patient_id
    ),
    -- CTE 7: last_consultant_message (NEW)
    last_consultant_message AS (
        SELECT c.patient_id, MAX(m."messagedAt") AS last_message_date
        FROM "public"."chats" c
        JOIN "public"."messages" m ON c.id = m.chat_id
        WHERE m.sender = c.consultant_id
        GROUP BY c.patient_id
    ),
    -- CTE 8: meal_log_last_3_days
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
    ),
    -- CTE 9: latest_weight
    latest_weight AS (
        SELECT patient_id, ROUND(value::numeric, 2) AS current_weight
        FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT') sub
        WHERE rn = 1
    ),
    -- CTE 10: weight_log_last_7_days
    weight_log_last_7_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days' GROUP BY patient_id
    ),
    -- CTE 11: all_patient_interactions
    all_patient_interactions AS (
        SELECT patient_id, date::date AS interaction_date FROM "public"."activity"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."patientfoodlogs"
        UNION
        SELECT patient_id, date::date AS interaction_date FROM "public"."metrics"
    ),
    -- CTE 12: activity_summary
    activity_summary AS (
        SELECT
            patient_id, MAX(interaction_date) AS last_active_day,
            COUNT(DISTINCT CASE WHEN interaction_date >= CURRENT_DATE - INTERVAL '7 days' THEN interaction_date ELSE NULL END) AS active_days_last_7
        FROM all_patient_interactions GROUP BY patient_id
    ),
    -- CTE 13: last_meal_log_date
    last_meal_log_date AS (
        SELECT patient_id, MAX(date::date) AS last_meal_log_date
        FROM "public"."patientfoodlogs" GROUP BY patient_id
    ),
    -- CTE 14: last_weight_log_date
    last_weight_log_date AS (
        SELECT patient_id, MAX(date::date) AS last_weight_log_date
        FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' GROUP BY patient_id
    )

-- FINAL SELECT
SELECT
    TRIM(pb.firstname) AS "Patient Name",
    pb.subscription_type AS "Subscription Type",

    om."Goals Set",
    om."Smart Scale Logged",
    om."Meal Logged",
    CASE WHEN om."Goals Set" = 'Yes' AND om."Smart Scale Logged" = 'Yes' AND om."Meal Logged" = 'Yes' THEN 'Yes' ELSE 'No' END AS "User Onboarded",

    pb.subscription_start_date AS "Subscription Start Date",
    (CURRENT_DATE - pb.subscription_start_date::date) AS "Days since Subscription Start",
    ROUND(pb.start_weight::numeric, 2) AS "Start Weight",
    
    -- REPLACED: "On/Off Track" with "Goal Weight"
    ROUND(pb.goal_weight::numeric, 2) AS "Goal Weight",

    lw.current_weight AS "Current Weight",
    ROUND((((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration, 0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date)))::numeric, 2) AS "On Track Weight Lose",
    ROUND((pb.start_weight - lw.current_weight)::numeric, 2) AS "Current Weight Lose",
    
    -- Removed "On/Off Track" status column

    -- Consultant Interaction Metrics
    lcn.last_note_date AS "Last Note Added Date",
    lcn.last_note_description AS "Last Note",
    -- UPDATED COLUMN: Days Since Last Interaction (Includes Messages)
    (CURRENT_DATE - GREATEST(lcn.last_note_date::date, lca.last_appt_date, lcm.last_message_date::date)) AS "Days Since Last Interaction",

    lml.last_meal_log_date AS "Last Meal Log Date",
    lwl.last_weight_log_date AS "Last Weight Log Date",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    COALESCE(act.active_days_last_7, 0) AS "Num Active days (in last 7 days)",
    act.last_active_day AS "Last Active Day"

FROM patient_base pb
LEFT JOIN onboarding_metrics om ON pb.id = om.patient_id
LEFT JOIN last_consultant_note_details lcn ON pb.id = lcn.patient_id
LEFT JOIN last_completed_appointment lca ON pb.id = lca.patient_id
LEFT JOIN last_consultant_message lcm ON pb.id = lcm.patient_id -- JOIN NEW CTE
LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN activity_summary act ON pb.id = act.patient_id
LEFT JOIN last_meal_log_date lml ON pb.id = lml.patient_id
LEFT JOIN last_weight_log_date lwl ON pb.id = lwl.patient_id

ORDER BY "Subscription Type", "Patient Name";""")
]

# Email sending toggle
ENABLE_EMAIL_SENDING = os.getenv("ENABLE_EMAIL_SENDING", "true").strip().lower() == "true"

# Google Sheets Configuration
GOOGLE_SHEETS_CONFIG = {
    "SERVICE_ACCOUNT_FILE": "landingpageconnections-e3325175d396.json",
    "SPREADSHEET_ID": os.getenv(
        "EARLYFIT_ANALYTICS_SHEET_ID",
        "12wZ7Y1qOSAG4-xV8QEGUSDYqMWxuV3-kMDM_Mj_Lbu0"
    ),
    "SHEET_NAME": "Daily analytics sheet ",
    "SCOPES": ["https://www.googleapis.com/auth/spreadsheets.readonly"]
}


def get_google_sheets_service():
    """Authenticate and return a Google Sheets service client."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG["SERVICE_ACCOUNT_FILE"],
            scopes=GOOGLE_SHEETS_CONFIG["SCOPES"]
        )
        service = build("sheets", "v4", credentials=creds)
        return service
    except FileNotFoundError:
        print(f"    [ERROR] Service account file '{GOOGLE_SHEETS_CONFIG['SERVICE_ACCOUNT_FILE']}' not found.")
    except Exception as exc:
        print(f"    [ERROR] Failed to authenticate with Google Sheets: {exc}")
    return None


def fetch_google_sheet_data() -> List[Dict[str, Any]]:
    """
    Fetch data from the configured Google Sheet and return as list of dicts.
    The first row is treated as headers.
    """
    service = get_google_sheets_service()
    if not service:
        return []

    try:
        sheet = service.spreadsheets()
        sheet_name = GOOGLE_SHEETS_CONFIG["SHEET_NAME"]
        range_name = f"'{sheet_name}'!A1:ZZ" if " " in sheet_name else f"{sheet_name}!A1:ZZ"
        print(f"    [DEBUG] Fetching Google Sheet range: {range_name}")
        result = sheet.values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
            range=range_name
        ).execute()
        values = result.get("values", [])

        if not values:
            print("    [INFO] Google Sheet is empty.")
            return []

        headers = values[0]
        records: List[Dict[str, Any]] = []

        for row in values[1:]:
            record = {}
            for idx, header in enumerate(headers):
                record[header] = row[idx] if idx < len(row) else ""
            records.append(record)

        return records

    except HttpError as http_err:
        print(f"    [ERROR] Google Sheets API error: {http_err}")
        try:
            metadata = service.spreadsheets().get(
                spreadsheetId=GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
                fields="sheets(properties(title))"
            ).execute()
            available_titles = [
                sheet["properties"]["title"]
                for sheet in metadata.get("sheets", [])
            ]
            print("    [INFO] Available sheet titles:", available_titles)
        except Exception as meta_exc:
            print(f"    [ERROR] Unable to fetch sheet metadata: {meta_exc}")
    except Exception as exc:
        print(f"    [ERROR] Failed to fetch Google Sheet data: {exc}")

    return []


def _normalize_name(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def _build_sheet_lookup(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in records:
        name = (
            row.get("Patient Name")
            or row.get("patient name")
            or row.get("Name")
            or row.get("name")
        )
        normalized_name = _normalize_name(name)
        if not normalized_name:
            continue
        lookup[normalized_name] = {
            "__original__": row,
            "__lower__": {str(k).strip().lower(): v for k, v in row.items()}
        }
    return lookup


def _get_sheet_value(entry: Optional[Dict[str, Any]], column_name: str) -> Optional[str]:
    if not entry or "__lower__" not in entry:
        return None
    return entry["__lower__"].get(column_name.strip().lower())


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def parse_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    value_str = str(value).strip()
    if not value_str:
        return None
    date_formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%Y/%m/%d",
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(value_str, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value_str)
    except ValueError:
        return None


def build_full_analytics_lookup(tables_data: List[tuple]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for heading, rows in tables_data:
        if heading != "Full Analytics":
            continue
        for row in rows:
            normalized = _normalize_name(row.get("Patient Name"))
            if not normalized:
                continue
            lookup[normalized] = row
    return lookup


def annotate_tables_with_sheet_data(
    tables_data: List[tuple],
    sheet_lookup: Dict[str, Dict[str, Any]],
    analytics_lookup: Dict[str, Dict[str, Any]]
) -> None:
    if not tables_data:
        return

    for heading, rows in tables_data:
        if not rows:
            continue

        is_coach = heading == "Coach +App User analytics"
        is_glp = heading == "GLP User analytics"
        if not (is_coach or is_glp):
            continue

        status_column = "OnTrack/OffTrack"
        for row in rows:
            normalized_name = _normalize_name(row.get("Patient Name"))
            sheet_entry = sheet_lookup.get(normalized_name) if sheet_lookup else None
            analytics_entry = analytics_lookup.get(normalized_name) if analytics_lookup else None

            # Update User Onboarded cell with reason
            reason = _get_sheet_value(sheet_entry, "Not onboarding reason")
            if reason and str(row.get("User Onboarded", "")).strip().lower() == "no":
                row["User Onboarded"] = reason
                cell_classes = row.setdefault("__cell_classes__", {})
                cell_classes["User Onboarded"] = "cell-blue cell-blue-text"

            # Compute on/off track replacement for Goal Weight column
            row[status_column] = compute_progress_status(
                sheet_entry=sheet_entry,
                analytics_entry=analytics_entry
            )

            # Replace Meal Log (3 days) with sheet reason when needed
            update_logging_reason(
                row=row,
                column_name="Meal Log (3 days)",
                sheet_entry=sheet_entry,
                sheet_column="Meal logging reason"
            )

            # Replace Weight Log (7 days) with sheet reason when needed
            update_logging_reason(
                row=row,
                column_name="Weight Log (7 days)",
                sheet_entry=sheet_entry,
                sheet_column="Weight logging reason"
            )


def update_summary_with_detailed_track_counts(tables_data: List[tuple]) -> None:
    """
    Update Query 1 (Summary Comparison) with On/Off Track counts from Query 2 and Query 3.
    Treats "Data Incomplete" as "Off Track".
    """
    # Find Query 1 (Summary Comparison) and Query 2/3 data
    summary_data = None
    coach_app_data = None
    glp_data = None
    
    for heading, data in tables_data:
        if heading == "Summary Comparison":
            summary_data = data
        elif heading == "Coach +App User analytics":
            coach_app_data = data
        elif heading == "GLP User analytics":
            glp_data = data
    
    if not summary_data or not coach_app_data or not glp_data:
        print("    [WARNING] Could not find all required queries for track count update")
        return
    
    # Count On Track and Off Track (treating Data Incomplete as Off Track) for Coach+App
    coach_on_track = 0
    coach_off_track = 0
    for row in coach_app_data:
        status = str(row.get("OnTrack/OffTrack", "")).strip()
        if status == "On Track":
            coach_on_track += 1
        else:  # "Off Track", "Data Incomplete", or any other value
            coach_off_track += 1
    
    # Count On Track and Off Track (treating Data Incomplete as Off Track) for GLP
    glp_on_track = 0
    glp_off_track = 0
    for row in glp_data:
        status = str(row.get("OnTrack/OffTrack", "")).strip()
        if status == "On Track":
            glp_on_track += 1
        else:  # "Off Track", "Data Incomplete", or any other value
            glp_off_track += 1
    
    # Update Query 1 summary table
    # Find rows with "Num On Track Users" and "Num Off Track Users"
    for row in summary_data:
        metric = row.get("Metric", "")
        if metric == "Num On Track Users":
            # Update with counts only (no yesterday comparison)
            row["Coach + App (vs Yesterday)"] = str(coach_on_track)
            row["GLP (vs Yesterday)"] = str(glp_on_track)
                
        elif metric == "Num Off Track Users":
            # Update with counts only (no yesterday comparison)
            row["Coach + App (vs Yesterday)"] = str(coach_off_track)
            row["GLP (vs Yesterday)"] = str(glp_off_track)
    
    print(f"    [OK] Updated Summary Comparison with track counts: Coach+App (On: {coach_on_track}, Off: {coach_off_track}), GLP (On: {glp_on_track}, Off: {glp_off_track})")


def compute_progress_status(
    sheet_entry: Optional[Dict[str, Any]],
    analytics_entry: Optional[Dict[str, Any]]
) -> str:
    if not analytics_entry:
        return "Data Incomplete"

    start_weight = parse_float(analytics_entry.get("Start Weight"))
    goal_weight = parse_float(analytics_entry.get("Goal Weight"))
    current_weight = parse_float(analytics_entry.get("Current Weight"))
    if start_weight is None or goal_weight is None:
        return "Data Incomplete"

    total_target_loss = start_weight - goal_weight
    if total_target_loss <= 0:
        return "Data Incomplete"

    glp_dose_value = _get_sheet_value(sheet_entry, "Glp 1st dose") if sheet_entry else None
    glp_dose_date = parse_date(glp_dose_value) if glp_dose_value else None
    if not glp_dose_date:
        return "Data Incomplete"

    days_since = max((datetime.now().date() - glp_dose_date.date()).days, 0)

    daily_target_loss = total_target_loss / 90.0
    ideal_loss = daily_target_loss * days_since

    current_loss = None
    if current_weight is not None:
        current_loss = start_weight - current_weight
    if current_loss is None:
        current_loss = parse_float(analytics_entry.get("Current Weight Lose"))
    if current_loss is None:
        return "Data Incomplete"

    return "On Track" if current_loss >= ideal_loss else "Off Track"


def update_logging_reason(
    row: Dict[str, Any],
    column_name: str,
    sheet_entry: Optional[Dict[str, Any]],
    sheet_column: str
) -> None:
    if not sheet_entry:
        return

    current_value = str(row.get(column_name, "")).strip().lower()
    if current_value != "no":
        return

    reason = _get_sheet_value(sheet_entry, sheet_column)
    if not reason:
        return

    row[column_name] = reason
    cell_classes = row.setdefault("__cell_classes__", {})
    cell_classes[column_name] = "cell-blue cell-blue-text"

# Email Configuration
EMAIL_CONFIG = {
    # SMTP Server Settings
    'smtp_host': os.getenv("SMTP_HOST", "smtp.gmail.com"),
    'smtp_port': int(os.getenv("SMTP_PORT", "587")),
    'smtp_username': os.getenv("SMTP_USERNAME"),
    'smtp_password': os.getenv("SMTP_PASSWORD"),
    'from_email': os.getenv("FROM_EMAIL"),
    'from_name': 'EarlyFit User Analytics',
    
    # Email Content
    'subject': f'Quick Summary and analytics for current users - {datetime.now().strftime("%Y-%m-%d")}',
    'title': 'EarlyFit User Analytics',
    'greeting': 'Dear Team,<br><br>Please find today\'s data report below.',
    'closing': 'Regards,<br><br>EarlyFit Product Team'
}

# Recipients List
RECIPIENTS = [
    'patient_ops@early.fit', 
    
]

# ============================================================================
# TABLE UTILITIES
# ============================================================================

def print_table_preview(data: List[Dict[Any, Any]]):
    """Print the complete table data to console"""
    if not data:
        print("No data to display")
        return
    
    columns = [col for col in data[0].keys() if not str(col).startswith("__")]
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


def generate_email_table(data: List[Dict[Any, Any]], title: str = None, conditional_formatting: bool = True, exclude_columns: List[str] = None) -> str:
    """Generate an email-compatible HTML table from JSON data using CSS classes"""
    if not data:
        return '<p class="no-data">No data available</p>'
    
    # Filter out metadata columns that shouldn't be displayed
    all_columns = list(data[0].keys())
    columns = [col for col in all_columns if not col.startswith("__") and col != "_cell_classes__"]
    
    # Filter out excluded columns if specified
    if exclude_columns:
        columns = [col for col in columns if col not in exclude_columns]
    
    def get_cell_class_and_style(column_name: str, value: Any) -> tuple:
        """Determine cell class and optional inline style based on value"""
        if not conditional_formatting:
            return "", ""
        
        value_str = str(value).strip() if value is not None else ""
        
        # User Onboarded = "No" → Bright Red
        if column_name == "User Onboarded" and value_str.lower() == "no":
            return "bg-red", ""
        
        # Goals Set, Smart Scale Logged, Meal Logged = "No" → Bright Yellow
        if column_name in ["Goals Set", "Smart Scale Logged", "Meal Logged"]:
            if value_str.lower() == "no":
                return "bg-yellow", ""
        
        # Interaction (5 Days), Meal Log (3 days), Weight Log (7 days) = "No" → Bright Yellow
        if column_name in ["Interaction (5 Days)", "Meal Log (3 days)", "Weight Log (7 days)"]:
            if value_str.lower() == "no":
                return "bg-yellow", ""
        
        # Days Since Last Interaction formatting
        if column_name == "Days Since Last Interaction":
            try:
                days = float(value_str)
                if days >= 5:
                    return "bg-red", ""
                elif 2 <= days < 5:
                    return "bg-yellow", ""
            except (ValueError, TypeError):
                pass

        # On/Off Track contains "off track" → Bright Orange
        if column_name == "On/Off Track" and "off track" in value_str.lower():
            return "bg-orange", ""
        
        # Current Weight Lose is negative → Bright Red
        if column_name == "Current Weight Lose":
            try:
                numeric_value = float(value_str)
                if numeric_value < 0:
                    return "bg-red", ""
            except (ValueError, TypeError):
                pass
        
        return "", ""
    
    html = []
    
    if title:
        html.append(f'''
        <div class="table-title">
            <h2>{title}</h2>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Total Records: {len(data)}</p>
        </div>
        ''')
    
    html.append('<table class="data-table">')
    
    html.append('<thead>')
    html.append('<tr>')
    for col in columns:
        html.append(f'<th>{col}</th>')
    html.append('</tr>')
    html.append('</thead>')
    
    html.append('<tbody>')
    for idx, row in enumerate(data):
        cell_classes_map = row.get("__cell_classes__", {})
        base_class = 'even' if idx % 2 == 0 else 'odd'
        custom_class = row.get("__row_class__")
        row_class = f"{base_class} {custom_class}" if custom_class else base_class
        html.append(f'<tr class="{row_class.strip()}">')
        
        for col in columns:
            value = row.get(col, "")
            
            if value is None:
                value = ""
            elif isinstance(value, (dict, list)):
                value = json.dumps(value)
            else:
                value = str(value)
            
            value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
            cell_class, cell_style = get_cell_class_and_style(col, row.get(col, ""))
            extra_cell_class = cell_classes_map.get(col)
            combined_class = " ".join(
                cls for cls in [cell_class, extra_cell_class] if cls
            )
            
            class_attr = f' class="{combined_class}"' if combined_class else ''
            style_attr = f' style="{cell_style}"' if cell_style else ''
            
            html.append(f'<td{class_attr}{style_attr}>{value}</td>')
        
        html.append('</tr>')
    
    html.append('</tbody>')
    html.append('</table>')
    
    return '\n'.join(html)


def generate_multiple_tables_email(tables: List[tuple], title: str = "Data Report", 
                                   greeting: str = None, closing: str = None) -> str:
    """Generate a complete email body with multiple tables using CSS styles"""
    html_parts = []
    
    # Define CSS styles
    css_styles = '''
    <style>
        body { margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f5f5f5; }
        .container { max-width: 100%; margin: 0 auto; padding: 20px; background-color: #ffffff; }
        h1 { color: #333; font-size: 24px; margin: 0; font-weight: bold; }
        h2 { color: #4CAF50; font-size: 18px; margin: 0 0 10px 0; font-weight: bold; border-bottom: 1px solid #ddd; padding-bottom: 5px; }
        p { color: #333; font-size: 14px; line-height: 1.6; }
        .header { border-bottom: 2px solid #4CAF50; padding-bottom: 15px; margin-bottom: 20px; }
        .table-section { margin-top: 30px; margin-bottom: 15px; }
        .table-section:first-child { margin-top: 0; }
        
        /* Table Styles */
        .data-table { border-collapse: collapse; width: 100%; font-size: 12px; border: 1px solid #ddd; }
        .data-table th { padding: 8px; text-align: left; background-color: #4CAF50; color: white; font-weight: bold; border: 1px solid #45a049; white-space: nowrap; }
        .data-table td { padding: 8px; border: 1px solid #ddd; color: #333; }
        .data-table tr.even { background-color: #f9f9f9; }
        .data-table tr.odd { background-color: #ffffff; }
        
        /* Conditional Formatting Classes */
        .bg-red { background-color: #ff6666 !important; }
        .bg-yellow { background-color: #ffff00 !important; }
        .bg-orange { background-color: #ff9900 !important; }
        .row-blue { background-color: #003d73 !important; color: #ffffff !important; }
        .cell-blue { background-color: #003d73 !important; color: #ffffff !important; }
        
        .footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #666; font-size: 11px; }
        .no-data { color: #666; font-style: italic; }
    </style>
    '''
    
    html_parts.append(f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {css_styles}
    </head>
    <body>
        <div class="container">
    ''')
    
    html_parts.append(f'''
            <div class="header">
                <h1>{title}</h1>
            </div>
    ''')
    
    if greeting:
        html_parts.append(f'''
            <p style="margin-bottom: 20px;">
                {greeting}
            </p>
        ''')
    
    for idx, (heading, data) in enumerate(tables):
        if data and len(data) > 0:
            html_parts.append(f'''
            <div class="table-section">
                <h2>{heading}</h2>
            </div>
            ''')
            
            use_formatting = heading in ["Coach +App User analytics", "GLP User analytics", "Full Analytics"]
            # Exclude consultant columns and subscription name from Coach +App and GLP tables
            exclude_cols = ["Consultant ID", "Consultant Name", "Subscription Name"] if heading in ["Coach +App User analytics", "GLP User analytics"] else None
            print(f"    DEBUG: Generating HTML for table '{heading}' with {len(data)} rows")
            table_html = generate_email_table(data, title=None, conditional_formatting=use_formatting, exclude_columns=exclude_cols)
            html_parts.append(table_html)
    
    if closing:
        html_parts.append(f'''
            <p style="margin-top: 30px;">
                {closing}
            </p>
        ''')
    
    html_parts.append(f'''
            <div class="footer">
                <p>
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

    # Fetch Google Sheet data for future formatting workflows
    print("[0/4] Fetching Google Sheet data for formatting reference...")
    google_sheet_records = fetch_google_sheet_data()
    sheet_lookup = _build_sheet_lookup(google_sheet_records)
    google_sheet_data_json = json.dumps(
        google_sheet_records,
        ensure_ascii=False,
        indent=2
    )
    if google_sheet_records:
        print(f"    [OK] Retrieved {len(google_sheet_records)} row(s) from '{GOOGLE_SHEETS_CONFIG['SHEET_NAME']}'")
    else:
        print("    [WARNING] No data retrieved from the Google Sheet.")
    print("    Google Sheet data (JSON):")
    print(google_sheet_data_json)
    
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
    
    # Process Google Sheet data to create a lookup map
    # Map: Patient Name (lowercase, stripped) -> { "Not onboarding reason": ..., "1st GLP Dose": ... }
    sheet_data_map = {}
    if google_sheet_records:
        print(f"\n    Processing {len(google_sheet_records)} Google Sheet records for data mapping...")
        for record in google_sheet_records:
            # Assuming 'Name' is the column for patient name in the sheet
            # Adjust column name if different in your sheet
            raw_name = record.get("Name", "") or record.get("Patient Name", "")
            if raw_name:
                key = str(raw_name).strip().lower()
                sheet_data_map[key] = {
                    "Not onboarding reason": record.get("Not onboarding reason", ""),
                    "1st GLP Dose": record.get("1st GLP Dose", "")
                }
        print(f"        [OK] Mapped {len(sheet_data_map)} patients from sheet")

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

    analytics_lookup = build_full_analytics_lookup(tables_data)
    annotate_tables_with_sheet_data(tables_data, sheet_lookup, analytics_lookup)
    
    # Update Query 1 (Summary Comparison) with On/Off Track counts from Query 2 and Query 3
    update_summary_with_detailed_track_counts(tables_data)
    
    # Generate "Actions Required" tables grouped by consultant from Query 2 and Query 3 data
    coach_app_data = next((data for heading, data in tables_data if heading == "Coach +App User analytics"), [])
    glp_data = next((data for heading, data in tables_data if heading == "GLP User analytics"), [])
    
    # Combine data from both queries
    all_patients_data = []
    if coach_app_data:
        all_patients_data.extend(coach_app_data)
    if glp_data:
        all_patients_data.extend(glp_data)
    
    if all_patients_data:
        print(f"\n    Generating 'Actions Required' tables by consultant from {len(all_patients_data)} patient records...")
        
        # Group patients by consultant
        consultant_actions = {}  # consultant_name -> {action_type -> [patient_names]}
        
        for row in all_patients_data:
            patient_name = row.get("Patient Name", "Unknown")
            consultant_name = row.get("Consultant Name", "Unknown Consultant")
            
            # Initialize consultant's action map if not exists
            if consultant_name not in consultant_actions:
                consultant_actions[consultant_name] = {
                    "User Not Onboarded": [],
                    "Meal Logging": [],
                    "Weight Logging": [],
                    "No Interaction (2+ days)": []
                }
            
            # Check User Onboarded
            if row.get("User Onboarded", "").strip().lower() == 'no':
                consultant_actions[consultant_name]["User Not Onboarded"].append(patient_name)
            
            # Check Meal Log (3 days)
            if row.get("Meal Log (3 days)", "").strip().lower() == 'no':
                consultant_actions[consultant_name]["Meal Logging"].append(patient_name)
            
            # Check Weight Log (7 days)
            if row.get("Weight Log (7 days)", "").strip().lower() == 'no':
                consultant_actions[consultant_name]["Weight Logging"].append(patient_name)
            
            # Check Days Since Last Interaction
            days_since_interaction = row.get("Days Since Last Interaction")
            if days_since_interaction is not None:
                try:
                    days = float(days_since_interaction)
                    if days >= 2:
                        days_int = int(days)
                        consultant_actions[consultant_name]["No Interaction (2+ days)"].append(f"{patient_name} ({days_int} days)")
                except (ValueError, TypeError):
                    pass
        
        # Create action tables for each consultant
        insert_index = 1
        for i, (heading, _) in enumerate(tables_data):
            if heading == "Summary Comparison":
                insert_index = i + 1
                break
        
        tables_added = 0
        for consultant_name, actions_map in sorted(consultant_actions.items()):
            # Convert map to list of dicts for table generation
            actions_data = []
            for action, patients in actions_map.items():
                if patients:
                    actions_data.append({
                        "Action": action,
                        "Patients Pending": ", ".join(sorted(patients))
                    })
            
            if actions_data:
                table_name = f"Actions Required for {consultant_name}"
                tables_data.insert(insert_index + tables_added, (table_name, actions_data))
                tables_added += 1
                print(f"        [OK] Generated '{table_name}' with {len(actions_data)} action items")
        
        if tables_added == 0:
            print("        [INFO] No actions required found for any consultant")
    
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
        print(f"    DEBUG: Total email HTML length: {len(email_html)} characters")
        
    except Exception as e:
        print(f"    [ERROR] Failed to generate email HTML: {e}")
        return False
    
    if not ENABLE_EMAIL_SENDING:
        print("\n[INFO] Email sending is currently disabled (ENABLE_EMAIL_SENDING=false).")
        print("       Skipping SMTP preparation and send steps.")
        return True
    
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
    elif RECIPIENTS[0] == 'recipient1@example.com':
        errors.append("  - Update RECIPIENTS list with actual email addresses")
    
    # Validate SQL Queries
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
        print("Please update the following settings in .env file:\n")
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

