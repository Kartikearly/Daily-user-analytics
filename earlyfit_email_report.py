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
    ("Summary Comparison", """
WITH
    coach_app_report AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true AND p.status = 'ACTIVE_SUBSCRIPTION' AND s.type = 'Coach+App'
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 18)
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
                SELECT patient_id, (CURRENT_DATE - MAX(date)::date) AS days_since_last_interaction
                FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, value AS current_weight
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
    glp_report AS (
        WITH
            patient_base AS (
                SELECT p.id, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
                FROM "public"."patients" AS p
                JOIN "public"."subscriptions" AS s ON p.active_subscription_id = s.id
                WHERE
                    p."isActive" = true AND p.status = 'ACTIVE_SUBSCRIPTION' AND s.type = 'GLP'
                    AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
                    AND LOWER(TRIM(p.firstname)) NOT IN (
                        'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                        'parushi', 'bhaumik', 'archana', 'mrinal'
                    )
                    AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
                    AND p.nutritionist_id IN (8, 18)
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
                SELECT patient_id, (CURRENT_DATE - MAX(date)::date) AS days_since_last_interaction
                FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL GROUP BY patient_id
            ),
            meal_log_last_3_days AS (
                SELECT patient_id, 'Yes' AS logged_in_last_3_days
                FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
            ),
            latest_weight AS (
                SELECT patient_id, value AS current_weight
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
    )
SELECT
    'Number of Paid Users' AS "Metric",
    (SELECT COUNT(*) FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num Completely not onboarded' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE user_onboarded = 'No') FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num On Track Users' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE on_track_status = 'On Track') FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num Off Track Users' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE on_track_status = 'Off Track') FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num users with no interaction in last 5 days' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 5) FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE days_since_last_interaction > 5) FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num users with no meal log in last 3 days' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE last_3_days_meal_log = 'No') FROM glp_report) AS "GLP"
UNION ALL
SELECT
    'Num users with no weight log in last 7 days' AS "Metric",
    (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM coach_app_report) AS "Coach + App",
    (SELECT COUNT(*) FILTER (WHERE logged_weight_last_7_days = 'No') FROM glp_report) AS "GLP";
    """),
    
    ("Coach +App User analytics", """
WITH
    patient_base AS (
        SELECT
            p.id, p.firstname, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
        FROM
            "public"."patients" AS p
        JOIN
            "public"."subscriptions" AS s
            ON p.active_subscription_id = s.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND s.type = 'Coach+App'
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 18)
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
        SELECT patient_id, MAX(date) AS last_note_date
        FROM "public"."patientnotes"
        WHERE consultant_id IS NOT NULL
        GROUP BY patient_id
    ),
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs"
        WHERE date::date >= CURRENT_DATE - INTERVAL '3 days'
        GROUP BY patient_id
    ),
    latest_weight AS (
        SELECT patient_id, value AS current_weight
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
    os.user_onboarded AS "User Onboarded",
    CASE
        WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
        WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
        ELSE
            CASE
                WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date))) THEN 'On Track'
                ELSE 'Off Track'
            END
    END AS "On/Off Track",
    CASE
        WHEN (CURRENT_DATE - lci.last_note_date::date) <= 5 THEN 'Yes'
        ELSE 'No'
    END AS "Interaction (5 Days)",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    act.last_active_day AS "Last Active Day"
FROM
    patient_base pb
LEFT JOIN
    onboarding_status os ON pb.id = os.patient_id
LEFT JOIN
    last_consultant_interaction lci ON pb.id = lci.patient_id
LEFT JOIN
    meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN
    latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN
    weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN
    activity_summary act ON pb.id = act.patient_id;
    """),
    
    ("GLP User analytics", """
WITH
    patient_base AS (
        SELECT
            p.id, p.firstname, p.start_weight, p.goal_weight, p.target_duration, p.subscription_start_date
        FROM
            "public"."patients" AS p
        JOIN
            "public"."subscriptions" AS s
            ON p.active_subscription_id = s.id
        WHERE
            p."isActive" = true
            AND p.status = 'ACTIVE_SUBSCRIPTION'
            AND s.type = 'GLP'
            AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 18)
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
        SELECT patient_id, MAX(date) AS last_note_date
        FROM "public"."patientnotes"
        WHERE consultant_id IS NOT NULL
        GROUP BY patient_id
    ),
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs"
        WHERE date::date >= CURRENT_DATE - INTERVAL '3 days'
        GROUP BY patient_id
    ),
    latest_weight AS (
        SELECT patient_id, value AS current_weight
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
    os.user_onboarded AS "User Onboarded",
    CASE
        WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
        WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
        ELSE
            CASE
                WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration,0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date))) THEN 'On Track'
                ELSE 'Off Track'
            END
    END AS "On/Off Track",
    CASE
        WHEN (CURRENT_DATE - lci.last_note_date::date) <= 5 THEN 'Yes'
        ELSE 'No'
    END AS "Interaction (5 Days)",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    act.last_active_day AS "Last Active Day"
FROM
    patient_base pb
LEFT JOIN
    onboarding_status os ON pb.id = os.patient_id
LEFT JOIN
    last_consultant_interaction lci ON pb.id = lci.patient_id
LEFT JOIN
    meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN
    latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN
    weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN
    activity_summary act ON pb.id = act.patient_id;
    """),
    
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
            AND s.type = 'Coach+App'
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 18)
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
            AND s.type = 'GLP'
            AND LOWER(s.name) NOT IN ('metabolic diagnosis test', 'metabolic screening', 'pre glp assesment')
            AND LOWER(TRIM(p.firstname)) NOT IN (
                'vandana', 'shalini', 'anushree', 'anita', 'nutan', 'dr. geeta', 'manish',
                'parushi', 'bhaumik', 'archana', 'mrinal'
            )
            AND p.firstname IS NOT NULL AND TRIM(p.firstname) <> ''
            AND p.nutritionist_id IN (8, 18)
    ),
    patient_base AS (
        SELECT * FROM coach_app_base
        UNION ALL
        SELECT * FROM glp_base
    ),
    onboarding_metrics AS (
        SELECT
            pb.id AS patient_id,
            CASE WHEN (pb.start_weight IS NOT NULL AND pb.goal_weight IS NOT NULL) THEN 'Yes' ELSE 'No' END AS "Goals Set",
            CASE WHEN EXISTS (SELECT 1 FROM "public"."metrics" m WHERE m.patient_id = pb.id AND m.name = 'BODY_FAT') THEN 'Yes' ELSE 'No' END AS "Smart Scale Logged",
            CASE WHEN EXISTS (SELECT 1 FROM "public"."patientfoodlogs" pfl WHERE pfl.patient_id = pb.id) THEN 'Yes' ELSE 'No' END AS "Meal Logged"
        FROM patient_base pb
    ),
    last_consultant_note_details AS (
        SELECT patient_id, last_note_date, last_note_description
        FROM (
            SELECT patient_id, date AS last_note_date, description AS last_note_description, ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY date DESC) as rn
            FROM "public"."patientnotes" WHERE consultant_id IS NOT NULL
        ) ranked_notes
        WHERE rn = 1
    ),
    meal_log_last_3_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."patientfoodlogs" WHERE date::date >= CURRENT_DATE - INTERVAL '3 days' GROUP BY patient_id
    ),
    latest_weight AS (
        SELECT patient_id, value AS current_weight
        FROM (SELECT patient_id, value, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC, "createdAt" DESC) AS rn FROM "public"."metrics" WHERE name = 'BODY_WEIGHT') sub
        WHERE rn = 1
    ),
    weight_log_last_7_days AS (
        SELECT patient_id, 'Yes' AS logged
        FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' AND date::date >= CURRENT_DATE - INTERVAL '7 days' GROUP BY patient_id
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
            patient_id, MAX(interaction_date) AS last_active_day,
            COUNT(DISTINCT CASE WHEN interaction_date >= CURRENT_DATE - INTERVAL '7 days' THEN interaction_date ELSE NULL END) AS active_days_last_7
        FROM all_patient_interactions GROUP BY patient_id
    ),
    last_meal_log_date AS (
        SELECT patient_id, MAX(date::date) AS last_meal_log_date
        FROM "public"."patientfoodlogs" GROUP BY patient_id
    ),
    last_weight_log_date AS (
        SELECT patient_id, MAX(date::date) AS last_weight_log_date
        FROM "public"."metrics" WHERE name = 'BODY_WEIGHT' GROUP BY patient_id
    )
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
    ROUND(pb.goal_weight::numeric, 2) AS "Goal Weight",
    ROUND(lw.current_weight::numeric, 2) AS "Current Weight",
    ROUND((((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration, 0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date)))::numeric, 2) AS "On Track Weight Lose",
    ROUND((pb.start_weight - lw.current_weight)::numeric, 2) AS "Current Weight Lose",
    CASE
        WHEN lw.current_weight IS NULL OR pb.start_weight IS NULL OR pb.goal_weight IS NULL OR pb.target_duration IS NULL OR pb.target_duration <= 0 OR pb.subscription_start_date IS NULL THEN 'Data Incomplete'
        WHEN pb.start_weight <= pb.goal_weight THEN 'Goal Achieved or Invalid'
        ELSE
            CASE WHEN (pb.start_weight - lw.current_weight) >= (((pb.start_weight - pb.goal_weight) / NULLIF(pb.target_duration, 0)) * GREATEST(0, (CURRENT_DATE - pb.subscription_start_date::date))) THEN 'On Track' ELSE 'Off Track' END
    END AS "On/Off Track",
    lcn.last_note_date AS "Last Note Added Date",
    lcn.last_note_description AS "Last Note",
    CASE WHEN (CURRENT_DATE - lcn.last_note_date::date) <= 5 THEN 'Yes' ELSE 'No' END AS "Interaction (5 Days)",
    lml.last_meal_log_date AS "Last Meal Log Date",
    lwl.last_weight_log_date AS "Last Weight Log Date",
    COALESCE(mll3d.logged, 'No') AS "Meal Log (3 days)",
    COALESCE(wll7d.logged, 'No') AS "Weight Log (7 days)",
    COALESCE(act.active_days_last_7, 0) AS "Num Active days (in last 7 days)",
    act.last_active_day AS "Last Active Day"
FROM patient_base pb
LEFT JOIN onboarding_metrics om ON pb.id = om.patient_id
LEFT JOIN last_consultant_note_details lcn ON pb.id = lcn.patient_id
LEFT JOIN meal_log_last_3_days mll3d ON pb.id = mll3d.patient_id
LEFT JOIN latest_weight lw ON pb.id = lw.patient_id
LEFT JOIN weight_log_last_7_days wll7d ON pb.id = wll7d.patient_id
LEFT JOIN activity_summary act ON pb.id = act.patient_id
LEFT JOIN last_meal_log_date lml ON pb.id = lml.patient_id
LEFT JOIN last_weight_log_date lwl ON pb.id = lwl.patient_id
ORDER BY "Subscription Type", "Patient Name";
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
    'from_name': 'EarlyFit User Analytics',
    
    # Email Content
    'subject': f'Quick Summary and analytics for current users - {datetime.now().strftime("%Y-%m-%d")}',
    'title': 'EarlyFit User Analytics',
    'greeting': 'Dear Team,<br><br>Please find today\'s data report below.',
    'closing': 'EarlyFit Product Team'
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
        
        # Interaction (5 Days), Meal Log (3 days), Weight Log (7 days) = "No" → Bright Yellow
        if column_name in ["Interaction (5 Days)", "Meal Log (3 days)", "Weight Log (7 days)"]:
            if value_str.lower() == "no":
                return "#ffff00"
        
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
            
            use_formatting = heading in ["Coach +App User analytics", "GLP User analytics", "Full Analytics"]
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
        print("Please update the following settings in earlyfit_email_report.py:\n")
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

