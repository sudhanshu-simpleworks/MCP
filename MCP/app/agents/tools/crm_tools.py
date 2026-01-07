import base64
import uuid
import re
import os
import json
import time
import requests
from contextvars import ContextVar
import pandas as pd
from datetime import datetime
from app.utils.logger import logger
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlencode
import plotly.express as px
from typing import Optional, List, Dict, Any, Tuple
from app.agents.tools.mcp_logger import mcp_logger
import warnings

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

warnings.filterwarnings("ignore")
load_dotenv()


# ============================================================================
# AUTHENTICATION (30-min token cache)
# ============================================================================

user_token_ctx: ContextVar[str] = ContextVar("user_token", default=None)
_user_session_cache = {
    "username": None,
    "access_token": None,
    "expires_at": 0
}

def encrypt_password(plain_password: str) -> str:
    """Encrypts password using the CRM specific AES/PBKDF2 logic."""
    try:
        passphrase = "373632764d5243706c706d6973"
        iterations = 999
        key_length = 32
        salt_length = 32
        iv_length = 16

        salt = os.urandom(salt_length)
        iv = os.urandom(iv_length)

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA512(),
            length=key_length,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        derived_key = kdf.derive(passphrase.encode('utf-8'))

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plain_password.encode('utf-8')) + padder.finalize()

        cipher = Cipher(algorithms.AES(derived_key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        payload = {
            "amtext": base64.b64encode(ciphertext).decode('utf-8'),
            "slam_ltol": salt.hex(),
            "iavmol": iv.hex()
        }
        return base64.b64encode(json.dumps(payload).encode('utf-8')).decode('utf-8')
    except Exception as e:
        logger.error(f"Encryption Error: {e}")
        raise ValueError("Password encryption failed internal check.")

def perform_user_login(username: str, password_plain: str) -> str:
    """
    Logs in to CRM, gets token, and stores it in Server Memory.
    """
    global _user_session_cache
    
    login_url = os.getenv("CRM_LOGIN_ENDPOINT")
    client_id = os.getenv("CRM_USER_CLIENT_ID")
    client_secret = os.getenv("CRM_USER_CLIENT_SECRET")

    if not all([login_url, client_id, client_secret]):
        raise ValueError("Missing CRM_USER_CLIENT_ID or CRM_USER_CLIENT_SECRET in .env")

    logger.info(f"Attempting login for user: {username}")

    encrypted_password = encrypt_password(password_plain)

    payload = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": encrypted_password
    }

    try:
        response = requests.post(login_url, data=payload, timeout=15, verify=False)
        
        if response.status_code >= 400:
            response = requests.post(login_url, json=payload, headers={"Content-Type": "application/json"}, timeout=15, verify=False)

        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        
        if not token:
            raise ValueError("CRM response missing access_token")

        _user_session_cache["username"] = username
        _user_session_cache["access_token"] = token
        _user_session_cache["expires_at"] = time.time() + expires_in - 300
        
        logger.info(f"Login Success. Token cached for user: {username}")
        return token

    except Exception as e:
        logger.error(f"Login Failed: {e}")
        raise ValueError(f"Login failed: {str(e)}")

def resolve_auth_token() -> str:
    """
    1. Check Headers (ContextVar)
    2. Check Internal Server Memory (Session Cache)
    """
    token = user_token_ctx.get()
    if token and len(token.strip()) > 10:
        return token

    if _user_session_cache["access_token"]:
        if time.time() < _user_session_cache["expires_at"]:
             return _user_session_cache["access_token"]
        else:
             _user_session_cache["access_token"] = None
             logger.warning("Cached session expired.")

    logger.error("Security Error: No active session found.")
    raise ValueError("You are not logged in. Please use the 'login_to_crm' tool first with your username and password.")

# ============================================================================
# CONFIGURATION
# ============================================================================
CRM_UI_BASE_URL = os.getenv("CRM_UI_BASE_URL")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
API_DOMAIN_URL = os.getenv("API_DOMAIN_URL")
CRM_LIST_ENDPOINT = os.getenv("CRM_LIST_ENDPOINT")
ROOT_PATH = Path(os.getenv("ROOT_PATH", "."))
plot_dir = ROOT_PATH / "app" / "agents" / "tools" / "plots"
plot_dir.mkdir(parents=True, exist_ok=True)
_data_cache = {}
CACHE_TTL = 3600

# ============================================================================
# MODULE DEFINITIONS
# ============================================================================

CRM_MODULES = {
    "Calls": {
        "aliases": ["calls", "call"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "date_entered",
            "date_modified",
            "assigned_user_id",
            "assigned_user_name",
            "date_start",
            "parent_type",
            "parent_name",
            "status",
            "direction",
            "parent_id",
            "disposition_c",
            "id",
            "duration" "created_by",
            "created_by_name",
        ],
        "field_mapping": {
            "subject": "name",
            "title": "name",
            "start_date": "date_start",
            "related_to": "parent_name",
            "parent": "parent_name",
            "assigned_to": "assigned_user_name",
            "creator": "created_by_name",
            "created_by": "created_by_name",
        },
        "column_renames": {
            "Subject": "name",
            "Start Date": "date_start",
            "Related to": "parent_name",
            "Status": "status",
            "Direction": "direction",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "Date Modified": "date_modified",
            "Created By": "created_by_name",
            "Disposition": "disposition_c",
            "ID": "id",
        },
        "default_display_columns": [
            "Subject",
            "Status",
            "Start Date",
            "Related to",
            "Assigned To",
        ],
        "enums": {
            "direction": ["Inbound", "Outbound"],
            "status": ["Planned", "Held", "Not Held"],
            "parent_type": [
                "Accounts",
                "Contacts",
                "Tasks",
                "Opportunities",
                "Bugs",
                "Cases",
                "Leads",
                "Project",
                "ProjectTask",
                "Prospects",
                "Simpl_Project_Milestones",
                "simpl_Stories",
                "Simpl_Milestone_Invoice",
                "Simpl_Projects",
                "scrm_Candidates",
                "simpl_Employees",
                "Calls",
                "Meetings",
                "scrm_Partners",
            ],
        },
    },
    "scrm_Internal_Tickets": {
        "aliases": [
            "internal tickets",
            "internal ticket",
            "it tickets",
            "internal_tickets",
        ],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "scrm_internal_tickets_number",
            "type",
            "status",
            "priority",
            "sub_type_c",
            "resolution",
            "date_entered",
            "date_modified",
            "assigned_user_name",
            "created_by_name",
            "assigned_user_id",
            "created_by",
            "accounts_scrm_internal_tickets_1_name",
            "parent_name",
            "id",
        ],
        "field_mapping": {
            "subject": "name",
            "title": "name",
            "number": "scrm_internal_tickets_number",
            "ticket_number": "scrm_internal_tickets_number",
            "team": "type",
            "department": "type",
            "request": "sub_type_c",
            "sub_type": "sub_type_c",
            "account": "accounts_scrm_internal_tickets_1_name",
            "assigned_to": "assigned_user_name",
            "creator": "created_by_name",
        },
        "column_renames": {
            "Subject": "name",
            "Number": "scrm_internal_tickets_number",
            "Team": "type",
            "Status": "status",
            "Priority": "priority",
            "Request Type": "sub_type_c",
            "Assigned To": "assigned_user_name",
            "Created By": "created_by_name",
            "Date Created": "date_entered",
            "Account Name": "accounts_scrm_internal_tickets_1_name",
            "ID": "id",
        },
        "default_display_columns": [
            "Number",
            "Subject",
            "Status",
            "Priority",
            "Team",
            "Assigned To",
            "Date Created",
        ],
        "enums": {
            "type": [
                "Admin",
                "DSTeam",
                "HR",
                "Infrastructure",
                "PreSales",
                "Product",
                "Sales",
                "Solutions",
                "Marketing",
                "ld",
            ],
            "status": [
                "New",
                "Assigned",
                "Closed",
                "Pending Input",
                "Rejected",
                "Duplicate",
            ],
            "sub_type_c": [
                "PreSales_RFPProcess",
                "PreSales_Demo",
                "Admin_TravelProcess",
                "Admin_TravelInternation",
                "Admin_Tender",
                "Infrastructure_InstanceCreation",
                "Infrastructure_HS",
                "Solutions_Efforts",
                "Solutions_Help",
                "Solutions_Upgrade",
                "Sales_RFP",
                "Sales_NonRFP",
                "Product_InstanceUpgrade",
                "Product_EffortsEtimate",
                "Product_Help",
                "Solutions_Discussion",
                "Solutions_Other",
                "Product_Other",
                "Sales_Other",
                "Admin_Other",
                "PreSales_Other",
                "HR_AnnualAppraisal",
                "HR_Probation",
                "HR_Other",
                "DSTeam_Other",
                "Infrastructure_Other",
                "Marketing_Other",
                "HR_ResourceRequirement",
                "ld_newemployee",
                "ld_employeeTraining",
                "ld_documentation",
                "ld_other",
                "Solutions_CR",
            ],
        },
    },
    "Cases": {
        "aliases": ["tickets", "cases", "ticket", "case", "bugs", "issues"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "case_number",
            "case_aging_c",
            "status",
            "priority",
            "type",
            "type_test_c",
            "tags_c",
            "date_entered",
            "date_modified",
            "assigned_user_name",
            "created_by_name",
            "assigned_user_id",
            "created_by",
            "account_name",
            "account_id",
            "date_closed_c",
            "exclude_ticket_c",
            "id",
        ],
        "field_mapping": {
            "subject": "name",
            "title": "name",
            "number": "case_number",
            "ticket_number": "case_number",
            "case_id": "case_number",
            "id": "case_number",
            "age": "case_aging_c",
            "aging": "case_aging_c",
            "assigned_to": "assigned_user_name",
            "created_by": "created_by_name",
            "creator": "created_by_name",
            "tag": "tags_c",
            "tags": "tags_c",
            "account": "account_name",
            "company": "account_name",
        },
        "column_renames": {
            "Subject": "name",
            "Case Number": "case_number",
            "Type": "type",
            "Status": "status",
            "Priority": "priority",
            "Tags": "tags_c",
            "Type Test": "type_test_c",
            "Age in day(s)": "case_aging_c",
            "Created By": "created_by_name",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "Date Modified": "date_modified",
            "Account Name": "account_name",
            "ID": "id",
        },
        "default_display_columns": [
            "Subject",
            "Number",
            "Status",
            "Priority",
            "Assigned To",
            "Date Created",
        ],
        "enums": {
            "priority": ["P1", "P2", "P3", "P0"],
            "status": [
                "New",
                "Assigned",
                "InProcess",
                "Pending Input",
                "Rejected",
                "Closed",
                "Assigned_to_QA",
                "ClosedApprovedInternal",
                "ClosedDeployedToStaging",
                "UAT Sign-off/Move to Production",
                "ClosedDeployedToProduction",
                "ClosedApprovedCustomer",
                "Under_Observation_Client",
                "Under_Observation_Internal",
                "Reopen",
                "Duplicate",
                "AssignToProductTeam",
                "On_Hold",
            ],
            "type": [
                "Defect",
                "MinorDefect",
                "ChangeRequest",
                "Project_Bug",
                "Bug",
                "ProductEnhancementRequest",
                "PreSalesRelated",
                "Training_Issue",
                "Usability_Issue",
                "Rework",
                "Other",
            ],
        },
    },
    "Meetings": {
        "aliases": ["meetings", "meeting", "appointments", "appointment"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "date_entered",
            "date_modified",
            "assigned_user_id",
            "assigned_user_name",
            "date_start",
            "parent_type",
            "parent_name",
            "status",
            "parent_id",
            "id",
            "created_by",
            "created_by_name",
        ],
        "field_mapping": {
            "subject": "name",
            "title": "name",
            "start_date": "date_start",
            "related_to": "parent_name",
            "parent": "parent_name",
            "assigned_to": "assigned_user_name",
            "creator": "created_by_name",
            "created_by": "created_by_name",
        },
        "column_renames": {
            "Subject": "name",
            "Start Date": "date_start",
            "Related To": "parent_name",
            "Status": "status",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "Date Modified": "date_modified",
            "Created By": "created_by_name",
            "ID": "id",
        },
        "default_display_columns": [
            "Subject",
            "Status",
            "Start Date",
            "Related To",
            "Assigned To",
        ],
        "enums": {
            "status": ["Planned", "Held", "Not Held"],
            "parent_type": [
                "Accounts",
                "Contacts",
                "Tasks",
                "Opportunities",
                "Bugs",
                "Cases",
                "Leads",
                "Project",
                "ProjectTask",
                "Prospects",
                "Simpl_Project_Milestones",
                "simpl_Stories",
                "Simpl_Milestone_Invoice",
                "Simpl_Projects",
                "scrm_Candidates",
                "simpl_Employees",
                "Calls",
                "Meetings",
                "scrm_Partners",
            ],
        },
    },
    "Accounts": {
        "aliases": [
            "accounts",
            "account",
            "companies",
            "company",
            "clients",
            "organizations",
        ],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "company_research_c",
            "industry",
            "account_type",
            "billing_address_street",
            "billing_address_country",
            "phone_fax",
            "phone_alternate",
            "website",
            "email1",
            "description",
            "remaining_hours_c",
            "assigned_user_name",
            "created_by_name",
            "assigned_user_id",
            "created_by",
            "date_entered",
            "date_modified",
            "id",
            "source_c",
            "marketing_status_c",
            "region_c",
        ],
        "field_mapping": {
            "company": "name",
            "type": "account_type",
            "billing_street": "billing_address_street",
            "billing_country": "billing_address_country",
            "address": "billing_address_street",
            "phone": "phone_alternate",
            "fax": "phone_fax",
            "email": "email1",
            "hours": "remaining_hours_c",
            "assigned_to": "assigned_user_name",
            "created_by": "created_by_name",
        },
        "column_renames": {
            "Name": "name",
            "Company Research": "company_research_c",
            "Assigned To": "assigned_user_name",
            "Industry": "industry",
            "Billing Street": "billing_address_street",
            "Billing Country": "billing_address_country",
            "Type": "account_type",
            "Website": "website",
            "Alternate Phone": "phone_alternate",
            "Email Address": "email1",
            "Description": "description",
            "Remaining Support Hours": "remaining_hours_c",
            "Created By": "created_by_name",
            "Date Created": "date_entered",
            "Date Modified": "date_modified",
            "ID": "id",
        },
        "default_display_columns": [
            "Name",
            "Type",
            "Industry",
            "Region",
            "Email Address",
            "Assigned To",
        ],
        "enums": {
            "industry": [
                "Apparel",
                "Banking",
                "Biotechnology",
                "Chemicals",
                "Communications",
                "Consulting",
                "Construction",
                "Education",
                "Electronics",
                "Energy",
                "Engineering",
                "Entertainment",
                "Environmental",
                "Finance",
                "Government",
                "Healthcare",
                "Hospitality",
                "Insurance",
                "Machinery",
                "Manufacturing",
                "Media",
                "Not For Profit",
                "Recreation",
                "Retail",
                "Shipping",
                "Technology",
                "Telecommunications",
                "Transportation",
                "Utilities",
                "Other",
                "Outsourcing_Offshoring",
            ],
            "account_type": [
                "Analyst",
                "Competitor",
                "Customer",
                "Integrator",
                "Investor",
                "Partner",
                "Press",
                "Prospect",
                "Reseller",
                "Other",
            ],
        },
    },
    "Contacts": {
        "aliases": ["contacts", "contact", "people", "person"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "title",
            "email1",
            "phone_work",
            "linkedin_c",
            "assigned_user_name",
            "date_entered",
            "account_name",
            "assigned_user_id",
            "salutation",
            "first_name",
            "last_name",
            "email",
            "lead_source",
            "account_id",
            "note_1_c",
            "nurture_campaign_c",
            "csat_survey_c",
            "stale_contact_c",
            "id",
        ],
        "field_mapping": {
            "full_name": "name",
            "job_title": "title",
            "designation": "title",
            "email": "email1",
            "phone": "phone_work",
            "mobile": "phone_work",
            "work_phone": "phone_work",
            "office_phone": "phone_work",
            "linkedin": "linkedin_c",
            "assigned_to": "assigned_user_name",
            "company": "account_name",
            "account": "account_name",
        },
        "column_renames": {
            "Name": "name",
            "LinkedIn": "linkedin_c",
            "Title": "title",
            "Email Address": "email1",
            "Office Phone": "phone_work",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "ID": "id",
            "Account Name": "account_name",
            "Notes": "note_1_c",
        },
        "default_display_columns": [
            "Name",
            "Title",
            "Account Name",
            "Email Address",
            "Office Phone",
            "Assigned To",
        ],
        "enums": {},
    },
    "Leads": {
        "aliases": ["leads", "lead", "prospects"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "assigned_user_name",
            "date_entered",
            "date_modified",
            "status",
            "lead_source",
            "account_name_c",
            "email",
            "full_name",
            "report_to_name",
            "lead_customer_type_c",
            "marketing_username_c",
            "marketing_stage_c",
            "region_c",
            "primary_address_country",
            "id",
        ],
        "field_mapping": {
            "subject": "name",
            "lead_name": "name",
            "full_name": "name",
            "assigned_to": "assigned_user_name",
            "reports_to": "report_to_name",
            "source": "lead_source",
            "company": "account_name_c",
            "account": "account_name_c",
            "type": "lead_customer_type_c",
            "customer_type": "lead_customer_type_c",
            "marketing_user": "marketing_username_c",
            "marketing_stage": "marketing_stage_c",
            "region": "region_c",
            "country": "primary_address_country",
            "email": "email",
        },
        "column_renames": {
            "Name": "name",
            "Status": "status",
            "Lead Source": "lead_source",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "Date Modified": "date_modified",
            "Reports To": "report_to_name",
            "Customer Type": "lead_customer_type_c",
            "Account Name": "account_name_c",
            "Marketing User": "marketing_username_c",
            "Marketing Stage": "marketing_stage_c",
            "Region": "region_c",
            "Country": "primary_address_country",
            "Email": "email",
            "ID": "id",
        },
        "default_display_columns": [
            "Name",
            "Status",
            "Lead Source",
            "Customer Type",
            "Account Name",
            "Assigned To",
            "Date Created",
        ],
        "enums": {
            "status": [
                "New",
                "Assigned",
                "In Process",
                "Converted",
                "Long_Term_Nurture",
                "Recycled",
                "Dead",
            ],
            "lead_source": [
                "Existing Customer",
                "Self Generated",
                "Partner",
                "LinkedIn",
                "Employee",
                "Cold Call",
                "Public Relations",
                "Direct Mail",
                "Conference",
                "Trade Show",
                "Web Site",
                "Word of mouth",
                "Email",
                "Campaign",
                "IndiaMART",
                "JustDial",
                "ChatBot",
                "Other",
                "MQL",
                "marketing",
            ],
        },
    },
    "Opportunities": {
        "aliases": ["opportunities", "opportunity", "opps", "opp", "deals", "deal"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "amount",
            "amount_usdollar",
            "account_name",
            "opp_aging_c",
            "sales_stage",
            "opportunity_stage_c",
            "sales_category_c",
            "opportunity_probability_c",
            "date_closed",
            "next_step",
            "assigned_user_name",
            "date_entered",
            "quarter_c",
            "buying_process_c",
            "id",
        ],
        "field_mapping": {
            "deal_name": "name",
            "value": "amount",
            "revenue": "amount",
            "amount": "amount",
            "aging": "opp_aging_c",
            "age": "opp_aging_c",
            "stage": "sales_stage",
            "sales_stage": "sales_stage",
            "opportunity_stage": "opportunity_stage_c",
            "opp_stage": "opportunity_stage_c",
            "probability": "opportunity_probability_c",
            "close_date": "date_closed",
            "expected_close_date": "date_closed",
            "assigned_to": "assigned_user_name",
            "client": "account_name",
            "opportunity_amount": "amount",
            "deal_amount": "amount_usdollar",
        },
        "column_renames": {
            "ID": "id",
            "Opportunity Name": "name",
            "Account Name": "account_name",
            "Opportunity Amount": "amount",
            "Deal Amount": "amount_usdollar",
            "Assigned To": "assigned_user_name",
            "Ageing (Days)": "opp_aging_c",
            "Sales Stage": "sales_stage",
            "Opportunity Stage": "opportunity_stage_c",
            "Sales Category": "sales_category_c",
            "Opportunity Probability(%)": "opportunity_probability_c",
            "Next Step": "next_step",
            "Date Closed": "date_closed",
            "Date Entered": "date_entered",
            "Quarter": "quarter_c",
            "Renewal": "renewal_c",
            "Created By": "created_by",
        },
        "default_display_columns": [
            "Opportunity Name",
            "Opportunity Stage",
            "Opportunity Amount",
            "Account Name",
            "Date Closed",
            "Assigned To",
        ],
        "enums": {
            "sales_stage": [
                "Prospecting",
                "Qualification",
                "Needs Analysis",
                "Value Proposition",
                "Id. Decision Makers",
                "Perception Analysis",
                "Proposal/Price Quote",
                "Negotiation/Review",
                "Closed Won",
                "Closed Lost",
                "Duplicate",
                "Dormant",
                "Dropped",
            ],
            "opportunity_stage_c": {
                "nonrfp_prospecting": "Prospecting",
                "nonrfp_qualification": "Qualification",
                "nonrfp_demo": "Demo",
                "nonrfp_proposalsubmission": "Proposal Submission",
                "nonrfp_negotiationround1": "Negotiation Round 1",
                "nonrfp_negotiationround2": "Negotiation Round 2",
                "nonrfp_closedwon": "Closed Won",
                "nonrfp_closedlost": "Closed Lost",
                "nonrfp_dropped": "Dropped",
                "nonrfp_duplicate": "Duplicate",
                "nonrfp_dormant": "Dormant",
                "rfp_eligibilityevaluation": "Eligibility Evaluation",
                "rfp_rfpsubmission": "RFP Submission",
                "rfp_technicalevaluation": "Technical Evaluation",
                "rfp_demo": "Demo",
                "rfp_referencecheck": "Reference Check",
                "rfp_closedwon": "Closed Won",
                "rfp_closedlost": "Closed Lost",
                "rfp_dropped": "Dropped",
                "rfp_duplicate": "Duplicate",
                "rfp_dormant": "Dormant",
            },
            "quarter_c": [
                "2024-25-Q1",
                "2024-25-Q2",
                "2024-25-Q3",
                "2024-25-Q4",
                "2025-26-Q1",
                "2025-26-Q2",
                "2025-26-Q3",
                "2025-26-Q4",
            ],
        },
    },
    "Tasks": {
        "aliases": ["tasks", "task", "todos", "reminders"],
        "date_field": "date_entered",
        "key_fields": [
            "name",
            "description",
            "created_by_name",
            "assigned_user_name",
            "date_entered",
            "priority",
            "status",
            "date_due",
            "date_start",
            "type_c",
            "parent_name",
            "parent_id",
            "contact_id",
            "contact_name",
            "date_completed_c",
            "assigned_user_id",
            "created_by",
            "date_modified",
            "id",
        ],
        "field_mapping": {
            "subject": "name",
            "title": "name",
            "creator": "created_by_name",
            "assigned_to": "assigned_user_name",
            "due_date": "date_due",
            "start_date": "date_start",
            "type": "type_c",
            "parent": "parent_name",
            "related_to": "parent_name",
            "contact": "contact_name",
        },
        "column_renames": {
            "Subject": "name",
            "Created By": "created_by_name",
            "Assigned To": "assigned_user_name",
            "Date Created": "date_entered",
            "Priority": "priority",
            "Status": "status",
            "Due Date": "date_due",
            "Start Date": "date_start",
            "Type": "type_c",
            "Related To": "parent_name",
            "Contact Name": "contact_name",
            "Date Completed": "date_completed_c",
            "Date Modified": "date_modified",
            "ID": "id",
        },
        "default_display_columns": [
            "Subject",
            "Status",
            "Priority",
            "Due Date",
            "Assigned To",
            "Related To",
        ],
        "enums": {
            "priority": ["High", "Medium", "Low"],
            "status": [
                "Not Started",
                "In Progress",
                "Completed",
                "Pending Input",
                "Deferred",
            ],
        },
    },
    "Users": {
        "aliases": ["users", "user", "employees", "staff", "agents", "reps"],
        "date_field": "date_entered",
        "key_fields": [
            "id",
            "description",
            "user_name",
            "first_name",
            "last_name",
            "name",
            "email1",
            "status",
            "employee_status",
            "reports_to_name",
            "reports_to_id",
            "is_admin",
            "date_entered",
            "address_postalcode",
            "customerportaluser_c",
        ],
        "field_mapping": {
            "name": "name",
            "full_name": "name",
            "username": "user_name",
            "login": "user_name",
            "email": "email1",
            "status": "status",
            "reports_to": "reports_to_name",
            "zip": "address_postalcode",
        },
        "column_renames": {
            "Full Name": "name",
            "Username": "user_name",
            "Email Address": "email1",
            "Status": "status",
            "Employee Status": "employee_status",
            "Reports To": "reports_to_name",
            "Is Administrator": "is_admin",
            "Date Created": "date_entered",
            "ID": "id",
        },
        "default_display_columns": [
            "Full Name",
            "Username",
            "Email Address",
            "Status",
            "Employee Status",
            "Reports To",
        ],
        "enums": {"status": ["Active", "Inactive"]},
    },
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def todays_date() -> str:
    """Get today's date in MM/DD/YYYY format."""
    today = datetime.now()
    return f"Today's date: {today.strftime('%m/%d/%Y')}"


def get_available_modules() -> Dict[str, Any]:
    """Get information about all available CRM modules."""
    modules_info = {}
    for module_name, config in CRM_MODULES.items():
        modules_info[module_name] = {
            "key_fields": config["key_fields"],
            "aliases": config["aliases"],
        }
    return {"available_modules": list(CRM_MODULES.keys()), "modules_info": modules_info}


def _resolve_module(module: str) -> Optional[str]:
    """Resolve module name from aliases."""
    module_lower = module.lower()
    for key, config in CRM_MODULES.items():
        if key.lower() == module_lower or module_lower in [a.lower() for a in config.get("aliases", [])]:
            return key
    return module


def _cache_data(data: List[Dict]) -> str:
    data_id = f"crm_data_{uuid.uuid4().hex[:8]}"
    _data_cache[data_id] = {"data": data, "expires_at": time.time() + CACHE_TTL}
    return data_id


def _get_cached_data(data_id: str) -> Optional[List[Dict]]:
    """Retrieve cached data by ID."""
    cached = _data_cache.get(data_id)
    if cached and time.time() < cached["expires_at"]:
        return cached["data"]
    return None


# ============================================================================
# WEB SEARCH & UTILITIES
# ============================================================================


def web_search(query: str, max_results: int = 3) -> Dict[str, Any]:
    """Web search via Serper API."""
    try:
        if not SERPER_API_KEY:
            return {"error": "Serper API key not configured", "query": query}

        url = "https://google.serper.dev/search"

        payload = {"q": query, "num": max_results, "gl": "in", "hl": "en"}

        headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        results = []
        for item in data.get("organic", [])[:max_results]:
            results.append(
                {
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                    "url": item.get("link", ""),
                }
            )

        answer = None
        if "answerBox" in data:
            answer_box = data["answerBox"]
            answer = (
                answer_box.get("answer")
                or answer_box.get("snippet")
                or answer_box.get("snippetHighlighted", [""])[0]
            )
        elif "knowledgeGraph" in data:
            kg = data["knowledgeGraph"]
            answer = kg.get("description") or kg.get("title")

        return {
            "query": query,
            "answer": answer,
            "results": results,
            "count": len(results),
        }

    except Exception as e:
        logger.error(f"Web search failed: {e}")
        return {"error": str(e), "query": query}


def get_current_time(timezone: str = "UTC") -> Dict[str, Any]:
    """Get current time via web search."""
    try:
        search_result = web_search(f"current time in {timezone}", max_results=1)

        if search_result.get("answer"):
            return {
                "timezone": timezone,
                "answer": search_result["answer"],
                "source": "serper",
            }

        return {
            "timezone": timezone,
            "search_results": search_result.get("results", []),
        }

    except Exception as e:
        return {"error": str(e), "timezone": timezone}


# ============================================================================
# COLUMN RESOLUTION HELPER
# ============================================================================


def resolve_display_columns(
    module: str,
    requested_columns: List[str],
    records: List[Dict],
    filters: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """
    Determines the final list of columns to display based on defaults, user request, AND active filters.
    """
    module_config = CRM_MODULES.get(module, {}) if module else {}
    rename_map = module_config.get("column_renames", {})
    default_display = module_config.get("default_display_columns", [])
    normalized_rename_map = {k.lower(): v for k, v in rename_map.items()}
    api_to_display_map = {v: k for k, v in rename_map.items()}
    id_to_name_map = {
        "assigned_user_id": "assigned_user_name",
        "assigned_user": "assigned_user_name",
        "account_id": "account_name",
    }

    final_columns = []
    seen_api_keys = set()

    if requested_columns:
        for col in requested_columns:
            col_lower = col.lower()
            api_key = normalized_rename_map.get(col_lower)
            if not api_key:
                api_key = col

            if api_key not in seen_api_keys:
                display_name = col
                for name, key in rename_map.items():
                    if key == api_key:
                        display_name = name
                        break
                final_columns.append((display_name, api_key))
                seen_api_keys.add(api_key)

    elif default_display:
        for disp_name in default_display:
            api_key = normalized_rename_map.get(disp_name.lower(), disp_name)
            if api_key not in seen_api_keys:
                final_columns.append((disp_name, api_key))
                seen_api_keys.add(api_key)

    if filters and not requested_columns:
        for filter_key in filters.keys():
            if filter_key in seen_api_keys:
                continue

            target_key = id_to_name_map.get(filter_key, filter_key)
            if target_key in seen_api_keys:
                continue

            display_name = api_to_display_map.get(target_key, target_key)
            if display_name == target_key:
                display_name = target_key.replace("_c", "").replace("_", " ").title()

            final_columns.append((display_name, target_key))
            seen_api_keys.add(target_key)

    if not final_columns:
        if records:
            keys = [
                k
                for k in list(records[0].keys())
                if k not in ["id", "module", "ACLAccess"]
            ]
            for k in keys[:8]:
                final_columns.append((k, k))
    return final_columns


# ============================================================================
# CRM LINK GENERATION
# ============================================================================


def generate_crm_link(module: str, filters: Dict[str, Any]) -> str:
    """
    Generates a simple link to the CRM Module List View.
    """
    if not CRM_UI_BASE_URL:
        return ""

    base = CRM_UI_BASE_URL.rstrip("/")
    return f"{base}/app/{module}"


# ============================================================================
# CALCULATE TOTAL AMOUNT (Optimized - No external conversion)
# ============================================================================


def calculate_total_amount(
    data_id_or_records: Any,
) -> Dict[str, Any]:
    """
    Calculate total amount using ONLY `amount`.
    """
    try:
        mcp_logger.log_step(
            step_type="tool_call",
            title="Calculate Total Amount (amount)",
            data={},
            status="in_progress",
        )

        records = None
        if isinstance(data_id_or_records, str) and data_id_or_records.startswith(
            "crm_data_"
        ):
            records = _get_cached_data(data_id_or_records)
        elif isinstance(data_id_or_records, list):
            records = data_id_or_records
        elif isinstance(data_id_or_records, dict):
            if "data_id" in data_id_or_records and data_id_or_records["data_id"]:
                records = _get_cached_data(data_id_or_records["data_id"])
            elif "records" in data_id_or_records:
                records = data_id_or_records["records"]

        if not records:
            raise ValueError("No records found")

        total_amount = 0.0
        records_without_amount = 0
        records_with_amount = 0
        target_field = "amount"

        for record in records:
            val = record.get(target_field)

            if not val or str(val).strip() in ["", "None", "null"]:
                records_without_amount += 1
                continue
            try:
                clean_val = re.sub(r"[^\d.-]", "", str(val))

                if not clean_val:
                    records_without_amount += 1
                    continue

                amount = float(clean_val)

                if amount == 0:
                    records_without_amount += 1
                    continue

                total_amount += amount
                records_with_amount += 1
            except ValueError:
                records_without_amount += 1
                continue

        formatted_total = f"₹{total_amount:,.2f}"

        result = {
            "total_amount": round(total_amount, 2),
            "formatted_total": formatted_total,
            "records_processed": len(records),
            "records_with_amount": records_with_amount,
            "records_without_amount": records_without_amount,
            "breakdown": {},
        }

        mcp_logger.log_step(
            step_type="tool_response",
            title="Calculation Complete",
            data={
                "total": formatted_total,
                "records_with_amounts": records_with_amount,
            },
            status="success",
        )

        return result

    except Exception as e:
        logger.error(f"Amount calculation failed: {e}", exc_info=True)
        mcp_logger.log_step(
            step_type="tool_response",
            title="Calculation Failed",
            data={"error": str(e)},
            status="error",
        )
        raise ValueError(f"Calculation failed: {e}")


# ============================================================================
# SMART DISPLAY RESULTS
# ============================================================================


def smart_display_results(
    data_id_or_records: Any,
    columns: Optional[List[str]] = None,
    include_export: bool = False,
) -> Dict[str, Any]:
    """
    Smart display with CRM link.
    """
    try:
        records = None
        if isinstance(data_id_or_records, str) and data_id_or_records.startswith(
            "crm_data_"
        ):
            records = _get_cached_data(data_id_or_records)
        elif isinstance(data_id_or_records, list):
            records = data_id_or_records
        elif isinstance(data_id_or_records, dict):
            if "data_id" in data_id_or_records and data_id_or_records["data_id"]:
                records = _get_cached_data(data_id_or_records["data_id"])
            elif "records" in data_id_or_records:
                records = data_id_or_records["records"]

        if not records:
            raise ValueError("No records found")

        total_count = len(records)
        sample_size = 20
        table_result = get_table_from_query(
            data_id_or_records, columns, max_rows=sample_size
        )

        result = {
            "display_type": "sample",
            "table": table_result["table"],
            "total_records": total_count,
            "displayed_records": min(total_count, sample_size),
            "message": f"Found {total_count:,} records. Showing top results:",
        }

        if include_export:
            pass

        return result

    except Exception as e:
        logger.error(f"Smart display failed: {e}", exc_info=True)
        raise


# ============================================================================
# TABLE FORMATTING
# ============================================================================


def format_records_as_markdown_table(
    records: List[Dict],
    max_rows: Optional[int] = None,
    columns: Optional[List[str]] = None,
    module: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    display_lookups: Optional[Dict[str, str]] = None,
) -> str:
    if not records:
        return "_No records found_"

    total_records = len(records)
    limit = 20
    if max_rows and max_rows > 0:
        limit = max_rows
    if total_records > 50:
        limit = min(limit, 20) if limit != -1 else 20

    display_records = records[:limit]
    displayed_count = len(display_records)

    final_columns = resolve_display_columns(module, columns, records, filters)

    headers = [col[0] for col in final_columns]
    header_str = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join(["---" for _ in headers]) + "|"

    module_config = CRM_MODULES.get(module, {}) if module else {}
    enums = module_config.get("enums", {})

    rows = []
    for rec in display_records:
        row_values = []
        for _, api_key in final_columns:
            val = rec.get(api_key, "")
            if api_key in enums and isinstance(enums[api_key], dict):
                val = enums[api_key].get(val, val)
            if display_lookups and api_key in display_lookups:
                val = display_lookups[api_key]
            if val is None:
                val = ""
            val = str(val).replace("|", "\\|").replace("\n", " ").strip()
            if len(val) > 100 and not val.startswith("["):
                val = val[:97] + "..."
            row_values.append(val)
        rows.append("| " + " | ".join(row_values) + " |")

    table = "\n".join([header_str, separator] + rows)

    if displayed_count < total_records:
        table += f"\n\n_Displaying {displayed_count} of {total_records:,} records._"
    else:
        table += f"\n\n_Total: {total_records:,} records displayed_"

    return table


def get_table_from_query(
    data_id_or_records: Any,
    columns: Optional[List[str]] = None,
    max_rows: int = -1,
    module: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    display_lookups: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Get formatted markdown table."""
    records = None
    if isinstance(data_id_or_records, str) and data_id_or_records.startswith(
        "crm_data_"
    ):
        records = _get_cached_data(data_id_or_records)
    elif isinstance(data_id_or_records, list):
        records = data_id_or_records
    elif isinstance(data_id_or_records, dict):
        records = data_id_or_records.get("records")
        if not module:
            module = data_id_or_records.get("module")

    if not records:
        return {"table": "_No records_"}

    table = format_records_as_markdown_table(
        records, max_rows, columns, module, filters, display_lookups
    )
    return {"table": table}


def query_and_format_table(
    module: str,
    filters: Optional[Dict[str, Any]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[List[str]] = None,
    max_rows: Optional[int] = None,
) -> str:
    """Query CRM and format as table in one call."""
    result = query_crm_data(
        module=module,
        filters=filters,
        start_date=start_date,
        end_date=end_date,
        max_records=max_rows,
    )

    table_result = get_table_from_query(
        data_id_or_records=result, columns=columns, max_rows=max_rows
    )

    return table_result["table"]


# ============================================================================
# MAIN QUERY FUNCTION (with MCP logging)
# ============================================================================


def _apply_local_filters(records: List[Dict], filters: Dict[str, Any]) -> List[Dict]:
    """Strictly filter records in Python to ensure accuracy."""
    if not filters or not records:
        return records

    filtered = []
    for rec in records:
        match = True
        for key, criteria in filters.items():
            if key not in rec:
                continue

            val = rec.get(key)

            if isinstance(criteria, list):
                target_values = [str(v).lower() for v in criteria]
                if str(val).lower() not in target_values:
                    match = False
                    break
            elif isinstance(criteria, dict) and isinstance(criteria.get("value"), list):
                target_values = [str(v).lower() for v in criteria["value"]]
                if str(val).lower() not in target_values:
                    match = False
                    break
            elif isinstance(criteria, str):
                if str(criteria).lower() not in str(val).lower():
                    match = False
                    break
            elif isinstance(criteria, (int, float)):
                if str(val).lower() != str(criteria).lower():
                    match = False
                    break
            elif isinstance(criteria, dict) and "value" in criteria:
                op = criteria.get("operator", "eq")
                if op in ["eq", "=", "equals"]:
                    target_val = str(criteria["value"]).lower()
                    actual_val = str(val).lower()
                    if target_val not in actual_val:
                        match = False
                        break
                elif op in ["not_equal", "neq"]:
                    target_val = str(criteria["value"]).lower()
                    actual_val = str(val).lower()
                    if target_val == actual_val:
                        match = False
                        break
        if match:
            filtered.append(rec)
    return filtered


def query_crm_data(
    module: str,
    filters: Optional[Dict[str, Any]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter_field: str = "date_entered",
    max_records: Optional[int] = None,
    fields_only: Optional[List[str]] = None,
    page_number: int = 1,
    iterate_pages: bool = False,
) -> Dict[str, Any]:

    mcp_logger.log_step(
        "tool_call",
        "Query CRM Data",
        {
            "module": module,
            "filters": filters,
            "start": start_date,
            "end": end_date,
            "page": page_number,
            "iterate": iterate_pages,
        },
    )

    module_key = _resolve_module(module)
    if start_date and not end_date:
        end_date = datetime.now().strftime("%m/%d/%Y")

    access_token = resolve_auth_token()
        
    if not access_token:
        raise ValueError("Authentication Error: No CRM Access Token provided for this user.")

    base_url = f"{CRM_LIST_ENDPOINT.rstrip('/')}/{module_key}/views/list"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    all_records = []
    page_size = 20
    if max_records and max_records > 0 and max_records < 20:
        page_size = max_records
    current_page = 1 if iterate_pages else page_number
    iteration_safety_limit = 20000
    if max_records and iterate_pages:
        iteration_safety_limit = max_records

    while True:
        params = {
            "page[number]": current_page,
            "page[size]": page_size,
            "filter[reset][eq]": "true",
        }
        should_fetch_all = fields_only and "__ALL__" in fields_only

        if not should_fetch_all:
            fields_to_fetch = set()

            fields_to_fetch.add("id")
            fields_to_fetch.add("name")
            if module_key == "Users":
                fields_to_fetch.add("user_name")
                fields_to_fetch.add("first_name")
                fields_to_fetch.add("last_name")

            mod_config = CRM_MODULES.get(module_key, {})
            defaults = mod_config.get("default_display_columns", [])
            renames = mod_config.get("column_renames", {})

            for d_col in defaults:
                api_key = renames.get(d_col)
                if not api_key:
                    api_key = d_col.lower().replace(" ", "_")
                fields_to_fetch.add(api_key)

            if module_key == "Accounts":
                fields_to_fetch.add("company_research_c")
            else:
                fields_to_fetch.add("description")

            if fields_only:
                for f in fields_only:
                    fields_to_fetch.add(f)

            aux_fields = set()
            non_relational_names = {"first_name", "last_name", "full_name", "user_name"}
            for f in fields_to_fetch:
                if f.endswith("_name") and f not in non_relational_names:
                    base_id = f[:-5] + "_id"
                    if base_id == "user_id":
                        continue
                    aux_fields.add(base_id)
            fields_to_fetch.update(aux_fields)
            params[f"fields[{module_key}]"] = ",".join(list(fields_to_fetch))
        params["sort"] = f"-{date_filter_field}"
        params["filter[sort_column][eq]"] = date_filter_field
        params["filter[sort_order][eq]"] = "desc"

        if start_date and end_date:
            params[f"filter[start_range_{date_filter_field}][eq]"] = start_date
            params[f"filter[end_range_{date_filter_field}][eq]"] = end_date
            params[f"filter[{date_filter_field}_range_choice]"] = "between"
        if filters:
            for key, value in filters.items():
                if isinstance(value, dict) and isinstance(value.get("value"), list):
                    continue
                if isinstance(value, list):
                    continue
                if key == "created_by":
                    params[f"filter[{key}][]"] = value
                    continue
                if isinstance(value, dict) and "operator" in value:
                    operator = value["operator"]
                    predefined_ops = [
                        "today",
                        "yesterday",
                        "tomorrow",
                        "this_week",
                        "next_week",
                        "last_week",
                        "next_7_days",
                        "last_7_days",
                        "next_30_days",
                        "last_30_days",
                        "this_month",
                        "last_month",
                        "next_month",
                        "this_year",
                        "last_year",
                        "next_year",
                    ]

                    if operator in predefined_ops:
                        params[f"filter[range_{key}][operator]"] = operator
                    elif operator == "between":
                        start_val = value.get("start")
                        end_val = value.get("end")
                        if start_val and end_val:
                            params[f"filter[start_range_{key}][eq]"] = start_val
                            params[f"filter[end_range_{key}][eq]"] = end_val
                            params[f"filter[{key}_range_choice][operator]"] = "between"
                    else:
                        val_str = value.get("value")
                        if val_str:
                            params[f"filter[range_{key}][eq]"] = val_str

                            crm_op = operator
                            if operator == "=" or operator == "equals":
                                crm_op = "="
                            elif operator == "not_equal":
                                crm_op = "not_equal"
                            elif operator == ">":
                                crm_op = "greater_than"
                            elif operator == "<":
                                crm_op = "less_than"
                            elif operator == ">=":
                                crm_op = "greater_than_equals"
                            elif operator == "<=":
                                crm_op = "less_than_equals"

                            params[f"filter[{key}_range_choice][operator]"] = crm_op

                elif isinstance(value, str):
                    if value.startswith(">") or value.startswith("<"):
                        operator = None
                        clean_val = value

                        if value.startswith(">="):
                            operator = "greater_than_equals"
                            clean_val = value[2:].strip()
                        elif value.startswith("<="):
                            operator = "less_than_equals"
                            clean_val = value[2:].strip()
                        elif value.startswith(">"):
                            operator = "greater_than"
                            clean_val = value[1:].strip()
                        elif value.startswith("<"):
                            operator = "less_than"
                            clean_val = value[1:].strip()

                        if operator:
                            params[f"filter[range_{key}][eq]"] = clean_val
                            params[f"filter[{key}_range_choice][operator]"] = operator
                        else:
                            params[f"filter[{key}][eq]"] = value
                    else:
                        params[f"filter[{key}][eq]"] = value
                else:
                    params[f"filter[{key}][eq]"] = value
            pass

        try:
            url = f"{base_url}?{urlencode(params)}"
            if current_page == 1 or not iterate_pages:
                mcp_logger.log_step(
                    "tool_call", f"API Request Page {current_page}", {"url": url}
                )

            response = requests.get(url, headers=headers, verify=False)
            if response.status_code == 401:
                logger.warning("401 Unauthorized. Token rejected.")
                global _system_token_cache
                if _system_token_cache["access_token"] == access_token:
                    _system_token_cache["access_token"] = None
                raise ValueError("CRM Authentication Failed. Please retry.")

            response.raise_for_status()

            page_data = (
                response.json()
                .get("data", {})
                .get("attributes", {})
                .get("tableData", [])
            )
            if not page_data:
                break
            all_records.extend(page_data)
            if not iterate_pages:
                break
            if len(all_records) >= iteration_safety_limit:
                all_records = all_records[:iteration_safety_limit]
                break
            if len(page_data) < page_size:
                break
            current_page += 1
            if current_page > 500:
                break

        except Exception as e:
            mcp_logger.log_step(
                "tool_response", "API Error", {"error": str(e)}, "error"
            )
            raise

    if filters:
        all_records = _apply_local_filters(all_records, filters)
    mcp_logger.log_step(
        "tool_response", "API Success", {"total_records": len(all_records)}
    )
    processed_records = []
    for r in all_records:
        rec = {"id": r.get("id"), "module": module_key, **r}
        if CRM_UI_BASE_URL:
            if rec.get("id") and rec.get("name"):
                original_name = str(rec["name"])
                link_text = original_name.replace("[", "(").replace("]", ")")
                url = f"{CRM_UI_BASE_URL.rstrip('/')}/app/detailview/{module_key}/{rec['id']}"
                rec["name"] = f"[{link_text}]({url})"

            if rec.get("account_id") and rec.get("account_name"):
                acc_name = str(rec["account_name"])
                link_text = acc_name.replace("[", "(").replace("]", ")")
                url = f"{CRM_UI_BASE_URL.rstrip('/')}/app/detailview/Accounts/{rec['account_id']}"
                rec["account_name"] = f"[{link_text}]({url})"

            if rec.get("assigned_user_id") and rec.get("assigned_user_name"):
                user_name = str(rec["assigned_user_name"])
                link_text = user_name.replace("[", "(").replace("]", ")")
                url = f"{CRM_UI_BASE_URL.rstrip('/')}/app/detailview/Users/{rec['assigned_user_id']}"
                rec["assigned_user_name"] = f"[{link_text}]({url})"
        processed_records.append(rec)
    data_id = _cache_data(processed_records) if len(processed_records) > 0 else None
    return {
        "count": len(processed_records),
        "records": processed_records,
        "module": module_key,
        "data_id": data_id,
    }


# ============================================================================
# CHART CREATION
# ============================================================================


def create_chart_from_crm_data(
    data_id_or_records: Any,
    x_col: str,
    chart_type: str = "bar",
    title: str = "CRM Data Visualization",
    y_col: Optional[str] = None,
    color_col: Optional[str] = None,
) -> Dict[str, Any]:
    """Create Plotly chart from CRM data."""
    records = None
    if isinstance(data_id_or_records, str) and data_id_or_records.startswith(
        "crm_data_"
    ):
        records = _get_cached_data(data_id_or_records)
        if not records:
            raise ValueError(
                f"Data ID '{data_id_or_records}' expired. Please re-run query."
            )
    elif isinstance(data_id_or_records, list):
        records = data_id_or_records
    elif isinstance(data_id_or_records, dict):
        records = data_id_or_records.get("records") or _get_cached_data(
            data_id_or_records.get("data_id")
        )

    if not records:
        raise ValueError("No records to plot")

    return _plot_crm_data(
        data_list=records,
        chart_type=chart_type,
        x_col=x_col,
        y_col=y_col,
        color_col=color_col,
        title=title,
    )


def _plot_crm_data(
    data_list: List[Dict],
    chart_type: str = "bar",
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    color_col: Optional[str] = None,
    title: str = "CRM Data Visualization",
) -> Dict[str, Any]:
    try:
        if not data_list:
            raise ValueError("Empty data list")
        module = data_list[0].get("module")
        module_config = CRM_MODULES.get(module, {}) if module else {}
        rename_map = module_config.get("column_renames", {})
        df = pd.DataFrame(data_list)

        cols_to_clean = [col for col in [x_col, y_col, color_col] if col]
        for col in cols_to_clean:
            if col in df.columns and df[col].dtype == object:
                df[col] = (
                    df[col].astype(str).replace(r"\[(.*?)\]\(.*?\)", r"\1", regex=True)
                )
        available_cols = df.columns.tolist()
        logger.info(f"Available columns for plotting: {available_cols}")
        if not x_col:
            raise ValueError("No column specified for x-axis.")

        if x_col not in available_cols:
            raise ValueError(f"Column '{x_col}' not found in data.")

        df = df[df[x_col].notna()]
        df = df[df[x_col].astype(str).str.strip() != ""]

        if df.empty:
            raise ValueError(f"All records have empty values for field '{x_col}'")

        if y_col is None:
            df = df.groupby(x_col, dropna=True).size().reset_index(name="Count")
            y_col = "Count"
            df = df.sort_values(by="Count", ascending=False)

        elif y_col in available_cols:
            if df[y_col].dtype == object:
                df[y_col] = (
                    df[y_col].astype(str).str.replace(r"[^\d.-]", "", regex=True)
                )
                df[y_col] = pd.to_numeric(df[y_col], errors="coerce").fillna(0)
            df = df.groupby(x_col, as_index=False)[y_col].sum()
            df = df.sort_values(by=y_col, ascending=False)

        else:
            logger.warning(f"Y column '{y_col}' not found. Defaulting to Count.")
            df = df.groupby(x_col, dropna=True).size().reset_index(name="Count")
            y_col = "Count"
            df = df.sort_values(by="Count", ascending=False)

        x_display = rename_map.get(x_col, x_col.replace("_", " ").title())
        y_display = (
            rename_map.get(y_col, y_col.replace("_", " ").title())
            if y_col != "Count"
            else "Count"
        )
        total_items = len(df)
        analysis_text = f"Analyzed **{total_items}** groups/items."

        try:
            if y_col in df.columns:
                max_row = df.loc[df[y_col].idxmax()]
                min_row = df.loc[df[y_col].idxmin()]

                max_val = (
                    f"{max_row[y_col]:,.2f}"
                    if isinstance(max_row[y_col], (int, float))
                    else max_row[y_col]
                )
                min_val = (
                    f"{min_row[y_col]:,.2f}"
                    if isinstance(min_row[y_col], (int, float))
                    else min_row[y_col]
                )

                analysis_text += (
                    f"\n- **Top ({x_display})**: {max_row[x_col]} ({max_val})"
                )

                if total_items > 1:
                    analysis_text += (
                        f"\n- **Lowest ({x_display})**: {min_row[x_col]} ({min_val})"
                    )

                if pd.api.types.is_numeric_dtype(df[y_col]):
                    total_val = df[y_col].sum()
                    analysis_text += f"\n- **Total {y_display}**: {total_val:,.2f}"
        except Exception as e:
            logger.warning(f"Failed to generate stats: {e}")

        warning_msg = None
        limit = 20

        if len(df) > limit:
            warning_msg = (
                f"**Note:** Display limit reached. Showing top {limit} of {len(df)} categories. "
                "The chart focuses on the most significant data points."
            )
            logger.warning(
                f"Too many data points ({len(df)}). Limiting to top {limit}."
            )

            if chart_type == "pie":
                top_df = df.head(limit)
                other_val = df.iloc[limit:][y_col].sum()
                other_row = pd.DataFrame({x_col: ["Others"], y_col: [other_val]})
                df = pd.concat([top_df, other_row], ignore_index=True)
            else:
                df = df.head(limit)

        col_labels = {}
        for col in df.columns:
            if col == "Count":
                col_labels[col] = "Count"
            else:
                col_labels[col] = rename_map.get(col, col.replace("_", " ").title())

        def create_figure():
            if chart_type == "bar":
                return px.bar(
                    df,
                    x=x_col,
                    y=y_col,
                    color=color_col,
                    title=title,
                    labels=col_labels,
                    template="plotly_white",
                    text_auto=".2s",
                )
            elif chart_type == "line":
                return px.line(
                    df,
                    x=x_col,
                    y=y_col,
                    color=color_col,
                    title=title,
                    labels=col_labels,
                    markers=True,
                )
            elif chart_type == "scatter":
                return px.scatter(
                    df,
                    x=x_col,
                    y=y_col,
                    color=color_col,
                    title=title,
                    labels=col_labels,
                )
            elif chart_type == "pie":
                return px.pie(
                    df, names=x_col, values=y_col, title=title, labels=col_labels
                )
            else:
                return px.bar(df, x=x_col, y=y_col, title=title, labels=col_labels)

        fig = create_figure()
        fig.update_layout(
            font=dict(size=12),
            showlegend=True,
            hovermode="closest",
            xaxis_title=col_labels.get(x_col, x_col),
            yaxis_title=col_labels.get(y_col, y_col),
        )
        os.makedirs(plot_dir, exist_ok=True)
        filename = f"plot_{uuid.uuid4().hex}.json"
        file_path = plot_dir / filename

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(fig.to_json())

        with open(file_path, "r", encoding="utf-8") as f:
            full_plot_json = json.load(f)

        return {
            "filename": filename,
            "url": f"{API_DOMAIN_URL.rstrip('/')}/api/v1/plots/{filename}",
            "type": "plotly_json",
            "chart_type": chart_type,
            "x_field": x_col,
            "y_field": y_col,
            "data_points": len(df),
            "data": full_plot_json.get("data", []),
            "layout": full_plot_json.get("layout", {}),
            "analysis": analysis_text,
            "warning": warning_msg,
        }
    except Exception as e:
        logger.error(f"Chart generation failed: {e}", exc_info=True)
        raise ValueError(f"Chart failed: {e}")


def clear_crm_cache() -> bool:
    """Clear all CRM cache entries."""
    try:
        _data_cache.clear()
        logger.info("CRM cache cleared")
        return True
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        return False
