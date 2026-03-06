#!/usr/bin/env python3
"""
Purchase Reconciliation Engine
"""

import json
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import re
import base64
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ENUMS & DATA CLASSES

class ReconciliationStatus(Enum):
    """Reconciliation match status."""
    LOOP1_SUGGESTED = "loop1_suggested"
    LOOP2_HEALED = "loop2_healed"
    LOOP3_VERIFIED = "loop3_verified"
    LOOP4_FALLBACK = "loop4_fallback"


@dataclass
class ReconciliationMatch:
    """Represents a reconciliation match between purchase and transaction."""
    skybox_id: str
    reveal_id: str
    purchase_activity_id: Optional[int]
    banking_transaction_id: int
    amount: float
    cc: Optional[str]
    status: ReconciliationStatus
    time_diff: Optional[float] = None


# CONFIGURATION

class Config:
    """Application configuration - centralized settings."""
    
    # Skybox API
    SKYBOX_BASE_URL: str = 'https://skybox.vividseats.com/services'
    SKYBOX_API_TOKEN: str = '8293a10f-6546-457c-8644-2b58a753617a'
    SKYBOX_ACCOUNT: str = '5052'
    SKYBOX_APP_TOKEN: str = '2140c962-2c86-4826-899a-20e6ae8fad31'
    
    # Reveal API
    REVEAL_BASE_URL: str = 'https://portal.revealmarkets.com/public/api/v1'
    REVEAL_API_TOKEN: str = 'Token 8915365073157a5e061cc9174ef262419b1220e9'
    
    # Gmail
    GMAIL_SCOPES: List[str] = ["https://www.googleapis.com/auth/gmail.readonly"]
    GMAIL_DELEGATED_USER: str = "pos@lotustickets.com"
    GMAIL_CREDENTIALS_PATH: str = 'credentials.json'
    
    # CC Mapping
    CC_MAPPING_FILE: str = 'credit_card_mapping.json'
    
    # Reconciliation Rules
    DATE_TOLERANCE_DAYS: int = 3
    TIME_TOLERANCE_MINUTES: float = 5
    AMOUNT_TOLERANCE: float = 0.01
    
    # Data Fetch
    FETCH_DATE_FROM: str = '2026-02-01'
    FETCH_DATE_TO: str = '2026-02-04'
    MIN_OUTSTANDING_BALANCE: float = 0.01
    GMAIL_SEARCH_DAYS_BACK: int = 7
    
    # API Timeouts
    API_TIMEOUT_SECONDS: int = 30
    
    # Logging
    LOG_LEVEL: int = logging.INFO
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_FILE: str = 'reconciliation.log'


# LOGGING SETUP

def setup_logger(name: str) -> logging.Logger:
    """Configure logger with file and console handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(Config.LOG_LEVEL)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(Config.LOG_LEVEL)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    
    # File handler
    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(Config.LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


logger = setup_logger(__name__)

# CC MAPPING CLASS

class CCMapper:
    """Maps credit card last 4 digits to Skybox internal card IDs."""
    
    def __init__(self, mapping_file: str = Config.CC_MAPPING_FILE) -> None:
        """Initialize CC Mapper with mapping data."""
        self.mapping_file: str = mapping_file
        self.mapping: Dict[str, Dict[str, Any]] = {}
        self.load_mapping()
    
    def load_mapping(self) -> bool:
        """Load CC mapping from JSON file."""
        try:
            if Path(self.mapping_file).exists():
                with open(self.mapping_file, 'r') as f:
                    self.mapping = json.load(f)
                logger.info(f" Loaded CC mapping from {self.mapping_file}")
                logger.info(f"  Total cards in mapping: {len(self.mapping)}")
                return True
            else:
                logger.warning(f" CC mapping file not found: {self.mapping_file}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f" Error parsing CC mapping JSON: {e}")
            return False
        except Exception as e:
            logger.error(f" Error loading CC mapping: {e}")
            return False
    
    def get_skybox_card_id(self, last_4_digits: Optional[str], account_name: Optional[str] = None) -> Optional[int]:
        """Get Skybox card ID for given last 4 digits."""
        if not last_4_digits:
            return None
        
        last_4: str = ''.join(filter(str.isdigit, str(last_4_digits)))
        
        if last_4 not in self.mapping:
            logger.debug(f" Card ending in {last_4} not found in CC mapping")
            return None
        
        card_info: Dict[str, Any] = self.mapping[last_4]
        
        if account_name and card_info.get('account_name') != account_name:
            logger.debug(
                f"⚠ Account name mismatch for card {last_4}: "
                f"expected '{account_name}', got '{card_info.get('account_name')}'"
            )
            return None
        
        skybox_id: Optional[int] = card_info.get('creditCardId')
        logger.debug(f" Found Skybox card ID {skybox_id} for card ending in {last_4}")
        return skybox_id
    
    def get_credit_card_group_id(self, last_4_digits: Optional[str]) -> Optional[int]:
        """Get credit card group ID for given last 4 digits."""
        if not last_4_digits:
            return None
        
        last_4: str = ''.join(filter(str.isdigit, str(last_4_digits)))
        
        if last_4 not in self.mapping:
            return None
        
        card_info: Dict[str, Any] = self.mapping[last_4]
        group_id: Optional[int] = card_info.get('creditCardGroupId')
        logger.debug(f" Found credit card group ID {group_id} for card ending in {last_4}")
        return group_id
    
    def is_card_mapped(self, last_4_digits: Optional[str]) -> bool:
        """Check if a card is in the mapping."""
        if not last_4_digits:
            return False
        last_4: str = ''.join(filter(str.isdigit, str(last_4_digits)))
        return last_4 in self.mapping


# UTILITY FUNCTIONS

def parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO 8601 date string to datetime object.
    
    Handles various ISO 8601 formats including timezone information.
    Returns None if parsing fails or input is None.
    
    Args:
        date_str: ISO 8601 formatted date string (e.g., "2026-03-06T10:30:00Z")
    
    Returns:
        datetime object without timezone info, or None if parsing fails
    
    Examples:
        >>> parse_iso_date("2026-03-06T10:30:00Z")
        datetime(2026, 3, 6, 10, 30, 0)
        >>> parse_iso_date(None)
        None
    """
    if not date_str:
        return None
    try:
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.replace(tzinfo=None)
        except ValueError:
            return datetime.fromisoformat(date_str.split('+')[0].split('Z')[0])
    except Exception as e:
        logger.debug(f"Failed to parse date {date_str}: {e}")
        return None


def days_between_dates(date1: Optional[datetime], date2: Optional[datetime]) -> int:
    """
    Calculate absolute days between two dates.
    
    Returns 999 if either date is None (used as sentinel value for no match).
    
    Args:
        date1: First datetime object
        date2: Second datetime object
    
    Returns:
        Absolute number of days between dates, or 999 if either is None
    """
    if not date1 or not date2:
        return 999
    return abs((date1 - date2).days)


def minutes_between_dates(date1: Optional[datetime], date2: Optional[datetime]) -> float:
    """
    Calculate absolute minutes between two dates.
    
    Returns 999 if either date is None (used as sentinel value for no match).
    
    Args:
        date1: First datetime object
        date2: Second datetime object
    
    Returns:
        Absolute number of minutes between dates, or 999 if either is None
    """
    if not date1 or not date2:
        return 999
    return abs((date1 - date2).total_seconds()) / 60


def extract_last_four_from_notes(internal_notes: Optional[str]) -> Optional[str]:
    """
    Extract last 4 credit card digits from internal notes.
    
    Searches for patterns like "CC#1234", "CC 1234", "CARD#1234", "CARD 1234".
    Case-insensitive matching.
    
    Args:
        internal_notes: Internal notes string from purchase record
    
    Returns:
        Last 4 digits as string, or None if not found
    
    Examples:
        >>> extract_last_four_from_notes("Order 123 CC#6339")
        "6339"
        >>> extract_last_four_from_notes("No card info")
        None
    """
    if not internal_notes:
        return None
    match = re.search(r'(?:cc|card)\s*#?\s*(\d{4})', internal_notes, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def extract_last_four_from_reveal(transaction: Dict[str, Any]) -> Optional[str]:
    """
    Extract last 4 credit card digits from Reveal transaction.
    
    Attempts extraction from multiple sources in order:
    1. plaid_mask from account
    2. sub_account field
    3. Account name pattern matching
    
    Args:
        transaction: Reveal transaction dictionary
    
    Returns:
        Last 4 digits as string, or None if not found
    """
    if not transaction:
        return None
    
    account = transaction.get('account', {})
    plaid_mask = account.get('plaid_mask')
    if plaid_mask:
        return str(plaid_mask)[-4:]
    
    sub_account = transaction.get('sub_account')
    if sub_account:
        return str(sub_account)[-4:]
    
    account_name = account.get('name', '')
    if account_name:
        match = re.search(r'ending\s+(?:in\s+)?(\d{4})', account_name, re.IGNORECASE)
        if match:
            return match.group(1)
        matches = re.findall(r'\b(\d{4})\b', account_name)
        if matches:
            return matches[-1]
    
    return None


def extract_cc_from_email_body(body: str) -> Optional[str]:
    """
    Extract credit card last 4 digits from email body.
    
    Searches for multiple patterns:
    - "CC#1234", "CC 1234", "CARD#1234", "CARD 1234"
    - "card ending in 1234"
    - "VISA - 1234", "AMEX - 1234", etc.
    
    Args:
        body: Email body text
    
    Returns:
        Last 4 digits as string, or None if not found
    """
    if not body:
        return None
    
    # Try CC/CARD pattern first
    match = re.search(r'(?:cc|card)\s*#?\s*(\d{4})', body, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Try "ending in" pattern
    match = re.search(r'(?:card\s+)?ending\s+(?:in\s+)?(\d{4})', body, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Try card brand pattern
    match = re.search(r'(?:VISA|AMEX|MASTERCARD|DISCOVER)\s*-\s*(\d{4})', body, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None


def extract_gmail_body(payload: Dict[str, Any]) -> str:
    """
    Extract email body text from Gmail API payload.
    
    Handles both multipart and single-part messages.
    Decodes base64-encoded content.
    
    Args:
        payload: Gmail message payload dictionary
    
    Returns:
        Decoded email body text, or empty string if extraction fails
    """
    if not payload:
        return ""
    
    try:
        if "parts" in payload:
            for part in payload["parts"]:
                if part.get("mimeType") == "text/plain":
                    data = part.get("body", {}).get("data", "")
                    if data:
                        return base64.urlsafe_b64decode(data).decode("utf-8")
        else:
            data = payload.get("body", {}).get("data", "")
            if data:
                return base64.urlsafe_b64decode(data).decode("utf-8")
        return ""
    except Exception as e:
        logger.debug(f"Failed to extract Gmail body: {e}")
        return ""


# API FUNCTIONS

def get_purchases() -> List[Dict[str, Any]]:
    """
    Fetch unpaid purchases from Skybox API.
    
    Retrieves credit card purchases with outstanding balance >= MIN_OUTSTANDING_BALANCE
    within the specified date range.
    
    Returns:
        List of purchase dictionaries from Skybox, or empty list on error
    
    Raises:
        Logs errors but does not raise exceptions (graceful degradation)
    """
    try:
        url = f'{Config.SKYBOX_BASE_URL}/purchases'
        headers = {
            'X-Api-Token': Config.SKYBOX_API_TOKEN,
            'X-Account': Config.SKYBOX_ACCOUNT,
            'X-Application-Token': Config.SKYBOX_APP_TOKEN,
            'Content-Type': 'application/json'
        }
        
        start_date = datetime.fromisoformat(Config.FETCH_DATE_FROM)
        end_date = datetime.fromisoformat(Config.FETCH_DATE_TO)
        
        formatted_start = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        formatted_end = end_date.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT%H:%M:%S.999Z")
        
        params = {
            'paymentStatus': 'UNPAID',
            'createdDateFrom': formatted_start,
            'createdDateTo': formatted_end,
            'minOutstandingBalance': Config.MIN_OUTSTANDING_BALANCE,
            'paymentMethod': 'CREDITCARD'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=Config.API_TIMEOUT_SECONDS)
        response.raise_for_status()
        
        data = response.json()
        purchases = data.get('rows', [])
        logger.info(f"? Fetched {len(purchases)} purchases from Skybox")
        return purchases
        
    except requests.RequestException as e:
        logger.error(f"✗ Failed to fetch purchases: {e}")
        return []
    except (ValueError, KeyError) as e:
        logger.error(f"✗ Error parsing Skybox response: {e}")
        return []


def get_reveal_transactions() -> List[Dict[str, Any]]:
    """
    Fetch banking transactions from Reveal Markets API.
    
    Retrieves transactions within the specified date range with pagination support.
    
    Returns:
        List of transaction dictionaries from Reveal, or empty list on error
    
    Raises:
        Logs errors but does not raise exceptions (graceful degradation)
    """
    try:
        url = f'{Config.REVEAL_BASE_URL}/purchasing/banking-transactions'
        headers = {
            "Authorization": Config.REVEAL_API_TOKEN,
            "Content-Type": "application/json"
        }
        
        params = {
            "date_from": Config.FETCH_DATE_FROM,
            "date_to": Config.FETCH_DATE_TO,
            "page_size": 10000
        }
        
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        transactions = data.get('results', [])
        logger.info(f"? Fetched {len(transactions)} transactions from Reveal")
        return transactions
        
    except requests.RequestException as e:
        logger.error(f"✗ Failed to fetch transactions: {e}")
        return []
    except (ValueError, KeyError) as e:
        logger.error(f"✗ Error parsing Reveal response: {e}")
        return []


def update_purchase_notes(purchase_id: str, internal_notes: str) -> bool:
    """
    Update purchase internal notes in Skybox.
    
    TESTING MODE - API CALL DISABLED - No real-time updates to Skybox.
    
    Args:
        purchase_id: Skybox purchase ID
        internal_notes: Internal notes text to set
    
    Returns:
        True (simulated success for testing)
    """
    # TESTING MODE - Disabled for testing phase
    logger.info(f"[TEST MODE] Would update purchase {purchase_id} with notes: {internal_notes}")
    return True


def update_skybox_purchase_card(purchase_id: str, credit_card_id: int, credit_card_group_id: int) -> bool:
    """
    Update Skybox purchase with credit card information.
    
    TESTING MODE - API CALL DISABLED - No real-time updates to Skybox.
    
    Sets the creditCardId and creditCardGroupId for a purchase, completing
    the reconciliation on the Skybox side.
    
    Args:
        purchase_id: Skybox purchase ID
        credit_card_id: Skybox internal credit card ID
        credit_card_group_id: Credit card group ID from mapping
    
    Returns:
        True (simulated success for testing)
    """
    # TESTING MODE - Disabled for testing phase
    logger.info(f"[TEST MODE] Would update Skybox purchase {purchase_id} with card {credit_card_id} (group {credit_card_group_id})")
    return True


def create_reveal_matching_group(purchase_activity_id: int, banking_transaction_id: int) -> bool:
    """
    Create matching group in Reveal Markets to mark transaction as reconciled.
    
    TESTING MODE - API CALL DISABLED - No real-time updates to Reveal.
    
    Links a Reveal purchase activity with a banking transaction, marking them
    as reconciled in the Reveal system.
    
    Args:
        purchase_activity_id: Reveal purchase activity ID (from range_match.id)
        banking_transaction_id: Reveal banking transaction ID
    
    Returns:
        True (simulated success for testing)
    """
    # TESTING MODE - Disabled for testing phase
    logger.info(
        f"[TEST MODE] Would create Reveal matching group: "
        f"purchase_activity={purchase_activity_id}, "
        f"banking_transactions={banking_transaction_id}"
    )
    return True


def initialize_gmail_service() -> Optional[Any]:
    """
    Initialize Gmail API service with service account credentials.
    
    Uses service account delegation to access Gmail on behalf of the
    configured delegated user (GMAIL_DELEGATED_USER).
    
    Returns:
        Gmail service object if successful, None otherwise
    
    Raises:
        Logs errors but does not raise exceptions (graceful degradation)
    """
    try:
        with open(Config.GMAIL_CREDENTIALS_PATH, 'r') as f:
            sa_info = json.load(f)
        
        creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=Config.GMAIL_SCOPES
        )
        delegated_creds = creds.with_subject(Config.GMAIL_DELEGATED_USER)
        service = build("gmail", "v1", credentials=delegated_creds)
        
        logger.info("? Gmail service initialized")
        return service
        
    except FileNotFoundError as e:
        logger.error(f"✗ Gmail credentials file not found: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"✗ Invalid Gmail credentials JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"✗ Failed to initialize Gmail service: {e}")
        return None


def search_gmail_by_order_number(
    gmail_service: Optional[Any],
    order_number: str,
    days_back: int = Config.GMAIL_SEARCH_DAYS_BACK
) -> List[Dict[str, str]]:
    """
    Search Gmail for emails containing order number.
    
    Searches emails from the past N days (configurable) for messages
    containing the specified order number.
    
    Args:
        gmail_service: Gmail API service object (can be None)
        order_number: Order number to search for
        days_back: Number of days to search back (default: GMAIL_SEARCH_DAYS_BACK)
    
    Returns:
        List of email dictionaries with keys: id, from, subject, body, snippet
        Returns empty list if service is None or search fails
    
    Raises:
        Logs errors but does not raise exceptions (graceful degradation)
    """
    if not gmail_service:
        logger.debug("Gmail service not available, skipping search")
        return []
    
    if not order_number:
        logger.debug("Order number is empty, skipping Gmail search")
        return []
    
    try:
        query = f'"{order_number}" newer_than:{days_back}d'
        results = gmail_service.users().messages().list(
            userId="me",
            q=query,
            maxResults=20
        ).execute()
        
        messages = results.get("messages", [])
        emails: List[Dict[str, str]] = []
        
        for msg in messages:
            try:
                full_msg = gmail_service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="full"
                ).execute()
                
                headers = {h["name"]: h["value"] for h in full_msg["payload"]["headers"]}
                body = extract_gmail_body(full_msg["payload"])
                
                emails.append({
                    'id': msg["id"],
                    'from': headers.get('From', ''),
                    'subject': headers.get('Subject', ''),
                    'body': body,
                    'snippet': full_msg.get('snippet', '')
                })
            except HttpError as e:
                logger.debug(f"Failed to fetch email {msg['id']}: {e}")
                continue
            except Exception as e:
                logger.debug(f"Unexpected error fetching email {msg['id']}: {e}")
                continue
        
        logger.debug(f"Found {len(emails)} emails for order {order_number}")
        return emails
        
    except HttpError as e:
        logger.error(f"✗ Gmail API error: {e}")
        return []
    except Exception as e:
        logger.error(f"✗ Failed to search Gmail: {e}")
        return []


# MAIN RECONCILIATION - 4 LOOPS

def main() -> bool:
    """
    Main reconciliation engine - 4-step algorithm.
    
    Orchestrates the complete purchase reconciliation workflow:
    
    LOOP 1 - Suggested Match:
        Uses Reveal's suggested matches (range_matches) with verification:
        - Date within +/- 3 days
        - Amount matches exactly
        - Credit card last 4 matches
        - Selects match with closest timestamp
    
    LOOP 2 - Healer (Gmail Fallback):
        Finds purchases with missing CC info and attempts to heal via Gmail:
        - Searches for order number in emails
        - Extracts CC from email body
        - Tags purchases as 'missingCC' if CC not found or not in mapping
    
    LOOP 3 - Verified Match (Post-Healing):
        Re-matches after healing with same criteria as Loop 1:
        - Uses healed CC information
        - Skips purchases tagged as 'missingCC'
    
    LOOP 4 - Hail Mary (Last Resort):
        Amount + Timestamp fallback for unmatched purchases:
        - Amount matches exactly
        - Timestamp within +/- 5 minutes
        - Ignores credit card (last resort)
        - Only processes purchases tagged as 'missingCC'
    
    Returns:
        True if reconciliation completed successfully, False otherwise
    
    Raises:
        Logs all errors but does not raise exceptions (graceful degradation)
    """
    
    logger.info("\n" + "="*80)
    logger.info("PURCHASE RECONCILIATION ENGINE - STARTING")
    logger.info("="*80 + "\n")
    
    # Initialize components
    cc_mapper = CCMapper()
    purchases = get_purchases()
    transactions = get_reveal_transactions()
    gmail_service = initialize_gmail_service()
    
    if not purchases or not transactions:
        logger.warning("⚠ No data fetched - cannot proceed with reconciliation")
        return False
    
    # Normalize purchase amounts once after fetching
    for p in purchases:
        p['normalized_amount'] = p.get('outstandingBalance') or p.get('amount') or 0
    
    matched_purchase_ids: set = set()
    loop1_matches: List[Dict] = []
    loop2_healed: int = 0
    loop3_matches: List[Dict] = []
    loop4_matches: List[Dict] = []
    
    # LOOP 1: The "Suggested" Match
    logger.info("="*80)
    logger.info("LOOP 1: SUGGESTED MATCH")
    logger.info("Rule: Use Reveal's suggested matches (range_matches)")
    logger.info("Verify: Date (+/- 3 days), Exact Amount, CC Last 4")
    logger.info("="*80 + "\n")
    
    for reveal_tx in transactions:
        reveal_id = reveal_tx.get('id')
        reveal_amount = abs(reveal_tx.get('amount', 0))
        reveal_date = parse_iso_date(
            reveal_tx.get("authorized_date") or reveal_tx.get("date")
        )
        reveal_cc = extract_last_four_from_reveal(reveal_tx)
        
        if not reveal_date:
            continue
        
        range_matches = reveal_tx.get('range_matches', [])
        if not range_matches:
            continue
        
        candidates = []
        
        for range_match in range_matches:
            purchase_activity_id = range_match.get('id')
            range_amount = abs(range_match.get('amount', 0))
            range_date = parse_iso_date(range_match.get('date'))
            
            if not range_date:
                continue
            
            # Verify range_amount matches reveal_amount
            if abs(range_amount - reveal_amount) > Config.AMOUNT_TOLERANCE:
                continue
            
            # Move reveal_card_id lookup outside purchase loop for performance
            reveal_card_id = cc_mapper.get_skybox_card_id(reveal_cc) if reveal_cc else None
            
            for purchase in purchases:
                if purchase.get('id') in matched_purchase_ids:
                    continue
                
                purchase_id = purchase.get('id')
                purchase_amount = purchase['normalized_amount']
                purchase_date = parse_iso_date(purchase.get('createdDate'))
                purchase_cc = extract_last_four_from_notes(purchase.get('internalNotes'))
                
                if not purchase_date:
                    continue
                
                date_diff = days_between_dates(purchase_date, range_date)
                amount_match = abs(purchase_amount - range_amount) < Config.AMOUNT_TOLERANCE
                
                purchase_card_id = cc_mapper.get_skybox_card_id(purchase_cc) if purchase_cc else None
                
                cc_match = (
                    purchase_card_id is not None and
                    reveal_card_id is not None and
                    purchase_card_id == reveal_card_id
                )
                
                if date_diff <= Config.DATE_TOLERANCE_DAYS and amount_match and cc_match:
                    time_diff = minutes_between_dates(purchase_date, range_date)
                    candidates.append({
                        'skybox_id': purchase_id,
                        'reveal_id': reveal_id,
                        'purchase_activity_id': purchase_activity_id,
                        'banking_transaction_id': reveal_id,
                        'amount': purchase_amount,
                        'cc': purchase_cc,
                        'time_diff': time_diff,
                        'reveal_cc': reveal_cc
                    })
        
        if candidates:
            best = min(candidates, key=lambda x: x['time_diff'])
            logger.info(f" LOOP 1 MATCH: Purchase {best['skybox_id']} -> Reveal {best['reveal_id']}")
            logger.info(f"   Amount: ${best['amount']} | CC: {best['cc']} | Time diff: {best['time_diff']:.1f} min")
            
            # API calls commented for testing
            # credit_card_id = cc_mapper.get_skybox_card_id(best['cc'])
            # credit_card_group_id = cc_mapper.get_credit_card_group_id(best['cc'])
            # if credit_card_id and credit_card_group_id:
            #     update_skybox_purchase_card(best['skybox_id'], credit_card_id, credit_card_group_id)
            #     create_reveal_matching_group(best['purchase_activity_id'], best['banking_transaction_id'])
            
            loop1_matches.append(best)
            matched_purchase_ids.add(best['skybox_id'])
    
    logger.info(f"\n[OK] Loop 1: {len(loop1_matches)} matches\n")
    
    # LOOP 2: The "Healer" (Gmail Fallback)
    logger.info("="*80)
    logger.info("LOOP 2: HEALER (Gmail Fallback)")
    logger.info("Rule: Find POs where CC is missing, search Gmail")
    logger.info("="*80 + "\n")
    
    purchases_needing_cc = [
        p for p in purchases
        if extract_last_four_from_notes(p.get("internalNotes")) is None
        and p.get("outstandingBalance", 0) >= Config.MIN_OUTSTANDING_BALANCE
    ]
    
    logger.info(f"[OK] Found {len(purchases_needing_cc)} purchases needing CC info")
    
    if not gmail_service:
        logger.warning(" Gmail service unavailable. Skipping email healing.")
    else:
        for purchase in purchases_needing_cc:
            purchase_id = purchase.get('id')
            order_number = purchase.get('externalRef')
            
            if not order_number:
                continue
            
            emails = search_gmail_by_order_number(gmail_service, order_number)
            
            if emails:
                cc_found = None
                email_used = None
                
                for email in emails:
                    body = email.get('body', '') + ' ' + email.get('snippet', '')
                    cc = extract_cc_from_email_body(body)
                    
                    if cc:
                        cc_found = cc
                        email_used = email.get('from', '')
                        break
                
                if cc_found:
                    if cc_mapper.is_card_mapped(cc_found):
                        internal_notes = f"{order_number} {email_used} CC#{cc_found}"
                        purchase['internalNotes'] = internal_notes
                        
                        # API call commented for testing
                        # update_purchase_notes(purchase_id, internal_notes)
                        
                        logger.info(f" HEALED: Purchase {purchase_id} - Found CC {cc_found}")
                        loop2_healed += 1
                    else:
                        logger.warning(f"  Card {cc_found} not in CC mapping. Tagging as missingCC.")
                        purchase['internalNotes'] = 'missingCC'
                        logger.info(f"  TAGGED: Purchase {purchase_id} - CC {cc_found} not in mapping")
                else:
                    purchase['internalNotes'] = 'missingCC'
                    logger.info(f"  TAGGED: Purchase {purchase_id} - CC not found in emails")
            else:
                purchase['internalNotes'] = 'missingCC'
                logger.info(f"  TAGGED: Purchase {purchase_id} - No emails found")
    
    logger.info(f"\n[OK] Loop 2: {loop2_healed} healed, {len(purchases_needing_cc) - loop2_healed} tagged\n")
    
    # LOOP 3: The "Verified" Match (Post-Healing)
    logger.info("="*80)
    logger.info("LOOP 3: VERIFIED MATCH (Post-Healing)")
    logger.info("Rule: Re-match after healing (same 3 checks)")
    logger.info("="*80 + "\n")
    
    for reveal_tx in transactions:
        reveal_id = reveal_tx.get('id')
        reveal_amount = abs(reveal_tx.get('amount', 0))
        reveal_date = parse_iso_date(
            reveal_tx.get("authorized_date") or reveal_tx.get("date")
        )
        reveal_cc = extract_last_four_from_reveal(reveal_tx)
        
        if not reveal_date:
            continue
        
        range_matches = reveal_tx.get('range_matches', [])
        if not range_matches:
            continue
        
        for range_match in range_matches:
            purchase_activity_id = range_match.get('id')
            range_amount = abs(range_match.get('amount', 0))
            range_date = parse_iso_date(range_match.get('date'))
            
            if not range_date:
                continue
            
            reveal_card_id = cc_mapper.get_skybox_card_id(reveal_cc)
            
            for purchase in purchases:
                if purchase.get('id') in matched_purchase_ids:
                    continue
                
                # Use exact comparison instead of 'in' to avoid false matches
                notes = purchase.get('internalNotes')
                if notes == 'missingCC':
                    continue
                
                purchase_id = purchase.get('id')
                purchase_amount = purchase['normalized_amount']
                purchase_date = parse_iso_date(purchase.get('createdDate'))
                purchase_cc = extract_last_four_from_notes(purchase.get('internalNotes'))
                
                if not purchase_date or not purchase_cc:
                    continue
                
                purchase_card_id = cc_mapper.get_skybox_card_id(purchase_cc)
                
                date_diff = days_between_dates(purchase_date, range_date)
                amount_match = abs(purchase_amount - range_amount) < Config.AMOUNT_TOLERANCE
                cc_match = (
                    purchase_card_id is not None and
                    reveal_card_id is not None and
                    purchase_card_id == reveal_card_id
                )
                
                if date_diff <= Config.DATE_TOLERANCE_DAYS and amount_match and cc_match:
                    time_diff = minutes_between_dates(purchase_date, range_date)
                    logger.info(f" LOOP 3 MATCH: Purchase {purchase_id} -> Reveal {reveal_id}")
                    logger.info(f"   Amount: ${purchase_amount} | CC: {purchase_cc} | Time diff: {time_diff:.1f} min")
                    
                    # API calls commented for testing
                    # credit_card_id = cc_mapper.get_skybox_card_id(purchase_cc)
                    # credit_card_group_id = cc_mapper.get_credit_card_group_id(purchase_cc)
                    # if credit_card_id and credit_card_group_id:
                    #     update_skybox_purchase_card(purchase_id, credit_card_id, credit_card_group_id)
                    #     create_reveal_matching_group(purchase_activity_id, reveal_id)
                    
                    loop3_matches.append({
                        'loop': 3,
                        'skybox_id': purchase_id,
                        'reveal_id': reveal_id,
                        'purchase_activity_id': purchase_activity_id,
                        'banking_transaction_id': reveal_id,
                        'amount': purchase_amount,
                        'cc': purchase_cc
                    })
                    
                    matched_purchase_ids.add(purchase_id)
                    break
    
    logger.info(f"\n[OK] Loop 3: {len(loop3_matches)} matches\n")
    
    # LOOP 4: The "Hail Mary" (Last Resort)
    logger.info("="*80)
    logger.info("LOOP 4: HAIL MARY (Last Resort)")
    logger.info("Rule: Amount + Timestamp (+/- 5 minutes), ignore CC")
    logger.info("="*80 + "\n")
    
    for purchase in purchases:
        if purchase.get('id') in matched_purchase_ids:
            continue
        
        # Use exact comparison instead of 'in' to avoid false matches
        notes = purchase.get('internalNotes')
        if notes != 'missingCC':
            continue
        
        purchase_id = purchase.get('id')
        purchase_amount = purchase['normalized_amount']
        purchase_date = parse_iso_date(purchase.get('createdDate'))
        
        if not purchase_date:
            continue
        
        best_match = None
        best_time_diff = Config.TIME_TOLERANCE_MINUTES + 1
        best_purchase_activity_id = None
        
        for reveal_tx in transactions:
            reveal_id = reveal_tx.get('id')
            reveal_amount = abs(reveal_tx.get('amount', 0))
            reveal_date = parse_iso_date(
                reveal_tx.get("authorized_date") or reveal_tx.get("date")
            )
            
            if not reveal_date:
                continue
            
            time_diff = minutes_between_dates(purchase_date, reveal_date)
            amount_match = abs(purchase_amount - reveal_amount) < Config.AMOUNT_TOLERANCE
            
            if amount_match and time_diff <= Config.TIME_TOLERANCE_MINUTES:
                if time_diff < best_time_diff:
                    best_match = (reveal_id, time_diff)
                    best_time_diff = time_diff
                    
                    range_matches = reveal_tx.get('range_matches', [])
                    if range_matches:
                        best_purchase_activity_id = range_matches[0].get('id')
        
        if best_match:
            reveal_id, time_diff = best_match
            logger.info(f"LOOP 4 MATCH: Purchase {purchase_id} -> Reveal {reveal_id}")
            logger.info(f"Amount: ${purchase_amount} | Time diff: {time_diff:.1f} minutes")
            
            loop4_matches.append({
                'loop': 4,
                'skybox_id': purchase_id,
                'reveal_id': reveal_id,
                'purchase_activity_id': best_purchase_activity_id,
                'banking_transaction_id': reveal_id,
                'amount': purchase_amount,
                'time_diff_minutes': time_diff
            })
            
            matched_purchase_ids.add(purchase_id)
    
    logger.info(f"\n[OK] Loop 4: {len(loop4_matches)} matches\n")

    # SUMMARY
    logger.info("="*80)
    logger.info("RECONCILIATION SUMMARY")
    logger.info("="*80)
    logger.info(f"Loop 1 (Suggested): {len(loop1_matches)} matches")
    logger.info(f"Loop 2 (Healer): {loop2_healed} healed")
    logger.info(f"Loop 3 (Verified): {len(loop3_matches)} matches")
    logger.info(f"Loop 4 (Hail Mary): {len(loop4_matches)} matches")
    logger.info(f"\nTotal matches: {len(loop1_matches) + len(loop3_matches) + len(loop4_matches)}")
    logger.info(f"Total matched purchases: {len(matched_purchase_ids)}")
    logger.info("="*80 + "\n")
    
    logger.info("? Reconciliation complete")
    return True


if __name__ == '__main__':
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        logger.error(f"✗ Fatal error: {e}", exc_info=True)
        exit(1)
