"""
Utility functions for TTB COLA processing.
"""
import time
import hashlib
from datetime import date, datetime
from typing import Dict, Any, Optional


class TTBIDUtils:
    """Utilities for working with TTB IDs."""

    @staticmethod
    def date_to_julian(input_date: date) -> int:
        """Convert date to Julian day of year."""
        return input_date.timetuple().tm_yday

    @staticmethod
    def build_ttb_id(year: int, julian_day: int, receipt_method: int, sequence: int) -> str:
        """
        Build TTB ID from components.

        Format: {year}{julian_day:03d}{receipt_method:03d}{sequence:06d}
        Example: 20240010010000001
        """
        return f"{year}{julian_day:03d}{receipt_method:03d}{sequence:06d}"

    @staticmethod
    def parse_ttb_id(ttb_id: str) -> Dict[str, int]:
        """Parse TTB ID into components."""
        if len(ttb_id) != 17:
            raise ValueError(f"Invalid TTB ID length: {ttb_id}")

        return {
            "year": int(ttb_id[:4]),
            "julian_day": int(ttb_id[4:7]),
            "receipt_method": int(ttb_id[7:10]),
            "sequence": int(ttb_id[10:])
        }

    @staticmethod
    def rate_limit_sleep(delay_seconds: float = 0.5):
        """Sleep for rate limiting."""
        time.sleep(delay_seconds)


class TTBSequenceTracker:
    """Track consecutive failures for stopping extraction."""

    def __init__(self, max_consecutive_failures: int = 10):
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures = 0
        self.total_successes = 0
        self.total_failures = 0

    def record_success(self):
        """Record a successful extraction."""
        self.consecutive_failures = 0
        self.total_successes += 1

    def record_failure(self):
        """Record a failed extraction."""
        self.consecutive_failures += 1
        self.total_failures += 1

    def should_stop(self) -> bool:
        """Check if we should stop extraction due to consecutive failures."""
        return self.consecutive_failures >= self.max_consecutive_failures


def is_ttb_error_page(content: bytes) -> bool:
    """
    Check if response content is a TTB error page.

    TTB returns a standard error page for invalid IDs.
    """
    # Hash of the standard TTB error page
    TTB_ERROR_PAGE_HASH = "50fa048f9cf8200c3d82d60add59b3b1f78f9e3ebc67f9395051595fc830a9e3"
    content_hash = hashlib.sha256(content).hexdigest()
    return content_hash == TTB_ERROR_PAGE_HASH


def validate_cola_data_consistency(detail_data: Dict[str, Any], cert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate consistency between detail and certificate views of the same COLA.

    Returns validation results with any inconsistencies found.
    """
    validation_results = {
        "is_consistent": True,
        "issues": [],
        "checked_fields": []
    }

    # Check TTB ID consistency
    detail_ttb_id = detail_data.get("ttb_id")
    cert_ttb_id = cert_data.get("ttb_id")

    if detail_ttb_id and cert_ttb_id:
        validation_results["checked_fields"].append("ttb_id")
        if detail_ttb_id != cert_ttb_id:
            validation_results["is_consistent"] = False
            validation_results["issues"].append(f"TTB ID mismatch: detail={detail_ttb_id}, cert={cert_ttb_id}")

    # Check applicant name consistency
    detail_applicant = detail_data.get("applicant_name")
    cert_applicant = cert_data.get("applicant_name")

    if detail_applicant and cert_applicant:
        validation_results["checked_fields"].append("applicant_name")
        if detail_applicant.strip().lower() != cert_applicant.strip().lower():
            validation_results["is_consistent"] = False
            validation_results["issues"].append(f"Applicant name mismatch: detail='{detail_applicant}', cert='{cert_applicant}'")

    return validation_results