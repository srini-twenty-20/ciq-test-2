"""
TTB ID utilities for parsing, validation, and generation.

TTB ID Structure (14 digits): YYJJJRRRSSSSS
- YY: Calendar year (last 2 digits)
- JJJ: Julian date (001-366)
- RRR: Receipt method (001=e-filed, 002/003=mailed/overnight, 000=hand delivered)
- SSSSSS: Sequential number (000001-999999, resets daily per receipt method)
"""
import time
from datetime import datetime, date, timedelta
from typing import NamedTuple, Iterator, Optional, List
from dataclasses import dataclass


class TTBIDComponents(NamedTuple):
    """Parsed components of a TTB ID."""
    year: int
    julian_day: int
    receipt_method: int
    sequence: int
    full_id: str


@dataclass
class TTBIDRange:
    """Represents a range of TTB IDs for processing."""
    start_date: date
    end_date: date
    receipt_methods: List[int]
    max_sequence: int = 999999


class TTBIDUtils:
    """Utilities for working with TTB IDs."""

    # Receipt method codes
    RECEIPT_METHODS = {
        1: "e-filed",
        2: "mailed",
        3: "overnight",
        0: "hand delivered"
    }

    @staticmethod
    def parse_ttb_id(ttb_id: str) -> TTBIDComponents:
        """
        Parse a TTB ID into its components.

        Args:
            ttb_id: 14-digit TTB ID string

        Returns:
            TTBIDComponents with parsed values

        Raises:
            ValueError: If TTB ID format is invalid
        """
        if not ttb_id or len(ttb_id) != 14 or not ttb_id.isdigit():
            raise ValueError(f"Invalid TTB ID format: {ttb_id}")

        year_digits = int(ttb_id[:2])
        julian_day = int(ttb_id[2:5])
        receipt_method = int(ttb_id[5:8])
        sequence = int(ttb_id[8:14])

        # Convert 2-digit year to 4-digit year
        # Assume years 00-30 are 2000s, 31-99 are 1900s
        if year_digits <= 30:
            year = 2000 + year_digits
        else:
            year = 1900 + year_digits

        # Validate components
        if not (1 <= julian_day <= 366):
            raise ValueError(f"Invalid Julian day: {julian_day}")

        if receipt_method not in [0, 1, 2, 3]:
            raise ValueError(f"Invalid receipt method: {receipt_method}")

        if not (1 <= sequence <= 999999):
            raise ValueError(f"Invalid sequence number: {sequence}")

        return TTBIDComponents(
            year=year,
            julian_day=julian_day,
            receipt_method=receipt_method,
            sequence=sequence,
            full_id=ttb_id
        )

    @staticmethod
    def build_ttb_id(year: int, julian_day: int, receipt_method: int, sequence: int) -> str:
        """
        Build a TTB ID from components.

        Args:
            year: 4-digit year
            julian_day: Julian day (1-366)
            receipt_method: Receipt method code (0, 1, 2, 3)
            sequence: Sequence number (1-999999)

        Returns:
            14-digit TTB ID string
        """
        # Convert to 2-digit year
        year_2digit = year % 100

        return f"{year_2digit:02d}{julian_day:03d}{receipt_method:03d}{sequence:06d}"

    @staticmethod
    def date_to_julian(date_obj: date) -> int:
        """Convert a date to Julian day of year."""
        return date_obj.timetuple().tm_yday

    @staticmethod
    def julian_to_date(year: int, julian_day: int) -> date:
        """Convert year and Julian day to date object."""
        return datetime.strptime(f"{year}-{julian_day}", "%Y-%j").date()

    @staticmethod
    def generate_ttb_ids_for_date(
        target_date: date,
        receipt_methods: List[int] = None,
        start_sequence: int = 1,
        max_sequence: int = 999999
    ) -> Iterator[str]:
        """
        Generate TTB IDs for a specific date.

        Args:
            target_date: Date to generate IDs for
            receipt_methods: List of receipt methods to include (default: [1, 2, 3])
            start_sequence: Starting sequence number
            max_sequence: Maximum sequence number to generate

        Yields:
            TTB ID strings
        """
        if receipt_methods is None:
            receipt_methods = [1, 2, 3, 0]  # All receipt methods including hand delivered

        julian_day = TTBIDUtils.date_to_julian(target_date)

        for receipt_method in receipt_methods:
            for sequence in range(start_sequence, max_sequence + 1):
                yield TTBIDUtils.build_ttb_id(
                    year=target_date.year,
                    julian_day=julian_day,
                    receipt_method=receipt_method,
                    sequence=sequence
                )

    @staticmethod
    def generate_date_range(start_date: date, end_date: date) -> Iterator[date]:
        """Generate dates between start and end date (inclusive)."""
        current = start_date
        while current <= end_date:
            yield current
            # Move to next day
            current = datetime.fromordinal(current.toordinal() + 1).date()

    @staticmethod
    def generate_date_range_backward(start_date: date, end_date: date) -> Iterator[date]:
        """Generate dates between start and end date (inclusive) in reverse order."""
        current = start_date
        while current >= end_date:
            yield current
            # Move to previous day
            current = datetime.fromordinal(current.toordinal() - 1).date()

    @staticmethod
    def get_partition_key(ttb_id: str) -> str:
        """
        Generate a partition key for a TTB ID.
        Format: YYYY-JJJ-RRR (year-julian_day-receipt_method)
        """
        components = TTBIDUtils.parse_ttb_id(ttb_id)
        return f"{components.year}-{components.julian_day:03d}-{components.receipt_method:03d}"

    @staticmethod
    def rate_limit_sleep():
        """Sleep for rate limiting (0.5 seconds)."""
        time.sleep(0.5)


class TTBSequenceTracker:
    """Tracks consecutive failures to detect end of sequence."""

    def __init__(self, max_consecutive_failures: int = 5):
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_success = 0
        self.total_failures = 0

    def record_success(self):
        """Record a successful request."""
        self.consecutive_failures = 0
        self.total_processed += 1
        self.total_success += 1

    def record_failure(self):
        """Record a failed request."""
        self.consecutive_failures += 1
        self.total_processed += 1
        self.total_failures += 1

    def should_stop(self) -> bool:
        """Check if we should stop processing due to consecutive failures."""
        return self.consecutive_failures >= self.max_consecutive_failures

    def get_stats(self) -> dict:
        """Get processing statistics."""
        return {
            "total_processed": self.total_processed,
            "total_success": self.total_success,
            "total_failures": self.total_failures,
            "consecutive_failures": self.consecutive_failures,
            "success_rate": self.total_success / max(1, self.total_processed)
        }


class TTBBackfillManager:
    """Manages backward-looking backfill logic with data existence checking."""

    def __init__(self, s3_client, bucket_name: str, stop_after_consecutive_found: int = 3):
        """
        Initialize backfill manager.

        Args:
            s3_client: S3 client for checking data existence
            bucket_name: S3 bucket name
            stop_after_consecutive_found: Stop backfill after this many consecutive days with existing data
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.stop_after_consecutive_found = stop_after_consecutive_found
        self.consecutive_found_days = 0

    def check_partition_exists(self, target_date: date, receipt_method: int) -> bool:
        """
        Check if data already exists for a specific date and receipt method.

        Args:
            target_date: Date to check
            receipt_method: Receipt method code

        Returns:
            True if any data exists for this partition
        """
        try:
            # Check S3 prefix for this partition (using numbered raw data structure)
            s3_prefix = f"1-ttb-raw-data/data_type=cola-detail/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/receipt_method={receipt_method:03d}/"

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix,
                MaxKeys=1  # Just check if any objects exist
            )

            return response.get('KeyCount', 0) > 0

        except Exception:
            # If we can't check, assume it doesn't exist
            return False

    def should_stop_backfill(self, target_date: date, receipt_methods: List[int]) -> bool:
        """
        Check if we should stop the backfill process.

        Args:
            target_date: Current date being processed
            receipt_methods: List of receipt methods to check

        Returns:
            True if backfill should stop (enough consecutive existing data found)
        """
        # Check if all receipt methods for this date have existing data
        all_methods_exist = True
        for receipt_method in receipt_methods:
            if not self.check_partition_exists(target_date, receipt_method):
                all_methods_exist = False
                break

        if all_methods_exist:
            self.consecutive_found_days += 1
            return self.consecutive_found_days >= self.stop_after_consecutive_found
        else:
            # Reset counter if we find missing data
            self.consecutive_found_days = 0
            return False

    def get_backfill_date_range(self, max_days_back: int = 365) -> Iterator[date]:
        """
        Generate dates for backward backfill starting from day-before-yesterday.

        Args:
            max_days_back: Maximum days to go back (safety limit)

        Yields:
            Dates in reverse chronological order
        """
        today = datetime.now().date()
        start_date = today - timedelta(days=2)  # Day before yesterday
        end_date = today - timedelta(days=max_days_back)  # Safety limit

        # Don't go back before 2015-01-01
        earliest_date = date(2015, 1, 1)
        if end_date < earliest_date:
            end_date = earliest_date

        return TTBIDUtils.generate_date_range_backward(start_date, end_date)