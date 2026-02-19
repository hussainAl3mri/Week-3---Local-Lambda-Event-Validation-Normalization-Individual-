"""
PL202 — Serverless Event Processing (Local Lambda Simulation)
Day 1 (45 min) — Individual Task

You will implement a Lambda-style handler:
    def handler(event, context): -> dict

Requirements (high level):
- Validate event structure
- Normalize important fields
- Return a JSON-serializable dict with:
    status: "ok" or "error"
    message: short explanation
    data: normalized data OR None
    errors: list of strings (only when status="error")

Event types you must support:
- USER_SIGNUP
- PAYMENT
- FILE_UPLOAD

Run locally:
    python run_local.py --event events/01_user_signup_valid.json
    python run_local.py --all
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
import re


ALLOWED_TYPES = {"USER_SIGNUP", "PAYMENT", "FILE_UPLOAD"}


def _err(*msgs: str) -> Dict[str, Any]:
    """Create a standard error response."""
    return {
        "status": "error",
        "message": "Event rejected",
        "data": None,
        "errors": list(msgs),
    }


def _ok(message: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a standard ok response."""
    return {
        "status": "ok",
        "message": message,
        "data": data,
        "errors": [],
    }


def _is_email(value: str) -> bool:
    # Simple email check (good enough for class)
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))


def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Main Lambda-style handler."""

    # TODO 1: Validate event is a dict and has 'type' field
    if not isinstance(event, dict):
        return _err("Event must be a dictionary")

    if "type" not in event:
        return _err("Missing 'type' field")

    event_type = event["type"]

    # TODO 2: Ensure event['type'] is one of ALLOWED_TYPES
    if event_type not in ALLOWED_TYPES:
        return _err(f"Unsupported event type: {event_type}")

    # TODO 3: Route to a per-type function:
    if event_type == "USER_SIGNUP":
        return handle_user_signup(event)

    if event_type == "PAYMENT":
        return handle_payment(event)

    if event_type == "FILE_UPLOAD":
        return handle_file_upload(event)

    # Safety fallback
    return _err("Unhandled event type")


def handle_user_signup(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process USER_SIGNUP events."""

    errors: List[str] = []

    user_id = event.get("user_id")
    email = event.get("email")
    plan = event.get("plan")

    # TODO 5: Validate required fields and types
    if not isinstance(user_id, int):
        errors.append("user_id must be an integer")

    if not isinstance(email, str):
        errors.append("email must be a string")

    if not isinstance(plan, str):
        errors.append("plan must be a string")

    # TODO 6: Validate email format with _is_email
    if isinstance(email, str) and not _is_email(email):
        errors.append("Invalid email format")

    # Normalize before checking plan
    if isinstance(plan, str):
        plan = plan.lower()

    # TODO 7: Validate plan is allowed
    if isinstance(plan, str) and plan not in {"free", "pro", "edu"}:
        errors.append("Invalid plan value")

    if errors:
        return _err(*errors)

    # TODO 8: Build normalized output data and return _ok(...)
    data = {
        "user_id": user_id,
        "email": email.lower(),
        "plan": plan,
        "welcome_email_subject": f"Welcome to the {plan} plan!"
    }

    return _ok("Signup processed", data)


def handle_payment(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process PAYMENT events."""

    errors: List[str] = []

    payment_id = event.get("payment_id")
    user_id = event.get("user_id")
    amount = event.get("amount")
    currency = event.get("currency")

    # TODO 9: Validate required fields and types
    if not isinstance(payment_id, str):
        errors.append("payment_id must be a string")

    if not isinstance(user_id, int):
        errors.append("user_id must be an integer")

    if not isinstance(amount, (int, float)):
        errors.append("amount must be a number")

    if not isinstance(currency, str):
        errors.append("currency must be a string")

    # TODO 10: Validate amount > 0
    if isinstance(amount, (int, float)) and amount <= 0:
        errors.append("amount must be greater than 0")

    # Normalize currency before checking
    if isinstance(currency, str):
        currency = currency.upper()

    # TODO 11: Validate currency allowed
    if isinstance(currency, str) and currency not in {"BHD", "USD", "EUR"}:
        errors.append("Unsupported currency")

    if errors:
        return _err(*errors)

    # TODO 12: Compute fee and net_amount and return _ok(...)
    amount = round(float(amount), 3)
    fee = round(amount * 0.02, 3)
    net_amount = round(amount - fee, 3)

    data = {
        "payment_id": payment_id,
        "user_id": user_id,
        "amount": amount,
        "currency": currency,
        "fee": fee,
        "net_amount": net_amount,
    }

    return _ok("Payment processed", data)


def handle_file_upload(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process FILE_UPLOAD events."""

    errors: List[str] = []

    file_name = event.get("file_name")
    size_bytes = event.get("size_bytes")
    bucket = event.get("bucket")
    uploader = event.get("uploader")

    # TODO 13: Validate required fields and types
    if not isinstance(file_name, str):
        errors.append("file_name must be a string")

    if not isinstance(size_bytes, int):
        errors.append("size_bytes must be an integer")

    if isinstance(size_bytes, int) and size_bytes < 0:
        errors.append("size_bytes must be >= 0")

    if not isinstance(bucket, str):
        errors.append("bucket must be a string")

    if not isinstance(uploader, str):
        errors.append("uploader must be a string")

    # TODO 14: Validate uploader email
    if isinstance(uploader, str) and not _is_email(uploader):
        errors.append("Invalid uploader email")

    if errors:
        return _err(*errors)

    # Normalize
    file_name = file_name.strip()
    bucket = bucket.lower()
    uploader = uploader.lower()

    # TODO 15: Compute storage_class and return _ok(...)
    if size_bytes < 1_000_000:
        storage_class = "STANDARD"
    elif size_bytes < 50_000_000:
        storage_class = "STANDARD_IA"
    else:
        storage_class = "GLACIER"

    data = {
        "file_name": file_name,
        "size_bytes": size_bytes,
        "bucket": bucket,
        "uploader": uploader,
        "storage_class": storage_class,
    }

    return _ok("Upload processed", data)
