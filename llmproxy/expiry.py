"""Exact, fail-closed API-key expiry shared by adapters and authentication."""
import datetime


UNPARSEABLE = object()


def normalize_expires(value):
    """Return an aware UTC datetime, None, or UNPARSEABLE for invalid data."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            return UNPARSEABLE
    if not isinstance(value, datetime.datetime):
        return UNPARSEABLE
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    try:
        return value.astimezone(datetime.UTC)
    except (OverflowError, ValueError):
        return UNPARSEABLE


def valid_expiry(value: object, now: datetime.datetime) -> bool:
    """No deadline is unlimited; malformed or reached deadlines deny access."""
    expires = normalize_expires(value)
    return expires is None or (expires is not UNPARSEABLE and expires > now)
