import datetime
from decimal import Decimal

import aiohttp.web

from . import attempts
from .products import product as product  # canonical identity, re-exported for handlers
from .db import DatabaseError, get_db


def token_count(value):
    """Backend token proof: exact nonnegative integer, never bool/coerced text."""
    if type(value) not in (int, Decimal):
        raise ValueError('Invalid backend token quantity')
    number = Decimal(value)
    if not number.is_finite() or not 0 <= number < 2**53 or number != int(number):
        raise ValueError('Invalid backend token quantity')
    return int(number)


def validate_resources(resources):
    """Last common boundary before any finance write, including legacy routes."""
    if type(resources) is not dict or not resources:
        raise ValueError('Invalid backend resources')
    for product_key, quantity in resources.items():
        if (not isinstance(product_key, str) or not product_key
                or type(quantity) not in (int, float, Decimal)):
            raise ValueError('Invalid backend resource quantity')
        if product_key != product_key.lower():
            raise ValueError('Non-canonical billing product identity')
        number = Decimal(str(quantity))
        if not number.is_finite() or number < 0:
            raise ValueError('Invalid backend resource quantity')


async def record(f_req, user, resources):
    """Write one billing record for a successful request.

    HTTP text routes first retain usage proof and settle their stable attempt;
    failure aborts success and leaves a recoverable/unresolved obligation.
    Other endpoints keep the legacy billing_record_add + DatabaseError to
    GracefulExit policy. No path interprets a storage fault as zero usage.
    """
    app = f_req.app
    try:
        validate_resources(resources)
    except ValueError:
        raise aiohttp.web.HTTPBadGateway(text='Backend returned invalid usage') from None

    db = await get_db(app["config"]["db"]["uri"], f_req)
    try:
        if (intent := f_req.get('http_attempt')) is not None:
            await attempts.confirm(db, intent, resources)
            await attempts.settle(db, intent)
            f_req['http_attempt_settled'] = True
            return
        await db.billing_record_add(
            user=user,
            time=datetime.datetime.now(datetime.UTC),
            resources=resources,
            request_id=f_req["request_id"],
        )
    except DatabaseError as e:
        app.logger.critical(e)
        raise aiohttp.web.GracefulExit() from e
