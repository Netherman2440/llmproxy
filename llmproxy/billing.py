import datetime

import aiohttp.web

from . import attempts
from .db import DatabaseError, get_db


async def record(f_req, user, resources):
    """Write one billing record for a successful request.

    HTTP text routes first retain usage proof and settle their stable attempt;
    failure aborts success and leaves a recoverable/unresolved obligation.
    Other endpoints keep the legacy billing_record_add + DatabaseError to
    GracefulExit policy. No path interprets a storage fault as zero usage.
    """
    app = f_req.app

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
