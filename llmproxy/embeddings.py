import decimal
import json

import aiohttp

from . import auth, billing, metrics, proxy
from .products import product


# Frontend related variables are prefixed with f_.
# Backend related variables are prefixed with b_.
async def embeddings(f_req):
    app = f_req.app

    user = await auth.require_auth(f_req)

    async with proxy.request(f_req, user=user,
            backend_type="embedding") as (b_res, b_name, b_cfg):
        app.logger.debug("Backend request completed")

        await proxy.check_response(app, b_name, b_res,
            request_id=f_req["request_id"])

        body = await proxy.read_response(f_req, b_res)
        try:
            data = json.loads(body, parse_float=decimal.Decimal)
            usage = data.get('usage') if isinstance(data, dict) else None
            if not isinstance(usage, dict):
                raise ValueError('Missing embedding usage')
            tokens = billing.token_count(usage.get('prompt_tokens'))
        except (ValueError, TypeError):
            app.logger.error('Embedding backend usage invalid: backend=%s request_id=%s',
                b_name, f_req['request_id'])
            raise aiohttp.web.HTTPBadGateway(text='Embedding backend returned invalid usage') from None
        f_hdrs = {"Content-Type":
            b_res.headers.get("Content-Type", "application/octet-stream")}
        f_res = aiohttp.web.Response(body=body, headers=f_hdrs)

        await billing.record(f_req, user, {
            product(b_name, b_cfg["device"], "embedding"): tokens,
        })

        app.logger.info("Client used: P:%d C:%d tokens of %s",
            tokens, 0, b_name)

        metrics.TOKENS_TOTAL.labels(b_name, "embedding").inc(
            tokens)

        return f_res
