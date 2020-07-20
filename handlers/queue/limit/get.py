from aiohttp.web import HTTPBadRequest, HTTPNotFound

from heidi.data import User
from heidi.etext.general import VALIDATION_ERROR
from heidi.etext.delivery import USER_NOT_FOUND

from handlers import route
from handlers.queue.put import daily_limit


@route.get('/queue/limit')
async def get_queue_limit(request):
    query = request.rel_url.query
    if 'uid' not in query:
        raise HTTPBadRequest(reason=VALIDATION_ERROR)

    uid = query['uid'].isdigit() and int(query['uid'])
    if uid < 1:
        raise HTTPBadRequest(reason=VALIDATION_ERROR)

    user = await User.query.where(User.id == uid).gino.one_or_none()
    if user is None:
        raise HTTPNotFound(reason=USER_NOT_FOUND)

    redis = request.app['redis']
    lim_key = f'lim_send:{uid}'

    ttl = -1
    value = await redis.get(lim_key)

    value = int(value) if value is not None else 0

    if value > 0:
        ttl = await redis.ttl(lim_key)

    # This case is real and tested
    # TODO: LUA script to fetch the value and it's ttl atomically
    if ttl == 0:
        value = 0
        ttl = -1

    return {
        'value': value,
        'ttl': ttl,
        'daily': daily_limit[user.role],
    }
