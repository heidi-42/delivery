import asyncio
import json

from aiohttp.web import HTTPNotFound
from jsonschema import validate

from heidi.etext.delivery import HISTORY_KEY_NOT_FOUND

from handlers import route

schema = {
    'type': 'object',
    'properties': {
        'history_key': {
            'type': 'string',
            'pattern': r'^history:\d{1,6}:\d{19}$'
        },

        # How many couriers must alter the history key in order for us
        # to assume that the delivery has been finished.
        'touch_count': {
            'type': 'integer',
            'minimum': 1,
            'maximum': 16,
        },
        'timeout': {
            'type': 'integer',
            # ~~ Just return the history contents
            'minimum': 0,
            'maximum': 10,
        },
    }
}


async def wait_n_touches(redis, history_key, n):
    reader, = await redis.subscribe(f'__keyspace@0__:{history_key}')

    # History creation also counts as one `set`
    count = -1
    async for event in reader.iter(encoding='utf-8'):
        if event == 'set':
            count += 1

        if count == n:
            return


@route.post('/track')
async def post_track(request):
    """You MUST enable redis key-space notifications in order
    for this to work.

    Add 'K$' to the 'notify-keyspace-events' in your redis.conf.
    """
    payload = await request.json()
    validate(payload, schema)

    redis = request.app['redis']
    history_key = payload['history_key']
    timeout = payload['timeout']

    timeouted = False
    if timeout > 0:
        try:
            await asyncio.wait_for(wait_n_touches(redis, history_key,
                                                  payload['touch_count']),
                                   timeout=timeout)
        except asyncio.TimeoutError:
            timeouted = True

    history = await redis.get(history_key)
    if history is None:
        raise HTTPNotFound(reason=HISTORY_KEY_NOT_FOUND)

    return {
        'history': json.loads(history),
        'timeouted': timeouted,
    }
