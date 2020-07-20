from time import time_ns

from aiohttp.web import HTTPBadRequest

from heidi.etext.general import VALIDATION_ERROR

from handlers import route


@route.get('/history_key')
async def get_history_key(request):
    query = request.rel_url.query
    if 'sender_id' not in query:
        raise HTTPBadRequest(reason=VALIDATION_ERROR)
    sender_id = query['sender_id']

    sender_id = sender_id.isdigit() and int(sender_id)
    if sender_id < 1:
        raise HTTPBadRequest(reason=VALIDATION_ERROR)

    return f'history:{sender_id}:{time_ns()}'
