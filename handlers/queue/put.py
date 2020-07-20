import random
from http import HTTPStatus
from datetime import datetime, time

from aiohttp.web import HTTPBadRequest, HTTPTooManyRequests, HTTPForbidden, \
    json_response
from dotmap import DotMap

from heidi.data import Group, Allegiance
from heidi.util import dumps
from heidi.etext.delivery import \
    NON_ISO_DATETIME, UNKNOWN_ROLE, DAILY_LIMIT_EXCEEDED

from jsonschema import validate

from handlers import route

user_schema = {
    'type': 'object',
    'properties': {
        'id': {
            'type': 'integer',
            'minimum': 1,
        },
        'email': {
            'type': 'string',
        },
        'name': {
            'type': 'string',
        },
        'role': {
            'type': 'string',
        }
    },
    'required': [
        'id',
        'email',
        'name',
        'role',
    ]
}

schema = {
    'type': 'object',
    'properties': {
        'history_key': {
            'type': 'string',
            'pattern': r'^history:\d{1,6}:\d{19}$'
        },
        'sender': {
            **user_schema
        },
        'recipients': {
            'type': 'array',
            'items': {
                **user_schema
            },
            'minItems': 1,
            # TODO: Max items?
        },
        'text': {
            'type': 'string',
            'maxLength': 4096,
        },
        'provider': {
            'type': 'string',
        },

        # jsonschema does not implement "date-time" format yet.
        # Absence is interpreted as ASAP.
        'deliver_at': {
            'type': 'string'
        }
    },
    'required': [
        'history_key',
        'sender',
        'recipients',
        'text',
        'provider',
    ]
}


def get_delivery_time(payload):
    if 'deliver_at' in payload:
        try:
            deliver_at = datetime.fromisoformat(payload['deliver_at'])
            scheduled = True
        except ValueError:
            raise HTTPBadRequest(reason=NON_ISO_DATETIME)
    else:
        deliver_at = datetime.now()
        scheduled = False

    # 00:00 - 07:00 seems like a reasonable DnD timing for a
    # "conventional" student
    if deliver_at.time() < time(hour=7):
        scheduled = True
        # Random is meant to prevent nighty messages from stacking up
        deliver_at = deliver_at.replace(hour=7,
                                        minute=random.randint(0, 5),
                                        second=random.randint(0, 59))

    return DotMap(iso=deliver_at.isoformat(),
                  unix=deliver_at.timestamp(),
                  scheduled=scheduled)


daily_limit = {
    'trainer': 4,
    'staff': 16,
}

day = 60 * 60 * 24


@route.put('/queue')
async def put_queue(request):
    """You MUST enable redis key-space notifications in order
    for this to work.

    Add 'Kxg' to the 'notify-keyspace-events' in your redis.conf.

    You MIGHT want to increase 'active-expire-effort' value to
    improve notification schedule precision.
    """
    payload = await request.json()
    validate(payload, schema)

    redis = request.app['redis']

    sender = payload['sender']
    uid = sender['id']

    if sender['role'] not in daily_limit:
        raise HTTPForbidden(reason=UNKNOWN_ROLE)

    lim_send = f'lim_send:{uid}'
    sent_before = int(await redis.get(lim_send) or 0)
    if sent_before >= daily_limit[sender['role']]:
        raise HTTPTooManyRequests(
            reason=DAILY_LIMIT_EXCEEDED,
            headers={'Retry-After': str(await redis.ttl(lim_send))})

    deliver_at = get_delivery_time(payload)
    payload['deliver_at'] = deliver_at.iso

    history_key = payload['history_key']
    delivery_key = f'delivery:{history_key[8:]}'

    del payload['history_key']

    for user in payload['recipients']:
        user['origin'] = await Group.query.where(
            (Allegiance.user == user['id'])
            & (Group.id == Allegiance.group)
            & (Group.is_virtual.isnot(True))).gino.all()

        user['received_in'] = []

    payload['recipients'].sort(key=lambda user: user['id'])

    transaction = redis.multi_exec()
    transaction.set(history_key, dumps(payload))

    transaction.set(delivery_key, 1)
    if not deliver_at.scheduled:
        transaction.delete(delivery_key)
    else:
        # EXPIREAT key <now / some time ago>, EXPIRE key 0
        # don't trigger EXPIRE events consistently.
        #
        # Tested on Redis-server 6.0.4.
        transaction.expireat(delivery_key, round(deliver_at.unix))

    transaction.incr(lim_send)
    if sent_before == 0:
        transaction.expire(lim_send, day)

    await transaction.execute()
    if deliver_at.scheduled:
        return json_response(deliver_at.iso, status=HTTPStatus.ACCEPTED.value)
    else:
        return deliver_at.iso
