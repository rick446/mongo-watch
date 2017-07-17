import time
import logging
from pprint import pformat

import bson
import pymongo

from mwatch.live_query import LiveQuery
from mwatch.oplog import Oplog


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def main():
    client = pymongo.MongoClient(port=27018)
    oplog = Oplog(client)
    db = client.eht
    coll_patient = db.patient
    coll_user = db.user
    for obj in coll_patient.find():
        user = coll_user.find_one({
            '_id': obj['user_id'],
            'primary_email.id': 'judy@example.com'})
        if user:
            log.info('Found patient %r', obj)
            break
    lq = LiveQuery(
        client, 'eht.condition',
        {'patient_id': obj['_id']})
    for op in oplog.register(lq):
        log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
    lq = LiveQuery(
        client, 'eht.treatment',
        {'patient_id': obj['_id']})
    for op in oplog.register(lq):
        log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
    lq = LiveQuery(
        client, 'eht.provider',
        {'patient_id': obj['_id']})
    for op in oplog.register(lq):
        log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
    while True:
        log.info('---Poll---')
        for op in oplog:
            log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
        time.sleep(5)


if __name__ == '__main__':
    main()