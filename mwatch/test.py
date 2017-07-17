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
    lq = LiveQuery(
        client, 'eht.condition',
        {'patient_id': bson.ObjectId('596d41a54e92ac8959657211')})
    for op in oplog.register(lq):
        log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
    while True:
        log.info('---Poll---')
        for op in oplog:
            log.info('%s %s:\n%s', op.op, op.ns, pformat(op.obj))
        time.sleep(1)


if __name__ == '__main__':
    main()