import logging
from collections import defaultdict
from pprint import pformat

import pymongo
try:
    from mongoquery import Query, QueryError
except ImportError:
    Query = QueryError = None

log = logging.getLogger(__name__)


class Oplog(object):

    def __init__(self, client, await_=False):
        self.oplog = client.local.oplog.rs
        self._lq_by_ns = defaultdict(dict)
        if await_:
            self._cursor_type = pymongo.CursorType.TAILABLE_AWAIT
        else:
            self._cursor_type = pymongo.CursorType.TAILABLE
        self._last_ts = self._get_last_ts()

    def register(self, lq):
        self._lq_by_ns[lq.ns][id(lq)] = lq
        return lq.refresh()

    def deregister(self, lq):
        ns_lqs = self._lq_by_ns[lq.ns]
        ns_lqs.pop(id(lq))
        if not ns_lqs:
            self._lq_by_ns.pop(lq.ns)

    def _get_last_ts(self):
        final_entry = self.oplog.find().sort('$natural', -1).limit(1).next()
        log.debug('final_entry: %s', final_entry)
        return final_entry['ts']

    def _get_cursor(self):
        assert self._lq_by_ns, 'Nothing to watch'
        nss = list(self._lq_by_ns.keys())
        if len(nss) == 1:
            spec = {'ns': nss[0]}
        else:
            spec = {'ns': {'$in': nss}}
        spec['ts'] = {'$gt': self._last_ts}
        log.debug('Query oplog with\n%s', pformat(spec))
        return self.oplog.find(
            spec,
            cursor_type=self._cursor_type,
            oplog_replay=True)

    def __iter__(self):
        curs = self._get_cursor()
        for doc in curs:
            log.debug('oplog: %r', doc)
            self._last_ts = doc['ts']
            for lq in self._lq_by_ns[doc['ns']].values():
                res = lq.handle(doc)
                if res:
                    yield res
