import logging
from itertools import chain
from pprint import pformat

import pymongo
try:
    from mongoquery import Query, QueryError
except ImportError:
    Query = QueryError = None

log = logging.getLogger(__name__)


class Watcher(object):

    def __init__(self, cli, await_=False):
        self.oplog = cli.local.oplog.rs
        self.watches = {}   # dict[id] = watch (so we can remove watches later)
        if await_:
            self._cursor_type = pymongo.CursorType.TAILABLE_AWAIT
        else:
            self._cursor_type = pymongo.CursorType.TAILABLE
        self._last_ts = self._get_last_ts()

    def watch_query(self, collection, qspec=None, check_inserts=False):
        res = QueryWatch(self, collection, qspec, check_inserts)
        self.watches[id(res)] = res
        return res

    def watch_inserts(self, collection, qspec=None):
        res = InsertWatch(self, collection, qspec)
        self.watches[id(res)] = res
        return res

    def watch_updates(self, collection, ids=None):
        res = UpdateWatch(self, collection, ids)
        self.watches[id(res)] = res
        return res

    def watch_deletes(self, collection, ids=None):
        res = DeleteWatch(self, collection, ids)
        self.watches[id(res)] = res
        return res

    def _get_last_ts(self):
        final_entry = self.oplog.find().sort('$natural', -1).limit(1).next()
        log.debug('final_entry: %s', final_entry)
        return final_entry['ts']

    def _get_cursor(self):
        branches = list(chain(*[
            w.oplog_branches() for w in self.watches.values()]))
        assert branches, 'Nothing to watch'
        if len(branches) == 1:
            spec = branches[0]
        else:
            spec = {'$or': branches}
        spec['ts'] = {'$gt': self._last_ts}
        log.debug('Query oplog with\n%s', pformat(spec))
        return self.oplog.find(
            spec,
            cursor_type=self._cursor_type,
            oplog_replay=True)

    def __iter__(self):
        curs = self._get_cursor()
        stateful_watches = [
            w for w in self.watches.values()
            if hasattr(w, 'process_entry')]
        needs_restart = False
        for doc in curs:
            for w in stateful_watches:
                needs_restart = needs_restart or w.process_entry(doc)
            self._last_ts = doc['ts']
            yield doc
            if needs_restart:
                break


class Watch:

    def __init__(self, watcher):
        self.watcher = watcher

    def unwatch(self):
        self.watcher.watches.pop(id(self), None)


class QueryWatch(Watch):
    """Insert/update/delete watch for a query (stateful)."""

    def __init__(
            self, watcher, collection, qspec=None, check_inserts=False):
        super().__init__(watcher)
        self.collection = collection
        self.qspec = qspec
        self.check_inserts = check_inserts
        if check_inserts:
            assert Query is not None, 'Cannot check inserts without mongoquery'
            self._mquery = Query(qspec)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        if qspec:
            self._ids = set(
                doc['_id'] for doc in self.collection.find(qspec, {'_id': 1}))
        else:
            self._ids = None

    def __repr__(self):
        return '<QueryWatch {} {}>'.format(self._ns, self.qspec)

    def oplog_branches(self):
        if self.qspec is None:
            yield {'ns': self._ns, 'op': {'$in': ['i', 'u', 'd']}}
            return
        ins_watch = InsertWatch(self.watcher, self.collection, self.qspec)
        if self._ids:
            watches = [
                ins_watch,
                UpdateWatch(self.watcher, self.collection, list(self._ids)),
                DeleteWatch(self.watcher, self.collection, list(self._ids))]
        else:
            watches = [ins_watch]
        for w in watches:
            yield from w.oplog_branches()

    def process_entry(self, entry):
        """Return true if the oplog query needs to be restarted."""
        if not self.qspec:
            # no need to track IDs
            return False
        if entry['ns'] != self._ns:
            # not my collection
            return False
        if entry['op'] == 'i':
            if self.check_inserts and not self._mquery.match(entry['o']):
                # I don't watch that doc
                return False
            self._ids.add(entry['o']['_id'])
            return True
        elif entry['op'] == 'd':
            self._ids.discard(entry['o']['_id'])
        else:
            return False


class InsertWatch(Watch):

    def __init__(self, watcher, collection, qspec=None):
        super().__init__(watcher)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        self.qspec = qspec

    def __repr__(self):
        return '<InsertWatch {} {}>'.format(self._ns, self.qspec)

    def oplog_branches(self):
        qspec = {
            'o.{}'.format(k): v
            for k, v in self.qspec.items()}
        if self.qspec:
            yield {'op': 'i', 'ns': self._ns, **qspec}
        else:
            yield {'op': 'i', 'ns': self._ns}


class UpdateWatch(Watch):

    def __init__(self, watcher, collection, ids=None):
        super().__init__(watcher)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        self._ids = ids

    def __repr__(self):
        return '<UpdateWatch {} {}>'.format(self._ns, self._ids)

    def oplog_branches(self):
        if self._ids is None:
            yield {'op': 'u', 'ns': self._ns}
            return
        ids = list(self._ids)
        if len(ids) == 1:
            yield {'op': 'u', 'ns': self._ns, 'o2._id': ids[0]}
        if len(ids) > 0:
            yield {'op': 'u', 'ns': self._ns, 'o2._id': {'$in': ids}}

    def unwatch(self, id):
        self._ids.remove(id)


class DeleteWatch(Watch):

    def __init__(self, watcher, collection, ids=None):
        super().__init__(watcher)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        self._ids = ids

    def __repr__(self):
        return '<DeleteWatch {} {}>'.format(self._ns, self._ids)

    def oplog_branches(self):
        if self._ids is None:
            yield {'op': 'd', 'ns': self._ns}
            return
        ids = list(self._ids)
        if len(ids) == 1:
            yield {'op': 'd', 'ns': self._ns, 'o._id': ids[0]}
        if len(ids) > 0:
            yield {'op': 'd', 'ns': self._ns, 'o._id': {'$in': ids}}

    def unwatch(self, id):
        self._ids.remove(id)
