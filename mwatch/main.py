import logging

import pymongo


log = logging.getLogger(__name__)


class Watcher:

    def __init__(self, cli, await_=False):
        self.oplog = cli.local.oplog.rs
        self.watches = {}   # dict[id] = watch (so we can remove watches later)
        if await_:
            self._cursor_type = pymongo.CursorType.TAILABLE_AWAIT
        else:
            self._cursor_type = pymongo.CursorType.TAILABLE
        self._last_ts = self._get_last_ts()

    def watch_query(self, collection, qspec=None):
        res = QueryWatch(self, collection, qspec)
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
        branches = (w.oplog_spec() for w in self.watches.values())
        branches = [br for br in branches if br is not None]
        assert branches, 'Nothing to watch'
        if len(branches) == 1:
            spec = branches[0]
        else:
            spec = {'$or': branches}
        spec['ts'] = {'$gt': self._last_ts}
        log.debug('Query oplog with %s', spec)
        return self.oplog.find(
            spec,
            cursor_type=self._cursor_type,
            oplog_replay=True,
            sort=[('$natural', 1)])

    def __iter__(self):
        curs = self._get_cursor()
        stateful_watches = [
            w for w in self.watches.values()
            if hasattr(w, 'process_cursor')]
        for w in stateful_watches:
            curs = w.process_cursor(curs)
        for doc in curs:
            self._last_ts = doc['ts']
            yield doc


class Watch:

    def __init__(self, watcher):
        self.watcher = watcher

    def unwatch(self):
        self.watcher.watches.pop(id(self), None)


class QueryWatch(Watch):
    """Insert/update/delete watch for a query (stateful)."""

    def __init__(self, watcher, collection, qspec=None):
        super().__init__(watcher)
        self.collection = collection
        self.qspec = qspec
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

    def oplog_spec(self):
        if self.qspec is None:
            return {'ns': self._ns, 'op': {'$in': ['i', 'u', 'd']}}
        ins_watch = InsertWatch(self.watcher, self.collection, self.qspec)
        if self._ids:
            watches = [
                ins_watch,
                UpdateWatch(self.watcher, self.collection, list(self._ids)),
                DeleteWatch(self.watcher, self.collection, list(self._ids))]
            return {'$or': [w.oplog_spec() for w in watches]}
        else:
            return ins_watch.oplog_spec()

    def process_cursor(self, cursor):
        if not self.qspec:
            return cursor

        def decorate_cursor():
            for entry in cursor:
                if entry['ns'] != self._ns:
                    return
                if entry['op'] == 'i' and self._match(entry['o']['_id']):
                    self._ids.add(entry['o']['_id'])
                elif entry['op'] == 'd':
                    self._ids.discard(entry['o']['_id'])
                yield entry
        return decorate_cursor()

    def _match(self, idval):
        spec = dict(self.qspec)
        if '_id' in spec:
            spec = {'$and': [spec, {'_id': idval}]}
        else:
            spec['_id'] = idval
        return 1 == self.collection.find(spec).count()


class InsertWatch(Watch):

    def __init__(self, watcher, collection, qspec=None):
        super().__init__(watcher)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        self.qspec = qspec

    def __repr__(self):
        return '<InsertWatch {} {}>'.format(self._ns, self.qspec)

    def oplog_spec(self):
        qspec = {
            'o.{}'.format(k): v
            for k, v in self.qspec.items()}
        if self.qspec:
            return {'op': 'i', 'ns': self._ns, **qspec}
        else:
            return {'op': 'i', 'ns': self._ns}


class UpdateWatch(Watch):

    def __init__(self, watcher, collection, ids=None):
        super().__init__(watcher)
        self._ns = '{}.{}'.format(
            collection.database.name,
            collection.name)
        self._ids = ids

    def __repr__(self):
        return '<UpdateWatch {} {}>'.format(self._ns, self._ids)

    def oplog_spec(self):
        if self._ids is None:
            return {'op': 'u', 'ns': self._ns}
        ids = list(self._ids)
        if len(ids) == 1:
            return {'op': 'u', 'ns': self._ns, 'o2._id': ids[0]}
        if len(ids) > 0:
            return {'op': 'u', 'ns': self._ns, 'o2._id': {'$in': ids}}
        return None

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

    def oplog_spec(self):
        if self._ids is None:
            return {'op': 'd', 'ns': self._ns}
        ids = list(self._ids)
        if len(ids) == 1:
            return {'op': 'd', 'ns': self._ns, 'o._id': ids[0]}
        if len(ids) > 0:
            return {'op': 'd', 'ns': self._ns, 'o._id': {'$in': ids}}
        return None

    def unwatch(self, id):
        self._ids.remove(id)
