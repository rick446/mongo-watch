from collections import OrderedDict, namedtuple

from mongoquery import Query, QueryError


Change = namedtuple('Change', 'op ns obj')


class LiveQuery(object):

    def __init__(self, client, ns, qspec):
        self.ns = ns
        self._qspec = qspec
        self._query = Query(qspec)
        if '_id' in qspec:
            self._query_by_id = Query({'_id': qspec['_id']})
        else:
            self._query_by_id = None
        self._results = OrderedDict()
        dbname, cname = ns.split('.', 1)
        self._db = getattr(client, dbname)
        self._collection = getattr(self._db, cname)

    def refresh(self):
        self._results = OrderedDict(
            (obj['_id'], obj)
            for obj in self._collection.find(self._qspec))
        return [Change('a', self.ns, obj) for obj in self._results.values()]

    def add(self, obj):
        self._results[obj['_id']] = obj
        return Change('a', self.ns, obj)

    def discard(self, obj):
        old_obj = self._results.pop(obj['_id'], None)
        if old_obj:
            return Change('d', self.ns, obj)
        else:
            return None

    def handle(self, entry):
        ns, op, o2, o = entry['ns'], entry['op'], entry.get('o2'), entry['o']
        if ns != self.ns:
            return
        if op == 'i':
            if self._query.match(o):
                return self.add(o)
        elif op == 'd':
            return self.discard(o)
        elif entry['op'] == 'u':
            if self._query_by_id:   # qspec includes an _id clause
                if not self._query_by_id.match(o2):
                    return  # the updated object's _id does not match
            # Load the (updated) object
            obj = self._collection.find_one(o2)
            if self._query.match(obj):
                return self.add(obj)
            else:
                return self.discard(o2)
