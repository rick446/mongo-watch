# mongo-watch
Some utilities for watching MongoDB's oplog (Python3)

Usage:

(you can check this with `python -mdoctest README.md  -o ELLIPSIS`)

Some imports & setup:

```
>>> from pymongo import MongoClient
>>> from mwatch.main import Watcher
>>> client = MongoClient(port=27018)  # Must be a replica set
>>> w = Watcher(client)
>>> db = client.test
>>> coll = db.test
>>> coll.delete_many({})
<pymongo.results.DeleteResult object at ...>

```

Set up our watches

```
>>> w = Watcher(client)
>>> w.watch_query(
...     coll, {'foo': 1}, check_inserts=True)
<QueryWatch test.test {'foo': 1}>
>>> w.watch_inserts(
...     coll, {'foo': 2})
<InsertWatch test.test {'foo': 2}>

```

Insert some data

```
>>> docs = [
...     {'_id': 0, 'foo': 1},
...     {'_id': 1, 'foo': 1},
...     {'_id': 2, 'foo': 1},
...     {'_id': 3, 'foo': 2},
...     {'_id': 4, 'foo': 2},
...     {'_id': 5, 'foo': 2}]
>>> with client:
...     coll.insert_many(docs)
<pymongo.results.InsertManyResult object at ...>

```

See what our watches observed

```
>>> for op in w:
...     print(op)
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 0, 'foo': 1}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 1, 'foo': 1}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 2, 'foo': 1}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 3, 'foo': 2}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 4, 'foo': 2}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'i', 'ns': 'test.test', 'o': {'_id': 5, 'foo': 2}}

```

Do some updates

```
>>> with client:
...     coll.update_many({}, {'$set': {'bar': 1}})
<pymongo.results.UpdateResult object at ...>

```

See what they observed. Note that we don't see anything for the `foo: 2` documents since we're not watching for
updates on them (only for inserts). Our query watch, however, _is_ tracking the `foo: 1` docs.

```
>>> for op in w:
...     print(op)
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'u', 'ns': 'test.test', 'o2': {'_id': 0}, 'o': {'$set': {'bar': 1}}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'u', 'ns': 'test.test', 'o2': {'_id': 1}, 'o': {'$set': {'bar': 1}}}
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'u', 'ns': 'test.test', 'o2': {'_id': 2}, 'o': {'$set': {'bar': 1}}}

```

... and a delete

```
>>> with client:
...     coll.delete_one({'_id': 1})
...     coll.delete_one({'_id': 4})
<pymongo.results.DeleteResult object at ...>
>>> for op in w:
...     print(op)
{'ts': Timestamp(...), 't': 3, 'h': ..., 'v': 2, 'op': 'd', 'ns': 'test.test', 'o': {'_id': 1}}

```

