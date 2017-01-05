import logging
import unittest

import pymongo

from mwatch.main import Watcher

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s')


class TestIt(unittest.TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient(port=27018)
        self.db = self.client.test
        self.coll = self.db.test
        self.coll.delete_many({})
        log.debug('setUp complete')

    def test_can_watch(self):
        w = Watcher(self.client)
        w.watch_query(
            self.coll, {'foo': 1})
        docs = [
            {'_id': 0, 'foo': 1},
            {'_id': 1, 'foo': 1},
            {'_id': 2, 'foo': 1},
            {'_id': 3, 'foo': 2},
            {'_id': 4, 'foo': 2},
            {'_id': 5, 'foo': 2}]
        with self.client:
            self.coll.insert_many(docs)
        log.debug('Inserts')
        ops = list(w)
        self.assertEqual(len(ops), 3)
        for op in ops:
            log.debug('op = %r', op)

        with self.client:
            self.coll.update_one({'_id': 1}, {'$set': {'bar': 1}})
        log.debug('Update #1: set bar')
        ops = list(w)
        self.assertEqual(len(ops), 1)
        for op in ops:
            log.debug('op = %r', op)

        with self.client:
            self.coll.update_one({'_id': 1}, {'$inc': {'foo': 1}})
            self.coll.update_one({'_id': 1}, {'$inc': {'bar': 1}})
        log.debug('Update #1: inc foo, inc bar')
        ops = list(w)
        self.assertEqual(len(ops), 2)
        for op in ops:
            log.debug('op = %r', op)

        with self.client:
            self.coll.delete_one({'_id': 1})
        log.debug('Delete #1')
        ops = list(w)
        self.assertEqual(len(ops), 1)
        for op in ops:
            log.debug('op = %r', op)

    def test_double_watch(self):
        w = Watcher(self.client)
        w.watch_query(
            self.coll, {'foo': 1})
        w.watch_inserts(
            self.coll, {'foo': 2})
        docs = [
            {'_id': 0, 'foo': 1},
            {'_id': 1, 'foo': 1},
            {'_id': 2, 'foo': 1},
            {'_id': 3, 'foo': 2},
            {'_id': 4, 'foo': 2},
            {'_id': 5, 'foo': 2}]
        with self.client:
            self.coll.insert_many(docs)
        log.debug('Inserts')
        ops = list(w)
        self.assertEqual(len(ops), 6)
        for op in ops:
            log.debug('op = %r', op)
        with self.client:
            self.coll.update_many({}, {'$set': {'bar': 1}})
        log.debug('Updates')
        ops = list(w)
        self.assertEqual(len(ops), 6)
        for op in ops:
            log.debug('op = %r', op)

    def test_double_watch_check(self):
        w = Watcher(self.client)
        w.watch_query(
            self.coll, {'foo': 1}, check_inserts=True)
        w.watch_inserts(
            self.coll, {'foo': 2})
        docs = [
            {'_id': 0, 'foo': 1},
            {'_id': 1, 'foo': 1},
            {'_id': 2, 'foo': 1},
            {'_id': 3, 'foo': 2},
            {'_id': 4, 'foo': 2},
            {'_id': 5, 'foo': 2}]
        with self.client:
            self.coll.insert_many(docs)
        log.debug('Inserts')
        ops = list(w)
        self.assertEqual(len(ops), 6)
        for op in ops:
            log.debug('op = %r', op)
        with self.client:
            self.coll.update_many({}, {'$set': {'bar': 1}})
        log.debug('Updates')
        ops = list(w)
        self.assertEqual(len(ops), 3)
        for op in ops:
            log.debug('op = %r', op)


if __name__ == '__main__':
    unittest.main()
