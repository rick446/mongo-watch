import logging
import unittest

import pymongo

from mwatch.main import Watcher


logging.basicConfig(level=logging.INFO)


class TestIt(unittest.TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient(port=27018)
        self.db = self.client.test
        self.coll = self.db.test
        self.coll.delete_many({})

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
        print('Inserts')
        for op in w:
            print(op)

        with self.client:
            self.coll.update_one({'_id': 1}, {'$set': {'bar': 1}})
        print('Update #1: set bar')
        for op in w:
            print(op)

        with self.client:
            self.coll.update_one({'_id': 1}, {'$inc': {'foo': 1}})
            self.coll.update_one({'_id': 1}, {'$inc': {'bar': 1}})
        print('Update #1: inc foo, inc bar')
        for op in w:
            print(op)

        with self.client:
            self.coll.delete_one({'_id': 1})
        print('Delete #1')
        for op in w:
            print(op)


if __name__ == '__main__':
    unittest.main()
