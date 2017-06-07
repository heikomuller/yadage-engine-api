import unittest
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


class TestWorkflowRepository(unittest.TestCase):

    def setUp(self):
        mongo = MongoClient()
        mongo.drop_database('yadage_test')
        self.collection = mongo.yadage_test.workflows

    def tearDown(self):
        mongo = MongoClient()
        mongo.drop_database('yadage_test')

    def test_duplicate_key(self):
        #Check if Mongo detects the duplicate key
        self.collection.insert_one({'_id' : 'ABC', 'name' : 'NAME'})
        with self.assertRaises(DuplicateKeyError):
            self.collection.insert_one({'_id' : 'ABC', 'name' : 'NAME'})


if __name__ == '__main__':
    unittest.main()
