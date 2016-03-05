import unittest
import mongomock
import mongoengine, pymongo
from pymongo.errors import InvalidURI

from flask.ext.mongoengine import MongoEngine, InvalidSettingsError
from tests import FlaskMongoEngineTestCase


class ConnectionTestCase(FlaskMongoEngineTestCase):

    def ensure_mongomock_connection(self):
        db = MongoEngine(self.app)
        db_info = db.connection.server_info()
        self.assertTrue(isinstance(db_info, dict))
        self.assertEqual(db_info['sysInfo'], "Mock", "Invalid MongoMock connection")
        self.assertTrue(isinstance(db.connection, mongomock.MongoClient))

        # Finally close connection
        self.assertTrue(db.disconnect())

    def test_mongomock_connection_request_on_most_recent_mongoengine(self):
        self.app.config['TESTING'] = True
        self.app.config['MONGODB_ALIAS'] = 'unittest'
        self.app.config['MONGODB_HOST'] = 'mongomock://localhost'

        if mongoengine.VERSION >= (0, 10, 6):
            self.ensure_mongomock_connection()

    def test_mongomock_connection_request_on_most_old_mongoengine(self):
        self.app.config['TESTING'] = 'True'
        self.assertRaises(InvalidSettingsError, MongoEngine, self.app)

        self.app.config['TESTING'] = True
        self.app.config['MONGODB_ALIAS'] = 'unittest'
        self.app.config['MONGODB_HOST'] = 'mongomock://localhost'

        if mongoengine.VERSION < (0, 10, 6):
            self.ensure_mongomock_connection()

    def test_mongodb_temp_instance(self):
        self.app.config['TESTING'] = True
        self.app.config['TEMP_DB'] = 'True'
        self.assertRaises(InvalidSettingsError, MongoEngine, self.app)

        self.app.config['TEMP_DB'] = True

        db = MongoEngine(self.app)
        db_info = db.connection.server_info()
        self.assertTrue(isinstance(db_info, dict))
        self.assertTrue(isinstance(db.connection, pymongo.MongoClient))

        # Finally close connection
        self.assertTrue(db.disconnect())

    def test_parse_uri_if_testing_true_and_not_uses_mongomock_schema(self):
        self.app.config['TESTING'] = True
        self.app.config['MONGODB_ALIAS'] = 'unittest'
        self.app.config['MONGODB_HOST'] = 'mongo://localhost'

        self.assertRaises(InvalidURI, MongoEngine, self.app)

    def test_parse_uri_if_testing_not_true(self):
        self.app.config['TESTING'] = False
        self.app.config['MONGODB_ALIAS'] = 'unittest'
        self.app.config['MONGODB_HOST'] = 'mongomock://localhost'

        self.assertRaises(InvalidURI, MongoEngine, self.app)

if __name__ == '__main__':
    unittest.main()
