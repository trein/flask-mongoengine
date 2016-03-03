import atexit, os, time, mongoengine
import shutil, subprocess, tempfile
from flask import current_app
from pymongo import MongoClient, ReadPreference, errors
from mongoengine.python_support import IS_PYMONGO_3

__all__ = ['create_connection', 'disconnect', 'get_connection',
           'DEFAULT_CONNECTION_NAME']

DEFAULT_CONNECTION_NAME = 'default-sandbox'

_connection_settings = {}
_connections = {}
_tmpdir = tempfile.mkdtemp()
_conn = None
_process = None
_dbs = {}

def disconnect(alias=DEFAULT_CONNECTION_NAME, preserved=False):
    global _connections, _dbsm, _process

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]
    if alias in _dbs:
        del _dbs[alias]

    if _process:
        _process.terminate()
        _process.wait()
        _process = None
        if not preserved:
            shutil.rmtree(_tmpdir, ignore_errors=True)


def get_connection(alias=DEFAULT_CONNECTION_NAME):
    global _connections

    # Establish new connection unless
    # already established
    if alias not in _connections:
        if alias not in _connection_settings:
            msg = 'Connection with alias "%s" has not been defined' % alias
            if alias == DEFAULT_CONNECTION_NAME:
                msg = 'You have not defined a default connection'
            raise ConnectionError(msg)
        conn_settings = _connection_settings[alias].copy()

        conn_host = conn_settings['host']
        db_name = conn_settings['name']

        conn_settings.pop('name', None)
        conn_settings.pop('username', None)
        conn_settings.pop('password', None)
        conn_settings.pop('authentication_source', None)

        if current_app.config['TESTING']:
            if (conn_host.startswith('mongomock://') and
                mongoengine.__version__ < (0, 10, 6)):
                # Use MongoClient from mongomock
                try:
                    import mongomock
                except ImportError:
                    raise RuntimeError('You need mongomock installed '
                                       'to mock MongoEngine.')
                connection_class = mongomock.MongoClient

            elif (mongoengine.__version__ >= (0, 10, 6) and
                  conn_host.startswith('mongomock://')):
                # Let mongoengine handle the default
                _connections[alias] = mongoengine.connect(db_name, **conn_settings)

            elif current_app.config['TEMP_DB']:
                db_alias = conn_settings['alias']
                preserved = conn_settings.get('preserve_temp_db', False)
                return _register_test_connection(conn_host, db_alias, preserved)
        else:
            # Let mongoengine handle the default
            _connections[alias] = mongoengine.connect(db_name, **conn_settings)

        try:
            connection = None
            # check for shared connections
            connection_settings_iterator = (
                    (db_alias, settings.copy())
                    for db_alias, settings in _connection_settings.iteritems())

            for db_alias, connection_settings in connection_settings_iterator:
                connection_settings.pop('name', None)
                connection_settings.pop('username', None)
                connection_settings.pop('password', None)
                if (conn_settings == connection_settings and
                    _connections.get(db_alias, None)):
                    connection = _connections[db_alias]
                    break

            _connections[alias] = connection if connection else connection_class(**conn_settings)
        except Exception, e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (alias, e))
    return _connections[alias]

def _register_test_connection(port, db_alias, preserved):
    # TEMP_DB setting uses 27111 as
    # default port
    if port == 27017:
        port = 27111

    _conn = _connections.get(db_alias, None)

    if _conn is None:
        _process = subprocess.Popen([
                'mongod', '--bind_ip', 'localhost',
                '--port', str(port),
                '--dbpath', _tmpdir,
                '--nojournal', '--nohttpinterface',
                '--noauth', '--smallfiles',
                '--syncdelay', '0',
                '--maxConns', '10',
                '--nssize', '1', ],
                stdout=open(os.devnull, 'wb'),
                stderr=subprocess.STDOUT)
        atexit.register(disconnect, preserved=preserved)

        # wait for the instance db to be ready
        # before opening a Connection.
        for i in range(3):
            time.sleep(0.1)
            try:
                _conn = MongoClient('localhost', port)
            except errors.ConnectionFailure:
                continue
            else:
                break
        else:
            msg = 'Cannot connect to the mongodb test instance'
            raise mongoengine.ConnectionError(msg)
        _connections[db_alias] = _conn
    return _conn

def create_connection(config):
    """
    Connection is created base on application configuration
    settings. Application settings which is enabled as TESTING
    can submit MongoMock URI or enable TEMP_DB setting to provide
    default temporary MongoDB instance on localhost for testing
    purposes.

    Unless PRESERVE_TEST_DB is setting is enabled in application
    configuration, temporary MongoDB instance will be deleted when
    application instance go out of scope.

    Setting to request MongoMock instance connection:
        >> app.config['TESTING'] = True
        >> app.config['MONGODB_ALIAS'] = 'unittest'
        >> app.config['MONGODB_HOST'] = 'mongo://localhost'

    Setting to request temporary localhost instance of MongoDB
    connection:
        >> app.config['TESTING'] = True
        >> app.config['TEMP_DB'] = True

    To avoid temporary localhost instance of MongoDB been deleted
    when application go out of scope:
        >> app.config['PRESERVE_TEMP_DB'] = true

    @param config: Flask-MongoEngine application configuration.

    """
    global _connections, _connection_settings

    if config is None or not isinstance(config, dict):
        raise Exception("Invalid application configuration");

    read_preference = False
    if IS_PYMONGO_3:
        read_preference = ReadPreference.PRIMARY

    conn_settings = {}
    alias = config.get('MONGODB_ALIAS', DEFAULT_CONNECTION_NAME)

    # Alway create new connection unless already
    # exist
    if alias not in _connections:
        if 'MONGODB_SETTINGS' in config:
            # Connection settings provided as a dictionary.
            conn_settings = config['MONGODB_SETTINGS']
        else:
            # Connection settings provided in standard format.
            conn_settings = {
                'alias'             : alias,
                'name'              : config.get('MONGODB_DB', 'test'),
                'preserve_temp_db'  : config.get('PRESERVE_TEMP_DB', False),
                'host'              : config.get('MONGODB_HOST', 'localhost'),
                'password'          : config.get('MONGODB_PASSWORD', None),
                'port'              : config.get('MONGODB_PORT', 27017),
                'username'          : config.get('MONGODB_USERNAME', None),
                'read_preference'   : read_preference,
            }

        # Ugly dict comprehention in order to support python 2.6
        conn = dict((k.lower(), v) for k, v in conn_settings.items() if v is not None)

        if 'db' in conn:
            conn['name'] = conn.pop('name', 'test')
        if 'replicaset' in conn:
            conn['replicaSet'] = conn.pop('replicaset')

        _connection_settings[alias] = conn

    # Handle multiple connections recursively
    if isinstance(conn_settings, list):
        connections = {}
        for conn in conn_settings:
            connections[alias] = get_connection(alias)
        return connections

    return get_connection(alias)

# Support for old naming convensions
_create_connection = create_connection