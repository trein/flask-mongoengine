import atexit, os, time, mongoengine, sys
import shutil, subprocess, tempfile
from flask import current_app
from pymongo import MongoClient, ReadPreference, errors, uri_parser
from mongoengine.python_support import IS_PYMONGO_3

__all__ = ['create_connection', 'disconnect', 'get_connection',
           'DEFAULT_CONNECTION_NAME', 'fetch_connection_settings']

DEFAULT_CONNECTION_NAME = 'default-sandbox'

_connection_settings = {}
_connections = {}
_tmpdir = tempfile.mkdtemp()
_conn = None
_process = None

def disconnect(alias=DEFAULT_CONNECTION_NAME, preserved=False):
    global _connections, _dbsm, _process

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]

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

        if current_app.config.get('TESTING', None):
            if current_app.config.get('TEMP_DB', None):
                db_alias = conn_settings['alias']
                preserved = conn_settings.get('preserve_temp_db', False)
                return _register_test_connection(conn_host, db_alias, preserved)

            elif (conn_host.startswith('mongomock://') and
                mongoengine.VERSION < (0, 10, 6)):
                # Use MongoClient from mongomock
                try:
                    import mongomock
                except ImportError:
                    raise RuntimeError('You need mongomock installed '
                                       'to mock MongoEngine.')
                connection_class = mongomock.MongoClient
            else:
                # Let mongoengine handle the default
                _connections[alias] = mongoengine.connect(db_name, **conn_settings)
        else:
            # Let mongoengine handle the default
            _connections[alias] = mongoengine.connect(db_name, **conn_settings)

        try:
            connection = None
            connection_iter_items = _connection_settings.items() \
                if (sys.version_info >= (3, 0)) else _connection_settings.iteritems()

            # check for shared connections
            connection_settings_iterator = \
                ((db_alias, settings.copy()) for db_alias, settings in connection_iter_items)

            for db_alias, connection_settings in connection_settings_iterator:
                connection_settings.pop('name', None)
                connection_settings.pop('username', None)
                connection_settings.pop('password', None)
                if (conn_settings == connection_settings and
                    _connections.get(db_alias, None)):
                    connection = _connections[db_alias]
                    break

            _connections[alias] = connection \
                if connection else connection_class(**conn_settings)

        except Exception as e:
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

def _resolve_settings(conn_setting, removePass=True):
    if conn_setting and isinstance(conn_setting, dict):
        read_preference = False
        if IS_PYMONGO_3:
            read_preference = ReadPreference.PRIMARY

        resolved = {}
        resolved['read_preference'] = read_preference
        resolved['alias'] = conn_setting.get('MONGODB_ALIAS', DEFAULT_CONNECTION_NAME)
        resolved['name'] = conn_setting.get('MONGODB_DB', 'test')
        resolved['preserve_temp_db'] = conn_setting.get('PRESERVE_TEMP_DB', False)
        resolved['host'] = conn_setting.get('MONGODB_HOST', 'localhost')
        resolved['password'] = conn_setting.get('MONGODB_PASSWORD', None)
        resolved['port'] = conn_setting.get('MONGODB_PORT', 27017)
        resolved['username'] = conn_setting.get('MONGODB_USERNAME', None)
        resolved['replicaSet'] = conn_setting.pop('replicaset', None)

        host = resolved['host']
        # Handle uri style connections
        if host.startswith('mongodb://'):
            uri_dict = uri_parser.parse_uri(host)
            if uri_dict['database']:
                resolved['host'] = uri_dict['database']
            if uri_dict['password']:
                resolved['password'] = uri_dict['password']
            if uri_dict['username']:
                resolved['username'] = uri_dict['username']
            if uri_dict['options'] and uri_dict['options']['replicaset']:
                resolved['replicaSet'] = uri_dict['options']['replicaset']

        if removePass:
            resolved.pop('password')
        return resolved
    return conn_setting

def fetch_connection_settings(config, removePass=True):
    """
    Fetch DB connection settings from FlaskMongoEngine
    application instance configuration. For backward
    compactibility reasons the settings name has not
    been replaced.

    It has instead been mapped correctly
    to avoid connection issues.

    @param config:          FlaskMongoEngine instance config

    @param removePass:      Flag to instruct the method to either
                            remove password or maintain as is.
                            By default a call to this method returns
                            settings without password.

    """

    if 'MONGODB_SETTINGS' in config:
        settings = config['MONGODB_SETTINGS']
        if isinstance(settings, list):
            # List of connection settings.
            settings_list = []
            for setting in settings:
                settings_list.append(_resolve_settings(setting, removePass))
            return settings_list
        else:
            # Connection settings provided as a dictionary.
            return _resolve_settings(settings, removePass)
    else:
        # Connection settings provided in standard format.
        return _resolve_settings(config, removePass)

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
    global _connection_settings

    if config is None or not isinstance(config, dict):
        raise Exception("Invalid application configuration");

    conn_settings = fetch_connection_settings(config, False)

    # Handle multiple connections recursively
    if isinstance(conn_settings, list):
        connections = {}
        for conn_setting in conn_settings:
            alias = conn_setting['alias']
            connections[alias] = get_connection(alias)
        return connections
    else:
        alias = conn_settings['alias']
        _connection_settings[alias] = conn_settings
        return get_connection(alias)

# Support for old naming convensions
_create_connection = create_connection
