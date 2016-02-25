import atexit, os, time, mongoengine
import pymongo, shutil, subprocess, tempfile
from pymongo import uri_parser
from flask import current_app

"""
Singleton DB test instance to manage
temporary MongoDB instance for test cases.

Use this for testing purpose only. The
instance is automatically destroyed at
the end of the program.

"""

__all__ = ['create_connection', ]

_tmpdir = tempfile.mkdtemp()
_conn = None
_process = None
_connections = {}

def create_connection(conn_settings):
    # Handle multiple connections recursively
    if isinstance(conn_settings, list):
        connections = {}
        for conn in conn_settings:
            connections[conn.get('alias')] = _create_connection(conn)
        return connections

    # Ugly dict comprehention in order to support python 2.6
    conn = dict((k.lower(), v) for k, v in conn_settings.items() if v is not None)

    if 'replicaset' in conn:
        conn['replicaSet'] = conn.pop('replicaset')

    # Use temporary MongoDB instance
    if(current_app.config['TESTING'] == True and not
        conn.get('host', '').startswith('mongomock://')):
        host = conn.get('port', 27111)
        preserve = conn.get('preserve_testdb', False)
        return _register_test_connection(host, preserve)

    if (mongoengine.__version__ >= (0, 10, 6) and
        current_app.config['TESTING'] == True and
        conn.get('host', '').startswith('mongomock://')):
        pass
    elif "://" in conn.get('host', ''):
        # Handle uri style connections
        uri_dict = uri_parser.parse_uri(conn['host'])
        conn['db'] = uri_dict['database']

    return mongoengine.connect(conn.pop('db', 'test'), **conn)

def _register_test_connection(port, preserved):
    db_alias = "DEFAULT_TEST_DB"
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
        atexit.register(_shutdown, preserved)

        # wait for the instance db to be ready
        # before opening a Connection.
        for i in range(3):
            time.sleep(0.1)
            try:
                _conn = pymongo.MongoClient('localhost', port)
            except pymongo.errors.ConnectionFailure:
                continue
            else:
                break
        else:
            msg = 'Cannot connect to the mongodb test instance'
            raise mongoengine.ConnectionError(msg)
        _connections[db_alias] = _conn
    return _conn

def _shutdown(preserved):
    global _process
    if _process:
        _process.terminate()
        _process.wait()
        _process = None
        if not preserved:
            shutil.rmtree(_tmpdir, ignore_errors=True)

# Support for old naming convensions
_create_connection = create_connection
