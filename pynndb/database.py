import lmdb
# from posix_ipc import Semaphore, ExistentialError, O_CREAT
from struct import pack
from ujson import dumps
from .table import Table
from .transaction import Transaction
from .utils import xTableMissing, xTableExists, semaphore_path


class Database(object):
    """
    Representation of a Database, this is the main API class

    **Arguments**:

    - `name` - The name of the database to open
    - `conf` - Any additional or custom options for this environment
    - `binlog` - Enable binary logging
    - `size` - Set the maximum database size
    - `master` - currently unused

    **Raises**:

    - `lmdb.NotFoundError` if the database folder is not found
    """
    _debug = False
    _conf = {
        'map_size': 1024 * 1024 * 1024 * 2,
        'subdir': True,
        'metasync': False,
        'sync': True,
        'lock': True,
        'max_dbs': 64,
        'writemap': True,
        'map_async': True
    }

    def __init__(self, name, conf=None, binlog=True, size=None, master=False):
        conf = dict(self._conf, **conf.get('env', {})) if conf else self._conf
        if size: conf['map_size'] = size
        self._tables = {}
        self._semaphore = False
        self._name = name
        self._env = lmdb.Environment(name, **conf)
        self._db = self._env.open_db()
        self._binlog = None
        self._binidx = None
        self._meta = self.table('__metadata__')
        doc = self._meta.get(b'__node__')
        self._node = doc.get('value') if doc else 0

        try:
            self.set_binlog(enable=False)
            if binlog:
                self.set_binlog(enable=True)
                # try:
                #     self._semaphore = Semaphore(semaphore_path(name))
                # except ExistentialError:
                #     pass
                self._binlog = self._env.open_db(b'__binlog__', create=binlog)
                self._binidx = self._env.open_db(b'__binidx__', create=binlog)
                with self._env.begin(write=True) as txn:
                    if not txn.stat(db=self._binlog)['entries']:
                        dat = dumps({'txn': []}).encode()
                        txn.put(pack('>Q', 1), dat, db=self._binlog, append=False)

        except lmdb.NotFoundError:
            pass
        except:
            raise

    def __del__(self):
        self.close()

    @property
    def node(self):
        return self._node

    @property
    def binlog(self):
        return self._binlog

    @property
    def binidx(self):
        return self._binidx

    @property
    def env(self):
        return self._env

    def set_binlog(self, enable=True):
        """
        Enable or disable binary logging, when enabled this option generates a transaction history
        entry each time a database routine executes.

        **Arguments**:

        - `enable` - If True, enable binary logging

        """
        if enable:
            if not self._binlog:
                self._binlog = self.env.open_db(b'__binlog__')
                self._binidx = self.env.open_db(b'__binidx__')
        else:
            if self._binlog:
                with self.env.begin(write=True) as txn:
                    txn.drop(self._binlog, True)
                    txn.drop(self._binidx, True)

            self._binlog = None
            self._binidx = None

    def begin(self, *args, **kwargs):
        """
        Begin a new (write) transaction returning a transaction reference (use with "with")

        **Arguments**:

        Any arguments accepted by the Transaction Object, typically None.

        **Returns**:

        A new `Transaction` object
        """
        return Transaction(self, *args, write=True, **kwargs)

    def close(self):
        """
        Close the current database
        """
        if hasattr(self, '_env') and self._env:
            self._env.close()
            self._env = None

    def sync(self, force=False):
        self.env.sync(force)

    def exists(self, name):
        """
        Check whether a table with a given name already exists

        **Arguments**:

        - `name` - The name of the table to check for

        **Returns**:

        `True` if the table exists, otherwise `False`
        """
        return name in self.tables

    @property
    def tables(self):
        return self._return_tables(False)

    @property
    def tables_all(self):
        return self._return_tables(True)

    def _return_tables(self, all, txn=None):
        """
        PROPERTY - Generate a list of names of the tables associated with this database

        :getter: Returns a list of table names
        :type: list
        """
        def tables():
            result = []
            with lmdb.Cursor(self._db, txn) as cursor:
                if cursor.first():
                    while True:
                        name = cursor.key().decode()
                        if all or name[0] not in ['_', '~']:
                            result.append(name)
                        if not cursor.next():
                            break
            return result

        if not txn:
            with self.env.begin() as txn:
                return tables()
        else:
            return tables()

    def drop(self, name, txn=None):
        """
        Drop a database table and delete the contents

        **Arguments**:

        - `name` - The name of the table to drop
        - `txn` - The transaction to work with or `None` if we're not inside a transaction
        """
        if name not in self._return_tables(True, txn):
            raise xTableMissing
        self.table(name, txn=txn)
        if name in self._tables:
            table = self._tables[name]
            del self._tables[name]
            if txn:
                table.drop(txn=txn)
            else:
                with self.env.begin(write=True) as txn:
                    table.drop(txn=txn)

    def restructure(self, name):
        """
        Restructure a table, copy to a temporary table, then copy back. This will recreate the table
        and all it's ID's but will retain the original indexes. (which it will regenerate)

        **Arguments**:

        - `name` - Name of the table to restructure
        """
        with self.env.begin(write=True) as txn:
            if name not in self.tables: raise xTableMissing
            src = self._tables[name]
            dst_name = '~' + name
            if dst_name in self.tables: raise xTableExists
            dst = self.table(dst_name, txn=txn)
            for doc in src.find(txn=txn):
                dst.append(doc, txn=txn)

            src.empty(txn=txn)
            for doc in dst.find(txn=txn):
                src.append(doc, txn=txn)
            self.drop(dst_name, txn=txn)

    def table(self, name, txn=None):
        """
        Return a reference to a table with a given name, creating first if it doesn't exist

        **Arguments**:

        - `name` - Name of the table to open
        - `txn` - The transaction to work with or `None` if we're not inside a transaction

        **Returns**:

        A `Table` object
        """
        if name not in self._tables:
            self._tables[name] = Table(self, name, txn)
        return self._tables[name]

    def size(self):
        """
        Return the current mapped size of the database

        **Returns**:

        The `size` of the database as a human-readable string
        """
        size = self._env.info()['map_size']
        size = size / (1024*1024)
        if size < 1000:
            return '{}M'.format(size)
        size = size / 1024
        return '{}G'.format(size)
