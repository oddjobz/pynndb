"""PynnDB is an alternative top-level wrapper for a database object.

This attemps to wrap a number of functionalities including logging and replication.
"""
#
# MIT License
#
# Copyright (c) 2017, 2018 Gareth Bult
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import lmdb
from pynndb import Table

METADATA = '__metadata__'


class PynnDB:

    def __init__(self, name, conf=None):
        """
        Set up a new database ready for access.

        Args:
            name (str): the (path)name of the database to access
            conf (dict): any overrides for the databnase environment
                - see the lmdb documentation for available [lmdb] options

        Returns:
            PynnDB: a :class:`PynnDB` instance
        """
        self._env = None
        self._conf = None
        self._name = name
        self._debug = None
        self._db = None
        self._meta = None
        self._txn = None
        self._tables = {}

        if not self._env:
            self._conf = {
                'subdir': True,
                'lock': True,
                'map_size': 2147483648,  # 2G
                'metasync': False,
                'sync': True,
                'max_dbs': 64,
                'writemap': True,
                'map_async': True
            }
            self._env = lmdb.Environment(name, **self._conf)
            if conf:
                self._conf.update(self._conf, **conf)

        self._db = self._env.open_db()
        with Transaction(self):
            self._meta = self.table(METADATA)

        self._initialise_logging()
        self._initialise_replication()

    def __del__(self):
        """Delete the instance"""
        self.close()

    def close(self):
        """Shutdown access to the database cleanly"""
        if self._env:
            self._env.close()
            self._env = None

    def sync(self):
        """
        Synchronise the database with backing store
        """
        self._env.sync(True)

    def exists(self, name):
        """
        Check to see if the named table exists

        Args:
            name (str): the name of the table to look for

        Returns:
            bool: true if table exists
        """
        return name in list(self.tables)

    def _fetch_tables(self, all=False):
        """
        Generate a list of table names from the current database

        Args:
            all (bool): whether to return extended metadata tables

        Returns:
            generator: next available table name
        """
        def table_names():
            with lmdb.Cursor(self._db, self._txn) as cursor:
                if cursor.first():
                    while True:
                        name = cursor.key().decode()
                        if all or name[0] not in ['_', '~']:
                            yield name
                        if not cursor.next():
                            break

        if self._txn:
            return table_names()
        else:
            with Transaction(self) as a:
                return list(table_names())

    def begin(self, txn):
        self._txn = txn

    def end(self):
        self._txn = None

    def _initialise_logging(self):
        pass

    def _initialise_replication(self):
        pass

    def table(self, name):
        """
        Request access to a specific database table

        Args:
            name (str): The name for the table to access

        Returns:
            Table: a :class:`Table` instance
        """
        assert self._txn, 'attempt to access table from outside of a transaction'

        if name not in self._tables:
            self._tables[name] = Table(self, name, self._txn)
        return self._tables[name]

    @property
    def env(self):
        return self._env

    @property
    def node(self):
        return None

    @property
    def tables(self):
        return self._fetch_tables()

    @property
    def tables_all(self):
        return self._fetch_tables(all=True)


class Transaction:

    def __init__(self, db, write=False):
        """
        Prepare a transaction that will allow us access to the database

        Args:
            db (PynnDB): a :class:`PynnDB` database instance

        Returns:
            Transaction: a :class:`Transaction` instance
        """
        self._db = db
        self._txn = lmdb.Transaction(db._env, write=write)

    def __enter__(self):
        """Tag our transaction to the database as our current transaction"""
        self._db.begin(self._txn)
        return self

    def __exit__(self, txn_type, txn_value, traceback):
        """Clear down the current transaction and commit or abort"""
        if txn_type:
            self._txn.abort()
        else:
            self._txn.commit()
        self._txn = None
        self._db.end()
