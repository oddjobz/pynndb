"""
PyMamba is a database schema that sits on top of the LMDB storage engine. It provides a
higher level of functionality than is provided by the RAW LMDB API, primarily the ability
to transparently handle indexes, and the ability to transparently read and write Python
variables (including list/dict structures). Currently it's *much* faster than Mongo, but 
not feature complete and not exhaustively tested in the real world.
"""
##############################################################################
# TODO Items go here ...
##############################################################################
#
# MIT License
#
# Copyright (c) 2017 Gareth Bult
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
##############################################################################
#
#   This is the current implementation of "mamba" which is a database layout
#   that sites directly on top of LMDB. Currently it's *much* faster than
#   Mongo, but currently incomplete and untested .. but it's great for 
#   playing with.
#
##############################################################################
from .database import *
from .table import *
from .index import *
from .transaction import *
from .utils import *
from .replication import *
from .mp import *

# from ujson import loads, dumps
# from sys import _getframe, maxsize
# from bson.objectid import ObjectId
# from ujson_delta import diff
# import lmdb
# from .database import Database
# from .replication import ReplicationSync
#
# __version__ = '0.3.0'
#
#
# def write_transaction(func):
#     """
#     Wrapper for write transactions to ensure a an appropriate transaction is in place
#     """
#     def wrapped_f(*args, **kwargs):
#         if 'txn' in kwargs and kwargs['txn']:
#             return func(*args, **kwargs)
#         with args[0]._ctx.env.begin(write=True) as kwargs['txn']:
#             return func(*args, **kwargs)
#     return wrapped_f


# class Transaction(object):
#
#     def __init__(self, database, write=False, buffers=False):
#         self._transactions = []
#         self._replicated = False
#         self._db = database
#         self._txn = lmdb.Transaction(self._db.env, write=write, buffers=buffers)
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, txn_type, txn_value, traceback):
#         self._db._locks = []
#         if txn_type or not len(self._transactions):
#             self._txn.abort()
#             return
#         self._record_binlog()
#         self._txn.commit()
#
#     def _record_binlog(self):
#         if not self._db.binlog or self._replicated:
#             return
#         key = str(ObjectId()) #+ ('-R' if self._replicated else '')
#         ok = self._txn.put(
#             key.encode(),
#             dumps({'txn': self._transactions}).encode(),
#             db=self._db.binlog,
#             append=False
#         )
#         if not ok:
#             raise xWriteFail('Fatal: Unable to record transaction #{}'.format(key))
#         self._db.event.set() if not self._db.event.is_set() else None
#
#     def append(self, table, doc):
#         self._transactions.append({'cmd': 'add', 'tab': table.name, 'doc': doc})
#         return table.append(doc, txn=self._txn)
#
#     def delete(self, table, keys):
#         self._transactions.append({'cmd': 'del', 'tab': table, 'keys': keys})
#         return table.delete(keys, txn=self._txn)
#
#     def save(self, table, doc):
#         delta = table.save(doc, txn=self._txn)
#         self._transactions.append({'cmd': 'upd', 'tab': table, 'key': doc['_id'], 'yyy': delta})
#
#     def empty_table(self, table):
#         self._transactions.append({'cmd': 'emp', 'tab': table})
#         return table.empty(txn=self._txn)
#
#     def create_index(self, table, name, func, duplicates):
#         self._transactions.append({'cmd': 'idx', 'tab': table, 'idx': name, 'fun': func, 'dup': duplicates})
#         return table.index(name, func, duplicates, txn=self._txn)
#
#     def drop_index(self, table, name):
#         self._transactions.append({'cmd': 'uix', 'tab': table, 'idx': name})
#         return table.drop_index(name, txn=self._txn)
#
#     def create_table(self, table):
#         self._transactions.append({'cmd': 'cre', 'tab': table})
#         return self._db.table(table.name, txn=self._txn)
#
#     def drop_table(self, table):
#         self._transactions.append({'cmd': 'drp', 'tab': table})
#         return table.drop(txn=self._txn)
#

# class Database(object):
#     """
#     Representation of a Database, this is the main API class
#
#     :param name: The name of the database to open
#     :type name: str
#     :param conf: Any additional or custom options for this environment
#     :type conf: dict
#     """
#     _debug = False
#     _conf = {
#         'map_size': 1024*1024*1024*2,
#         'subdir': True,
#         'metasync': False,
#         'sync': True,
#         'lock': True,
#         'max_dbs': 64,
#         'writemap': True,
#         'map_async': True
#     }
#
#     def __init__(self, name, conf=None, binlog=True, size=None, event=None):
#         conf = dict(self._conf, **conf.get('env', {})) if conf else self._conf
#         if size: conf['map_size'] = size
#         self._tables = {}
#         self._name = name
#         self._env = lmdb.Environment(name, **conf)
#         self._db = self._env.open_db()
#         self._event = event
#         try:
#             self._binlog = self._env.open_db(b'__binlog__', create=binlog)
#             #self.binlog(enable=False)
#             #self.binlog(enable=True)
#         except lmdb.NotFoundError:
#             self._binlog = None
#         except Exception as e:
#             raise e
#         self._metadata = self.env.open_db(b'__metadata__', create=True)
#
#     def __del__(self):
#         self.close()
#
#     @property
#     def binlog(self):
#         return self._binlog
#
#     @property
#     def event(self):
#         return self._event
#
#     @property
#     def env(self):
#         """
#         Return a reference to the current database environment
#
#         :return: A Database Environment
#         :rtype: Environment
#         """
#         return self._env
#
#     def set_binlog(self, enable=True):
#         """
#         Enable or disable binary logging, disable with delete the transaction history too ...
#
#         :param enable: Whether to enable or disable logging
#         """
#         if enable:
#             if not self._binlog:
#                 self._binlog = self.env.open_db('__binlog__'.encode())
#         else:
#             if self._binlog:
#                 with self.env.begin(write=True) as txn:
#                     txn.drop(self._binlog, True)
#
#             self._binlog = None
#
#     def begin(self, *args, **kwargs):
#         """
#         Begin a new transaction returning a transaction reference (use with "with")
#         :return: Reference to the new transaction
#         :rtype: DBTransaction
#         """
#         return Transaction(self, *args, write=True, **kwargs)
#
#     def close(self):
#         """
#         Close the current database
#         """
#         if self._env:
#             self._env.close()
#             self._env = None
#
#     def sync(self, force=False):
#         self.env.sync(force)
#
#     def exists(self, name):
#         """
#         Test whether a table with a given name already exists
#
#         :param name: Table name
#         :type name: str
#         :return: True if table exists
#         :rtype: bool
#         """
#         return name in self.tables
#
#     @property
#     def tables(self):
#         return self._return_tables(False)
#
#     @property
#     def tables_all(self):
#         return self._return_tables(True)
#
#     def _return_tables(self, all):
#         """
#         PROPERTY - Generate a list of names of the tables associated with this database
#
#         :getter: Returns a list of table names
#         :type: list
#         """
#         with self.env.begin() as txn:
#             result = []
#             with lmdb.Cursor(self._db, txn) as cursor:
#                 if cursor.first():
#                     while True:
#                         name = cursor.key().decode()
#                         if all or name[0] not in ['_', '~']:
#                             result.append(name)
#                         if not cursor.next():
#                             break
#             return result
#
#     def drop(self, name, txn=None):
#         """
#         Drop a database table
#
#         :param name: Name of table to drop
#         :type name: str
#         """
#         if name not in self.tables_all:
#             raise xTableMissing
#
#         self.table(name)
#         if name in self._tables:
#             if txn:
#                 self._tables[name].drop(txn=txn)
#             else:
#                 with self.env.begin(write=True) as txn:
#                     self._tables[name].drop(txn=txn)
#             del self._tables[name]
#
#     def restructure(self, name):
#         """
#         Restructure a table, copy to a temporary table, then copy back. This will recreate the table
#         and all it's ID's but will retain the original indexes. (which it will regenerate)
#
#         :param name: Name of the table to restructure
#         :type name: str
#         """
#         txn = self.transaction.txn
#         if name not in self.tables: raise xTableMissing
#         src = self._tables[name]
#         dst_name = '~'+name
#         if dst_name in self.tables: raise xTableExists
#         dst = self.table(dst_name)
#         for doc in src.find():
#             dst.append(doc, txn=txn)
#
#         src.empty(txn=txn)
#         for doc in dst.find(txn=txn):
#             src.append(doc, txn=txn)
#         dst._drop(txn=txn)
#         del self._tables[dst_name]
#
#     def table(self, name, txn=None):
#         """
#         Return a reference to a table with a given name, creating first if it doesn't exist
#
#         :param name: Name of table
#         :type name: str
#         :return: Reference to table
#         :rtype: Table
#         """
#         if name not in self._tables:
#             self._tables[name] = Table(self, name, txn)
#         return self._tables[name]


# class Table(object):
#     """
#     Representation of a database table
#
#     :param ctx: An open database Environment
#     :type ctx: Database
#     :param name: A table name
#     :type name: str
#     """
#     def __init__(self, ctx, name=None, txn=None):
#
#         self._debug = False
#         self._ctx = ctx
#         self._name = name
#         self._indexes = {}
#         self._open_(txn=txn)
#
#     def begin(self):
#         return lmdb.Transaction(self._ctx.env)
#
#     @property
#     def name(self):
#         return self._name
#
#     @write_transaction
#     def _open_(self, txn=None):
#         self._db = self._ctx.env.open_db(self._name.encode(), txn=txn)
#         for index in self.indexes:
#             key = _index_name(self, index).encode()
#             doc = loads(bytes(txn.get(key, db=self._ctx._metadata)))
#             self._indexes[index] = Index(self._ctx, index, doc['func'], doc['conf'], txn)
#
#     @write_transaction
#     def append(self, record, txn=None):
#         """
#         Append a new record to this table
#
#         :param record: The record to append
#         :type record: dict
#         :param txn: An open transaction
#         :type txn: Transaction
#         :raises: xWriteFail on write error
#         """
#         if not '_id' in record:
#             key = str(ObjectId()).encode()
#             append=True
#         else:
#             append=False
#             key = record['_id']
#             if not isinstance(key, bytes):
#                 if isinstance(key, str):
#                     key = key.encode()
#                 elif isinstance(key, int):
#                     key = str(key).encode()
#                 else:
#                     raise TypeError('_id is type {}'.format(type(key)))
#         if not txn.put(key, dumps(record).encode(), db=self._db, append=append): raise xWriteFail(key)
#         record['_id'] = key
#         for name in self._indexes:
#             if not self._indexes[name].put(txn, key, record): raise xWriteFail(name)
#
#     @write_transaction
#     def delete(self, keys, txn=None):
#         """
#         Delete a record from this table
#
#         :param keys: A list of database keys to delete or a record
#         :type keys: list|dict
#         :param txn: Transaction
#         :type txn: An options transaction
#         """
#         if not isinstance(keys, list):
#             if isinstance(keys, dict):
#                 keys = [keys['_id']]
#             else:
#                 keys = [keys]
#
#         for key in keys:
#             doc = loads(bytes(txn.get(key, db=self._db)))
#             if not txn.delete(key, db=self._db): raise xWriteFail
#             for name in self._indexes:
#                 print("DELTXN:", txn)
#                 if not self._indexes[name].delete(txn, key, doc): raise xWriteFail
#
#     @write_transaction
#     def save(self, record, txn):
#         """
#         Save an changes to a pre-existing record
#
#         :param record: The record to be saved
#         :type record: dict
#         :param txn: An open transaction
#         :type txn: Transaction
#         """
#         if not '_id' in record: raise xNoKey
#         key = record['_id']
#         rec = dict(record)
#         del rec['_id']
#         doc = txn.get(key, db=self._db)
#         if not doc: raise xWriteFail('old record is missing')
#         old = loads(bytes(doc))
#         if not txn.put(key, dumps(rec).encode(), db=self._db): raise xWriteFail('main record')
#         for name in self._indexes:
#             self._indexes[name].save(txn, key, old, rec)
#         return diff(old, record, verbose=False)
#
#     @write_transaction
#     def empty(self, txn):
#         """
#         Clear all records from the current table
#         """
#         for name in self.indexes:
#             self._indexes[name].empty(txn)
#         txn.drop(self._db, False)
#
#     @write_transaction
#     def index(self, name, func=None, duplicates=False, txn=None):
#         """
#         Return a reference for a names index, or create if not available
#
#         :param name: The name of the index to create
#         :type name: str
#         :param func: A specification of the index, !<function>|<field name>
#         :type func: str
#         :param duplicates: Whether this index will allow duplicate keys
#         :type duplicates: bool
#         :param txn: An optional transaction
#         :type txn: Transaction
#         :return: A reference to the index, created index, or None if index creation fails
#         :rtype: Index
#         """
#         if name not in self._indexes:
#             conf = {
#                 'key': _index_name(self, name),
#                 'dupsort': duplicates,
#                 'create': True,
#             }
#             self._indexes[name] = Index(self._ctx, name, func, conf, txn)
#             key = _index_name(self, name).encode()
#             val = dumps({'conf': conf, 'func': func}).encode()
#             if not txn.put(key, val, db=self._ctx._metadata): raise xWriteFail
#             self._reindex(name, txn)
#
#         return self._indexes[name]
#
#     @write_transaction
#     def drop_index(self, name, txn):
#         """
#         Drop an index
#
#         :param name: Name of the index to drop
#         :type name: str
#         :param txn: An optional transaction
#         :type txn: Transaction
#         """
#         if name not in self._indexes:
#             raise xIndexMissing
#         return self._unindex(name, txn)
#
#     @write_transaction
#     def drop(self, txn=None):
#         """
#         Drop this tablex and all it's indecies
#
#         :param delete: Whether we delete the table after removing all items
#         :type delete: bool
#         :param txn: An optional transaction
#         :type txn: Transaction
#         """
#         for name in self.indexes:
#             self._unindex(name, txn)
#         return txn.drop(self._db, True)
#
#     def exists(self, name):
#         """
#         See whether an index already exists or not
#
#         :param name: Name of the index
#         :type name: str
#         :return: True if index already exists
#         :rtype: bool
#         """
#         return name in self._indexes
#
#     def find(self, index=None, expression=None, limit=maxsize, txn=None, abort=False):
#         """
#         Find all records either sequential or based on an index
#
#         :param index: The name of the index to use [OR use natural order]
#         :type index: str
#         :param expression: An optional filter expression
#         :type expression: function
#         :param limit: The maximum number of records to return
#         :type limit: int
#         :param txn: An optional transaction
#         :type txn: Transaction
#         :return: The next record (generator)
#         :rtype: dict
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             if not index:
#                 db = self._db
#             else:
#                 if index not in self._indexes:
#                     raise xIndexMissing(index)
#                 index = self._indexes[index]
#                 db = index._db
#             with txn.cursor(db) as cursor:
#                 count = 0
#                 first = True
#                 while count < limit:
#                     if not (cursor.first() if first else cursor.next()):
#                         break
#                     first = False
#                     record = cursor.value()
#                     if index:
#                         key = record
#                         record = txn.get(record, db=self._db)
#                     else:
#                         key = cursor.key()
#                     record = loads(bytes(record))
#                     if callable(expression) and not expression(record):
#                         continue
#                     record['_id'] = key
#                     yield record
#                     count += 1
#
#
#     def range(self, index, lower=None, upper=None, inclusive=True, txn=None, abort=False):
#         """
#         Find all records with a key >= lower and <= upper. If you set inclusive to false the range
#         becomes key > lower and key < upper. Upper and/or Lower can be set to None, if lower is none
#         the range starts at the beginning of the table, and if upper is None searching will continue
#         to the end.
#
#         :param index: The name of the index to search
#         :type index: str
#         :param lower: A template record containing the lower end of the range
#         :type lower: dict
#         :param upper: A template record containing the upper end of the range
#         :type upper: dict
#         :param inclusive: Whether to include items at each boundary
#         :type inclusive: bool
#         :param txn: An optional transaction
#         :type txn: Transaction
#         :return: The records with keys within the specified range (generator)
#         :type: dict
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             if not index:
#                 with txn.cursor(self._db) as cursor:
#                     def forward():
#                         if not cursor.next(): return False
#                         if upper and cursor.key() > upper: return False
#                         return True
#                     lower = lower['_id'] if lower else None
#                     upper = upper['_id'] if upper else None
#                     cursor.set_range(lower) if lower else cursor.first()
#                     while not inclusive and cursor.key() == lower:
#                         if not cursor.next() or cursor.key() == upper: return
#                     while True:
#                         key = cursor.key()
#                         if not key: break
#                         record = loads(bytes(cursor.value()))
#                         record['_id'] = key
#                         if not inclusive:
#                             if not forward(): break
#                             yield record
#                         else:
#                             yield record
#                             if not forward(): break
#             else:
#                 if index not in self._indexes: raise xIndexMissing
#                 index = self._indexes[index]
#                 with txn.cursor(index._db) as cursor:
#                     def forward():
#                         if upper:
#                             if index.match(key, upper) or not index.set_next(cursor, upper): return False
#                         else:
#                             if not cursor.next(): return False
#                         return True
#                     if lower:
#                         index.set_range(cursor, lower)
#                         while not inclusive and index.match(cursor.key(), lower):
#                             if not index.set_next(cursor, upper): break
#                     else:
#                         if cursor.first() and not inclusive: cursor.next()
#                     while True:
#                         key = cursor.key()
#                         if not key: break
#                         record = txn.get(cursor.value(), db=self._db)
#                         if not record: raise xNotFound(cursor.value())
#                         record = loads(bytes(record))
#                         record['_id'] = cursor.value()
#                         if not inclusive:
#                             if not forward(): break
#                             yield record
#                         else:
#                             yield record
#                             if not forward(): break
#
#     def get(self, key, txn=None, abort=False):
#         """
#         Get a single record based on it's key
#
#         :param key: The _id of the record to get
#         :type key: str
#         :return: The requested record
#         :rtype: dict
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             record = txn.get(key, db=self._db)
#             if not record: return None
#             record = loads(bytes(record))
#             record['_id'] = key
#             return record
#
#     def ensure(self, index, func, duplicates=False, force=True):
#         """
#         Ensure than an index exists and create if it's missing
#
#         :param index: The name of the index we're checking
#         :type index: str
#         :param func: The indexing function for this index
#         :type func: str
#         :param duplicates: Whether the index should allow for duplicates
#         :type duplicates: bool
#         :param force: whether to force creation even if it already exists
#         :type force: bool
#         :return: The index we're checking for
#         :rtype: Index
#         """
#         if index in self._indexes:
#             if force:
#                 self.drop_index(index)
#             else:
#                 return self._indexes[index]
#         return self.index(index, func, duplicates)
#
#     @write_transaction
#     def reindex(self, txn):
#         """
#         Reindex all indexes for a given table
#
#         :param name: Name of the table to reindex
#         :type name: str
#         :param txn: An optional transaction
#         :type txn: Trnsaction
#         """
#         for name in self._indexes:
#             self._reindex(name, txn)
#
#     def _reindex(self, name, txn=None):
#         """
#         Reindex an index
#
#         :param name: The name of the index to reindex
#         :type name: str
#         :param txn: An open transaction
#         :type txn: Transaction
#         :return: Number of index entries created
#         :rtype: int
#         """
#         if name not in self._indexes: raise xIndexMissing
#         index = self._indexes[name]
#
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             count = 0
#             self._indexes[name].empty(txn)
#
#             with lmdb.Cursor(self._db, txn) as cursor:
#                 if cursor.first():
#                     while True:
#                         record = loads(bytes(cursor.value()))
#                         if index.put(txn, cursor.key().decode(), record):
#                             count += 1
#                         if not cursor.next():
#                             break
#             return count
#
#     def seek(self, index, record, txn=None):
#         """
#         Find all records matching the key in the specified index.
#
#         :param index: Name of the index to seek on
#         :type index: str
#         :param record: A template record containing the fields to search on
#         :type record: dict
#         :param txn: An optional transaction
#         :type txn: Transaction
#         :return: The records with matching keys (generator)
#         :type: dict
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             index = self._indexes[index]
#             with index.cursor(txn) as cursor:
#                 index.set_key(cursor, record)
#                 while True:
#                     if not cursor.key():
#                         break
#                     key = cursor.value()
#                     record = txn.get(key, db=self._db)
#                     record = loads(bytes(record))
#                     record['_id'] = key
#                     yield record
#                     if not cursor.next_dup():
#                         break
#
#     def seek_one(self, index, record, txn=None, abort=False):
#         """
#         Find the first records matching the key in the specified index.
#
#         :param index: Name of the index to seek on
#         :type index: str
#         :param record: A template record containing the fields to search on
#         :type record: dict
#         :return: The record with matching key
#         :type: dict
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             index = self._indexes[index]
#             entry = index.get(txn, record)
#             if not entry: return None
#             record = txn.get(entry, db=self._db)
#             if not record: return None
#             record = loads(bytes(record))
#             record['_id'] = entry
#             return record
#
#     def _unindex(self, name, txn):
#         """
#         Delete the named index
#
#         :return:
#         :param name: The name of the index
#         :type name: str
#         :param txn: An active transaction
#         :type txn: Transaction
#         :raises: lmdb_IndexMissing if the index does not exist
#         """
#         if name not in self._indexes: raise xIndexMissing
#
#         if name not in self._indexes: raise xIndexMissing
#         self._indexes[name].drop(txn)
#         del self._indexes[name]
#         if not txn.delete(_index_name(self, name).encode(), db=self._ctx._metadata): raise xWriteFail
#
#     @property
#     def indexes(self, txn=None, abort=False):
#         """
#         Return a list of indexes for this table
#
#         :getter: The indexes for this table
#         :type: list
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             results = []
#             index_name = _index_name(self, '')
#             pos = len(index_name)
#             db = self._ctx.env.open_db(txn=txn)
#             with lmdb.Cursor(db, txn) as cursor:
#                 if cursor.set_range(index_name.encode()):
#                     while True:
#                         name = cursor.key().decode()
#                         if not name.startswith(index_name) or not cursor.next():
#                             break
#                         results.append(name[pos:])
#             return results
#
#     @property
#     def records(self, txn=None, abort=False):
#         """
#         Return the number of records in this table
#
#         :getter: Record count
#         :type: int
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             return txn.stat(self._db).get('entries', 0)
#
#     @property
#     def name(self):
#         """
#         PROPERTY - Recover the name of this table
#         :getter: Table Name
#         :type: str
#         """
#         return self._name

# class Index(object):
#     """
#     Mapping for table indecies, this is version #2 with a much simplified indexing scheme.
#
#     :param context: A reference to the controlling Database object
#     :type context: Database
#     :param name: The name of the index we're working with
#     :type name: str
#     :param func: Is a Python format string that specified the index layout
#     :type func: str
#     :param conf: Configuration options for this index
#     :type conf: dict
#
#     """
#     _debug = False
#
#     def __init__(self, ctx, name, func, conf, txn):
#         self._ctx = ctx
#         self._name = name
#         self._conf = conf
#         self._conf['key'] = self._conf['key'].encode()
#         self._func = _anonymous('(r): return "{}".format(**r).encode()'.format(func))
#         self._db = self._ctx.env.open_db(**self._conf, txn=txn)
#
#     def begin(self):
#         return lmdb.Transaction(self._ctx.env)
#
#     def count(self, txn=None, abort=False):
#         """
#         Count the number of items currently present in this index
#
#         :param txn: Is an open Transaction
#         :param txn: Is an open Transaction
#         :type txn: Transaction
#         :return: The number if items in the index
#         :rtype: int
#         """
#         with self.begin() as transaction:
#             txn = txn if txn else transaction
#             return txn.stat(self._db).get('entries', 0)
#
#     def cursor(self, txn=None):
#         """
#         Return a cursor into the current index
#
#         :param txn: Is an open Transaction
#         :type txn: Transaction
#         :return: An active Cursor object
#         :rtype: Cursor
#         """
#         return lmdb.Cursor(self._db, txn)
#
#     def match(self, value, record):
#         """
#         Test for equality between a key value and a record specification
#
#         :param value: A key value
#         :type param: str
#         :param record: An index spec
#         :type record: dict
#         :return: True if equal
#         :rtype: bool
#         """
#         return value == self._func(record)
#
#     def set_key(self, cursor, record):
#         """
#         Set the cursor to the first matching record
#
#         :param cursor: An active cursor
#         :type cursor: Cursor
#         :param record: A template record specifying the key to use
#         :type record: dict
#         """
#         cursor.set_key(self._func(record))
#
#     def set_range(self, cursor, record):
#         """
#         Set the cursor to the first matching record
#
#         :param cursor: An active cursor
#         :type cursor: Cursor
#         :param record: A template record specifying the key to use
#         :type record: dict
#         """
#         cursor.set_range(self._func(record))
#
#     def set_next(self, cursor, record):
#         """
#         Find the next matching record lower than the key specified
#
#         :param cursor: An active cursor
#         :type cursor: Cursor
#         :param record: A template record specifying the key to use
#         :type record: dict
#         """
#         cursor.next()
#         return cursor.key() <= self._func(record)
#
#     def delete(self, txn, key, record):
#         """
#         Delete the selected record from the current index
#
#         :param txn: Is an open (write) Transaction
#         :type txn: Transaction
#         :param key: A database key
#         :type key: str
#         :param record: A currently existing record
#         :type record: dict
#         :return: True if the record was deleted
#         :rtype: boolean
#         """
#         return txn.delete(self._func(record), key, self._db)
#
#     def drop(self, txn):
#         """
#         Drop the current index
#
#         :param txn: Is an open Transaction
#         :type txn: Transaction
#         """
#         txn.drop(self._db, delete=True)
#
#     def empty(self, txn):
#         """
#         Empty the current index of all records
#
#         :param txn: Is an open Transaction
#         """
#         return txn.drop(self._db, delete=False)
#
#     def get(self, txn, record):
#         """
#         Read a single record from the index
#
#         :param txn: Is an open Transaction
#         :type txn: Transaction
#         :param record: Is a record template from which we can extract an index field
#         :type record: dict
#         :return: The record recovered from the index
#         :rtype: str
#         """
#         return txn.get(self._func(record), db=self._db)
#
#     def put(self, txn, key, record):
#         """
#         Write a new entry into the index
#
#         :param txn: Is an open Transaction
#         :type txn: Transaction
#         :param key: Is the key to of the record to write
#         :type key: str|int
#         :param record: Is the record to write
#         :type record: dict
#         :return: True if the record was written successfully
#         :rtype: boolean
#         """
#         try:
#             ikey = self._func(record)
#             if type(key) is not bytes:
#                 key = key.encode()
#             return txn.put(ikey, key, db=self._db)
#         except KeyError:
#             return False
#
#     def save(self, txn, key, old, rec):
#         """
#         Save any changes to the keys for this record
#
#         :param txn: An active transaction
#         :type txn: Transaction
#         :param key: The key for the record in question
#         :type key: str
#         :param old: The record in it's previous state
#         :type old: dict
#         :param rec: The record in it's amended state
#         :type rec: dict
#         """
#         old_key = self._func(old)
#         new_key = self._func(rec)
#         if old_key != new_key:
#             if not txn.delete(old_key, key, db=self._db): raise xReindexNoKey1
#             if not txn.put(new_key, key, db=self._db): raise xReindexNoKey2
#
#
# def _debug(self, msg):
#     """
#     Display a debug message with current line number and function name
#
#     :param self: A reference to the object calling this routine
#     :type self: object
#     :param msg: The message you wish to display
#     :type msg: str
#     """
#     if hasattr(self, '_debug') and self._debug:
#         line = _getframe(1).f_lineno
#         name = _getframe(1).f_code.co_name
#         print("{}: #{} - {}".format(name, line, msg))
#
#
# def _anonymous(text):
#     """
#     An function used to generate anonymous functions for database indecies
#
#     :param text: The body of the function call to generate
#     :type text: str
#     :return: Anonymous function to calculate key value
#     :rtype: function
#     """
#     scope = {}
#     exec('def func{0}'.format(text), scope)
#     return scope['func']
#
#
# def _index_name(self, name):
#     """
#     Generate the name of the object in which to store index records
#
#     :param name: The name of the table
#     :type name: str
#     :return: A string representation of the full table name
#     :rtype: str
#     """
#     return '_{}_{}'.format(self._name, name)
#
#
# def size_mb(size):
#     """
#     Helper function when creating database
#     :param size: interger (size in Mb)
#     :return: integer (bytes)
#     """
#     return 1024*1024*size
#
#
# def size_gb(size):
#     """
#     Helper function when creating database
#     :param size: interger (size in Gb)
#     :return: integer (bytes)
#     """
#     return 1024*1024*1024*size
#
#
# class xTableExists(Exception):
#     """Exception - database table already exists"""
#     pass
#
#
# class xIndexExists(Exception):
#     """Exception - index already exists"""
#     pass
#
#
# class xTableMissing(Exception):
#     """Exception - database table does not exist"""
#     pass
#
#
# class xIndexMissing(Exception):
#     """Exception - index does not exist"""
#     pass
#
#
# class xNotFound(Exception):
#     """Exception - expected record was not found"""
#     pass
#
#
# class xAborted(Exception):
#     """Exception - transaction did not complete"""
#
#
# class xWriteFail(Exception):
#     """Exception - write failed"""
#
#
# class xReindexNoKey1(Exception):
#     """Exception - write failed"""
#
#
# class xReindexNoKey2(Exception):
#     """Exception - write failed"""
#
#
# class xDropFail(Exception):
#     """Exception - drop failed"""
#
#
# class xNoKey(Exception):
#     """No key was specified for operation"""
