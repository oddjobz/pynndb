import lmdb
from sys import maxsize
from bson import ObjectId
from ujson import loads, dumps
# from ujson_delta import diff
from .index import Index
from .utils import _index_name, xWriteFail, xNoKey, xIndexMissing, xNotFound


def write_transaction(func):
    """
    Wrapper for write transactions to ensure a an appropriate transaction is in place
    """
    def wrapped_f(*args, **kwargs):
        if 'txn' in kwargs and kwargs['txn']:
            return func(*args, **kwargs)
        with args[0]._ctx.env.begin(write=True) as kwargs['txn']:
            return func(*args, **kwargs)
    return wrapped_f


class Table(object):
    """
    Representation of a database table

    :param ctx: An open database Environment
    :type ctx: Database
    :param name: A table name
    :type name: str
    """

    def __init__(self, ctx, name=None, txn=None):

        self._debug = False
        self._ctx = ctx
        self._name = name
        self._indexes = {}
        self._open_(txn=txn)

    def begin(self):
        return lmdb.Transaction(self._ctx.env)

    @property
    def name(self):
        return self._name

    @write_transaction
    def _open_(self, txn=None):
        self._db = self._ctx.env.open_db(self._name.encode(), txn=txn)
        for index in self.indexes(txn):
            key = _index_name(self, index)
            doc = loads(txn.get(key.encode(), db=self._ctx._meta._db).decode())
            self._indexes[index] = Index(self._ctx, index, doc['func'], doc['conf'], txn)

    @write_transaction
    def append(self, record, txn=None):
        """
        Append a new record to this table

        :param record: The record to append
        :type record: dict
        :param txn: An open transaction
        :type txn: Transaction
        :raises: xWriteFail on write error
        """
        if '_id' not in record:
            key = str(ObjectId()).encode()
            append = True
        else:
            append = False
            key = record['_id']
            if not isinstance(key, bytes):
                if isinstance(key, str):
                    key = key.encode()
                elif isinstance(key, int):
                    key = str(key).encode()
                else:
                    raise TypeError('_id is type {}'.format(type(key)))

        if '_id' in record:
            del record['_id']
        if not txn.put(key, dumps(record).encode(), db=self._db, append=False): raise xWriteFail(key)

        record['_id'] = key
        for name in self._indexes:
            if not self._indexes[name].put(txn, key, record): raise xWriteFail(name)

    @write_transaction
    def delete(self, keys, txn=None):
        """
        Delete a record from this table

        :param keys: A list of database keys to delete or a record
        :type keys: list|dict
        :param txn: Transaction
        :type txn: An options transaction
        """
        if not isinstance(keys, list):
            if isinstance(keys, dict):
                keys = [keys['_id']]
            else:
                keys = [keys]

        for key in keys:
            doc = loads(txn.get(key, db=self._db).decode())
            if not txn.delete(key, db=self._db): raise xWriteFail
            for name in self._indexes:
                self._indexes[name].delete(txn, key, doc)

    @write_transaction
    def save(self, record, txn):
        """
        Save an changes to a pre-existing record

        :param record: The record to be saved
        :type record: dict
        :param txn: An open transaction
        :type txn: Transaction
        """
        if not '_id' in record: raise xNoKey
        key = record['_id']
        rec = dict(record)
        del rec['_id']
        doc = txn.get(key, db=self._db)
        if not doc: raise xWriteFail('old record is missing')
        old = loads(doc.decode())
        if not txn.put(key, dumps(rec).encode(), db=self._db): raise xWriteFail('main record')
        for name in self._indexes:
            self._indexes[name].save(txn, key, old, rec)
        # if self._ctx.binlog:
        #     return diff(old, record, verbose=False)

    @write_transaction
    def empty(self, txn):
        """
        Clear all records from the current table
        """
        for name in self.indexes(txn):
            self._indexes[name].empty(txn)
        txn.drop(self._db, False)

    @write_transaction
    def index(self, name, func=None, duplicates=False, txn=None):
        """
        Return a reference for a names index, or create if not available

        :param name: The name of the index to create
        :type name: str
        :param func: A specification of the index, !<function>|<field name>
        :type func: str
        :param duplicates: Whether this index will allow duplicate keys
        :type duplicates: bool
        :param txn: An optional transaction
        :type txn: Transaction
        :return: A reference to the index, created index, or None if index creation fails
        :rtype: Index
        """
        if name not in self._indexes:
            conf = {
                'key': _index_name(self, name),
                'dupsort': duplicates,
                'create': True,
            }
            self._indexes[name] = Index(self._ctx, name, func, conf, txn)
            key = _index_name(self, name)
            val = {'conf': conf, 'func': func}
            val = dumps(val)
            if not txn.put(key.encode(), val.encode(), db=self._ctx._meta._db): raise xWriteFail
            self._reindex(name, txn)

        return self._indexes[name]

    @write_transaction
    def drop_index(self, name, txn):
        """
        Drop an index

        :param name: Name of the index to drop
        :type name: str
        :param txn: An optional transaction
        :type txn: Transaction
        """
        if name not in self._indexes:
            raise xIndexMissing
        return self._unindex(name, txn)

    @write_transaction
    def drop(self, txn=None):
        """
        Drop this tablex and all it's indecies

        :param delete: Whether we delete the table after removing all items
        :type delete: bool
        :param txn: An optional transaction
        :type txn: Transaction
        """
        for name in self.indexes(txn):
            self._unindex(name, txn)
        return txn.drop(self._db, True)

    def exists(self, name):
        """
        See whether an index already exists or not

        :param name: Name of the index
        :type name: str
        :return: True if index already exists
        :rtype: bool
        """
        return name in self._indexes

    def find(self, index=None, expression=None, limit=maxsize, txn=None, abort=False):
        """
        Find all records either sequential or based on an index

        :param index: The name of the index to use [OR use natural order]
        :type index: str
        :param expression: An optional filter expression
        :type expression: function
        :param limit: The maximum number of records to return
        :type limit: int
        :param txn: An optional transaction
        :type txn: Transaction
        :return: The next record (generator)
        :rtype: dict
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            if not index:
                db = self._db
            else:
                if index not in self._indexes:
                    raise xIndexMissing(index)
                index = self._indexes[index]
                db = index._db
            with txn.cursor(db) as cursor:
                count = 0
                first = True
                while count < limit:
                    if not (cursor.first() if first else cursor.next()):
                        break
                    first = False
                    record = cursor.value()
                    if index:
                        key = record
                        record = txn.get(record, db=self._db)
                    else:
                        key = cursor.key()
                    try:
                        record = record.decode()
                        record = loads(record)
                        if callable(expression) and not expression(record):
                            continue
                        record['_id'] = key
                    except ValueError:
                        record = {'_id': key, 'value': record}
                    yield record
                    count += 1

    def range(self, index, lower=None, upper=None, txn=None, keyonly=False):
        """
        Find all records with a key >= lower and <= upper. If you set inclusive to false the range
        becomes key > lower and key < upper. Upper and/or Lower can be set to None, if lower is none
        the range starts at the beginning of the table, and if upper is None searching will continue
        to the end.

        :param index: The name of the index to search
        :type index: str
        :param lower: A template record containing the lower end of the range
        :type lower: dict
        :param upper: A template record containing the upper end of the range
        :type upper: dict
        :param txn: An optional transaction
        :type txn: Transaction
        :return: The records with keys within the specified range (generator)
        :type: dict
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            if not index:
                with txn.cursor(self._db) as cursor:
                    def forward():
                        if not cursor.next(): return False
                        if upper and cursor.key() > upper: return False
                        return True

                    lower = lower['_id'] if lower else None
                    upper = upper['_id'] if upper else None
                    inclusive = True
                    cursor.set_range(lower) if lower else cursor.first()
                    while not inclusive and cursor.key() == lower:
                        if not cursor.next() or cursor.key() == upper: return
                    while True:
                        key = cursor.key()
                        if not key: break
                        record = loads(cursor.value().decode())
                        record['_id'] = key
                        if not inclusive:
                            if not forward(): break
                            yield record
                        else:
                            yield record
                            if not forward(): break
            else:
                index = self._indexes[index]
                with txn.cursor(index._db) as cursor:
                    if lower:
                        index.set_range(cursor, lower)
                        if upper:
                            have_data = index.match(cursor.key(), lower) >= 0 and index.match(cursor.key(), upper) <= 0
                        else:
                            have_data = index.match(cursor.key(), lower) >= 0
                    else:
                        have_data = cursor.first()
                    while have_data:
                        if keyonly:
                            yield cursor
                        else:
                            record = txn.get(cursor.value(), db=self._db)
                            if not record: raise xNotFound(cursor.value())
                            record = loads(record.decode())
                            record['_id'] = cursor.value()
                            yield record
                        have_data = index.set_next(cursor, upper) if upper else cursor.next()

    def get(self, key, txn=None, abort=False):
        """
        Get a single record based on it's key

        :param key: The _id of the record to get
        :type key: str
        :return: The requested record
        :rtype: dict
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            record = txn.get(key, db=self._db)
            if not record: return None
            try:
                record = loads(record.decode())
                record['_id'] = key
            except ValueError:
                record = {'_id': key, 'value': record}

            return record

    def tail(self, key, txn=None):
        """Recover all records from this point onwards

        :param key: First key to examine
        :rtype: list of records
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            with txn.cursor(db=self._db) as cursor:
                if key and key != '0':
                    if type(key) != bytes:
                        key = key.encode()
                    if not cursor.get(key):
                        return None
                    if not cursor.next():
                        return None
                for key, val in cursor.iternext(keys=True, values=True):
                    record = loads(val.decode())
                    record['_id'] = key
                    yield record

    def first(self, txn=None):
        with self.begin() as transaction:
            txn = txn if txn else transaction
            with txn.cursor(db=self._db) as cursor:
                if not cursor.first():
                    return None
                key, val = cursor.item()
                doc = loads(val.decode())
                doc['_id'] = key
                return doc

    def last(self, txn=None):
        with self.begin() as transaction:
            txn = txn if txn else transaction
            with txn.cursor(db=self._db) as cursor:
                if not cursor.last():
                    return None
                key, val = cursor.item()
                doc = loads(val.decode())
                doc['_id'] = key
                return doc

    def ensure(self, index, func, duplicates=False, force=False):
        """
        Ensure than an index exists and create if it's missing

        :param index: The name of the index we're checking
        :type index: str
        :param func: The indexing function for this index
        :type func: str
        :param duplicates: Whether the index should allow for duplicates
        :type duplicates: bool
        :param force: whether to force creation even if it already exists
        :type force: bool
        :return: The index we're checking for
        :rtype: Index
        """
        if index in self._indexes:
            if force:
                self.drop_index(index)
            else:
                return self._indexes[index]
        return self.index(index, func, duplicates)

    @write_transaction
    def reindex(self, txn):
        """
        Reindex all indexes for a given table

        :param name: Name of the table to reindex
        :type name: str
        :param txn: An optional transaction
        :type txn: Trnsaction
        """
        for name in self._indexes:
            self._reindex(name, txn)

    def _reindex(self, name, txn=None):
        """
        Reindex an index

        :param name: The name of the index to reindex
        :type name: str
        :param txn: An open transaction
        :type txn: Transaction
        :return: Number of index entries created
        :rtype: int
        """
        if name not in self._indexes: raise xIndexMissing
        index = self._indexes[name]

        with self.begin() as transaction:
            txn = txn if txn else transaction
            count = 0
            self._indexes[name].empty(txn)

            with lmdb.Cursor(self._db, txn) as cursor:
                if cursor.first():
                    while True:
                        record = loads(cursor.value().decode())
                        if index.put(txn, cursor.key().decode(), record):
                            count += 1
                        if not cursor.next():
                            break
            return count

    def seek(self, index, record, limit=maxsize, txn=None):
        """
        Find all records matching the key in the specified index.

        :param index: Name of the index to seek on
        :type index: str
        :param record: A template record containing the fields to search on
        :type record: dict
        :param limit: maximum number of records to return
        :param txn: An optional transaction
        :type txn: Transaction
        :return: The records with matching keys (generator)
        :type: dict
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            index = self._indexes[index]
            with index.cursor(txn) as cursor:
                index.set_key(cursor, record)
                count = 0
                while count < limit:
                    count += 1
                    if not cursor.key():
                        break
                    key = cursor.value()
                    record = txn.get(key, db=self._db)
                    record = loads(record.decode())
                    record['_id'] = key
                    yield record
                    if not cursor.next_dup():
                        break

    def seek_one(self, index, record, txn=None, abort=False):
        """
        Find the first records matching the key in the specified index.

        :param index: Name of the index to seek on
        :type index: str
        :param record: A template record containing the fields to search on
        :type record: dict
        :return: The record with matching key
        :type: dict
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            index = self._indexes[index]
            entry = index.get(txn, record)
            if not entry: return None
            record = txn.get(entry, db=self._db)
            if not record: return None
            record = loads(record.decode())
            record['_id'] = entry
            return record

    def _unindex(self, name, txn):
        """
        Delete the named index

        :return:
        :param name: The name of the index
        :type name: str
        :param txn: An active transaction
        :type txn: Transaction
        :raises: lmdb_IndexMissing if the index does not exist
        """
        if name not in self._indexes: raise xIndexMissing
        self._indexes[name].drop(txn)
        del self._indexes[name]
        if not txn.delete(_index_name(self, name).encode(), db=self._ctx._meta._db): raise xWriteFail

    def indexes(self, txn=None, abort=False):
        """
        Return a list of indexes for this table

        :getter: The indexes for this table
        :type: list
        """
        def indexes_inner():
            results = []
            index_name = _index_name(self, '')
            pos = len(index_name)
            db = self._ctx.env.open_db(txn=txn)
            with txn.cursor(db=db) as cursor:
                if cursor.set_range(index_name.encode()):
                    while True:
                        name = cursor.key().decode()
                        if not name.startswith(index_name) or not cursor.next():
                            break
                        results.append(name[pos:])
            return results

        if txn:
            return indexes_inner()
        with self.begin() as txn:
            return indexes_inner()

    @property
    def records(self, txn=None, abort=False):
        """
        Return the number of records in this table

        :getter: Record count
        :type: int
        """
        with self._ctx.env.begin() as txn:
            return txn.stat(self._db).get('entries', 0)

    @property
    def name(self):
        """
        PROPERTY - Recover the name of this table
        :getter: Table Name
        :type: str
        """
        return self._name
