import lmdb
from .utils import _anonymous, xReindexNoKey1, xReindexNoKey2


class Index(object):
    """
    Mapping for table indecies, this is version #2 with a much simplified indexing scheme.

    :param context: A reference to the controlling Database object
    :type context: Database
    :param name: The name of the index we're working with
    :type name: str
    :param func: Is a Python format string that specified the index layout
    :type func: str
    :param conf: Configuration options for this index
    :type conf: dict

    """
    _debug = False

    def __init__(self, ctx, name, func, conf, txn):
        self._ctx = ctx
        self._name = name
        self._conf = conf
        self._func = _anonymous('(r): return "{}".format(**r).encode()'.format(func))
        options = dict(self._conf)
        if type(options['key']) is not bytes:
            options['key'] = options['key'].encode()
        self._db = self._ctx.env.open_db(**options, txn=txn)

    def begin(self):
        return lmdb.Transaction(self._ctx.env)

    def count(self, txn=None, abort=False):
        """
        Count the number of items currently present in this index

        :param txn: Is an open Transaction
        :param txn: Is an open Transaction
        :type txn: Transaction
        :return: The number if items in the index
        :rtype: int
        """
        with self.begin() as transaction:
            txn = txn if txn else transaction
            return txn.stat(self._db).get('entries', 0)

    def cursor(self, txn=None):
        """
        Return a cursor into the current index

        :param txn: Is an open Transaction
        :type txn: Transaction
        :return: An active Cursor object
        :rtype: Cursor
        """
        return lmdb.Cursor(self._db, txn)

    def match(self, value, record):
        """
        Test for equality between a key value and a record specification

        :param value: A key value
        :type param: str
        :param record: An index spec
        :type record: dict
        :return: True if equal
        :rtype: bool
        """
        test = self._func(record)
        if test == value:
            return 0
        return -1 if test > value else 1

    def set_key(self, cursor, record):
        """
        Set the cursor to the first matching record

        :param cursor: An active cursor
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        cursor.set_key(self._func(record))

    def set_range(self, cursor, record):
        """
        Set the cursor to the first matching record

        :param cursor: An active cursor
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        return cursor.set_range(self._func(record))

    def set_next(self, cursor, record):
        """
        Find the next matching record lower than the key specified

        :param cursor: An active cursor
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        return cursor.key() <= self._func(record) if cursor.next() else False

    def delete(self, txn, key, record):
        """
        Delete the selected record from the current index

        :param txn: Is an open (write) Transaction
        :type txn: Transaction
        :param key: A database key
        :type key: str
        :param record: A currently existing record
        :type record: dict
        :return: True if the record was deleted
        :rtype: boolean
        """
        return txn.delete(self._func(record), key, self._db)

    def drop(self, txn):
        """
        Drop the current index

        :param txn: Is an open Transaction
        :type txn: Transaction
        """
        txn.drop(self._db, delete=True)

    def empty(self, txn):
        """
        Empty the current index of all records

        :param txn: Is an open Transaction
        """
        return txn.drop(self._db, delete=False)

    def get(self, txn, record):
        """
        Read a single record from the index

        :param txn: Is an open Transaction
        :type txn: Transaction
        :param record: Is a record template from which we can extract an index field
        :type record: dict
        :return: The record recovered from the index
        :rtype: str
        """
        return txn.get(self._func(record), db=self._db)

    def put(self, txn, key, record):
        """
        Write a new entry into the index

        :param txn: Is an open Transaction
        :type txn: Transaction
        :param key: Is the key to of the record to write
        :type key: str|int
        :param record: Is the record to write
        :type record: dict
        :return: True if the record was written successfully
        :rtype: boolean
        """
        try:
            ikey = self._func(record)
            if type(key) is not bytes:
                key = key.encode()

            return txn.put(ikey, key, db=self._db)
        except KeyError:
            return False

    def save(self, txn, key, old, rec):
        """
        Save any changes to the keys for this record

        :param txn: An active transaction
        :type txn: Transaction
        :param key: The key for the record in question
        :type key: str
        :param old: The record in it's previous state
        :type old: dict
        :param rec: The record in it's amended state
        :type rec: dict
        """
        old_key = self._func(old)
        new_key = self._func(rec)
        if old_key != new_key:
            if not txn.put(new_key, key, db=self._db):
                raise xReindexNoKey2
            if not txn.delete(old_key, key, db=self._db):
                raise xReindexNoKey1

