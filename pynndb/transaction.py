import lmdb
from .utils import xWriteFail
from bson import ObjectId
from struct import pack, unpack
from ujson import dumps


class Transaction(object):

    def __init__(self, database, write=False, buffers=False):
        self._transactions = []
        self._coc = [database.node]
        self._tid = None
        self._replicated = False
        self._db = database
        self._txn = lmdb.Transaction(self._db.env, write=write, buffers=buffers)

    def __enter__(self):
        return self

    def __exit__(self, txn_type, txn_value, traceback):
        self._db._locks = []
        if txn_type or not len(self._transactions):
            self._txn.abort()
            return
        if not self._db.binlog: # or self._replicated:
            self._txn.commit()
            return

        self._record_binlog()
        self._txn.commit()
        self._db._semaphore.release() if self._db._semaphore else None
        self._txn = None

    def _record_binlog(self):
        cursor = self._txn.cursor(db=self._db._binlog)
        key = pack('>Q', 1 if not cursor.last() else unpack('>Q', cursor.key())[0] + 1)
        if not self._tid:
            self._tid = str(ObjectId())
        doc = dumps({
            'txn': self._transactions,
            'coc': self._coc,
            'tid': self._tid
        }).encode()
        if not self._txn.put(key, doc, db=self._db.binlog, append=False):
            raise xWriteFail('Fatal: Unable to record transaction #{}'.format(key))
        if not self._txn.put(self._tid.encode(), key, db=self._db.binidx, append=False):
            raise xWriteFail('Fatal: Unable to record transaction #{}'.format(key))

    def append(self, table, doc):
        if '_id' not in doc:
            doc['_id'] = str(ObjectId()).encode()
        self._transactions.append({'cmd': 'add', 'tab': table.name, 'doc': doc})
        return table.append(doc, txn=self._txn)

    def delete(self, table, keys):
        self._transactions.append({'cmd': 'del', 'tab': table, 'keys': keys})
        return table.delete(keys, txn=self._txn)

    def save(self, table, doc):
        delta = table.save(doc, txn=self._txn)
        self._transactions.append({'cmd': 'upd', 'tab': table, 'key': doc['_id'], 'yyy': delta})

    def empty_table(self, table):
        self._transactions.append({'cmd': 'emp', 'tab': table})
        return table.empty(txn=self._txn)

    def create_index(self, table, name, func, duplicates):
        self._transactions.append({'cmd': 'idx', 'tab': table, 'idx': name, 'fun': func, 'dup': duplicates})
        return table.index(name, func, duplicates, txn=self._txn)

    def drop_index(self, table, name):
        self._transactions.append({'cmd': 'uix', 'tab': table, 'idx': name})
        return table.drop_index(name, txn=self._txn)

    def create_table(self, table):
        self._transactions.append({'cmd': 'cre', 'tab': table})
        return self._db.table(table.name, txn=self._txn)

    def drop_table(self, table):
        self._transactions.append({'cmd': 'drp', 'tab': table})
        return table.drop(txn=self._txn)

