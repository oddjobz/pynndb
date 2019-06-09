from threading import Thread
from queue import Empty
# from posix_ipc import Semaphore, ExistentialError, O_CREAT
from time import sleep
from datetime import datetime
from ujson import loads, dumps
from bson import ObjectId
# from posix_ipc import BusyError
from .database import Database
#from server_protocol import Protocol
from .utils import size_gb, semaphore_path

SYNC_CMD = '@'
SYNC_KEY = 'K'
SYNC_DATA = 'D'
SYNC_NODE = 'N'
SYNC_TABLES = 'T'
SYNC_HELLO = 'H'
SYNC_ACK = 'A'
SYNC_NAK = 'Z'
SYNC_HWM = 'M'
SYNC_RECORD_NEW = 'N'
SYNC_RECORD_UPDATE = 'U'


# TODO: sync db structure (attributes and indexes)
# TODO: changes to attributes and indexes should be cause a sync


class ReplicationSync(object):

    def __init__(self, node, peer, incoming, outgoing, path, logger):
        """
        Setup replication between this node and our peer for the chosen database
        :param database: database handle
        :param peer: peer name
        :param incoming: incoming messages from other peers
        :param outgoing: outgoing messages to other peers
        """
        self._debug = True
        self._node = node
        self._path = path
        self._database = None
        self._peer = peer
        self._log = logger
        self._incoming = incoming
        self._outgoing = outgoing
        self._syncing = False
        self._hwm = None
        self._finished = False


        #self._handlers = {
        #    SYNC_HELLO: self.hello,
        #    SYNC_ACK: self.ack,
        #    SYNC_RECORD_NEW: self.record_new,
        #    SYNC_RECORD_UPDATE: self.record_update
        #}
        self._listener = Thread(target=self._protocol.start)
        self._syncer = Thread(target=self._protocol.rsync)
        self._listener.start()
        self._listener.join()
        if self._syncer.is_alive():
            self._syncer.join()

    def debug(self, text):
        self._log('{}'.format(text))

    # def listener(self):
    #     """
    #     Listen for new messages and dispatch via handler
    #     """
    #     self._protocol.start()
    #     self.debug('* Listener thread running for "{}:{}"'.format(self._node, self._peer.name))
    #     #self.send_hello()
    #     while True:
    #         try:
    #             obj = self._incoming.get(block=True, timeout=1.0)
    #             #print(">>", obj)
    #             if not obj:
    #                 self.debug('* Incoming break signal')
    #                 break
    #             self._protocol.handle(obj)
    #             # if obj[SYNC_CMD] == SYNC_HELLO:
    #             #     self.hello(obj)
    #             #     continue
    #             # elif obj[SYNC_CMD] == SYNC_ACK:
    #             #     self.ack(obj)
    #             #     continue
    #             # elif obj[SYNC_CMD] == SYNC_NAK:
    #             #     self.debug('* DB sync is broken')
    #             #     sleep(10)
    #             #     break
    #             # elif obj[SYNC_CMD] == SYNC_HWM:
    #             #     self.hwm(obj)
    #             #     continue
    #             # batch = 0
    #             # beg = datetime.now()
    #             # with self._database.begin() as txn:
    #             #     last = obj
    #             #     while obj[SYNC_CMD] == SYNC_RECORD_NEW:
    #             #         self.record_new(obj, txn)
    #             #         batch += 1
    #             #         if batch > 5000: break
    #             #         try:
    #             #             last = obj
    #             #             obj = self._incoming.get(block=True, timeout=0.01)
    #             #         except Empty:
    #             #             break
    #             #         except ConnectionResetError:
    #             #             break
    #             #     self.update_hwm(last, txn)
    #
    #         except KeyboardInterrupt:
    #             self._finished = True
    #         except (EOFError, BrokenPipeError):
    #             break
    #         except Empty:
    #             continue
    #         # end = datetime.now()
    #         # span = end - beg
    #         # self.debug('+ Completed batch of "{}" @ {}/sec'.format(batch, int(batch/span.total_seconds())))
    #
    #         #print("Batch=", batch, span, span.total_seconds(), 10*batch/span.total_seconds(), "/sec")
    #         #except Exception as e:
    #         ##    self.debug('Receiver error> {} - {}'.format(e, obj))
    #         #    break
    #
    #     self._finished = True
    #     self.debug('* Listener thread terminated for "{}:{}"'.format(self._node, self._peer.name))

    # def resync(self):
    #
    #     with self._database.env.begin(write=True) as txn:
    #
    #         with txn.cursor(db=self._database._binlog) as cursor:
    #             if cursor.last():
    #                 self._hwm = cursor.key()
    #             else:
    #                 self._hwm = str(ObjectId()).encode()
    #                 txn.put(self._hwm, dumps({'txn': []}).encode(), db=self._database.binlog, append=False)
    #
    #         for table_name in self._database.tables:
    #             table = self._database.table(table_name, txn=txn)
    #             self.debug('+ Begin Sync for table "{}"'.format(table_name))
    #             count = 0
    #             with txn.cursor(db=table._db) as cursor:
    #                 move = cursor.first()
    #                 while move:
    #                     key, val = cursor.item()
    #                     rec = {'txn': [{'tab': table_name, 'doc': loads(val.decode())}]}
    #                     self.send_new(self._hwm, rec)
    #                     move = cursor.next()
    #                     count += 1
    #             self.debug('+ End Sync, copied {} records'.format(count))
    #         self.send_hwm(self._hwm)

    # def syncer(self):
    #     """
    #     Continually push out updates to our peer when they happen
    #
    #     Syncer Startup Logic
    #
    #     Is incoming HWM == None?
    #         If it is, remote wants to do a full sync with this, which is valid only if we're empty ...
    #         If not len(db.tables) -> Resync
    #         Else, go into sleep mode until recovery complete
    #     Otherwise, set HWM ..
    #         If we can't, then the binlog is out of date and we need a resync so
    #             go into sleep mode until recovery complete
    #
    #     Flush Semaphores
    #     Sync HWM to current
    #     Sync on Semaphore
    #
    #     Recovery modes;
    #
    #     a. Delete one db and retry
    #     b. Compare all records and update one from the other
    #         - this requires a last updated timestamp
    #
    #     - Resync required a host level lock to prevent full sync from two peers ...
    #
    #     """
    #
    #     self.debug('* Syncer thread running for "{}:{}"'.format(self._node, self._peer.name))
    #     self.debug('* Syncer HWM = "{}"'.format(self._hwm))
    #
    #     if not self._hwm:
    #         self.debug('* Initial synchronisation')
    #         self.resync()
    #     with self._database.env.begin() as txn:
    #         with txn.cursor(self._database._binlog) as cursor:
    #             if not cursor.set_key(self._hwm):
    #                 self.debug('* Synchronisation lost - recovering')
    #                 self.resync()
    #
    #     semaphore = Semaphore(semaphore_path(self._path, self._peer.name))
    #     count = 0
    #     while semaphore.value:
    #         semaphore.acquire()
    #         count += 1
    #
    #     self.debug('* Cleared "{}" old semaphores'.format(count))
    #
    #     with self._database.env.begin() as txn:
    #         with txn.cursor(self._database._binlog) as cursor:
    #             if not cursor.set_key(self._hwm):
    #                 self.debug('* Failed to sync BINLOG')
    #                 return
    #             count = 0
    #             while cursor.next():
    #                 key, val = cursor.item()
    #                 self.send_new(key, loads(val.decode()))
    #                 count += 1
    #                 self._hwm = key
    #             self.debug('* Sync "{}" new items'.format(count))
    #
    #     try:
    #         while not self._finished:
    #             try:
    #                 semaphore.acquire(1.0)
    #             except BusyError:
    #                 continue
    #
    #             count = 0
    #             while True:
    #                 with self._database.env.begin() as txn:
    #                     with txn.cursor(self._database._binlog) as cursor:
    #                         if not cursor.set_key(self._hwm):
    #                             raise PyMambaBinlogCorrupt
    #                         if not cursor.next():
    #                             self.debug('* Semaphore was too quick!')
    #                             break
    #                         key, val = cursor.item()
    #                         self.send_new(key, loads(val.decode()))
    #                         count += 1
    #                         if not self._database._semaphore.value:
    #                             break
    #                         self._database._semaphore.acquire()
    #                         self._hwm = key
    #
    #     except KeyboardInterrupt:
    #         self.debug('* Ctrl+c in syncer')
    #         self._finished = True
    #     except EOFError:
    #         self.debug('* Exit replication syncer')
    #         raise
    #
    #     self._outgoing.put({})
    #     self.debug('* Syncer thread terminated for "{}:{}"'.format(self._node, self._peer.name))

    # def send_hello(self):
    #     doc = {
    #         SYNC_CMD: SYNC_HELLO,
    #         SYNC_NODE: self._node,
    #         SYNC_TABLES: len(self._database.tables)
    #     }
    #     self._outgoing.put(doc)
    #     #print("HELLO:", doc)
    #
    # def hello(self, obj, txn=None):
    #     """
    #     Handle a hello request
    #     :param obj: incoming HELLO packet
    #     """
    #     with self._database.env.begin() as txn:
    #         key = '__hwm__{}__'.format(self._peer.name).encode()
    #         value = txn.get(key, default=None, db=self._database._metadata)
    #
    #     #if obj[SYNC_TABLES] and not value:
    #     #    self.send_nak()
    #     #else:
    #     self.debug('* Table length = {} and hwm = {}'.format(obj[SYNC_TABLES], value))
    #
    #     self.send_ack(value)
    #
    # def send_new(self, key, val):
    #     doc = {
    #         SYNC_CMD: SYNC_RECORD_NEW,
    #         SYNC_KEY: key,
    #         SYNC_DATA: val,
    #         SYNC_NODE: self._node
    #     }
    #     self._outgoing.put(doc)
    #
    # def send_ack(self, value):
    #     doc = {
    #         SYNC_CMD: SYNC_ACK,
    #         SYNC_NODE: self._node,
    #         SYNC_KEY: value
    #     }
    #     self._outgoing.put(doc)
    #     #print("ACK:", doc)
    #
    # def send_nak(self):
    #     doc = {
    #         SYNC_CMD: SYNC_NAK,
    #         SYNC_NODE: self._node,
    #     }
    #     self._outgoing.put(doc)
    #
    # def send_hwm(self, value):
    #     doc = {
    #         SYNC_CMD: SYNC_HWM,
    #         SYNC_NODE: self._node,
    #         SYNC_KEY: value
    #     }
    #     self._outgoing.put(doc)
    #     #print("HWM:", doc)
    #
    #
    # def ack(self, obj, txn=None):
    #     """
    #     (re)Initialise the syncer
    #     :param obj: an incoming RECORD_INIT packet
    #     """
    #     self._hwm = obj[SYNC_KEY]
    #     #self.debug('* Setting HWM to [{}]'.format(self._hwm))
    #     self._syncer.start()
    #
    # def hwm(self, obj):
    #     with self._database.env.begin(write=True) as txn:
    #         key = '__hwm__{}__'.format(self._peer.name).encode()
    #         txn.put(key, obj[SYNC_KEY], db=self._database._metadata)
    #
    # def record_new(self, obj, txn=None):
    #     """
    #     Handle a new incoming record
    #     """
    #     transactions = obj[SYNC_DATA]['txn']
    #     #
    #     #   Make sure all the tables are open
    #     #
    #     for transaction in transactions:
    #         self._database.table(transaction['tab'], txn._txn)
    #
    #     for transaction in transactions:
    #         table = self._database.table(transaction['tab'])
    #         doc = transaction['doc']
    #         txn.append(table, doc)
    #
    # def update_hwm(self, obj, txn):
    #     txn._replicated = True
    #     key = '__hwm__{}__'.format(self._peer.name).encode()
    #     txn._txn.put(key, obj[SYNC_KEY], db=self._database._metadata)
    #
    # def record_update(self, obj, txn):
    #     """
    #     Handle and update to a pre-existing record
    #     """
    #     pass
    #

# class PyMambaBinlogCorrupt(Exception):
#     pass

# def make_pass():
#     """
#     Make a single pass through this transaction
#     """
#     with self._database.env.begin() as txn:
#         with txn.cursor(self._database._binlog) as cursor:
#             if not self._hwm:
#                 move = cursor.first()
#             else:
#                 if not cursor.set_key(self._hwm):
#                     raise PyMambaBinlogCorrupt
#                 move = cursor.next()
#
#             count = 0
#             if move:
#                 while True:
#                     key, val = cursor.item()
#                     print("<{}> <{}>".format(key, val))
#                     self.send_new(key, loads(val.decode()))
#                     count += 1
#                     if not cursor.next():
#                         break
#                     self._database._semaphore.acquire()
#                 self._hwm = key
#                 print("Batch=", count, move)
#             return move
#
#   We listen for _events until _finished
#
#
# last_transaction = None
# retry = 0
# semaphore = Semaphore(semaphore_path(self._path, self._peer.name))
# try:
#     while not self._finished:
#         try:
#             semaphore.acquire(1.0)
#         except BusyError:
#             continue
#         while True:
#             with self._database.env.begin() as txn:
#                 with txn.cursor(self._database._binlog) as cursor:
#                     if not self._hwm:
#                         move = cursor.first()
#                     else:
#                         if not cursor.set_key(self._hwm):
#                             raise PyMambaBinlogCorrupt
#                         move = cursor.next()
#                     if move:
#                         key, val = cursor.item()
#                         self.send_new(key, loads(val.decode()))
#                         self._hwm = key
#                         break
#                     else:
#                         retry += 1
#                         print("Retry=", retry, "Value=", self._database._semaphore.value,self._database._semaphore.name)

# last_txnid = self._database._env.info()['last_txnid']
# if last_txnid != last_transaction:
#     make_pass()
#     last_transaction = last_txnid
# else:
#     sleep(0.01)
#     if self._outgoing.qsize():
#         print("OLen=", self._outgoing.qsize())
#
#   After we've quit, make sure we flush the pending queue
#
# while make_pass():
#    sleep(0.01)
#
#   Shut down the listener too ...
#
#     self._outgoing.put({})
#     self.debug('* Syncer thread terminated for "{}:{}"'.format(self._node, self._peer.name))
# except (KeyboardInterrupt, EOFError) as e:
#     self.debug('* Exit replication syncer')
#     raise

"""
    if a.binlog.empty:
        create dummy entry

    if a.empty and b.empty:
        if a.binlog.empty:








"""
# if not hwm:
#     if not len(self._rs._database.tables) and siz:
#         self.debug('Request for a full resync')
#     else:
#         self.debug('Everyone is empty, sync and start')
#         with self._rs._database.env.begin(write=True) as txn:
#             with txn.cursor(self._rs._database._binlog) as cursor:
#                 cursor.last()
#                 hwm = cursor.key()
#         # set hwm to new entry
#         self.send({
#             PROTOCOL_CMD: PROTOCOL_HWM,
#             PROTOCOL_VAL: hwm
#         })
#         # start syncer
#         self._hwm = hwm
#         self._rs._syncer.start()
#
# else:
#     with self._rs._database.env.begin() as self._txn:
#         with self._txn.cursor(self._rs._database._binlog) as cursor:
#             if not cursor.set_key(hwm):
#                 self.debug('Binlog is out of sync - manual recovery required')
#             else:
#                 count = 0
#                 self.flush()
#                 while cursor.next():
#                     count += 1
#                     hwm, val = cursor.item()
#                     self.send({
#                         PROTOCOL_CMD: PROTOCOL_NEW,
#                         PROTOCOL_KEY: hwm,
#                         PROTOCOL_VAL: loads(val.decode())
#                     })
#                 if count:
#                     self.debug('Sync "{}" new items'.format(count))
#                 self._hwm = hwm
#                 self._rs._syncer.start()

