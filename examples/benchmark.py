#!/usr/bin/env python3

from time import time
from random import random
from subprocess import call
from pynndb import Database


def chunk(db, tbl, start, count):
    """
    Create a chunk of data

    :param tbl: Table to operate on
    :type tbl: Table
    :param start: Starting index
    :type start: int
    :param count: Number of items
    :type count: int
    """
    begin = time()
    for index, session in enumerate(range(start, start + count)):
        with db.begin() as txn:
            rnd = random()
            record = {
                'origin': 'linux.co.uk',
                'sid': start + count - index,
                'when': time(),
                'day': int(rnd * 6),
                'hour': int(rnd * 24)
            }
            txn.append(tbl, record)

    finish = time()
    print("  - {:5}:{:5} - Append Speed/sec = {:.0f}".format(start, count, count / (finish - begin)))


print('** SINGLE Threaded benchmark **')
print('** Probably better throughput with multiple processes')
print('')

call(['rm', '-rf', 'databases/perfDB'])
print("* No Indecies")
db = Database('databases/perfDB', conf={'writemap': False})
table = db.table('sessions')
chunk(db,table, 0, 5000)
chunk(db,table, 5000, 5000)
chunk(db,table, 10000, 5000)
db.close()

call(['rm', '-rf', 'databases/perfDB'])
print("* Indexed by sid, day, hour")
db = Database('databases/perfDB', conf={'writemap': False})
# db = Database('databases/perfDB')
table = db.table('sessions')
table.index('by_sid', '{sid}')
table.index('by_day', '{day}')
table.index('by_hour', '{hour}')
chunk(db,table, 0, 5000)
chunk(db,table, 5000, 5000)
chunk(db,table, 10000, 5000)
db.close()

db = Database('databases/perfDB', conf={'writemap': False})
#call(['rm', '-rf', 'databases/perfDB'])
print("* Indexed by function")
db = Database('databases/perfDB')
table = db.table('sessions')
table.index('by_multiple', '!{origin}|{day:02}|{hour:02}|{sid:05}')
chunk(db, table, 0, 5000)
chunk(db,table, 5000, 5000)
chunk(db,table, 10000, 5000)

print("* Linear scan through most recent index")
start = 0
count = 15000
begin = time()
for doc in table.find('by_multiple'): pass
finish = time()
print("  - {:5}:{:5} - Read Speed/sec   = {:.0f}".format(start, count, count / (finish - begin)))

db.close()
