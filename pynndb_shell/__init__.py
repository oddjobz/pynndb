#!/usr/bin/env python3
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
#   This module is for use with "pyipe" although it could be used elsewhere.
#   It's sole purpose is to buffer items passing through the pipe and reformat
#   them in a traditional / recognisable way. It's completely database
#   agnostic and will with with a flexible schema, so applied to both SQL
#   and noSQL tables / collections.
#
##############################################################################
import readline
from os import listdir
from datetime import datetime
from ujson import loads, dumps
from cmd2 import Cmd, with_argparser
from argparse import ArgumentParser
from pynndb import Database
from pathlib import Path, PosixPath
from termcolor import colored
from ascii_graph import Pyasciigraph
from pygments import highlight, lexers, formatters
from termcolor import colored

__version__ = '1.0.0'

# TODO: add a command to adjust the mapsize
# TODO: unique without an index
# TODO: add uuid1 to db as database identifier
# TODO: use 'replace' when indexing instead of delete/put
# TODO: implement Queue structure in table
# TODO: handle list types var a.b.[c]
# TODO: make a generic function available for indexing
# TODO: date serialisation
# TODO: full-text-index - Xapian


class placeholder():

    def __format__(self, spec):
        return '~'

    def __getitem__(self, name):
        return self


class preformat(dict):
    def __getitem__(self, name):
        value = self.get(name)
        if isinstance(value, dict):
            value = str(value)
        if isinstance(value, bytes):
            value = value.encode()
        return value if value is not None else placeholder()


class db_pretty_print(object):

    def __init__(self, *args, **kwargs):
        self._items = []
        self._lengths = {}
        self._kwargs = kwargs
        self._out = None

    def __iter__(self):
        self.index = 0
        return self

    def __contains__(self, key):
        return key in self._items

    def __next__(self):
        if not self.index:
            self.reformat()
            self.limit = len(self._out)
        if self.index == self.limit:
            raise StopIteration
        self.index += 1
        return self._out[self.index-1]

    @property
    def len(self):
        return len(self._items)

    def append(self, data):
        if not data: return
        output = {}
        for k, v in data.items():
            k = k.replace('.', '_')
            if isinstance(v, list) or isinstance(v,dict):
                v = str(v)
                if len(v) > 60:
                    v = v[:60] + '...'

            if k in self._kwargs:
                if self._kwargs[k] == 'datetime':
                    v = datetime.fromtimestamp(v).ctime()
            kl = len(k)
            vl = len(str(v))
            mx = max(kl, vl)
            if ((k in self._lengths) and (mx > self._lengths[k])) or (k not in self._lengths):
                self._lengths[k] = mx
            output[k] = v

        self._items.append(output)

    def reformat(self):
        separator = ''
        fmt = ''
        data = {}
        #
        c = '┌'
        for k,v in self._lengths.items():
            separator += c + '─'*(v+2)
            c = '┬'
        for k,v in self._lengths.items():
            fmt += colored('│ ', 'green')+colored('{'+k+':'+str(v)+'} ', 'cyan')
            data[k] = k
        separator += '┐'
        fmt += colored('│', 'green')
        separator1 = colored(separator, 'green')
        separator2 = separator1.replace('┌', '├').replace('┐', '┤').replace('┬', '┼')
        separator3 = separator1.replace('┌', '└').replace('┐', '┘').replace('┬', '┴')
        self._out = []
        self._out.append(separator1)
        try:
            self._out.append(fmt.format(**data))
        except:
            print('Error while formatting')
            print('Format=', fmt)
            print('Data=', data)
            exit(1)
        self._out.append(separator2)
        fmt = ''
        for k, v in self._lengths.items():
            fmt += colored('│ ', 'green')+colored('{'+k+':'+str(v)+'} ', 'yellow')
        fmt += colored('│', 'green')
        for item in self._items:
            if isinstance(item.get('_id'), bytes):
                item['_id'] = item['_id'].decode()
            try:
                self._out.append(fmt.format(**preformat(item)))
            except Exception as e:
                for i in item.keys():
                    if item[i] is None:
                        item[i] = '(None)'
                self._out.append(fmt.format(**preformat(item)))
        self._out.append(separator3)


class App(Cmd):

    limit = 10

    _data = None
    _base = None
    _db = None
    _default_prompt = colored('pynndb', 'cyan') + colored('>', 'blue') + ' '

    def __init__(self):
        super().__init__()
        path = PosixPath('~/.pynndb').expanduser()
        Path.mkdir(path, exist_ok=True)
        if not path.is_dir():
            self.pfeedback('error: unable to open configuration folder')
            exit(1)
        self._data = path / 'local_data'
        self._base = path / 'registered'
        self._line = path / '.readline_history'
        Path.mkdir(self._data, exist_ok=True)
        Path.mkdir(self._base, exist_ok=True)
        if not self._data.is_dir() or not self._base.is_dir():
            self.pfeedback('error: unable to open configuration folder')
            exit(1)

        self.settable.update({'limit': 'The maximum number of records to return'})
        self.prompt = self._default_prompt

        self.do_shell = None
        self.do_edit = None
        self.do_load = None
        self.do_pyscript = None
        self.do_py = None

    def preloop(self):
        print()
        print(colored('PyNNDB Command Line Interface '.format(__version__), 'green'), end='')
        print(colored('v{}'.format(__version__), 'red'))


        try:
            readline.read_history_file(str(self._line))
        except FileNotFoundError:
            pass

    def postloop(self):
        readline.set_history_length(5000)
        readline.write_history_file(str(self._line))

    def ppfeedback(self, method, level, msg):
        self.pfeedback(colored(method, 'cyan') + ': ' + colored(level, 'yellow') + ': ' + colored(msg, 'red'))
        return False

    parser = ArgumentParser()
    parser.add_argument('database', nargs=1, help='path name of database to register')
    parser.add_argument('alias', nargs=1, help='the local alias for the database')

    @with_argparser(parser)
    def do_register(self, opts):
        """Register a new database with this tool\n"""
        database = opts.database[0]
        alias = opts.alias[0]
        path = Path(database).expanduser()
        if not path.exists():
            self.ppfeedback('register', 'error', 'failed to find path "{}"'.format(database))
            return
        try:
            db = Database(str(path))
            db.close()
        except Exception as e:
            print(e)
        if Path(self._base / alias).exists():
            self.ppfeedback('register', 'failed', 'the alias already exists "{}"'.format(alias))
            return
        Path(self._base / alias).symlink_to(str(path), target_is_directory=True)

    complete_register = Cmd.path_complete

    parser = ArgumentParser()
    parser.add_argument('database', nargs='?', help='name of database to use')

    @with_argparser(parser)
    def do_use(self, opts):
        """Select the database you want to work with\n"""
        if self._db:
            self._db.close()
            self._db = None
            self.prompt = self._default_prompt

        if not opts.database:
            return

        database = opts.database
        if not Path(self._base / database).exists():
            return self.ppfeedback('use', 'error', 'database path not found "{}"'.format(database))
        try:
            path_name = str(Path(self._base / database))
            self._db = Database(path_name)
            self.prompt = colored(database, 'green') + colored('>', 'blue') + ' '
        except Exception as e:
            return self.ppfeedback('register', 'error', 'failed to open database "{}"'.format(database))

    def complete_use(self, text, line, begidx, endidx):
        return [f for f in listdir(str(self._base)) if f.startswith(text)]

    parser = ArgumentParser()
    parser.add_argument('table', nargs=1, help='the name of the table')

    @with_argparser(parser)
    def do_explain(self, opts):
        """Sample the fields and field types in use in this table\n"""
        if not self._db:
            return self.ppfeedback('explain', 'error', 'no database selected')

        table_name = opts.table[0]
        if table_name not in self._db.tables:
            return self.ppfeedback('register', 'error', 'table does not exist "{}"'.format(table_name))

        table = self._db.table(table_name)
        keys = {}
        samples = {}
        for doc in table.find(limit=10):
            for key in doc.keys():
                if key == '_id':
                    continue
                ktype = type(doc[key]).__name__
                if ktype in ['str', 'int', 'bool', 'bytes', 'float']:
                    sample = doc.get(key)
                    if sample:
                        if ktype == 'bytes':
                            sample = sample.decode()
                        samples[key] = sample
                else:
                    sample = str(doc.get(key))
                    if len(sample) > 60:
                        sample = sample[:60] + '...'
                    samples[key] = sample

                if key not in keys:
                    keys[key] = [ktype]
                else:
                    if ktype not in keys[key]:
                        keys[key].append(ktype)

        dbpp = db_pretty_print()
        [dbpp.append({'Field name': key, 'Field Types': keys[key], 'Sample': samples.get(key, '')}) for key in keys]
        dbpp.reformat()
        for line in dbpp:
            print(line)

    def complete_explain(self, text, line, begidx, endidx):
        return [t for t in self._db.tables if t.startswith(text)]

    parser = ArgumentParser()
    parser.add_argument('table', nargs=1, help='the name of the table')

    @with_argparser(parser)
    def do_analyse(self, opts):
        """Analyse a table to see how record sizes are broken down\n"""
        if not self._db:
            return self.ppfeedback('explain', 'error', 'no database selected')

        table_name = opts.table[0]
        if table_name not in self._db.tables:
            return self.ppfeedback('register', 'error', 'table does not exist "{}"'.format(table_name))

        db = self._db.env.open_db(table_name.encode())
        with self._db.env.begin() as txn:
            with txn.cursor(db) as cursor:
                count = 0
                rtot = 0
                rmax = 0
                vals = []
                fn = cursor.first
                while fn():
                    rlen = len(cursor.value().decode())
                    rtot += rlen
                    vals.append(rlen)
                    if rlen > rmax:
                        rmax = rlen
                    count += 1
                    fn = cursor.next

        MAX=20
        div = rmax / MAX
        arr = [0 for i in range(MAX+1)]
        for item in vals:
            idx = int(item / div)
            arr[idx] += 1

        test = []
        n = div
        for item in arr:
            label = int(n)
            if n > 1024:
                label = str(int(n / 1024)) + 'K' if n > 1024 else str(label)
            else:
                label = str(label)

            test.append((label, item))
            n += div

        graph = Pyasciigraph()
        print()
        for line in graph.graph('Breakdown of record size distribution', test):
            print(line)

    def complete_analyse(self, text, line, begidx, endidx):
        return [t for t in self._db.tables if t.startswith(text)]

    parser = ArgumentParser()
    parser.add_argument('table', nargs=1, help='the table you want records from')
    parser.add_argument('fields', nargs='*', help='the fields to display: field:format [field:format..]')
    parser.add_argument('-b', '--by', type=str, help='index to search and sort by')
    parser.add_argument('-k', '--key', type=str, help='key expression to search by')
    parser.add_argument('-e', '--expr', type=str, help='expression to filter by')
    parser.add_argument('-l', '--limit', nargs=1, help='limit output to (n) records')
    parser.add_argument('-d', '--dump', action='store_true', help='output in JSON format')
    parser.add_argument('-c', '--count', action='store_true', help='count the total number of results available')


    @with_argparser(parser)
    def do_find(self, opts):
        """Select records from a table

        find --by=(index) --key=(key) table field:format [field:format..]
        """
        if not self._db:
            return self.ppfeedback('find', 'error', 'no database selected')

        table_name = opts.table[0]
        if table_name not in self._db.tables:
            return self.ppfeedback('find', 'error', 'table does not exist "{}"'.format(table_name))

        table = self._db.table(table_name)
        if opts.by and opts.by not in table.indexes():
            return self.ppfeedback('find', 'error', 'index does not exist "{}"'.format(opts.by))

        limit = int(opts.limit[0]) if opts.limit else self.limit

        args = []
        count = 0
        kwrgs = {'limit': limit}
        action = table.find

        ## implement expr

        if opts.by:
            args.append(opts.by)
        if opts.key and opts.by:
            action = table.seek
            args.append(loads(opts.key))

        def docval(doc, k):
            if '.' not in k:
                return doc.get(k, 'null')
            parts = k.split('.')
            while len(parts):
                k = parts.pop(0)
                doc = doc.get(k, {})
            return doc

        if opts.count:
            maxrec = sum(1 for doc in action(*args))

        query = action(*args, **kwrgs)

        if opts.dump:
            for doc in query:
                json = dumps(doc, sort_keys=True, indent=4)
                print(highlight(json, lexers.JsonLexer(), formatters.TerminalFormatter()))
        else:
            dbpp = db_pretty_print()
            beg = datetime.now()
            [dbpp.append({k: docval(doc, k) for k in opts.fields}) for doc in query]
            end = datetime.now()
            dbpp.reformat()
            for line in dbpp:
                print(line)

            tspan = colored('{:0.4f}'.format((end-beg).total_seconds()), 'yellow')
            limit = '' if dbpp.len < self.limit else colored('(Limited view)', 'red')
            persc = colored('{}/sec'.format(int(1 / (end-beg).total_seconds() * dbpp.len)), 'cyan')
            displayed = colored('Displayed {}'.format(colored(str(dbpp.len), 'yellow')),'green')
            if opts.count:
                displayed += colored(' of {}'.format(colored(maxrec,'yellow')), 'green')
            displayed += colored(' records', 'green')
            self.pfeedback(colored('{} in {}s {} {}'.format(displayed, tspan, limit, persc), 'green'))

    def complete_find(self, text, line, begidx, endidx):
        words = line.split(' ')
        if len(words) > 2:
            table_name = words[1]
            if table_name in self._db.tables:
                table = self._db.table(table_name)
                doc = table.first()
                fields = [f for f in doc.keys()]
                return [f for f in fields if f.startswith(text)]

        return [t for t in self._db.tables if t.startswith(text)]

    parser = ArgumentParser()
    parser.add_argument('table', nargs=1, help='the table you want records from')
    parser.add_argument('field', nargs=1, help='the name of the field you are interested in')
    parser.add_argument('-b', '--by', type=str, help='index to search and sort by')

    @with_argparser(parser)
    def do_unique(self, opts):
        """Display a list of unique values for a chosen field

        unique find --by=(index) table field
        """
        if not self._db:
            return self.ppfeedback('unique', 'error', 'no database selected')

        table_name = opts.table[0]
        if table_name not in self._db.tables:
            return self.ppfeedback('unique', 'error', 'table does not exist "{}"'.format(table_name))

        table = self._db.table(table_name)
        if opts.by and opts.by not in table.indexes():
            return self.ppfeedback('unique', 'error', 'index does not exist "{}"'.format(opts.by))
        else:
            index = table.index(opts.by)

        field_name = opts.field[0]

        dbpp = db_pretty_print()
        counter = 0
        with table.begin() as txn:
            if index:
                cursor = index.cursor(txn)
                action = cursor.first
                while action():
                    dbpp.append({'id': str(counter), field_name: cursor.key().decode(), 'count': str(cursor.count())})
                    action = cursor.next_nodup
                    counter += 1

        dbpp.reformat()
        for line in dbpp:
            print(line)

    def complete_unique(self, text, line, begidx, endidx):
        return [t for t in self._db.tables if t.startswith(text)]

    def show_databases(self):
        """Show available databases"""
        M = 1024 * 1024
        dbpp = db_pretty_print()
        for database in Path(self._base).iterdir():
            mdb = database / 'data.mdb'
            stat = mdb.stat()
            dbpp.append({
                'Database name': database.parts[-1],
                'Mapped (M)': int(stat.st_size / M),
                'Used (M)': int(stat.st_blocks * 512 / M),
                'Util (%)': int((stat.st_blocks * 512 * 100) / stat.st_size)
            })
        dbpp.reformat()
        for line in dbpp:
            print(line)

    def show_tables(self, *args):
        """Display a list of tables available within this database\n"""
        dbpp = db_pretty_print()
        for name in self._db.tables:
            table = self._db.table(name)
            db = self._db.env.open_db(name.encode())
            with self._db.env.begin() as txn:
                stat = txn.stat(db)
            l = int(stat['leaf_pages'])
            dbpp.append({
                'Table name': name,
                '# Recs': stat['entries'],
                'Depth': stat['depth'],
                'Oflow%': int(int(stat['overflow_pages'])*100/(l if l else 1)),
                'Index names': ', '.join(table.indexes())
            })
        dbpp.reformat()
        for line in dbpp:
            print(line)

    def show_indexes(self, table_name):
        """Display a list of indexes for the specified table\n"""
        table = self._db.table(table_name)
        dbpp = db_pretty_print()

        with table.begin() as txn:
            for index in table.indexes(txn=txn):
                key = '_{}_{}'.format(table_name, index)
                doc = loads(txn.get(key.encode(), db=self._db._meta._db).decode())
                dbpp.append({
                    'Table name': table_name if table_name else 'None',
                    'Index name': index if index else 'None',
                    'Entries': table.index(index).count(),
                    'Key': doc.get('func') if doc.get('func') else 'None',
                    'Dups': 'True' if doc['conf'].get('dupsort') else 'False',
                    'Create': 'True' if doc['conf'].get('create') else 'False'
                })
        dbpp.reformat()
        for line in dbpp:
            print(line)

    parser = ArgumentParser()
    parser.add_argument('option', choices=['settings', 'databases', 'tables', 'indexes'], help='what it is we want to show')
    parser.add_argument('table', nargs='?', help='')

    @with_argparser(parser)
    def do_show(self, opts):
        """Show various settings"""

        if opts.option == 'databases':
            return self.show_databases()

        if opts.option == 'settings':
            return super().do_show('')

        if not self._db:
            return self.ppfeedback('unique', 'error', 'no database selected')

        if opts.option == 'tables':
            return self.show_tables()

        if opts.option == 'indexes':
            table_name = opts.table
            if table_name not in self._db.tables:
                return self.ppfeedback('register', 'error', 'table does not exist "{}"'.format(table_name))
            return self.show_indexes(table_name)

    def complete_show(self, text, line, begidx, endidx):
        words = line.split(' ')
        if len(words) < 3:
            return [i for i in ['settings', 'databases', 'indexes', 'tables'] if i.startswith(text)]

        if words[1] == 'indexes':
            return [t for t in self._db.tables if t.startswith(text)]


app = App()
app.cmdloop()
