#!/usr/bin/env python3

import readline
import argcomplete
from os import listdir
from datetime import datetime
from ujson import loads
from cmd2 import Cmd, with_argument_parser
from argparse import ArgumentParser
from pynndb import Database
from pathlib import Path, PosixPath
from termcolor import colored
from cli.dbpp import db_pretty_print
from ascii_graph import Pyasciigraph

__version__ = '0.0.1'

# TODO: auto table complete for indexes
# TODO: add -c flag to display total count
# TODO: unique without an index
# TODO: handle list types var a.b.[c]
# TODO: make a generic function available for indexing
# TODO: date serialisation
# TODO: full-text-index
# TODO: add -d / dump output https://stackoverflow.com/questions/25638905/coloring-json-output-in-python


class App(Cmd):

    limit = 10

    _data = None
    _base = None
    _db = None
    _default_prompt = colored('pynndb', 'cyan') + colored('>', 'blue') + ' '

    def __init__(self):
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
        self.exclude_from_help.append('do_save')
        self.exclude_from_help.append('do_py')
        self.exclude_from_help.append('do__relative_load')
        self.exclude_from_help.append('do_run')
        self.exclude_from_help.append('do_cmdenvironment')
        self.exclude_from_help.append('do_load')
        self.exclude_from_help.append('do_pyscript')
        super().__init__()

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

    @with_argument_parser(parser)
    def do_register(self, argv, opts):
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

    @with_argument_parser(parser)
    def do_use(self, argv, opts):
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
        return [f for f in listdir(self._base) if f.startswith(text)]

    parser = ArgumentParser()
    parser.add_argument('table', nargs=1, help='the name of the table')

    @with_argument_parser(parser)
    def do_explain(self, argv, opts):
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

    @with_argument_parser(parser)
    def do_analyse(self, argv, opts):
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

    @with_argument_parser(parser)
    def do_find(self, argv, opts):
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

        args = []
        kwrgs = {'limit': self.limit}
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

        query = action(*args, **kwrgs)
        dbpp = db_pretty_print()
        beg = datetime.now()
        [dbpp.append({k: docval(doc, k) for k in opts.fields}) for doc in query]
        end = datetime.now()
        dbpp.reformat()
        for line in dbpp:
            print(line)

        count = colored(str(dbpp.len), 'yellow')
        tspan = colored('{:0.4f}'.format((end-beg).total_seconds()), 'yellow')
        limit = '' if dbpp.len < self.limit else colored('(Limited view)', 'red')
        persc = colored('{}/sec'.format(int(1 / (end-beg).total_seconds() * dbpp.len)), 'cyan')
        self.pfeedback(colored('Displayed {} records in {}s {} {}'.format(count, tspan, limit, persc), 'green'))
        return False

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

    @with_argument_parser(parser)
    def do_unique(self, argv, opts):
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
            dbpp.append({
                'Table name': name,
                '# Recs': stat['entries'],
                'Depth': stat['depth'],
                'Oflow%': int(int(stat['overflow_pages'])*100/int(stat['leaf_pages'])),
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

    @with_argument_parser(parser)
    def do_show(self, argv, opts):
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
