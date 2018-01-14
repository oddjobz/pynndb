#!/usr/bin/env python3

import readline
from cmd2 import Cmd, with_argument_parser
from argparse import ArgumentParser
from pynndb import Database
from pathlib import Path, PosixPath
from termcolor import colored
from cli.dbpp import db_pretty_print


class App(Cmd):

    database = ''

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

        self.settable.update({'database': 'Name of currently selected database'})
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

    @with_argument_parser(ArgumentParser())
    def do_show_databases(self, *args):
        """Display a list of registered databases\n"""
        M = 1024 * 1024
        dbpp = db_pretty_print()
        for database in Path(self._base).iterdir():
            mdb = database / 'data.mdb'
            stat = mdb.stat()
            dbpp.append({
                'name': database.parts[-1],
                'mapped': int(stat.st_size / M),
                'used': int(stat.st_blocks * 512 / M),
                'percent':  int((stat.st_blocks * 512 * 100) / stat.st_size)
            })
        dbpp.reformat()
        for line in dbpp:
            print(line)

    @with_argument_parser(ArgumentParser())
    def do_show_tables(self, *args):
        """Display a list of tables available within this database\n"""
        if not self._db:
            return self.ppfeedback('show_tables', 'error', 'no database selected')

        M = 1024 * 1024
        dbpp = db_pretty_print()
        for name in self._db.tables:
            table = self._db.table(name)
            dbpp.append({
                'name': name,
                'records': table.records,
                'indexes': ', '.join(table.indexes())
            })
        dbpp.reformat()
        for line in dbpp:
            print(line)

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


app = App()
app.cmdloop()
