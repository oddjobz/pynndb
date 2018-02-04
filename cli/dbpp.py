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
from termcolor import colored
from datetime import datetime


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
            self._out.append(fmt.format(**preformat(item)))
        self._out.append(separator3)
