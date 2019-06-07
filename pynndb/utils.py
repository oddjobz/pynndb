from sys import _getframe, maxsize
from getpass import getuser


def size_mb(size):
    """
    Helper function when creating database
    :param size: interger (size in Mb)
    :return: integer (bytes)
    """
    return int(1024*1024*size)


def size_gb(size):
    """
    Helper function when creating database
    :param size: interger (size in Gb)
    :return: integer (bytes)
    """
    return int(1024*1024*1024*size)


def _debug(self, msg):
    """
    Display a debug message with current line number and function name

    :param self: A reference to the object calling this routine
    :type self: object
    :param msg: The message you wish to display
    :type msg: str
    """
    if hasattr(self, '_debug') and self._debug:
        line = _getframe(1).f_lineno
        name = _getframe(1).f_code.co_name
        print("{}: #{} - {}".format(name, line, msg))


def _anonymous(text):
    """
    An function used to generate anonymous functions for database indecies

    :param text: The body of the function call to generate
    :type text: str
    :return: Anonymous function to calculate key value
    :rtype: function
    """
    scope = {}
    exec('def func{0}'.format(text), scope)
    return scope['func']


def _index_name(self, name):
    """
    Generate the name of the object in which to store index records

    :param name: The name of the table
    :type name: str
    :return: A string representation of the full table name
    :rtype: str
    """
    return '_{}_{}'.format(self._name, name)


def semaphore_path(path, peer=None):
    """
    Generate a name/path for a semaphore
    :param path: our database path
    :param peer: the host this connection refers to
    :return: semaphore path
    """
    template = '/_pymamba_{}_{}' if not peer else '/_pymamba_{}_{}_{}'
    args = (getuser(), path.replace('/', '_'), peer)
    return template.format(*args)


def get_posixtime(uuid1):
    """Convert the uuid1 timestamp to a standard posix timestamp
    """
    assert uuid1.version == 1, ValueError('only applies to type 1')
    t = uuid1.time
    t = t - 0x01b21dd213814000
    t = t / 1e7
    return t


class xTableExists(Exception):
    """Exception - database table already exists"""
    pass


class xIndexExists(Exception):
    """Exception - index already exists"""
    pass


class xTableMissing(Exception):
    """Exception - database table does not exist"""
    pass


class xIndexMissing(Exception):
    """Exception - index does not exist"""
    pass


class xNotFound(Exception):
    """Exception - expected record was not found"""
    pass


class xAborted(Exception):
    """Exception - transaction did not complete"""


class xWriteFail(Exception):
    """Exception - write failed"""


class xReindexNoKey1(Exception):
    """Exception - write failed"""


class xReindexNoKey2(Exception):
    """Exception - write failed"""


class xDropFail(Exception):
    """Exception - drop failed"""


class xNoKey(Exception):
    """No key was specified for operation"""
