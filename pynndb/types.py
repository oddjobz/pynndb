from datetime import datetime


class BaseType(object):
    """
    Base class for all calculated fields
    """
    _base_name = None

    def __init__(self, name='undef'):
        """
        Create a new custom field
        :param name: name of field
        """
        self._base_name = name

    @property
    def name(self):
        """
        :return: the name of the field (string)
        """
        return self._base_name

    def from_internal(self, doc):
        """
        Return the field in 'external' format
        :param doc: the current record
        :return: the field in external format
        """
        return doc.get(self._base_name, None)

    def to_internal(self, doc, value):
        """
        Return the field in internal format
        :param doc: the document we're dealing with
        :param value: the field in external format
        :return: the field in internal format
        """
        doc[self._base_name] = value


class DateType(BaseType):

    _format = None

    def __init__(self, name, fmt='%d/%m/%Y'):
        """
        Set the date format
        :param fmt: valid date format string
        """
        super().__init__(name)
        self._format = fmt

    def from_internal(self, doc):
        """
        Convert from an integer (internal) to a string (external)
        :param doc: current record
        :return: date (string)
        """
        val = doc.get(self._base_name, None)
        return datetime.fromtimestamp(val).strftime(self._format)

    def to_internal(self, doc, value):
        """
        Convert from external string representation to internal int format
        :param doc: current record
        :param value: string date
        :return: integer date
        """
        doc[self._base_name] = datetime.strptime(value, self._format).timestamp()


class AgeType(BaseType):

    def from_internal(self, doc):
        """
        Calculate age based on current time and 'dob' field
        :param doc: the current record
        :return: age (integer)
        """
        now = datetime.now()
        val = doc.get(self._base_name, now)
        dob = datetime.fromtimestamp(val)
        return (now - dob).days // 365


class NameType(BaseType):

    def from_internal(self, doc):
        """
        Return a 'full name' as a concatenation of fields
        :param doc: the current record
        :return: name (string)
        """
        name = ''
        for field in self._base_name:
            if len(name):
                name += ' '
            name += doc.get(field, '')
        return name
