from json import loads
from .types import BaseType


class BaseModel(object):

    def __init__(self, *args, **kwargs):
        """
        Create a new instance of BaseModel which will wrap a model (dict item)
        :param args: a dict item that constitutes a record
        :param kwargs: 'instance', points to an instance of 'Table'
        """
        instance = kwargs.get('instance')
        self.__dict__['_instance'] = instance
        self.__dict__['_dirty'] = False
        self.__dict__['_doc'] = args[0] if len(args) else {}
        self.__dict__['_map'] = {}
        self.__dict__['_dif'] = {}
        self.__dict__['_calculated'] = instance.calculated

    def __getattr__(self, key):
        """
        Get the value of a real or calculated field
        :param key: field name
        :return: calculated field value
        """
        if key in self.__dict__['_calculated']:
            cls = self.__dict__['_calculated'][key]
            if type(cls) in [ManyToManyLink]:
                if key not in self.__dict__['_map']:
                    val = cls.from_internal(self.__dict__['_doc'])
                    self.__dict__['_map'][key] = val
                    self.__dict__['_dif'][key] = val[:]
                return self.__dict__['_map'][key]
            else:
                return cls.from_internal(self.__dict__['_doc'])
        return self.__dict__['_doc'].get(key, '')

    def __setattr__(self, key, value):
        """
        Override the default set for local calculated fields
        :param key: the attribute name
        :param value: the new value for the attribute
        """
        if key in self.__dict__: self.__dict__[key] = value
        elif key not in self._calculated:
            self._doc[key] = value
            self.__dict__['_dirty'] = True
        else:
            self._calculated[key].to_internal(self._doc, value)
            self.__dict__['_dirty'] = True

    def __repr__(self):
        """
        Default display string
        :return: string representation of the record
        """
        return str(self.__dict__['_doc'])

    def modify(self, keyval):
        """
        Simple single attribute modification routine
        :param keyval: key=val string
        """
        setattr(self, *keyval.split('='))
        self.save()

    def save(self):
        """
        Save this record in the database
        """
        if self._dirty:
            self.validate()
            self._instance.save(self._doc)
        self._instance.update_links(self)

    def validate(self):
        """
        Validate the current record against any available validators
        """
        for field in list(self.__dict__['_doc']):
            if field in self._calculated:
                self.__setattr__(field, self.__dict__['_doc'][field])
                del self.__dict__['_doc'][field]
        return self

    def is_new(self):
        """
        Determine if this is a new record that has yet to be written to the DB
        :return: bool
        """
        return '_id' not in self.__dict__['_doc']

    def is_dirty(self, dirty=None):
        """
        Determine if this record is dirty and needs to be written
        :return: bool
        """
        if dirty is None:
            return self.__dict__['_dirty']
        self.__dict__['_dirty'] = dirty

    def doc(self):
        return self._doc

    def update_links(self):
        """
        Update links in associated objects
        """
        self._instance.update_links(self)

    @property
    def uuid(self):
        """
        Convenience function to return the record's _id as a native string
        :return: str (uuid)
        """
        return self.__dict__['_doc']['_id'].decode()


class Table(object):

    _calculated = {}
    _display = []

    def __init__(self, **kwargs):
        """
        Create a model object based on a supplied dict object
        o table, is the database table we map to
        """
        self._table = kwargs.get('table')
        self._links = []

    def get(self, key):
        """
        Get a record from the database using it's UUID
        :param key: uuid (primary key)
        :return: record (as a Model)
        """
        doc = self._table.get(key)
        return BaseModel(doc, instance=self) if doc else None

    def find(self, **kwargs):
        """
        Facilitate a sequential search of the database
        :return: the next record (as a Model)
        """
        for doc in self._table.find(**kwargs):
            yield BaseModel(doc, instance=self)

    def list(self, *uuids):
        """
        Generic boxed listing routine
        :param uuids: the options uuid(s) to display
        :return:
        """
        fields = []
        functions = []
        format_line = '+'
        format_head = '| '
        format_data = '| '
        format_spcs = []
        for field in self._display:
            name = field.get('name', None)
            width = field.get('width', None)
            precision = field.get('precision', None)
            func = field.get('function', None)
            functions.append(func) if func else None
            fields.append(name)
            format_line += '{}+'
            format_head += '{{:{}.{}}} | '.format(width, width)
            if precision:
                format_data += '{{d.{}:{}.{}}} | '.format(name, width, precision)
            else:
                format_data += '{{d.{}:{}}} | '.format(name, width)
            format_spcs.append('-'*(width+2))
        line = format_line.format(*format_spcs)
        head = format_head.format(*fields)

        print(line)
        print(head)
        print(line)
        if not uuids:
            for doc in self.find():
                for func in functions:
                    fn = getattr(self, func, None)
                    fn(doc) if fn else None
                print(format_data.format(d=doc))
        else:
            for uuid in uuids:
                doc = self.get(uuid.encode())
                print(format_data.format(d=doc))
        print(line)

    def add(self, doc):
        """
        Append the current record to the Database
        :param doc: is the document to add, it can be JSON, dict or BaseModel
        :return: BaseModel(the new record)
        """
        doc = loads(doc) if type(doc) is str else doc
        doc = BaseModel(doc, instance=self) if type(doc) is dict else doc
        doc.validate()
        self._table.append(doc.doc())
        self.update_links(doc)
        return doc

    def update_links(self, context):
        """
        Update any linkages attached to this model
        :param context: the base we're updating against
        """
        for link in self._links:
            results = context.__dict__['_map'].get(link._dst_key)
            diff = context.__dict__['_dif'].get(link._dst_key)
            if results is not None:
                for doc in results:
                    if doc.is_new():
                        link.add_dependent(doc, context)
                    elif doc.is_dirty():
                        link.upd_dependent(doc, context)
                    doc.update_links()
                link.del_dependent(context, set(diff) - set(results))
                context.__dict__['_dif'][link._dst_key] = results[:]

    def save(self, doc):
        """
        Simple interface to the database 'save' method
        :param doc: the document (dict) to save
        """
        self._table.save(doc)

    def append(self, doc):
        """
        Simple interface to the database 'append' method
        :param doc: the document (dict) to append
        """
        self._table.append(doc)

    def add_calculated(self, key, val):
        """
        Add a new link field to the list of calculated fields for this model
        :param key: calculated field name
        :param val: a Link object
        """
        self._calculated[key] = val
        self._links.append(val)

    @property
    def table_name(self):
        """
        Simple property to return the database table name
        :return: str
        """
        return self._table.name

    @property
    def calculated(self):
        """
        Simple property to return the _calculated property
        :return: str
        """
        return self._calculated


class ManyToMany(object):
    """
    This class represents a connection between two tables
    """
    def __init__(self, database, class_a, class_b):
        """
        Create a new instance
        :param database: the database we are using
        :param class_a: the first instance of Table
        :param class_b: the second instance of Table
        """
        a = class_a.table_name
        b = class_b.table_name
        table_name = 'rel_{}_{}'.format(class_a.table_name, class_b.table_name)
        table = self._table = database.table(table_name)
        table.index(a, '{{{}}}'.format(a), duplicates=True)
        table.index(b, '{{{}}}'.format(b), duplicates=True)
        table.index(a+b, '{{{}}}{{{}}}'.format(a,b), duplicates=False)
        class_a.add_calculated(class_b.table_name, ManyToManyLink(table, class_a, class_b))
        class_b.add_calculated(class_a.table_name, ManyToManyLink(table, class_b, class_a))


class DirtyList(list):
    """
    This is a customised implementation of list that tracks when new items are added and when
    they are, convert them to a BaseClass type and set their dirty flag to true.
    """

    def __init__(self, instance):
        """
        Create a new list
        :param instance: the model instance we're attached to
        """
        super().__init__([])
        self._instance = instance

    def append(self, obj):
        """
        Append a new item to the list
        :param obj: the object to append (either a BaseModel or dict)
        """
        if type(obj) == dict:
            obj = BaseModel(obj, instance=self._instance)
        super().append(obj)
        obj.is_dirty(True)
        return obj


class ManyToManyLink(BaseType):

    def __init__(self, table, class_a, class_b):
        """
        Create an object linking two classes on link table
        :param table: a reference to the link table
        :param class_a: The master class
        :param class_b: The slave class
        """
        self._table = table
        self._classA = class_a
        self._classB = class_b
        self._src_key = self._classA.table_name
        self._dst_key = self._classB.table_name
        super().__init__(self._dst_key)

    def from_internal(self, doc):
        """
        Present the contrived field as a list containing linked records
        :param doc: our current document
        :return: a list containing linked records
        """
        results = DirtyList(self._classB)
        if '_id' in doc:
            key = {self._src_key: doc['_id'].decode()}
            for link in self._table.seek(self._src_key, key):
                results.append(self._classB.get(link[self._dst_key].encode()))
        return results

    def add_link(self, doc, context):
        """
        Add a new entry to the link table
        :param doc: the document we're linking to
        :param context: the master document we're linking from
        """
        lhs = self._classB.table_name
        rhs = self._classA.table_name
        linkage = {lhs: doc.uuid, rhs: context.uuid}
        self._table.append(linkage)

    def add_dependent(self, doc, context):
        """
        Commit the current document and add an appropriate link to it
        :param doc: the document we're writing
        :param context: the master document we're linking from
        """
        self._classB.append(doc.doc())
        self.add_link(doc, context)

    def _lookup(self, context, doc):
        """
        Lookup an entry in the table linkage index
        :param context: the master document
        :param doc: the slave document
        :return: the index item
        """
        a = self._classA.table_name
        b = self._classB.table_name
        return self._table.seek_one(a+b, {a: context.uuid, b: doc.uuid})

    def upd_dependent(self, doc, context):
        """
        Update a dependent record that has been changed
        :param doc: the document that has changed
        :param context: the master document
        """
        if doc.is_dirty():
            doc.validate()
            self._classB.save(doc.doc())
            doc.is_dirty(False)
        item = self._lookup(context, doc)
        self.add_link(doc, context) if not item else None

    def del_dependent(self, context, docs):
        """
        Delete links to models that no longer exist
        :param context: the master document
        """
        for doc in docs:
            item = self._lookup(context, doc)
            if not item: raise PyMambaForeignKeyViolation('link table item is missing')
            self._table.delete(item['_id'], ),


class PyMambaForeignKeyViolation(BaseException):
    """Foreign key entry is missing"""
