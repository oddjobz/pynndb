PyNNDB - Python Native NoSQL Database
=====================================

.. image:: https://travis-ci.org/oddjobz/pynndb.svg?branch=master
    :target: https://travis-ci.org/oddjobz/pynndb

.. image:: coverage.svg
    :target: https://codecov.io/gh/oddjobz/pymamba

PyNNDB is the second iteration of PyMamba, the name change reflects a conflict with another project
of the same name, and a degree of incompatibility between the API in the old and new versions. At
this time the core components of the project should be considered relatively stable (beta) and are
being used in production systems. The ORM module is experimental / alpha and has had very limited
testing, while the replication module is incomplete / experimental and still needs work.

PyNNDB is a key / value based NoSQL database based on the LMDB storage engine. If you are looking to access your
database from a low-level language such as C/C++/Java, then you will probably find faster database options. However,
if you only intend to access your database 'from' Python, then you will probably find PyNNDB a good bit quicker than
the more traditional alternatives.

What stable features does it have?
----------------------------------
* Variable length records, each stored with a Mongo compatible UUID
* Records are read / written as Python Dict objects
* Secondary indexes based on a lambda function of the contents of the record
* Duplicate keys on secondary indexes
* ACID Transactions handling
* Multi-thread and multi-process access to bypass GIL performance limitations

What Features can I try?
------------------------
* A native Object Relational Mapper, typically seen with SQL databases, loosely based on SQLAlchemy

What Features are in development?
---------------------------------
* High speed multi-node replication
* Native command line shell
* ReadTheDocs documentation

-------------------
Some basic Examples
-------------------

This is an example of how to create a new database called my-database, then within that database to create a table called people, then to add some people. (this is all from the Python shell)

.. code-block:: python

    from pymamba import Database
    db = Database('my-database')
    people = db.table('people')
    people.append({'name': 'Fred Bloggs', 'age': 21})
    people.append({'name': 'Joe Smith', 'age': 22})
    people.append({'name': 'John Doe', 'age': 19})

Now there are lots of different ways of recovering information from the database, the simplest is just to use find() which can be used to scan through the entire table. As find returns a generator, you can either use it within a for-loop, or use list to recover the results as a single list object.

.. code-block:: python

    >>> for doc in people.find():
    ...     print(doc)
    ...
    {'_id': b'58ed69161839fc5e5a57bc35', 'name': 'Fred Bloggs', 'age': 21}
    {'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 22}
    {'_id': b'58ed69301839fc5e5a57bc37', 'name': 'John Doe', 'age': 19}

Note that the returned record includes an _id field, this is almost identical to the ObjectId field used by Mongo, except we're returning a simple byte-string rather than an ObjectId class. A nice feature of dealing with data in this form when matched with Python's new 'format' function is the ability to easily format this data like so;

.. code-block:: python

    >>> for doc in people.find():
    ...     print('Name: {name:20} Age:{age:3}'.format(**doc))
    ...
    Name: Fred Bloggs          Age: 21
    Name: Joe Smith            Age: 22
    Name: John Doe             Age: 19

Or if we just want a subset of the data, we can use an anonymous function to filter our results; (note that this is a linear / sequential scan with a filter)

.. code-block:: python

    >>> for doc in people.find(expression=lambda doc: doc['age'] > 21):
    ...     print('Name: {name:20} Age:{age:3}'.format(**doc))
    ...
    Name: Joe Smith            Age: 22

--------
Indexing
--------

Transparent indexes are a key part of any database system, and I struggled for a while trying to decide which mechanism to use. On the one hand I wanted the functionality of being able to index tables by compound fields and functions, and on the other I just wanted to be able to simply index based on a single clean field. In the end I settled on the following;

.. code-block:: python

    >>> people.ensure('by_name', '{name}')
    >>> people.ensure('by_age_name', '{age:03}{name}')

If you're really familiar with Python format strings, you're going to see fairly quickly what's going on here, essentially we're indexing by expression only, but the expression comes from a Python format string when supplied with the record in dict format. So you can't directly use a function to do anything with regards to key generation, but you can do an awful lot with the Python format mini-language. (and adding actual functions is relatively easy for anyone who can think of a must-have use-case)

So, once we have an index we can search using the index and also find records in order based on the index, so we can re-use find but this time give it an index to use;

.. code-block:: python

    >>> for doc in people.find('by_age_name'):
    ...     print('Name: {name:20} Age:{age:3}'.format(**doc))
    ...
    Name: John Doe             Age: 19
    Name: Fred Bloggs          Age: 21
    Name: Joe Smith            Age: 22

Or we can look for specific records;

.. code-block:: python

    >>> people.seek_one('by_name', {'name': 'Joe Smith'})
    {'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 22}

Or we can look for a range of records;

.. code-block:: python

    >>> for doc in people.range('by_name', {'name': 'J'}, {'name': 'K'}):
    ...     print('Name: {name:20} Age:{age:3}'.format(**doc))
    ...
    Name: Joe Smith            Age: 22
    Name: John Doe             Age: 19

----------------
Updating Records
----------------

We've already covered adding new records to the database, so that leaves us with updating and deleting records. How about this;

.. code-block:: python

    >>> person = people.seek_one('by_name', {'name': 'Joe Smith'})
    >>> person['age'] += 1
    >>> people.save(person)
    >>> people.seek_one('by_name', {'name': 'Joe Smith'})
    {'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 23}

And to delete;

.. code-block:: python

    >>> person = people.seek_one('by_name', {'name': 'Fred Bloggs'})
    >>> people.delete(person['_id'])
    >>> for doc in people.find():
    ...     print('Name: {name:20} Age:{age:3}'.format(**doc))
    ...
    Name: Joe Smith            Age: 23
    Name: John Doe             Age: 19
    >>>

There's a lot more to come, but so far it's looking pretty promising. On my workstation a for-loop based on a find yields around 200k results per second, and an append yields around 30k new items per second. This seems to be fairly respectable for a high level language database and seems to be much faster than Mongo when used with either Python or Node.

.. code-block:: text

    ** SINGLE Threaded benchmark **
    ** Probably better throughput with multiple processes

    * No Indecies
      -     0: 5000 - Append Speed/sec = 48882
      -  5000: 5000 - Append Speed/sec = 52778
      - 10000: 5000 - Append Speed/sec = 52882
    * Indexed by sid, day, hour
      -     0: 5000 - Append Speed/sec = 34420
      -  5000: 5000 - Append Speed/sec = 36096
      - 10000: 5000 - Append Speed/sec = 35885
    * Indexed by function
      -     0: 5000 - Append Speed/sec = 39235
      -  5000: 5000 - Append Speed/sec = 39822
      - 10000: 5000 - Append Speed/sec = 41116
    * Linear scan through most recent index
      -     0:15000 - Read Speed/sec   = 234615

