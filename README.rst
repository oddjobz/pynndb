PyNNDB - Python Native NoSQL Database
=====================================

.. image:: https://travis-ci.org/oddjobz/pynndb.svg?branch=master
    :target: https://travis-ci.org/oddjobz/pynndb

.. image:: coverage.svg
    :target: https://github.com/oddjobz/pynndb

.. image:: https://badge.fury.io/py/pynndb.svg
    :target: https://badge.fury.io/py/pynndb

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
* Native command line shell [pynndb_ docs are here]

.. _pynndb: docs/shell.md

What Features are in development?
---------------------------------
* High speed multi-node replication
* ReadTheDocs documentation
* A native Object Relational Mapper, typically seen with SQL databases, loosely based on SQLAlchemy

-------------------
Some basic Examples
-------------------

This is an example of how to create a new database called my-database, then within that database to create a table called people, then to add some people. (this is all from the Python shell)

.. code-block:: python

    from pynndb import Database
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

ORM - Object Relational Mapper
==============================

The native PyNNDB interface is not unlike Mongo in that it treats each record (or document) as a Python dictionary. For databases that involve single / unrelated tables, this is fine and the most efficient means to access data. If however you're mapping relationships between tables, as you might with a traditional SQL database, maintaining linkage tables can be a bit fiddly, and it you're used to something like SQLAlchemy, the standard interface may seem a little raw.

To this end we have a built-in mechanism for overlaying some structure onto our raw tables to give things a bit of an Alchemy feel. If you're not used to ORM's then this might look a bit like magic, but for SQLAlchemy users, you should feel right at home and hopefully wondering why SQLAlchemy isn't this easy ... ;-)

Current Features
----------------

So, what we're catering for at the moment;

* Calculated fields

  - Date

  - Age

  - Name

  - UUID

  - Custom

* ManyToMany links between tables
* Table pretty-printer
* OneToMany links between tables [TODO]
* Referential integrity control [TODO]
* Link attributes [TODO]

We do have a little work left to do as you can see, but the heart of the ORM is up and running and seem to work fairly well.

There's a blog posting with more detail here. <`Article on ORM for NoSQL`__>.

.. __: https://gareth.bult.co.uk/2017/09/14/orm_for_nosql/

-----------------
How to use Models
-----------------

The idea is that we wrap each table up in a dedicated class then we can create additional classes to link the (wrapped) tables together. Here's a very simple example;

.. code-block:: python

    from pynndb import Database
    from pynndb.models import ManyToMany, Table
    from pynndb.types import AgeType, DateType

    class UserModel(Table):
        _calculated = {
            'age': AgeType('dob'),
            'birthday': DateType('dob')
        }
        _display = [
            {'name': 'forename', 'width': 20},
            {'name': 'surname', 'width': 20},
            {'name': 'birthday', 'width': 15},
            {'name': 'age', 'width': 3}
        ]

    db = Database('my_db', {'env': {'map_size': 1024 * 1024 * 10}})
    user_model = UserModel(table=db.table('users'))

If you save this to a file (demo.py) you should then be able to do the following;

.. code-block:: python

    >>> from demo import user_model
    >>> import datetime
    >>> user_model.add({'forename':'fred','surname':'bloggs','dob':datetime.date(1970,12,1)})
    >>> user_model.list()
    +----------------------+----------------------+-----------------+-----+
    | forename             | surname              | dob             | age |
    +----------------------+----------------------+-----------------+-----+
    | fred                 | bloggs               |        28857600 |  46 |
    +----------------------+----------------------+-----------------+-----+

Note that age isn't a stored field, it's generated on the fly from the 'dob' field hence will dynamically change whenever the dob field is updated. Also, the list function is driven (by default) by the attributes listed in _display.

As it stands the date of birth isn't terribly readable, so we could add another field to the mix to get around this, in calculated add;

.. code-block:: python

    'birthday': DateType('dob')

And change the display section to show birthday rather then dob, then try the above operation again and you should get (don't forget to add DateType to your imports);

.. code-block:: python

    >>> from demo import user_model
    >>> user_model.list()
    +----------------------+----------------------+-----------------+-----+
    | forename             | surname              | birthday        | age |
    +----------------------+----------------------+-----------------+-----+
    | fred                 | bloggs               | 01/12/1970      |  46 |
    +----------------------+----------------------+-----------------+-----+

So far this all looks relatively trivial, the real value comes in what it's doing under the hood. Let's try to update this data, take a look at the following;

.. code-block:: python

    >>> from demo import user_model
    >>> user = list(user_model.find())[0]
    >>> user.surname='Bloggs Updated'
    >>> user.save()
    >>> user_model.list()
    +----------------------+----------------------+-----------------+-----+
    | forename             | surname              | birthday        | age |
    +----------------------+----------------------+-----------------+-----+
    | fred                 | Bloggs Updated       | 01/12/1970      |  46 |
    +----------------------+----------------------+-----------------+-----+

The .find() method for a model just returns all records (as an array) so all we're doing here is assigning 'user' to the first record in the table. Each field in the table is then accessible as an attribute (i.e. user.forename, user.surename, user.dob etc) which is a little more natural than updating a dict, then save updates changes in the model back to the actual table. Again relatively trivial, however this is quite neat;

.. code-block:: python

    >>> print(user.age, user.birthday)
    46 01/12/1970

i.e. when you access the model, you will see attributes that are generated on the fly in additional to any stored data, and (!) if you don't access them they're not generated so there's no overhead in having lots of rarely used calculated fields.

------------------------
How to use Relationships
------------------------

So this is where things get a little more interesting. In standard NoSQL, typically there is no real concept of table linkage, foreign keys or referential integrity. However, that doesn't mean the concepts are invalid or no longer needed, so, here is NoSQL with inter- table relationships, managed by a built-in ORM (!)

First, let's start by defining a second table, we're going to make it really easy by just having an address table, then working on the premise that users can have multiple addresses, and that a number of users can live at each address.

.. code-block:: python

    class AddressModel(Table):

        _display = [
            {'name': 'address', 'width': 30},
            {'name': 'postcode', 'width': 15}
        ]

And we will create a relationship between the UserModel and the AddressModel by adding this to our previous code;

.. code-block:: python

    address_model = AddressModel(table=db.table('addresses'))
    links = ManyToMany(db, user_model, address_model)

So, starting up as before we can do this;

.. code-block:: python

    from demo import user_model, address_model, UserModel
    import datetime
    >>> user = user_model.add({'forename':'john','surname':'smith','dob':datetime.date(1971,12,1)})
    >>> user.addresses.append({'address': 'address1', 'postcode': 'postcode1'})
    >>> user.addresses.append({'address': 'address2', 'postcode': 'postcode2'})
    >>> user.save()
    >>> user_model.list()
    +----------------------+----------------------+-----------------+-----+
    | forename             | surname              | birthday        | age |
    +----------------------+----------------------+-----------------+-----+
    | john                 | smith                | 01/12/1971      |  45 |
    +----------------------+----------------------+-----------------+-----+
    >>> address_model.list()
    +--------------------------------+-----------------+
    | address                        | postcode        |
    +--------------------------------+-----------------+
    | address1                       | postcode1       |
    | address2                       | postcode2       |
    +--------------------------------+-----------------+

So there are some interesting things going on here, we have created a new instance of UserModel, then added two new addresses by appending to it's address property. Now the address property is a virtual field created by the "ManyToMany" link and not only is it populated from the address table, but it can also be used to append, update and delete entries in the address table. On further inspection we see;

.. code-block:: python

    >>> user
    {'surname': 'smith', '_id': b'59b6860b1839fc4ee8c00596', 'forename': 'john', 'dob': datetime.date(1971, 12, 1)}
    >>> user.addresses
    [{'address': 'address1', 'postcode': 'postcode1', '_id': b'59b6860b1839fc4ee8c00597'}, {'address': 'address2', 'postcode': 'postcode2', '_id': b'59b6860b1839fc4ee8c00599'}]
    >>> type(user.addresses[0])
    <class 'pynndb.models.BaseModel'>

Again, virtual and calculated fields are only evaluated when reading through the users table, the cost of reading associated tables is only incurred if the linked attributes (addresses in this case) are accessed. Note that the addresses field is a list, but of type BaseModel, rather than of a raw dict.

----------------------
Updating linked tables
----------------------

In a similar fashion, we can do updates to the linked table;

.. code-block:: python

    >>> user = list(user_model.find())[0]
    >>> user
    {'surname': 'smith', '_id': b'59b6860b1839fc4ee8c00596', 'forename': 'john', 'dob': 60393600}
    >>> user.addresses[1]
    {'address': 'address2', 'postcode': 'postcode2', '_id': b'59b6860b1839fc4ee8c00599'}
    >>> user.addresses[1].postcode = 'A new postcode'
    >>> user.save()
    >>> address_model.list()
    +--------------------------------+-----------------+
    | address                        | postcode        |
    +--------------------------------+-----------------+
    | address1                       | postcode1       |
    | address2                       | A new postcode  |
    +--------------------------------+-----------------+

---------------------------------
Deleting entries in linked tables
---------------------------------

And of course, we can delete in the same way, but be aware that this will only sever the link rather than deleting the address, so future references to addresses in this example will only show the user linked to one address, but a listing of the address table will show both addresses. Deleting target objects with a zero reference count will be an option when the referential integrity code is added.

.. code-block:: python

    >>> del user.addresses[0]
    >>> user.save()
    >>> user = list(user_model.find())[0]
    >>> user.addresses
    [{'address': 'address2', 'postcode': 'A new postcode', '_id': b'59b6860b1839fc4ee8c00599'}]

If we wanted to re-instate the relationship in this instance we could do;

.. code-block:: python

    >>> address = list(address_model.find())[0]
    >>> address
    {'_id': b'59b800e41839fc41593c9894', 'address': 'address1', 'postcode': 'postcode1'}
    >>> user.addresses.append(address)
    >>> user.save()
    >>> user = list(user_model.find())[0]
    >>> user.addresses
    [{'_id': b'59b800e41839fc41593c9896', 'address': 'address2', 'postcode': 'A new postcode'}, {'_id': b'59b800e41839fc41593c9894', 'address': 'address1', 'postcode': 'postcode1'}]

The funny looking "user = list(...)" function is only being used to force a re-read on the database following an update. The user variable will still be instantiated and in theory a re-read should make no difference to it's value, but for testing, it's always good to be sure it's actually storing what you think it is.

