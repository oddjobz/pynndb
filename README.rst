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
* Transactions with commit and rollback

What Features can I try?
------------------------
* A native Object Relational Mapper, typically seen with SQL databases, loosely based on SQLAlchemy

What Features are in development?
---------------------------------
* High speed multi-node replication
* Native command line shell
* ReadTheDocs documentation

