# Python Native NoSQL Database

PyNNDB is a key / value based NoSQL database based on the LMDB storage engine that supports multiple
tables, unique and compound indexes, and ACID transactions. It is designed to be used from Python and
as such is pretty quick compared to other databases when used from within Python. That said, PyNNDB as
as pure C extension is in the works and is considerably quicker.

### Usage

```python
from pynndb import Database
data = [
    {'name': 'Fred Bloggs', 'age': 21},
    {'name': 'Joe Smith', 'age': 25},
    {'name': 'Harry Jones', 'age': 19},
    {'name': 'John Doe', 'age': 28},
    {'name': 'John Smith', 'age': 40},
]
db = Database("databases/my_database")  # Open (/create) a database
table = db.table('people')   			# Open (/create) a table
# Add all our data into the table
[table.append(item) for item in data]
```
##### Now we can recover the data with:
```python
from pynndb import Database
db = Database("databases/my_database")  # Open (/create) a database
for person in db.table('people').find():
	print(person)
```
##### Which yields:
```json
{'name': 'Fred Bloggs', 'age': 21, '_id': b'5ded744e9d1a2a1bc40f279e'}
{'name': 'Joe Smith', 'age': 25, '_id': b'5ded744e9d1a2a1bc40f279f'}
{'name': 'Harry Jones', 'age': 19, '_id': b'5ded744e9d1a2a1bc40f27a0'}
{'name': 'John Doe', 'age': 28, '_id': b'5ded744e9d1a2a1bc40f27a1'}
{'name': 'John Smith', 'age': 40, '_id': b'5ded744e9d1a2a1bc40f27a2'}
```
Note that every record written is automatically assigned a unique UID which can be used
to uniquely identify any given record.