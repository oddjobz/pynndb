#!/usr/bin/env python
from pynndb import Database
#
print(">> Define some arbitrary data")
data = [
    {'name': 'Gareth Bult', 'age': 21},
    {'name': 'Squizzey', 'age': 3000},
    {'name': 'Fred Bloggs', 'age': 45},
    {'name': 'John Doe', 'age': 0},
    {'name': 'John Smith', 'age': 40},
]

db = Database("databases/raw")  # Open (/create) a database
table = db.table('people')   # Open (/create) a table

print('>> Index table by name and age')
table.index('by_name', '{name}')
table.index('by_age', '{age:03}', duplicates=True)

print('>> Adding data')
for item in data:
    table.append(item)
print("Count=", table.records)

print('>> Scanning table sequentially')
for record in table.find():
    print('{name} is {age} years old'.format(**record))

print('>> Scanning tables in name order [string index]')
for record in table.find('by_name'):
    print('{name} sorted alphabetically'.format(**record))

print('>> Scanning table in age order [numerical index]')
for record in table.find('by_age'):
    print('{age} - {name} in ascending order of age'.format(**record))

print('>> Scanning on name index with filter')
for record in table.find('by_name', expression=lambda doc: doc['age'] > 40):
    print('{name} is {age} years old (filtered age>40)'.format(**record))

db.drop('people')
db.close()