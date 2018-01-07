#!/usr/bin/env python

from pynndb import Database, size_mb
from pynndb.models import ManyToMany, Table


class AddressBookModel(Table):
    _calculated = {}
    _display = [
        {'name': 'name', 'width': 30},
    ]


class BusinessModel(Table):
    _calculated = {}
    _display = [
        {'name': 'name', 'width': 30},
    ]


class PersonModel(Table):
    _calculated = {}
    _display = [
        {'name': 'name', 'width': 30},
    ]


class AddressModel(Table):
    _calculated = {}
    _display = [
        {'name': 'line1', 'width': 30},
        {'name': 'postcode', 'width': 15}
    ]


db = Database('databases/contacts_db', size=size_mb(10))

tbl_address_book = AddressBookModel(table=db.table('address_book'))
tbl_business = BusinessModel(table=db.table('business'))
tbl_person = PersonModel(table=db.table('person'))
tbl_address = AddressModel(table=db.table('address'))

address_book_business = ManyToMany(db, tbl_address_book, tbl_business)
address_book_person = ManyToMany(db, tbl_address_book, tbl_person)
business_person = ManyToMany(db, tbl_business, tbl_person)
business_address = ManyToMany(db, tbl_business, tbl_address)
person_address = ManyToMany(db, tbl_person, tbl_address)

print("Tables: ", db.tables)

doc = tbl_address_book.add({'name': 'Personal'})
doc.business.append({'name': 'My Business Name'}).person.append({'name': 'My Person Name'}).address.append({'line1': 'My Address Line 1', 'postcode': 'MY POSTCODE'})
doc.save()

for doc in tbl_business.find(expression=lambda doc: doc['name'] == 'My Business Name'):
   doc.person.append({'name': 'Extra person # 1'})
   doc.person.append({'name': 'Extra person # 2'})
   doc.person.append({'name': 'Extra person # 3'})
   doc.person.append({'name': 'Extra person # 4'}).address.append({'line1': 'Extra # 1', 'postcode': 'MY Extra Post'})
   doc.save()

book = tbl_address_book.add({'name': 'Business Book'})
book.save()

for bus in tbl_business.find(expression=lambda doc: doc['name'] == 'My Business Name'):
    book.business.append(bus)
    book.save()

tbl_address_book.list()
tbl_business.list()
tbl_person.list()
tbl_address.list()

for book in tbl_address_book.find():
    for bus in book.business:
        for per in bus.person:
            for add in per.address:
                print(book.name, '->', bus.name, '->', per.name, '->', add.line1, '->', add.postcode)

addresses = tbl_address.find(expression=lambda doc: doc['postcode'] == 'MY Extra Post')
address_books = tbl_address_book.find()

print([book.name
       for address in addresses
       for person in address.person
       for business in person.business
       for book in business.address_book
       ])

print([address.postcode
       for book in address_books
       for business in book.business
       for person in business.person
       for address in person.address
       ])