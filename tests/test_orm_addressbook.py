#!/usr/bin/python3

import unittest
import pytest
from pynndb import Database, size_mb
from pynndb.models import ManyToMany, Table
from subprocess import call


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
        {'name': 'name', 'width': 30},
        {'name': 'postcode', 'width': 15}
    ]

class UnitTests(unittest.TestCase):

    def setUp(self):
        call(['rm', '-rf', 'databases/test_orm_addressbook'])
        self._db = Database('databases/test_orm_addressbook', size=size_mb(10))
        self.tbl_address_book = AddressBookModel(table=self._db.table('address_book'))
        self.tbl_business = BusinessModel(table=self._db.table('business'))
        self.tbl_person = PersonModel(table=self._db.table('person'))
        self.tbl_address = AddressModel(table=self._db.table('address'))

        ManyToMany(self._db, self.tbl_address_book, self.tbl_business)
        ManyToMany(self._db, self.tbl_address_book, self.tbl_person)
        ManyToMany(self._db, self.tbl_business, self.tbl_person)
        ManyToMany(self._db, self.tbl_business, self.tbl_address)
        ManyToMany(self._db, self.tbl_person, self.tbl_address)

    def tearDown(self):
        self._db.close()

    @pytest.fixture(autouse=True)
    def capfd(self, capfd):
        self.capfd = capfd

    def test_01_add_addressbook(self):
        self.tbl_address_book.add({'name': 'Personal'}).save()
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book.name, 'Personal')

    def test_02_add_business(self):
        book = self.tbl_address_book.add({'name': 'Personal'})
        book.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book.save()
        self.assertEqual(book.business[0].name, 'Business#1')
        self.assertEqual(book.business[1].name, 'Business#2')
        self.assertEqual(book.business[2].name, 'Business#3')
        self.assertEqual(book.business[0].person[0].name, 'Person#1')
        self.assertEqual(book.business[1].person[0].name, 'Person#2')
        self.assertEqual(book.business[2].person[0].name, 'Person#3')
        self.assertEqual(book.business[0].person[0].address[0].postcode, 'PC001')
        self.assertEqual(book.business[1].person[0].address[0].postcode, 'PC002')
        self.assertEqual(book.business[2].person[0].address[0].postcode, 'PC003')
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        print(book)
        print(book.business)
        self.assertEqual(book.business[0].name, 'Business#1')
        self.assertEqual(book.business[1].name, 'Business#2')
        self.assertEqual(book.business[2].name, 'Business#3')
        self.assertEqual(book.business[0].person[0].name, 'Person#1')
        self.assertEqual(book.business[1].person[0].name, 'Person#2')
        self.assertEqual(book.business[2].person[0].name, 'Person#3')
        self.assertEqual(book.business[0].person[0].address[0].postcode, 'PC001')
        self.assertEqual(book.business[1].person[0].address[0].postcode, 'PC002')
        self.assertEqual(book.business[2].person[0].address[0].postcode, 'PC003')

    def test_03_add_multiple_person(self):
        book = self.tbl_address_book.add({'name': 'Personal'})
        book.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book.save()
        business = list(self.tbl_business.find(expression=lambda doc: doc['name'] == 'Business#1'))[0]
        business.person.append({'name': 'Appended Person#1'})
        business.person.append({'name': 'Appended Person#2'})
        business.person.append({'name': 'Appended Person#3'})
        self.assertEqual(len(business.person), 4)
        business.save()
        self.assertEqual(len(business.person), 4)
        self.assertEqual(business.person[1].name, 'Appended Person#1')
        self.assertEqual(business.person[2].name, 'Appended Person#2')
        self.assertEqual(business.person[3].name, 'Appended Person#3')
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book.business[0].person[1].name, 'Appended Person#1')
        self.assertEqual(book.business[0].person[2].name, 'Appended Person#2')
        self.assertEqual(book.business[0].person[3].name, 'Appended Person#3')

    def test_04_add_and_delete(self):
        book = self.tbl_address_book.add({'name': 'Personal'})
        book.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book.save()
        business = list(self.tbl_business.find(expression=lambda doc: doc['name'] == 'Business#1'))[0]
        business.person.append({'name': 'Appended Person#1'})
        business.person.append({'name': 'Appended Person#2'})
        business.person.append({'name': 'Appended Person#3'})
        business.save()
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        del book.business[0].person[2]
        book.save()
        self.assertEqual(book.business[0].person[1].name, 'Appended Person#1')
        self.assertEqual(book.business[0].person[2].name, 'Appended Person#3')
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book.business[0].person[1].name, 'Appended Person#1')
        self.assertEqual(book.business[0].person[2].name, 'Appended Person#3')

    def test_04_add_and_delete_and_add(self):
        book = self.tbl_address_book.add({'name': 'Personal'})
        book.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode': 'PC001'})
        book.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode': 'PC002'})
        book.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode': 'PC003'})
        book.save()
        business = list(self.tbl_business.find(expression=lambda doc: doc['name'] == 'Business#1'))[0]
        business.person.append({'name': 'Appended Person#1'})
        business.person.append({'name': 'Appended Person#2'})
        business.person.append({'name': 'Appended Person#3'})
        business.save()
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        del book.business[0].person[2]
        book.save()
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        book.business[0].person.append({'name': 'Appended Person#2'})
        book.save()
        book = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book.business[0].person[1].name, 'Appended Person#1')
        self.assertEqual(book.business[0].person[2].name, 'Appended Person#3')
        self.assertEqual(book.business[0].person[3].name, 'Appended Person#2')
        self.assertEqual(len(book.business[0].person), 4)

    def test_05_traverse(self):
        book = self.tbl_address_book.add({'name': 'Personal'})
        book.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book.save()
        results = ([address.postcode
                    for book in self.tbl_address_book.find()
                    for business in book.business
                    for person in business.person
                    for address in person.address
        ])
        self.assertEqual(results, ['PC001', 'PC002', 'PC003'])
        books = list(set([book.name
                          for address in self.tbl_address.find()
                          for person in address.person
                          for business in person.business
                          for book in business.address_book
        ]))
        self.assertEqual(books, ['Personal'])

    def test_06_sharing_businesses(self):
        book1 = self.tbl_address_book.add({'name': 'Personal'})
        book1.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book1.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book1.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book1.save()
        book2 = self.tbl_address_book.add({'name': 'Business'})
        book2.business.append(book1.business[0])
        book2.business.append(book1.business[1])
        book2.business.append(book1.business[2])
        book2.save()
        self.tbl_address_book.list()
        self.tbl_business.list()
        self.tbl_person.list()
        self.tbl_address.list()
        results = ([address.postcode
                    for book in self.tbl_address_book.find()
                    for business in book.business
                    for person in business.person
                    for address in person.address
        ])
        self.assertEqual(results, ['PC001', 'PC002', 'PC003', 'PC001', 'PC002', 'PC003'])
        books = list(set([book.name
                          for address in self.tbl_address.find()
                          for person in address.person
                          for business in person.business
                          for book in business.address_book
        ]))
        books.sort()
        self.assertEqual(books, ['Business', 'Personal'])

    def test_07_crosspatch_book_person(self):
        book1 = self.tbl_address_book.add({'name': 'Personal'})
        book1.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book1.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book1.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book1.save()
        book2 = self.tbl_address_book.add({'name': 'Business'})
        book2.business.append(book1.business[0])
        book2.business.append(book1.business[1])
        book2.business.append(book1.business[2])
        book2.save()
        book1.person.append(list(self.tbl_person.find(expression=lambda doc: doc['name'] == 'Person#1'))[0])
        book1.person.append(list(self.tbl_person.find(expression=lambda doc: doc['name'] == 'Person#3'))[0])
        book1.save()
        book1 = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book1.person[0].name, "Person#1")
        self.assertEqual(book1.person[1].name, "Person#3")

    def test_08_amend_crosspatch(self):
        book1 = self.tbl_address_book.add({'name': 'Personal'})
        book1.business.append({'name': 'Business#1'}).person.append({'name': 'Person#1'}).address.append({'postcode':'PC001'})
        book1.business.append({'name': 'Business#2'}).person.append({'name': 'Person#2'}).address.append({'postcode':'PC002'})
        book1.business.append({'name': 'Business#3'}).person.append({'name': 'Person#3'}).address.append({'postcode':'PC003'})
        book1.save()
        book2 = self.tbl_address_book.add({'name': 'Business'})
        book2.business.append(book1.business[0])
        book2.business.append(book1.business[1])
        book2.business.append(book1.business[2])
        book2.save()
        book1.person.append(list(self.tbl_person.find(expression=lambda doc: doc['name'] == 'Person#1'))[0])
        book1.person.append(list(self.tbl_person.find(expression=lambda doc: doc['name'] == 'Person#3'))[0])
        book1.save()
        book1 = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        book1.person[0].name += "!!"
        book1.save()
        book1 = list(self.tbl_address_book.find(expression=lambda doc: doc['name'] == 'Personal'))[0]
        self.assertEqual(book1.person[0].name, "Person#1!!")
        self.assertEqual(book1.business[0].person[0].name, "Person#1!!")

if __name__ == "__main__":
    test = UnitTests()
    test.setUp()
    test.tearDown()



