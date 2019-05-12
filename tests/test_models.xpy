#!/usr/bin/python3

import unittest
import pytest
from pynndb import Database
from pynndb.models import Table
from pynndb.types import BaseType, DateType, AgeType, NameType
from subprocess import call


class Model(Table):
    """
    Implement a model that covers all data-types
    """
    _calculated = {
        'dob_ddmmyyyy': DateType('dob'),
        'age': AgeType('dob'),
        'name': NameType(('forename', 'surname'))
    }

    _display = [
        {'name': 'name', 'width': 30, 'precision': 30},
        {'name': 'age', 'width': 3},
        {'name': 'dob_ddmmyyyy', 'width': 10}
    ]


class UnitTests(unittest.TestCase):

    _database = None
    _model = None

    def setUp(self):
        call(['rm', '-rf', "databases/test_models"])
        self._database = Database('databases/test_models', {'env': {'map_size': 1024 * 1024 * 10}})
        self._model = Model(table=self._database.table('model'))

    def tearDown(self):
        pass

    def generate_data_1(self):
        self._model.add('{"forename":"tom", "surname": "smith", "dob_ddmmyyyy": "01/01/1971", "uid": 1}')
        self._model.add('{"forename":"dick", "surname": "smith", "dob_ddmmyyyy": "01/01/1972", "uid": 2}')
        self._model.add('{"forename":"harry", "surname": "smith", "dob_ddmmyyyy": "01/01/1973", "uid": 3}')
        self._model.add('{"forename":"sally", "surname": "jones", "dob_ddmmyyyy": "01/01/1969", "uid": 4}')
        self._model.add('{"forename":"mary", "surname": "jones", "dob_ddmmyyyy": "01/01/1968", "uid": 5}')
        self._model.add('{"forename":"sophie", "surname": "jones", "dob_ddmmyyyy": "01/01/1967", "uid": 6}')
        self._model.add('{"forename":"joker", "surname": "wildcard", "dob_ddmmyyyy": "01/01/1966", "uid": 7}')

    @pytest.fixture(autouse=True)
    def capfd(self, capfd):
        self.capfd = capfd

    def test_check_records(self):
        self.generate_data_1()
        self.assertEqual(self._model._table.records, 7)
        self._model.list()

    def test_check_get(self):
        self.generate_data_1()
        for doc in self._model.find():
            get = self._model.get(doc._id)
            self.assertEqual(str(doc), str(get))
        self.assertEqual(len(doc.fred), 0)
        x = doc.uuid

    def test_check_set(self):
        self.generate_data_1()
        for doc in self._model.find():
            doc.surname = 'CHANGED'
            doc.save()
            get = self._model.get(doc._id)
            self.assertEqual(get.surname, "CHANGED")

    def test_check_modify(self):
        self.generate_data_1()
        for doc in self._model.find():
            doc.modify('forename=ME')
            get = self._model.get(doc._id)
            self.assertEqual(get.forename, "ME")

    def test_check_list(self):
        self.generate_data_1()
        self._model.list()
        out, err = self.capfd.readouterr()
        with open('tests/test_models_table1.txt', 'r') as io:
            compare = io.read()

        self.assertEqual(compare, out)

        for doc in self._model.find():
            self._model.list(str(doc._id.decode()))
            break

        out, err = self.capfd.readouterr()
        with open('tests/test_models_table2.txt', 'r') as io:
            compare = io.read()

        self.assertEqual(compare, out)

    def test_data_type_name(self):
        age = AgeType('dob')
        self.assertEqual(age.name, 'dob')

    def test_from_internal(self):
        name = BaseType('name')
        doc = {'name': 'fred'}
        self.assertEqual(name.from_internal(doc), 'fred')

    def test_to_internal(self):
        name = BaseType('name')
        doc = {'name': 'fred'}
        name.to_internal(doc, 'jim')
        self.assertEqual(name.from_internal(doc), 'jim')

if __name__ == "__main__":
    test = UnitTests()
    test.setUp()
    test.generate_data_1()
    test._model.list()
