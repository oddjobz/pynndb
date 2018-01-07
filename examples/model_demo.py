#!/usr/bin/env python

from pynndb import Database
from pynndb.models import ManyToMany, Table
from pynndb.types import AgeType, DateType
import datetime


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


class AddressModel(Table):
    _display = [
        {'name': 'address', 'width': 30},
        {'name': 'postcode', 'width': 15}
    ]

db = Database('databases', {'env': {'map_size': 1024 * 1024 * 10}})
user_model = UserModel(table=db.table('users'))
address_model = AddressModel(table=db.table('addresses'))
links = ManyToMany(db, user_model, address_model)

user = user_model.add({'forename':'john','surname':'smith','dob':datetime.date(1971,12,1)})
print(user)
user.addresses.append({'address': 'address1', 'postcode': 'postcode1'})
user.addresses.append({'address': 'address2', 'postcode': 'postcode2'})
user.save()
print(user)
print(user.addresses)
user_model.list()
address_model.list()

user = list(user_model.find())[0]
del user.addresses[0]
user.save()
user = list(user_model.find())[0]
print("After deletion ...", user.addresses)

address = list(address_model.find())[0]
user.addresses.append(address)
user.save()
print("After addition ...", user.addresses)

user = list(user_model.find())[0]
user.addresses[0].postcode = "GB"
user.save()
print("After update .....", user.addresses)