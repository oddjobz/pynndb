#!/usr/bin/env python

from sys import argv
from pynndb import Database
from pynndb.models import Table, ManyToMany
from pynndb.types import DateType, AgeType, NameType


class UserModel(Table):
    """
    Model definition for objects of type 'UserModel'
    _calculated holds field definitions for all customised fields we want to use.
    _display holds field definitions for the 'list' function
    """
    _calculated = {
        'dob_ddmmyyyy': DateType('dob'),
        'age': AgeType('dob'),
        'name': NameType(('forename', 'surname'))
    }
    _display = [
        {'name': 'name', 'width': 30, 'precision': 30},
        {'name': 'age', 'width': 3},
        {'name': 'dob_ddmmyyyy', 'width': 10},
        {'name': 'uuid', 'width': 24},
        {'name': 'postcodes', 'width': 60, 'precision': 60, 'function': '_postcodes'}
    ]

    def _postcodes(self, doc):
        pcs = ''
        for address in doc.addresses:
            if len(pcs):
                pcs += ' | '
            pcs += address.postcode
        self.__setattr__('postcodes', format('[{}] {}'.format(len(doc.addresses), str(pcs))))


class AddressModel(Table):

    _display = [
        {'name': 'uuid', 'width': 24},
        {'name': 'line1', 'width': 30, 'precision': 30},
        {'name': 'line2', 'width': 30, 'precision': 30},
        {'name': 'line3', 'width': 30, 'precision': 30},
        {'name': 'line4', 'width': 30, 'precision': 30},
        {'name': 'postcode', 'width': 9, 'precision': 9},
    ]


def modify_user(key, change):
    return modify(user_model, key, change)


def modify_address(key, change):
    return modify(address_model, key, change)


def modify(model, key, change):
    doc = model.get(key.encode())
    if not doc:
        print('Unable to locate key "{}"'.format(key))
        return
    doc.modify(change)
    doc.save()


if __name__ == '__main__':

    database = Database('databases/people_database', {'env': {'map_size': 1024 * 1024 * 10}})
    user_model = UserModel(table=database.table('users'))
    address_model = AddressModel(table=database.table('addresses'))
    user_address = ManyToMany(database, user_model, address_model)
    #
    #   Really basic interface using functions built-in to the BaseModel class.
    #
    commands = {
        'lst': user_model.list,
        'add': user_model.add,
        'mod': modify_user,
        'lst_address': address_model.list,
        'add_address': address_model.add,
        'mod_address': modify_address,
    }
    try:
        commands[argv[1]](*argv[2:])
        exit()
    except IndexError:
        print('Insufficient parameters')
    except KeyError:
        print('No such command "{}"'.format(argv[1]))
    except Exception as e:
        raise e
    print('Usage: {} [{}] [parameters]'.format(argv[0], '|'.join(list(commands.keys()))))
    print('Examples:')
    print('  python3 lst                                        # list contents of user table')
    print('  python3 lst_address                                # list contents of address table')
    print('  python3 mod 59afba1d1839fc03ce96d0c8 surname=Jones # set surname of record')
    print("  python3 add '{\"forename\":\"John\",\"surname\":\"Smith\", \"dob_ddmmyyyy\":\"01/01/1966\"}' # add record")
    print('')