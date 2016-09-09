"""
The TLObjects are stored as follow:
- Every TLObject has their own unique ID, which is made by the hash of the
  tuple of all its attributes (constructor id, and custom additional attributes)

- If one of those attributes is another TLObject, this is stored in its own table,
  and referenced by the parent TLObject with its unique generated ID.

  Also, an additional field will be created to know the exact child TLObject's
  constructor ID, so at retrieval, the parent can know where to look.

- When retrieving an object, its original .tl definition is **required** in order
  to determine how to retrieve the object.
"""
import pickle
import sqlite3
import os

from parser import TLObject
from parser import TLParser
from tl import MTProtoRequest


class Database:
    def __init__(self):
        db_path = 'db/tl_database.db'
        create_new = not os.path.isfile(db_path)

        # Connect to the database and create the tables if it is the first time
        self.conn = sqlite3.connect('db/tl_database.db')
        if create_new:
            self.create_db(tuple(TLParser.parse_file('scheme.tl')))

    def create_db(self, tlobjects):
        c = self.conn.cursor()
        for i, tlobject in enumerate(tlobjects):
            if i % 5 == 0:
                print('{} tables left'.format(len(tlobjects) - i))

            # Retrieve SQL information about the object
            table_name = Database.get_sql_table_name(tlobject)
            columns = Database.get_sql_columns(tlobject)
            if columns:
                columns = ', '.join('{} {}'.format(name, type) for name, type in columns)
                c.execute('create table {} (unique_id integer primary key, {})'.format(table_name, columns))

            else:
                c.execute('create table {} (unique_id integer primary key)'.format(table_name))

        self.conn.commit()

    def insert_or_replace_object(self, object):
        """Inserts or replaces the object into its corresponding database"""
        # First ensure that this is a MtProtoRequest
        assert issubclass(type(object), MTProtoRequest), 'The object must be a MtProtoRequest'

        # Parse the TLObject definition back from the representation of the object
        is_function = object.__class__.__name__.endswith('Request')
        tlobject = TLObject.from_tl(repr(object)+';', is_function=is_function)

        # Determine the table name and what columns this object has
        table_name = Database.get_sql_table_name(tlobject)
        columns = Database.get_sql_columns(tlobject)

        # Define the values (?,?,...), equal to the count of columns
        # +1 to include the unique_id column at the beginning
        values_definition = ','.join('?' for _ in range(len(columns) + 1))

        # Store here the object values
        values = [object.get_unique_id()]  # Start with its ID

        # Iterate over the arguments sorted to append them to the values
        for arg in sorted(tlobject.get_real_args(), key=lambda a: a.name):
            # Find both the SQL type and the value from the object
            sql_type = Database.get_sql_type(arg)
            value = getattr(object, arg.name)

            # If the SQL type is `tl`, this means that it is another TLObject
            if sql_type == 'tl':
                if not value:
                    # If it's None, we need to append two None's
                    values.extend([None] * 2)
                else:
                    # Otherwise, append the ref_id and constructor ID
                    # to be able to reference to the object
                    values.append(value.get_unique_id())
                    values.append(value.constructor_id)

                    # And also insert the object itself into its corresponding table
                    self.insert_or_replace_object(value)

            # Otherwise, it was an integral type
            else:
                # Although if it was a list, we need to store it as binary data
                if type(value) is list:
                    values.append(pickle.dumps(value))
                else:
                    values.append(value)

        # Finally, insert or replace the values in the database
        self.conn.cursor().execute('insert or replace into {} values ({})'
                                   .format(table_name, values_definition),
                                   tuple(values))

    def commit(self):
        """Commits changes to the database"""
        self.conn.commit()

    @staticmethod
    def get_sql_table_name(tlobject):
        # Find the table name for the given object and append its constructor_id
        # in order to reference to an unique table (if an object gets updated,
        # its constructor ID may vary; hence, this ensures one type per table)
        return '{}_{}'.format(tlobject.name, hex(tlobject.id)[2:])

    @staticmethod
    def get_sql_columns(tlobject):
        """Returns the SQL columns of the given TLObject
        expressed as a list of (column name, type) tuples"""

        # Store here the tuples
        columns = []
        for arg in sorted(tlobject.get_real_args(), key=lambda a: a.name):
            sql_type = Database.get_sql_type(arg)

            # When storing the name, make sure to prefix it with `tl_` to avoid conflicts
            if sql_type == 'tl':
                # If the SQL type is another TLObject, we need
                # an additional column for its constructor ID
                columns.append(('tl_{}_ref_id'.format(arg.name), 'integer'))
                columns.append(('tl_{}_constructor_id'.format(arg.name), 'integer'))
            else:
                columns.append(('tl_{}'.format(arg.name), sql_type))

        return columns

    @staticmethod
    def get_sql_type(tlarg):
        """Gets the SQL type of the given TL argument"""
        if tlarg.is_vector:
            return 'blob'  # Store vectors as blobs

        type = tlarg.type.lower()
        if type in ['true', 'bool', 'int', 'long', 'int128', 'int256']:
            return 'integer'

        if type in ['double', 'float']:
            return 'real'

        if type in ['string']:
            return 'text'

        if type in ['bytes']:
            return 'blob'

        # If it wasn't an integral type, then it has to be another TLObject
        return 'tl'
