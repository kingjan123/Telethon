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
import sqlite3


class Database:
    def __init__(self):
        self.conn = sqlite3.connect('db/tl_database.db')

    def create_db(self, tlobjects):
        c = self.conn.cursor()
        for i, tlobject in enumerate(tlobjects):
            if i % 5 == 0:
                print('{} tables left'.format(len(tlobjects) - i))

            columns = []  # List of (column name, type) tuples
            for arg in sorted(tlobject.get_real_args(), key=lambda a: a.name):
                sql_type = self.get_sql_type(arg)
                # If the SQL type is another TLObject, we need
                # an additional column for its constructor ID
                if sql_type == 'tl':
                    columns.append(('{}_ref_id'.format(arg.name), 'integer'))
                    columns.append(('{}_constructor_id'.format(arg.name), 'integer'))
                else:
                    columns.append((arg.name, sql_type))

            # Find the table name for the given object and append its constructor_id
            # in order to reference to an unique table (if an object gets updated,
            # its constructor ID may vary; hence, this ensures one type per table)
            table_name = '{}_{}'.format(tlobject.name, hex(tlobject.id)[2:])

            if columns:
                # Prefix the names with tl_ to avoid conflicts
                columns = ', '.join('tl_{} {}'.format(name, type) for name, type in columns)
                c.execute('create table {} (unique_id integer primary key, {})'.format(table_name, columns))

            else:
                c.execute('create table {} (unique_id integer primary key)'.format(table_name))

        self.conn.commit()

    @staticmethod
    def get_sql_type(tlarg):
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

        # If it wasn't an integral type, then chances are it was another TLObject
        return 'tl'
