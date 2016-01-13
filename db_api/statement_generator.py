# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Simple library to generate SQL queries.  Since none of the data being placed
# in the SQL statements is not under our control, we can assume that the values
# are 'safe'.  All we do is verify it will fit in the integer size for its
# column.
#
# pylint: disable=invalid-name,line-too-long
"""Library for generating SQL statements."""
import os
import logging
from importlib import import_module
from pprint import pformat
from db_api.datatypes.SQL import types

# Logging config
sqllogger = logging.getLogger('mimus.db_api_query')
sqllogger.setLevel(logging.INFO)
if os.getenv('SQL_STATEMENT_PRINT', None):
    sqllogger.setLevel(logging.DEBUG)


def _validate_data(table, data):
    '''Validate that data falls within the minimum and maximum allowed values

    Args:
        table: The table definition dictionary.  For examples, look at the
            'table_schema' variable in one of the db_api/object files.
        data: The data to insert into the table, in dictionary format.

    Returns:
        data: The data to insert into the table, in dictionary format, updated
            to fit within min/max values where necessary.
    '''
    #sqllogger.debug(pformat(table))
    for field, data_type in table['schema'].iteritems():
        if not field == table['primary_key'] and field in data:
            if int(data[field]) < 0:
                # put the minimum possible value if it underflowed
                data[field] = 0
            elif int(data[field]) > types[data_type]['max_value']:
                error_msg = "Field:'%s', value: '%s' is of type '%s'" % (
                    str(field), str(data[field]), data_type)
                error_msg = error_msg + ' and is not in the valid range of [%d,%d]' % (
                    0, types[data_type]['max_value'])
                sqllogger.error(error_msg)
                # put the maximum possible value if it overflowed
                data[field] = types[data_type]['max_value']
                #raise ValueError(error_msg)
                #return None
    return data


def insert(table, data):
    '''Generate a SQL statement to insert a row after validating that row's data.

    NOTES:
      - If any data provided doesn't match the schema, the row is discarded.
      - Fields provided that don't exist in the table's schema are discarded, but
        if all remaining data is valid, the row will be inserted.

    Args:
        table: The table definition dictionary.  For examples, look at the
            'table_schema' variable in one of the db_api/object files.
        data: The data to insert into the table, in dictionary format.

    Returns:
        insert_SQL: a string containing the resulting SQL query.
    '''

    sqllogger.debug('Preparing INSERT')
    sqllogger.debug(pformat(data))
    data = _validate_data(table, data)
    if data:
        # make SQL statement
        fields = data.keys()
        SQL = []
        SQL.append('INSERT INTO %s (%s) values ' % (table['name'], ','.join(fields)))
        SQL.append('(')
        for field in fields:
            SQL.append("'%s'," % data[field])
        SQL[-1] = SQL[-1][:-1]  # Remove final trailing comma
        SQL.append(')')

        insert_SQL = ''.join(SQL)
        sqllogger.debug(insert_SQL)
        return insert_SQL


def select(table, values=None, field=None):
    '''Generate a SQL statement to select all the rows where the value of 'field'
        is in the list 'values'.

    Args:
        table: The table definition dictionary.  For examples, look at the
            'table_schema' variable in one of the db_api/object files.
        values: List of values to look for in the field specified.
            Passing a False values list results in returning all rows.
        field: The name of the database column in which to look for the specified
            values. Passing a False field key results in the table's primary key
            being used as the field.

    Returns:
        select_SQL: a string containing the resulting SQL query.
    '''
    # If no field specified, use the primary key
    if not field:
        field = table['primary_key']

    sqllogger.debug('Preparing SELECT')
    SQL = []
    SQL.append('SELECT * FROM %s ' % table['name'])
    if values:
        SQL.append('WHERE %s IN (' % field)
        for value in values:
            SQL.append(str(value) + ',')
        SQL[-1] = SQL[-1][:-1]  # Remove final trailing comma
        SQL.append(')')

    select_SQL = ''.join(SQL)
    sqllogger.debug(select_SQL)
    return select_SQL


def update(table, pkey, data):
    '''Generate a SQL statement that updates a row.

    Args:
        table: The table definition dictionary.  For examples, look at the
            'table_schema' variable in one of the db_api/object files.
        pkey: the primary key value of the row in which to insert data.
        data: The data to insert into the table, in dictionary format.

    Returns:
        update_SQL: a string containing the resulting SQL query.
    '''

    sqllogger.debug('Preparing UPDATE')
    SQL = []
    SQL.append('UPDATE %s SET ' % table['name'])
    data = _validate_data(table, data)
    if data:
        for key, value in data.iteritems():
            SQL.append('%s=%s,' % (key, value))
        SQL[-1] = SQL[-1][:-1]  # Remove final trailing comma
        SQL.append(' WHERE %s=%s' % (table['primary_key'], pkey))

    update_SQL = ''.join(SQL)
    sqllogger.debug(update_SQL)
    return update_SQL


def create_table(tname, c):
    '''Generate a SQL statement to create a table, if it does not exist.

    Args:
        tname: Table name to create. Information about the table will be
            loaded from db_api/objects/<tname>.py.
        c: config, typically loaded from mimus_cfg.py

    Returns:
        create_SQL: a string containing the resulting SQL query.
    '''

    # import the module for this table
    table = import_module(os.path.join(c['db_api']['dir'], 'objects',
                                       tname).replace(r'/', r'.')).table_schema

    # Generate create SQL statement based on the schema provided
    sqllogger.debug('Preparing CREATE TABLE')
    SQL = []
    SQL.append('CREATE TABLE IF NOT EXISTS %s (' % tname)
    for field, data_type in table['schema'].iteritems():
        # Nothing is allowed to be null in our schemas.
        SQL.append('%s %s UNSIGNED NOT NULL' % (field, data_type))
        # Auto increment the primary key.
        if field == table['primary_key']:
            SQL.append(' AUTO_INCREMENT UNIQUE')
        else:
            SQL.append(' DEFAULT 0')
        SQL.append(', ')

    # Index if necessary
    for field in table['indexed_fields']:
        SQL.append('INDEX %s_idx (%s), ' % (field, field))

    # Designate the primary key.
    SQL.append('PRIMARY KEY(%s)) ' % table['primary_key'])

    # Turn on table compresssion
    SQL.append('ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8')

    create_SQL = ''.join(SQL)
    sqllogger.debug(create_SQL)
    return create_SQL
