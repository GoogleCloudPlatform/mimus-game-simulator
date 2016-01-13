# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# pylint: disable=invalid-name
"""Configuration file for the database.  Includes connection parameters, and
a convenience function for connecting to the database."""
import os
import MySQLdb as mysql

# Connection type and parameters can be specified in the environment
# itself by manipulating the appropriate env vars.
connection = os.getenv('DB_CONNECTION_TYPE', 'tcp_direct')

# One dictionary entry for each connection type.  This allows us to
# store multiple connections in our config and select them at runtime.
con_type = {
    'tcp_direct': {},
    'cloudsql_proxy': {},
    'cloudsql_tcp': {},
}
con_type['tcp_direct'] = {
    # params for connecting to a db over tcp.
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', 3306)),
}

con_type['cloudsql_proxy'] = {
    # https://github.com/GoogleCloudPlatform/cloudsql-proxy
    'project': os.getenv('GCP_PROJECT', 'your-project-name'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'zone': os.getenv('DB_ZONE', 'your-cloudsql-zone'),
    'instance_name': os.getenv('DB_NAME', 'your-cloudsql-instance-name'),
}

con_type['cloudsql_tcp'] = {
    # params for connecting to a db over tcp.
    'host': os.getenv('DB_HOST', 'your-cloudsql-ip-address'),
    'port': int(os.getenv('DB_PORT', 3306)),
}

# Look in env var for what kind of db connection this environment is using.
dbc = con_type[connection]

# Elements common to all connection types.
dbc.update({
    #'user': 'root',
    #'pass': '/!GLwL`W$)+BdnF',
    'user': 'mimus',
    'pass': '9dc1b3ae-584c-434e-b899-da2c8ad093fb',
    'name': os.getenv('DB_TABLE', 'mimus'),
})

# Build the fully qualified cloud sql db name from the config if all
# the pieces are there.
if 'project' in dbc and 'zone' in dbc and 'instance_name' in dbc:
    dbc['cloud_sql_db'] = ':'.join(
        [dbc['project'], dbc['zone'], dbc['instance_name']])
    # Build the Cloud SQL proxy socket path from the config
    # https://cloud.google.com/sql/docs/sql-proxy
    dbc['path'] = os.path.join('/cloudsql', dbc['cloud_sql_db'])

def db_connect():
    '''Convenience mysql connect function with args already populated.'''
    if connection in ['cloudsql_proxy']:
        # Use the cloudsql proxy
        return mysql.connect(host=dbc['host'],
                             user=dbc['user'],
                             passwd=dbc['pass'],
                             db=dbc['name'],
                             unix_socket=dbc['path'])
    else:
        # standard TCP mysql connection
        return mysql.connect(host=dbc['host'],
                             port=dbc['port'],
                             user=dbc['user'],
                             passwd=dbc['pass'],
                             db=dbc['name'])
