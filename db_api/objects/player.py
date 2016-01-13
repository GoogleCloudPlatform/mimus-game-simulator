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
#
# pylint: disable=line-too-long,invalid-name
"""Schema definition/access methods for the player object's table."""
from __future__ import with_statement
from collections import OrderedDict
import logging
import binascii

# Custom modules
import db_api.statement_generator as db_api_query

playerlogger = logging.getLogger('mimus.player')
playerlogger.setLevel(logging.INFO)

table_schema = {
    'name': 'player',
    # No indexes in this table but key must exist, other modules depend on it
    'indexed_fields': [],
    'primary_key': 'id',
    'schema': OrderedDict([('id', 'INT'), ('slots', 'SMALLINT'),
                           ('points', 'SMALLINT'), ('stones', 'SMALLINT'),
                           ('stamina', 'SMALLINT')])
}


def name_to_id(name):
    """convert player name to ID"""
    # This is fairly unsophisticated, just does a CRC32 on the name.  Can be
    # optimized both for compute requirements and collision frequency using
    # another hashing algorithm.
    return binascii.crc32(name) & 0xFFFFFFFF


def get(player_id):
    """Return query to get a player by ID.

    Args:
        player_id: Hashed player name.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to get player from the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """

    get_player = db_api_query.select(table_schema, values=[player_id, ])
    return [(get_player, 'player')]


def update(player):
    """Return query to update player row.

    Args:
        player: Dictionary of player stat key/value pairs.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to update player in the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """

    update_player = db_api_query.update(table_schema, player['id'], player)
    return [(update_player, 'affected'), ]


def create(player_id, c):
    """Returns query to create a player and give them the initial currency
    & card loadout.

    Args:
        player_id: Hashed player name.
        c: Config dictionary, typically read from mimus_cfg.py.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to create player in the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """
    loadout = {'id': player_id}
    loadout.update(c['player']['initial_loadout'])
    create_player = db_api_query.insert(table_schema, loadout)

    return [(create_player, 'affected')]
