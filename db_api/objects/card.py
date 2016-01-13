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
# pylint: disable=line-too-long,invalid-name
"""Schema definition/access methods for the card object's table."""
from __future__ import with_statement
from collections import OrderedDict
import logging

# Custom modules
import db_api.statement_generator as db_api_query

# DEBUGGING
cardlogger = logging.getLogger('mimus.card')
cardlogger.setLevel(logging.INFO)

# Schema for the card table in the inventory db
table_schema = {
    'name': 'card',
    'indexed_fields': ['ownerid'],
    'primary_key': 'id',
    'schema': OrderedDict(
        [('id', 'INT'), ('ownerid', 'INT'), ('type', 'MEDIUMINT'),
         ('stones', 'TINYINT'), ('points', 'TINYINT'), ('evolves', 'INT'),
         ('levels', 'INT'), ('xp01', 'MEDIUMINT'), ('xp02', 'MEDIUMINT')])
}


def get_all(player_id):
    """Return query to list all cards owned by a player

    Args:
        player_id: Hashed player name.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to get cards from the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """
    get_cards_query = db_api_query.select(table_schema,
                                          values=[player_id, ],
                                          field='ownerid')
    return [(get_cards_query, 'cardlist'), ]


def combine(dest, card_ids):
    """Return query to combine cards.

    Args:
        dest: Dictionary of destination card key/value pairs.
        card_ids: List of ids of the cards to combine.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to update/retrieve cards from the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """

    # Var init
    queries_to_execute = []
    value = 100
    xp = 0

    # Loop through cards to consume, generate queries to remove their owners
    cardlogger.debug("Consuming ids: " + str(card_ids))
    for key in card_ids:
        query = db_api_query.update(table_schema, key, {'ownerid': 0,
                                                       'levels': dest['id']})
        queries_to_execute.append((query, 'affected'))
        # pylint: disable=fixme
        # TODO: determine XP granted by level of consumed card instead of using a
        # flat amount
        xp += value

    # add the XP into the destination card
    query = db_api_query.update(table_schema, dest['id'],
                                {'xp01': xp + dest['xp01']})
    queries_to_execute.append((query, 'affected'))
    return queries_to_execute


def evolve(dest, card_ids):
    """Return query to evolve a card.

    Args:
        dest: Dictionary of destination card key/value pairs.
        card_ids: List of ids of the cards to combine.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to update/retrieve cards from the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """

    # Var init
    queries_to_execute = []

    # Loop through cards to consume, generate queries to remove their owners
    cardlogger.debug("Consuming ids: " + str(card_ids))
    for key in card_ids:
        query = db_api_query.update(table_schema, key, {'ownerid': 0,
                                                       'evolves': dest['id']})
        queries_to_execute.append((query, 'affected'))

# 'Evolve' the destination card by changing it card type to a rarer one.
    query = db_api_query.update(table_schema, dest['id'],
                                {'type': dest['type'] + 1,
                                 'xp01': 0})
    queries_to_execute.append((query, 'affected'))
    return queries_to_execute


def create(player_id, card_type, cost_type=None, cost_amount=None):
    """Return query to create a specific card and give it to the player.

    Args:
        player_id: Hashed player name.
        card_type: Integer for the type of card to make.
        cost_type: (optional) What kind of currency was used to purchase
            this card. If none (the default), the card was a drop.
        cost_amount: (optional) The amount of the cost_type spent on this
            card.

    Returns:
        queries_to_execute: List of (query_string, results_key) pairs.
            query_string: Query to get cards from the database.
            results_key: Dictionary key under which to look for the results
                of this query.
    """

    # Make the card to create
    card = {
        'type': card_type,
        'ownerid': player_id,
    }
    if cost_type:
        # If a cost was provided, add it to the card (cards without cost were
        # drops or gifts)
        card[cost_type] = cost_amount

    # Create the card
    create_card = db_api_query.insert(table_schema, card)
    return [(create_card, 'affected'), ]
