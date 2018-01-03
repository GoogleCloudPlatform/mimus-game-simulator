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
"""Server API."""
from __future__ import with_statement
from pprint import pformat
from redis import StrictRedis
from gcloud import pubsub
import uuid
import logging
import random

# Set up logging.
logname = 'mimus.server'
logger = logging.getLogger(logname)

# Custom modules
import db_api.objects.card as card
import db_api.objects.player as player
import db_api.enqueue as enqueue
from mimus_cfg import cfg


# Game 'session' object.  One per player.
class Session(object):
    """Object represention of this player's game session.

    General strategy of each of the public methods:
      - Validate the client can take the requested action (if necessary)
      - Make a transaction (list of DB API queries) that should all run to
        completion to update the database and get the latest results of the action
      - Generate a unique transaction ID.  Currently only used by the DB API to
        track completion of the transaction, but in the future could also be used
        by the server to look up past results or track reasons for transaction failure.
      - Run the transaction through the private '__execute_db_transaction' function,
        which queues the queries and parses results into the object attributes as
        necessary
      - Return result to the client.

    Attributes:
       log: log file handle.
       cfg: configuration dictionary (typically read from mimus_cfg.py)
       session_id: an alias for player_id.
       workq: Google Cloud Pub/Sub topic to place db work into.
       redis: Redis connection to read db results from.
       player: Local cache copy of the player row from the db.
       cards: Local cache copy of the player's cards from the db.
    """

    def __init__(self, player_id):
        """Initialize session object.

        Sets up DB API connections to Redis and Pub/Sub for this session, and
        does initial fetch of player and cards from the database.

        Args:
            player_id: Hashed player name.
        """
        # Logging and configuration
        self.log = open('backend_issues.log', 'a+')
        self.cfg = cfg
        self.session_id = player_id

        # Connect to DB API Cloud Pub/Sub and Redis
        logger.info("Connecting to DB API...")
        client = pubsub.Client(project=self.cfg['gcp']['project'])
        logger.info("Connecting: DB API pubsub topic '%s'",
                    self.cfg['pubsub']['topic'])
        self.workq = client.topic(self.cfg['pubsub']['topic'])
        logger.info("Connecting: DB API Redis instance at '%s:%s'",
            self.cfg['redis_con']['hostname'], self.cfg['redis_con']['port'])
        self.redis = StrictRedis(host=self.cfg['redis_con']['hostname'],
                                 port=self.cfg['redis_con']['port'],
                                 db=self.cfg['redis_con']['db'],
                                 password=self.cfg['redis_con']['password'])

        # Initialize attributes to empty
        self.player = None
        self.cards = {}

        # Attempt to get initial attribute values from DB
        self._get_player(player_id)
        self._get_cards()

    def _execute_db_transaction(self, trans_id, transaction):
        """Runs a prepared transaction against the database.

        Attempts to update session object attributes (self.player, self.cards,
        etc) with the results.

        Args:
            trans_id: The transaction ID.
            transaction: The list of queries that make up this transaction.

        Returns:
            If the transaction succeeds: number of rows affected.
            If the transaction fails: boolean value False.
            Doesn't explicitly return results; if successful the updated results
                are available in the object's attributes.
        """
        logger.debug(pformat(transaction))
        # Execute against db
        data = enqueue.execute_batch(trans_id=trans_id,
                                     queries=transaction,
                                     worker_q=self.workq,
                                     ack_redis=self.redis,
                                     srv_id=self.session_id,
                                     log=self.log)
        # Look through the results for updates to the session.cards or session.player
        if data:
            if 'cardlist' in data and data['cardlist']:
                self.cards = {card['id']: card for card in data['cardlist']}
            if 'player' in data and data['player']:
                self.player = data['player'][0]
            return data['affected']

        # Explicitly return false if no data was returned from the database -
        # something went wrong.
        return False

    def _get_player(self, player_id):
        """Build and execute DB API transaction to retrieve the player row.

        If player doesn't exist, build and execute DB API transaction to
        initialize the player row and then retrieve it.

        Args:
            player_id: Hashed player name.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest player stats; if successful the
                updated results are available in the object's attributes.

        Raises:
            RuntimeError: There was an issue retrieving the player's row.
        """
        logger.info("Getting Player!")

        # Var init
        self.player = None
        trans_id = str(uuid.uuid4())

        transaction = player.get(player_id)
        # Since a query that returns no rows will return a 0, explicitly check for
        # the False keyword value
        if self._execute_db_transaction(trans_id, transaction) is not False:
            logger.debug("Printing player! %s", self.player)
            if self.player:
                return True
            else:
                # There was nothing to fetch from the db, need to make this player
                trans_id = str(uuid.uuid4())  # Generate a new transaction ID
                transaction = player.create(player_id, cfg)

                # Get queries to make the specified number of each kind of card,
                # defined in the config file.
                initial_cards = self.cfg['player']['initial_cards']
                for loot_type in initial_cards:
                    for i in range(initial_cards[loot_type]):  # pylint: disable=unused-variable
                        card_type = random.randint(
                            self.cfg['loot_tables'][loot_type]['min'],
                            self.cfg['loot_tables'][loot_type]['max'])
                        transaction.extend(card.create(player_id, card_type))

                logger.info("Creating initial cards for player '%d'",
                            player_id)

                # Create player, create n cards.  _get_cards is called immediately
                # after, so no need to get cards yet.
                transaction.extend(player.get(player_id))
                return self._execute_db_transaction(trans_id, transaction)
        else:
            raise RuntimeError(
                "Unable to retrieve player %s from the database!" % player_id)

    def _get_cards(self):
        """Build and execute DB API transaction to retrieve latest player
            cardlist.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest cardlist; if successful the
                updated results are available in the object's attributes.

        Raises:
            RuntimeError: There was an issue retrieving the cards from the db.
        """
        trans_id = str(uuid.uuid4())
        transaction = card.get_all(self.player['id'])
        results = self._execute_db_transaction(trans_id, transaction)
        # Since a query that returns no rows will return a 0, explicitly check for
        # the False keyword value
        if results is False:
            raise RuntimeError(
                "Unable to retreive cards for player %s from the database!" %
                self.player['id'])
        return results

    def level_card(self, dest_id, cards_to_consume):
        """Build and execute DB API transaction to combine cards
            and retrieve latest player cardlist.

        Args:
            Same as those of card.combine(), which this method calls.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest cardlist; if successful the
                updated results are available in the object's attributes.

        Raises:
            RuntimeError: There was an issue with the database transaction
                required to level the card.
        """
        transaction = card.combine(self.cards[dest_id], cards_to_consume)
        transaction.extend(card.get_all(self.player['id']))
        trans_id = str(uuid.uuid4())
        results = self._execute_db_transaction(trans_id, transaction)
        # Since a database transaction that returns no rows will return a 0,
        # explicitly check for the False keyword value
        if results is False:
            raise RuntimeError("Unable to combine cards for player %s!" %
                               self.player['id'])

        # Otherwise, everything looks successful
        logger.info("Leveled cardID %d by consuming %d cards",
                    dest_id, len(cards_to_consume))
        return results

    def evolve_card(self, dest_id, cards_to_consume):
        """Build and execute DB API transaction to combine cards
            and retrieve latest player cardlist.

        Args:
            Same as those of card.evolve(), which this method calls.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest cardlist; if successful the
                updated results are available in the object's attributes.

        Raises:
            RuntimeError: There was an issue with the database transaction
                required to evolve the card.
        """
        transaction = card.evolve(self.cards[dest_id], cards_to_consume)
        transaction.extend(card.get_all(self.player['id']))
        trans_id = str(uuid.uuid4())
        results = self._execute_db_transaction(trans_id, transaction)
        # Since a query that returns no rows will return a 0, explicitly check for
        # the False keyword value
        if results is False:
            raise RuntimeError("Unable to evolve cards for player %s!" %
                               player['id'])

        # Otherwise, everything looks successful
        logger.info("Evolved cardID %d by consuming %d cards",
                    dest_id, len(cards_to_consume))
        return results

    def play_stage(self):
        """Build and execute DB API transaction to simulate player playing a stage.

        Note: Stamina is not currently validated.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest cardlist/player stats;
                if successful the updated results are available in the
                object's attributes.

        Raises:
            RuntimeError: There was an issue with the database transaction
                required to evolve the card.
        """

        # Obviously this could be a call out to a key/value store to get a constantly
        # updating chance of drops
        loot_table = self.cfg['loot_tables']['std']  # Standard loot table
        num_rounds = 5  # rounds in this level

        transaction = []
        # Test to see if the player failed the stage
        if random.random() <= self.cfg['stage']['failure_chance']:

            # Roll for card drops
            for i in range(num_rounds):
                if (len(self.cards) + len(transaction)) < self.player['slots']:
                    #logger.debug(" Playing round %d" % i)
                    card_type = None
                    # Roll d100
                    roll = random.random()
                    if roll <= loot_table['drop_chance']:
                        # This can be replaced with a more advanced probabilistic function, just
                        # random for now
                        card_type = random.randint(loot_table['min'],
                                                   loot_table['max'])
                        transaction.extend(card.create(self.player['id'],
                                                       card_type))
                    loot_msg = " Round %2d: Rolled %.2f/%.2f for player %d, dropped card %s"
                    logger.info(loot_msg, i, roll, loot_table['drop_chance'],
                                            self.player['id'], str(card_type))
                else:
                    full_msg = "****Player (%d) doesn't have any more slots! Discarding remaining drops..."
                    logger.warning(full_msg, self.player['id'])
                    break
            logger.info(" Player completed stage - %2d loot cards acquired.",
                        len(transaction))

            # Assume player took a friend along, give them friend points
            updated_player = self.player.copy()
            updated_player['points'] = self.player['points'] + self.cfg[
                'stage']['points_per_run']
            # Test that query generation is successful. Necessary as query generation
            # will fail if, for example, the player already has max friend points
            update_player_query = player.update(updated_player)
            if update_player_query:
                transaction.extend(update_player_query)
            else:
                logger.error(
                    "Unable to update player! (continuing without update!)")

            # After updates, get the latest player/cardlist
            transaction.extend(player.get(self.player['id']))
            transaction.extend(card.get_all(self.player['id']))

            # Run transaction
            trans_id = str(uuid.uuid4())
            results = self._execute_db_transaction(trans_id, transaction)

            # Since a query that returns no rows will return a 0, explicitly check for
            # the False keyword value
            if results is False:
                raise RuntimeError("Unable to Play Stage for player %s!" %
                                   self.player['id'])
            return results
        else:
            logger.info("  Player failed stage!")
            return False

    def add_slots(self, num_slots):
        """Build and execute DB API transaction to add slots to a player.

        If player doesn't exist, build and execute DB API transaction to
        initialize the player row and then retrieve it.

        Args:
            player_id: Hashed player name.

        Returns:
            If successful: boolean True or a positive integer indicating the number
                of rows affected.
            If unsuccessful: boolean False or a zero-value integer.
            Note: Doesn't explicitly return latest player stats; if successful the
                updated results are available in the object's attributes.

        Raises:
            RuntimeError: There was an issue updating or retrieving the player's
                row.
        """

        transaction = []
        # Test that query generation is successful. Necessary as query generation
        # will fail if, for example, the player already has max slots
        updated_player = self.player
        updated_player['slots'] = self.player['slots'] + num_slots
        update_player_query = player.update(updated_player)
        if update_player_query:
            transaction.extend(update_player_query)
        else:
            logger.error(
                "Unable to update player! (continuing without update!)")

        # Get player after updating slots
        transaction.extend(player.get(self.player['id']))

        # Run transaction
        trans_id = str(uuid.uuid4())
        results = self._execute_db_transaction(trans_id, transaction)
        # Since a query that returns no rows will return a 0, explicitly check for
        # the False keyword value
        if results is False:
            raise RuntimeError("Unable to add slots to player %s!" % player['id'])
        return results
