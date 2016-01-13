#!/usr/bin/python2
"""Mimus mock client"""
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
# Limitations:
# - In production, you'd have the client cache all necessary data (player &
#   cards), make decisions based on it's local cache, and count on the server
#   to validate.  In this mock client and minimum viable server, the layer of
#   validation at the server doesn't exist, as it is unnecessary for peformance
#   testing.
#
# pylint: disable=invalid-name,line-too-long,star-args
from __future__ import with_statement
from logging.handlers import RotatingFileHandler
from functools import partial
from random import randint, choice, sample
import sys
import os
import logging
import time
import optparse
import binascii
import socket

# Custom modules
from mimus_cfg import cfg
from timer import Timer
import mimus_server

def name_to_id(player_name):
    """convert player name to id"""
    # This is fairly unsophisticated, just does a CRC32 on the name.  Can be
    # optimized both for compute requirements and collision frequency using
    # another hashing algorithm.
    return binascii.crc32(player_name) & 0xFFFFFFFF


def attributes(c):
    """
    Return if this card is levelable or evolveable.
    Can be expanded to return other attributes in the future.
    """
    global cfg  # pylint: disable=global-variable-not-assigned
    if int(c['xp01']) >= cfg['card']['xp_limit']:
        return 'evolve'
    else:
        return 'level'


def evaluate_cards(cards):
    """
    Return a dictionary 'card_attrs' of cards sorted by attribute.
    - card_attrs['evolve'] contains all evolvable cards.
    - card_attrs['level'] contains all levelable cards.
    """
    card_attrs = {'evolve': {}, 'level': {}}
    for this_id, c in cards.iteritems():
        a = attributes(c)
        card_attrs[a][this_id] = c
    return card_attrs


def try_server_call(partial_function):
    """
    Simple server call wrapper function.
    - Take a functools.partial object with the function to call and arguments populated.
    - Calls the method, and logs how long the server takes to respond.
    """
    with Timer() as t:
        try:
            results = partial_function()
        except Exception, err:
            logger.critical(
                "User '%s' (id: %s) failed to get response from server.",
                name, name_to_id(name))
            logger.critical("Function call: %s(%s)",
                            str(partial_function.func).split()[2], None)
            logger.critical("Error: %s", repr(err))
            raise  # Debug
            # return None
    logger.debug("%.03f - %s (results: %s)",
                 t.elapsed, str(partial_function.func).split()[2], results)
    return results


def swizzle(data, field):
    """
    Sort items in a dictionary of dictionaries
    Returns a list of dictionary keys sorted by ascending value of 'field'
    """
    sorted_array = []
    working_dict = {}

    # Get all the keys sorted into buckets by the field value
    for k, v in data.iteritems():
        if not v[field] in working_dict:
            working_dict[v[field]] = []
        working_dict[v[field]].append(k)

    # Put all the keys into an array sorted by field
    for i in sorted(working_dict.keys()):
        # For multiple keys with the same value, put them into the array in
        # random order.
        if len(working_dict[i]) > 1:
            for j in sample(working_dict[i], len(working_dict[i])):
                sorted_array.append(j)
        else:
            # only one item in this array, flatten it.
            sorted_array.append(working_dict[i][0])
    return sorted_array


def can_play_stage(stamina, free_slots):
    """
    Test to see if the player can play a stage.
    (Simulate a player looking at their stamina and free slots to
    determine if they can play a stage.  For sake of simplicity, assume
    players don't want to play a stage if they don't have enough free
    slots to hold all potential drops.
    """
    if free_slots >= 5 and stamina > 0:
        return True
    return False


def remove_rarest_third(cards_by_xp, cards_by_rarity):
    """
    Builds card lists that are needed to determine which cards are leveled or evolved.
    - 'cards_by_xp' -
    """
    cards_by_rarity.reverse()
    top_third = cards_by_rarity[:(len(cards_by_rarity) / 3)]
    # Remove the most rare cards from the list of cards by XP
    for j in top_third:
        if j in cards_by_xp:
            cards_by_xp.remove(j)
    return cards_by_xp, cards_by_rarity, top_third


def get_leveling_args(cards, card_attrs):
    """
    Test to see if there are levelable cards in the player's collection.
    (Simulate a player looking in their inventory for cards to level).
    If leveling is possible, return the card we prefer to level, and the
    cards we prefer to consume.
    - Requires player has at least 15 cards.
    - Tends to target a card that has a higher rarity
    - Tends to target a card that has more XP (NYI)
    - Tends to consume cards with little XP
    """
    if (len(card_attrs['evolve']) < len(card_attrs['level']) and
            len(cards) > 15):
        cards_to_consume = set()
        candidates = set(card_attrs['level'].keys())
        cards_by_xp = list(set(swizzle(cards, 'xp01')) & candidates)
        cards_by_rarity = list(set(swizzle(cards, 'type')) & candidates)
        cards_by_xp, cards_by_rarity, top_third = remove_rarest_third(
            cards_by_xp, cards_by_rarity)

        if cards_by_xp and top_third:
            # Number of cards to consume into our destination card will be between
            # min and max values (defined in config).
            num_to_consume = randint(
                cfg['level']['min_cards'],
                min(cfg['level']['max_cards'], len(top_third)))

            # Get the bottom n number of cards by xp to consume into a rare card
            lesser = min(num_to_consume, len(cards_by_xp))
            for i in range(lesser):  # pylint: disable=unused-variable
                cur_card = cards_by_xp.pop(0)
                if cur_card in cards_by_rarity:
                    cards_by_rarity.remove(cur_card)
                if cur_card not in cards_to_consume:
                    cards_to_consume.add(cur_card)

            logger.debug("Cards to consume:")
            logger.debug(cards_to_consume)

            # Choose one of the more rare cards as the target to level.
            # TODO: prefer rare cards with more xp pylint: disable=fixme
            dest_id = choice(top_third)

            return (dest_id, cards_to_consume)

    return False


def get_evolving_args(cards, card_attrs):
    """
    Simulate a player consuming cards to 'evolve' a target card.
    - This assumes the player has at least 15 cards.
    - Tends to target a card that has a higher rarity
    - Tends to target a card that has more XP (NYI)
    - Tends to consume cards with little XP
    - Tends to consume cards which common (lower 'type' number)
    """
    if len(card_attrs['evolve']) and len(cards) >= 15:
        try:
            # Get top 1/3 of evolvable cards, sorted rare to common.  Card to evolve
            # will be selected from these, and none of these will be consumed.
            top_candidates = swizzle(card_attrs['evolve'], 'type')
            top_candidates.reverse()
            top_candidates = top_candidates[:(len(top_candidates) / 3)]
            # Select the target card to evolve.
            dest_id = choice(top_candidates)
        except IndexError:
            # Not enough candidates to evolve.
            return False

        cards_to_consume = set()
        # Get lists of cards to potentially consume, with all candidates removed
        cards_by_xp = list(set(swizzle(cards, 'xp01')) - set(top_candidates))
        cards_by_rarity = list(set(swizzle(cards, 'type')) - set(
            top_candidates))
        cards_by_xp_less_rares = remove_rarest_third(cards_by_xp,
                                                   cards_by_rarity)[0]

        # Make sure that we still have enough cards to evolve
        if cards_by_xp_less_rares:
            num_to_consume = randint(
                cfg['level']['min_cards'],
                min(cfg['level']['max_cards'], len(cards_by_xp_less_rares)))

            # Get the n number of cards to consume, starting with the lowest XP card
            # in the collection and working our way up
            for i in range(num_to_consume):  # pylint: disable=unused-variable
                cards_to_consume.add(cards_by_xp_less_rares.pop(0))

            logger.debug("Cards to consume:")
            logger.debug(cards_to_consume)

            return (dest_id, cards_to_consume)

    return False

def run():
    """Main function"""

    # Request session on the server
    session = mimus_server.Session(name_to_id(name))

    # Set player stamina. For simplicity, always simulate player starting
    # the session with full stamina.
    stamina = session.player['stamina']
    # DEBUG - Don't play any stages.
    #stamina = 0

    logger.debug("Retrieved Player info %s", str(session.player))

    # Main loop.
    while True:
        # Var init
        action = None
        results = None
        result = "Successful"  # Assume success.
        free_slots = session.player['slots'] - len(session.cards)

        # Player action logic
        if can_play_stage(stamina, free_slots):
            action = 'stage'
            # Leverage functools.partial to set up the method we want to call.
            server_method = partial(session.play_stage)
            stamina = stamina - 1
        else:
            card_attrs = evaluate_cards(session.cards)

            # Check to see if we can perform an action on a card.
            leveling_args = get_leveling_args(session.cards, card_attrs)
            if leveling_args:
                # If there are more cards that can be leveled than evolved, favor leveling.
                action = 'level'
                # Leverage functools.partial to set up the method we want to call.
                server_method = partial(session.level_card, *leveling_args)
            else:
                # See if we can evolve a card.
                evolving_args = get_evolving_args(session.cards, card_attrs)
                if evolving_args:
                    action = 'evolve'
                    # Leverage functools.partial to set up the method we want to call.
                    server_method = partial(session.evolve_card, *evolving_args)
                elif stamina == 0:
                    # No cards to evolve or level, and out of stamina.  End main loop.
                    break

        # Take the action
        if action:
            logger.debug(" Attempting action: %s", action)
            with Timer() as server_results_timer:
                results = try_server_call(server_method)
            # Print results
            if not results:
                result = "FAILED"

            logger.info("%10s action %6s (%d/%d stamina remaining)",
                        result, action, stamina, session.player['stamina'])
            # Sleep for the proscribed time, minus how long we've already waited for
            # the server to return results.
            # This is to simulate something client-side that takes time (animations, gameplay, etc)
            required_wait_time = randint(cfg[action]['min_time'],
                                         cfg[action]['max_time'])
            if result == "FAILED":
                required_wait_time = cfg[action]['fail_time']
            additional_sleep_time = required_wait_time - server_results_timer.elapsed

            if additional_sleep_time > 0:
                logger.debug("---(sleeping for %5.02f/%5.02f)---",
                             additional_sleep_time, required_wait_time)
                time.sleep(additional_sleep_time)
        else:
            # No action determined! Just sleep. (Shouldn't happen unless debugging)
            time.sleep(1)
    logger.info("Stamina exhausted.  Exiting.")

# Run main loop.
if __name__ == "__main__":

    # Parse input options
    parser = optparse.OptionParser()
    parser.add_option('-f',
                      '--file-logging',
                      help='turn on logging to log/* files (default:off)',
                      dest='flogging',
                      default=False,
                      action='store_true')
    parser.add_option('-d',
                      '--debug',
                      help='lower miminum logging level to DEBUG (default:off)',
                      dest='debug',
                      default=False,
                      action='store_true')
    parser.add_option('-q',
                      '--quietish',
                      help='raise minimum logging level to WARNING, and use a shorter format (default:off)',
                      dest='quiet',
                      default=False,
                      action='store_true')
    (options, args) = parser.parse_args()
    name = args[0]

    # Set up logging to stdout.
    logname = "mimus"
    player_id = "%s.%s" % (name, name_to_id(name))
    logid = "%s:%s" % (socket.gethostname(), player_id)
    logger = logging.getLogger(logname)
    cl_format = ('%40s' % logid) + ' - %(name)-15s - %(message)s'
    stdout_format = logging.Formatter('%(asctime)s - %(levelname)8s ' + cl_format)
    quiet_format = logging.Formatter(('%18s' % player_id) +
                                     ' - %(name)-15s - %(message)s')
    if options.quiet:
        stdout_format = quiet_format
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(stdout_format)
    logger.addHandler(handler)

    # Set up logging to per-client files
    if options.flogging:
        MB = 1024 * 1024
        file_formatter = quiet_format
        if not os.path.exists('logs'):
            os.makedirs('logs')
        file_handler = RotatingFileHandler('logs/%s' % player_id,
                                           mode='a+',
                                           maxBytes=10 * MB,
                                           backupCount=1)
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

    logger.setLevel(logging.INFO)
    logger.info("Initializing Logging...")

    # Enable debug logging
    if options.debug:
        handler.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.debug(" Debug logging enabled!!")

    # Disable all but warning logging.
    if options.quiet:
        logger.info(
            " Quiet(-ish) logging selected, only warning or above will be logged to stdout.")
        handler.setLevel(logging.WARNING)

    run()
