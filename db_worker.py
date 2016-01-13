#!/usr/bin/python2
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
# Database worker process.  Basic outline:
#  - Sets up a pubsub client, db connection, and redis connection
#  - Reads lists of queries from a pubsub topic
#  - Runs those queries in order on the database
#   - Each query comes with a 'return_type' string that is used to
#     determine where to put the query's results in the dictionary
#     put in redis.
#  - Stores any query results under the appropriate key in the results
#    dictionary
#  - Acks the pubsub message
#  - Puts the results dictionary in redis under the transaction id
#
# Limitations/NYI:
#  - Currently, it's possible for the db connection to timeout, and this script
#    doesn't attempt to reconnect.
#  - The way timers are done could be cleaned up, they are pretty rough.
#    (Currently using numbers in the keys to preserve order when printing out)
#  - Script currently only processes one message per loop; this isn't a huge
#    limitation as the limiting factor is the time the database query takes
#
# pylint: disable=line-too-long,invalid-name,
"""Database worker process."""

from __future__ import with_statement
from retrying import retry
from redis import StrictRedis
from uuid import uuid4
from gcloud import pubsub
from time import sleep, time
from imp import load_source
from pprint import pformat
import os, sys
import MySQLdb as mysql
import logging.handlers as handlers
import optparse
import logging
import binascii
import warnings

# Custom Modules
# Mimus config is loaded from the file specified on the commandline.
from db_config import db_connect
from db_config import dbc as db_config
from db_api.statement_generator import create_table

#############################
# DB CONNECTION SETUP
# Retry for up to 60 seconds with exponential backoff
@retry(stop_max_delay=10000,
       wait_exponential_multiplier=1000,
       wait_exponential_max=10000)
def connect():
    """wrapper for db_connect that handles retries"""
    logger.debug("DB Connection type: %s", os.getenv('DB_CONNECTION_TYPE',
                                                      'cloudsql_proxy'))
    logger.debug(
        "DB Connection config: %s",
        pformat(db_config))  # Insecure: prints password! Don't use in production!
    if 'path' in db_config:
        mydb = db_config['path']
    else:
        mydb = "%s:%d" % (db_config['host'], db_config['port'])
    logger.info("Attempting to connect to database at %s", mydb)
    con = db_connect()
    logger.info("Connected to %s", mydb)
    return con

def run():  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
    """Main process loop"""
    global timers # pylint: disable=global-statement
    try:
        con = connect()
    except mysql.OperationalError, err:
        # We ran out of retries.  Die.
        logger.error("Failed to connect to the database!")
        logger.error("%s", repr(err))
        sys.exit(1)
    con.autocommit(False)
    with con:

        # Create cursor, verify database exists and use it
        # You can specify db name when making the connection, but it will fail if
        # the db doesn't exist yet.
        cursor = con.cursor(mysql.cursors.DictCursor)
        cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % db_config['name'])
        cursor.execute("USE %s" % db_config['name'])

        # Initialize all DB tables if they don't exist
        for tname in TABLE_NAMES:
            SQL = create_table(tname, cfg)
            cursor.execute(SQL)
            cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
        con.commit()
        # END DB CONNECTION SETUP
        #############################

        # Var init
        time_to_sleep = 0.1
        start_time = time()
        prev_warn = 0
        warning_threshes = {
            'sql': 10,
            'default': 10,
        }

        # Loop & pull
        logger.info(
            "Ready to begin polling pubsub subscription '%s:%s' for messages",
            cfg['pubsub']['topic'], cfg['pubsub']['sub'])
        if not options.verbose:
            logger.info("Logging to file %s", options.log_file)
            logger.removeHandler(verbose_handler)
        else:
            logger.info("Logging to screen instead of file")
            logger.removeHandler(file_handler)

        while True:
            # The timers dictionary is used to store keys with start times and
            # intervals for how long certain portions of the message processing takes.
            # For the sake of being able to easily print the timers in the order that
            # the actions occurred, we pre-pend a number to the action name as the key
            # for the timer dictionary.  Then, when printing the timers, we simply
            # discard the number so it doesn't clutter up the output and we get our
            # timers printed in the order we want.
            #
            # - Timers without parenthesis measure the processing time of the pubsub
            #   message
            # - Timers in parenthesis measure the processing time of the worker thread
            # example:
            #
            # time   | action
            # elapsed| name
            #---------------------------------------------------------------------------
            # 00.017 - q wait
            # 00.939 - (pull wait)
            # 00.000 - json_load
            # 00.038 - INSERT 1224460250 3063853833:9298b7b6-3d20-4f86-a380-c91613882493
            # 00.038 - SELECT 4039320666 3063853833:9298b7b6-3d20-4f86-a380-c91613882493
            # 00.041 - commit
            # 00.006 - ack
            # 00.001 - redis ack
            # 00.370 - ===TOTAL===
            # 01.292 - (===WORKER PROCESSING===)
            #
            # For this message:
            # - The total time from when it entered the pubsub queue until the results
            # were put in redis was 0.37 seconds. This measures the client latency.
            # - The total time the worker spent waiting for message from pubsub to
            # arrive and then processing it was 1.292 seconds. This measures how busy
            # the worker process is.
            #
            # Legend for SQL actions:
            # time   | Query  Query      Transaction ID
            # elasped| type   CRC        (Redis key for the results of this query)
            #---------------------------------------------------------------------------
            # 00.038 - INSERT 1224460250 3063853833:9298b7b6-3d20-4f86-a380-c91613882493
            timers = {}

            timer_start('900 ===TOTAL===')
            timer_start('910 (===WORKER PROCESSING===)')
            timer_start('020 (pull wait)')
            # Pull message, wait if nothing to pull
            recv = None
            try:
                recv = sub.pull(return_immediately=False, max_messages=1)
            except Exception, e: # pylint: disable=broad-except
                recv = None
                logger.error(str(repr(e)))
            timer_stop('020 (pull wait)')

            # Log how long the pull took.
            if recv:

                try:
                    # load json message into a dict for easy access
                    timer_start('050 json_load')
                    ack_id = msg = None
                    ack_id, msg = recv[0]
                    uniq_trans_id = "%s:%s" % (msg.attributes['srv_id'],
                                               msg.attributes['trans_id'])

                    # This timer is done differently because it was started
                    # in the originating process
                    if 'insertion_time' in msg.attributes:
                        timers['900 ===TOTAL==='] = float(msg.attributes[
                            'insertion_time'])
                        timers['010 q wait'] = time() - float(msg.attributes[
                            'insertion_time'])
                        if timers['010 q wait'] > cfg['db_con']['timeout']:
                            # This message is so old, it's client has already considered it
                            # discarded.  Just trash it and log an error.
                            logger.error("%s ack, %d secs old",
                                         uniq_trans_id, timers['010 q wait'])
                            # ack message receipt
                            sub.acknowledge([ack_id, ])
                            continue
                    logger.debug(msg.data)
                    json_data = json.loads(msg.data)
                    timer_stop('050 json_load')

                    results = {'affected': 0}

                    # Get query, and the key under which to return it
                    num = 100
                    for query, return_type in json_data['queries']:
                        try:
                            if not return_type in results:
                                results[return_type] = []
                            query_hash = "%s %s %s %s" % (
                                str(num), query.split()[0],
                                str(binascii.crc32(query) & 0xFFFFFFFF),
                                uniq_trans_id)
                            timer_start(query_hash)
                            warning_threshes[query_hash] = warning_threshes[
                                'sql']
                            logger.debug("Executing '%s'", query)
                            try:
                                cursor.execute(query)
                                logger.debug("Executed '%s'", query)
                                for result in cursor.fetchall():
                                    if result:
                                        # Add to the message directly
                                        results[return_type].append(result)
                            except Exception:
                                raise
                            results['affected'] = results['affected'] + int(
                                cursor.rowcount)
                            logger.debug("query affected %d rows: '%s'",
                                         cursor.rowcount, query)
                            timer_stop(query_hash)
                            num = num + 1
                        except mysql.IntegrityError, err:
                            logger.error("%s", repr(err))
                            logger.error("%s", query)

                    # commit db transaction
                    timer_start('800 commit')
                    con.commit()
                    timer_stop('800 commit')

                    # ack message receipt
                    timer_start('801 ack')
                    sub.acknowledge([ack_id, ])
                    timer_stop('801 ack')

                    # put results in redis
                    timer_start('802 redis ack')
                    # put the timers in results, so the message originator can also access them
                    results['timers'] = timers
                    redis.setex(name=uniq_trans_id,
                                value=json.dumps(results),
                                time=30)
                    timer_stop('802 redis ack')

                    timer_stop('900 ===TOTAL===')
                    timer_stop('910 (===WORKER PROCESSING===)')

                    # log the timers in correct order
                    for tmr in sorted(timers.keys()):
                        tmr_msg = "%06.03f - %s" % (timers[tmr], tmr[4:])
                        if not tmr in warning_threshes:
                            warning_threshes[tmr] = warning_threshes['default']
                        if timers[tmr] > warning_threshes[tmr]:
                            logger.warning(tmr_msg)
                        else:
                            logger.info(tmr_msg)

                except Exception:  # pylint: disable=broad-except
                    logger.error("Unable to process message:")
                    if recv[0][1]:
                        logger.error(recv[0][1].data)
                    logger.error(
                        "Removing message from subscription and continuing...")
                    if ack_id:
                        sub.acknowledge([ack_id, ])
                    # DEBUG
                    #raise

                    # outside of timer block: if we didn't get a message, print how long we waited
            if not recv:
                so_far = time() - start_time
                if (so_far - prev_warn) > warning_threshes['default']:
                    prev_warn = so_far
                    logger.warning(
                        "No msg received from %s:%s in %.03f seconds!",
                        topic.name, sub.name, so_far)
                sleep(time_to_sleep)
            else:
                start_time = time()
                prev_warn = 0


if __name__ == "__main__":

    # Var init
    MB = 1024 * 1024
    worker_id = str(uuid4())[:12]
    logname = "db_worker"
    logid = logname + '.' + worker_id
    ack_queues = {}
    timers = {}

    # Parse input options
    parser = optparse.OptionParser()
    parser.add_option('-v',
                      '--verbose',
                      help='turn on logging to stdout(default:off)',
                      dest='verbose',
                      default=False,
                      action='store_true')
    parser.add_option('-d',
                      '--debug',
                      help='turn on debug output (default:off)',
                      dest='debug',
                      default=False,
                      action='store_true')
    parser.add_option('-f',
                      '--cfg-file',
                      help='specify config file (default: %default)',
                      dest='cfg_file',
                      default='mimus_cfg.py')
    parser.add_option('-l',
                      '--log-file',
                      help='specify log file (default: %default)',
                      dest='log_file',
                      default='%s.log' % logname)
    (options, args) = parser.parse_args()

    # Turn off mysql 'table already exists' warnings
    warnings.filterwarnings('ignore')

    # Set up logging.
    logger = logging.getLogger(logid)
    full_format = logging.Formatter(
        '%(asctime)s - %(levelname)8s - %(name)28s - %(message)s')

    # Logging to file
    file_format = logging.Formatter('%(message)s')
    file_handler = handlers.RotatingFileHandler(options.log_file,
                                                mode='a+',
                                                maxBytes=500 * MB,
                                                backupCount=1)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_format)

    # Logging to console
    verbose_handler = logging.StreamHandler(sys.stdout)  # for containerization
    verbose_handler.setLevel(logging.INFO)
    verbose_handler.setFormatter(full_format)

    # Initialize to INFO logging
    logger.setLevel(logging.INFO)
    logger.addHandler(verbose_handler)
    logger.addHandler(file_handler)
    logger.info("Initializing for worker %s...", worker_id)

    try:
        import simplejson as json
        logger.info("Using simplejson module")
    except ImportError:
        import json
        logger.info("Falling back to json module")

    if options.debug:
        # Switch to DEBUG logging if specified
        file_handler.setLevel(logging.DEBUG)
        verbose_handler.setLevel(logging.DEBUG)
        verbose_handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Load mimus config.
    cfg = load_source('mimus_cfg', options.cfg_file).cfg
    logger.info("Loaded config %s", options.cfg_file)

    # Connect to pubsub
    client = pubsub.Client(project=cfg['gcp']['project'])

    # Get the names of all tables in the db.  All python files in the 'schemas'
    # directory (except for __init__.py) describe tables in the database.
    logger.info("Loading database information")
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    MODULE_FILES = [filename
                    for filename in os.listdir(os.path.join(CURRENT_DIR, cfg[
                        'db_api']['dir'], 'objects'))
                    if filename.endswith('.py') and not filename.startswith('__')]
    TABLE_NAMES = [os.path.splitext(filename)[0] for filename in MODULE_FILES]

    #############################
    # Timer utils
    def timer_start(name):
        """Start timer"""
        global timers # pylint: disable=global-variable-not-assigned
        timers[name] = time()

    def timer_stop(name):
        """Stop timer"""
        global timers # pylint: disable=global-variable-not-assigned
        timers[name] = time() - timers[name]

    #############################
    # CLOUD PUBSUB CONNECTION SETUP
    # Get topic & subscription
    logger.info("Initializing for worker %s...", worker_id)
    topic = client.topic(cfg['pubsub']['topic'])
    sub = topic.subscription(cfg['pubsub']['sub'])
    if not topic.exists():
        topic.create()
    if not sub.exists():
        sub.create()
    logger.info("Connecting to pubsub subscription '%s:%s'...",
                cfg['pubsub']['topic'], cfg['pubsub']['sub'])
    # END CLOUD PUBSUB CONNECTION SETUP
    #############################

    #############################
    # REDIS CONNECTION SETUP
    # Get topic & subscription
    redis = StrictRedis(host=cfg['redis_con']['hostname'],
                        port=cfg['redis_con']['port'], db=cfg['redis_con']['db'],
                        password=cfg['redis_con']['password'])
    logger.info("Connecting to redis instance at '%s:%d'",
                cfg['redis_con']['hostname'], cfg['redis_con']['port'])

    # END REDIS CONNECTION SETUP
    #############################

    if options.debug:
        # Print topic names
        topics, next_page_token = client.list_topics()
        logger.debug([t.name for t in topics])
        # Print sub names
        subs, next_page_token = topic.list_subscriptions()
        logger.debug([s.name for s in subs])

    # Start main loop
    run()
