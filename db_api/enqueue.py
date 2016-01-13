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
"""Module for enqueuing a database transaction and waiting for it to complete."""
from __future__ import with_statement
from retrying import retry
import logging
import time

# Custom modules
from db_api.timer import Timer

# DEBUGGING
nh = logging.NullHandler()
dblogger = logging.getLogger('mimus.enqueue')
dblogger.addHandler(nh)
dblogger.setLevel(logging.INFO)

# Import json module
try:
    import simplejson as json
    dblogger.info("Using simplejson")
except ImportError:
    import json
    dblogger.warning("Falling back to json module")

# Cut down logging level.
#dblogger.setLevel(logging.WARN)


@retry(stop_max_delay=30000,
       wait_exponential_multiplier=100,
       wait_exponential_max=2500)
def _check_for_ack(ack_redis, ack_id):
    """Loop and wait for results from the batch to show up in redis.

    Args:
        ack_redis: Redis connection to query for results.
        ack_id: Redis key under which the results will appear

    Returns:
       acked: boolean value true if the results were found before the timeout.
       results: Dictionary of the results and result metadata.
    """

    acked = False
    dblogger.debug("Looking in redis for %s", ack_id)
    with Timer() as t:
        while not acked:
            try:
                with Timer() as in_t:
                    results = json.loads(ack_redis.get(ack_id))
                # Explanation of the timers can be found in the ../db_worker.py file.
                results['timers']['802 redis ack'] = time.time() - results[
                    'timers']['802 redis ack']
                results['timers']['900 ===TOTAL==='] = time.time() - results[
                    'timers']['900 ===TOTAL===']
                acked = True
            except TypeError, e:
                # Json module can't load the string if the redis query returned nothing.
                # In this case, our transaction isn't done yet.  Raise exception and let
                # the retry library try again
                dblogger.debug("Unable to find %s key in redis, checking again",
                               ack_id)
                raise KeyError(
                    "%s\nUnable to find %s key in redis, ran out of retries" %
                    (repr(e), ack_id))
    if acked and results:
        if t.elapsed > 0.01 or in_t.elapsed > 0.01:
            dblogger.info("%.03f/%.03f redis_pull/ack_search",
                          t.elapsed, in_t.elapsed)
    return acked, results


# pylint: disable=too-many-arguments,too-many-locals
def execute_batch(trans_id, queries, worker_q, ack_redis, srv_id, log):
    """Enqueue batch of db queries to be processed by the db worker processes.
    Wait for it to complete and return the results.

    Args:
        trans_id: Transaction ID for this batch.
        queries: List of queries that make up the batch.
        worker_q: Google Cloud Pub/Sub topic to publish batches to.
        ack_redis: Redis instance to query for batch results.
        srv_id: Unique ID for the originating server instance.
        log: slow query log file handle.

    Returns:
        results: Dictionary of database query results and metadata.
    """
    warning_thresh = 10
    redis_key = '%s:%s' % (srv_id, trans_id)

    # Start a timer
    with Timer() as t:

        # Prepare queries
        queries_json = json.dumps({'queries': queries})

        # Publish queries to the db worker queue
        with Timer() as in_t:
            worker_q.publish(message=queries_json,
                             srv_id=str(srv_id),
                             trans_id=str(trans_id),
                             insertion_time=str(time.time()))

        q_msg = "%.03f - Pubsub Publish" % in_t.elapsed
        if in_t.elapsed > warning_thresh:
            dblogger.warning(q_msg)
            log.write(q_msg + '\n')
        else:
            dblogger.debug(q_msg)

        # Wait for acknowledgement that the work is complete
        acked = False
        ack_timer = time.time()
        while not acked:
            try:
                # Look for this transaction result in the redis instance
                acked, results = _check_for_ack(ack_redis, redis_key)
            except KeyError, e:
                dblogger.warning(repr(e))
                log.write(repr(e))
                return False
        results['timers']['803 ack check'] = time.time() - ack_timer

    # Print timer elapsed
    sql_msg = "%.03f - SQL roundtrip " % t.elapsed
    results['timers']['999 SQL roundtrip'] = t.elapsed
    if results['timers']['999 SQL roundtrip'] > warning_thresh:
        #dblogger.warning(sql_msg)
        #log.write(sql_msg + '\n')
        for k in sorted(results['timers'].keys()):
            # Timers specific to the worker process backend are in parens.
            # Don't print them here from the frontend.
            if not '(' in k:
                i = "%s - %.03f" % (k[4:], results['timers'][k])
                dblogger.warning(i)
                log.write(i)
    else:
        dblogger.debug(sql_msg)
    return results
