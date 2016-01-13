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
"""Config dictionary for mimus."""
import os
cfg = c = {}

# GCP Auth parameters
c['gcp'] = {}
c['gcp']['project'] = os.getenv('GCP_PROJECT', 'joeholley-mimus01')

# db api parameters (for choosing different db backends)
c['db_api'] = {}
c['db_api']['dir'] = 'db_api'

# db connection parameters
c['db_con'] = {}
c['db_con']['timeout'] = 30

# DB API Cloud Pub/Sub connection parameters
c['pubsub'] = {}
c['pubsub']['topic'] = os.getenv('DB_WORKER_TOPIC', 'queriestoprocess')
c['pubsub']['sub'] = os.getenv('DB_WORKER_SUB', 'dbworkersub')

# DB API Redis connection parameters
c['redis_con'] = {}
c['redis_con']['db'] = 0
# Docker env var format: REDIS_PORT=tcp://172.17.0.2:6379
redis_host, redis_port = os.getenv(
    'REDIS_PORT', 'tcp://mimus-redis:6379').split('/')[-1].split(':')
c['redis_con']['hostname'] = redis_host
c['redis_con']['port'] = int(redis_port)
c['redis_con']['password'] = '9dc1b3ae-584c-434e-b899-da2c8ad093fb'

# Player parameters
c['player'] = {}
c['player']['initial_cards'] = {}
c['player']['initial_cards']['std'] = 5
c['player']['initial_cards']['stone'] = 1
c['player']['initial_loadout'] = {}
c['player']['initial_loadout']['stones'] = 5
c['player']['initial_loadout']['points'] = 1000
c['player']['initial_loadout']['slots'] = 50
c['player']['initial_loadout']['stamina'] = 5

# Card (unit) parameters
c['card'] = {}
c['card']['xp_limit'] = 10000  # Max XP for a unit

# Parameters for evolving ('consuming') cards
c['evolve'] = {}
c['evolve']['min_time'] = 3  # min time this action will take
c['evolve']['max_time'] = 3  # max time this action will take
c['evolve']['fail_time'] = 3  # time this action takes if it fails
c['evolve']['min_cards'] = 2  # min number of cards consumed
c['evolve']['max_cards'] = 5  # max number of cards consumed

# Parameters for leveling ('combining') cards
c['level'] = {}
c['level']['min_time'] = 3  # min time this action will take
c['level']['max_time'] = 3  # max time this action will take
c['level']['fail_time'] = 3  # time this action takes if it fails
c['level']['min_cards'] = 1  # min number of cards consumed
c['level']['max_cards'] = 5  # max number of cards consumed

# Stage parameters
c['stage'] = {}
c['stage']['min_time'] = 30  # min time this action will take
c['stage']['max_time'] = 90  # max time this action will take
c['stage']['fail_time'] = 30  # time this action takes if it fails
c['stage']['failure_chance'] = 0.90  # chance to simulate player failing stage
c['stage']['points_per_run'] = 10  # friends points earned per stage played

# Loot tables
c['loot_tables'] = {}
c['loot_tables']['std'] = {'drop_chance': 0.35, 'min': 1, 'max': 500}
c['loot_tables']['point'] = {'drop_chance': 1.00, 'min': 1, 'max': 750}
c['loot_tables']['stone'] = {'drop_chance': 1.00, 'min': 500, 'max': 1000}
