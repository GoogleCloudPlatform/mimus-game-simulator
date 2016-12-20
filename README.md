# Mimus MASS stress testing framework

Mimus is a python 2.7 reference implementation/mock client stress-testing
framework for massively single-player social (MASS) games.  It contains
four major components and two executables.

This sample source code was used to arrive at the load numbers 
recommended in 
https://cloud.google.com/solutions/mobile/mobile-game-backend-cloud-sql and
is provided for reference.  It is not intended as a production service 
or tutorial.

The current release only comes with a MySQL backend implementation.

## Prerequisites

Both executables require the following python modules be installed:
 - google-api-python-client
 - gcloud
 - redis
 - retrying

In addition, the DB worker requires this python module to be installed:
 - MySQLdb

> **Note**: Current implementation assumes that everything is running on Google
> Compute Engine or Google Container Engine instances with the following scopes
> (in addition to the default scopes):
 - https://www.googleapis.com/auth/pubsub
 - https://www.googleapis.com/auth/sqlservice.admin

## Components

### Mimus mock client

This script simulates a player playing a MASS client app.
Simulated player actions include:

* Playing a round of the game
* Leveling or evolving units
* Buying currency (NYI)
* Spending currency (NYI)

#### Running

Configuration is loaded from `mimus_cfg.py`. Descriptions of the configuration
options are inline in the config file.

Provided the correct python modules are installed, the Mimus client can be run
with `python mimus_client.py <player_name>`.  It will exit if there is no DB worker
process running to service its requests (after the timeout defined in the config file).

### Mimus server

The server is implemented as a module used by the client (in a production
game, it would be run as a separate process and would communicate with the
client using a REST or RPC protocol).  It stores a local copy of the player state
and sends requests to the DB API to store data permanently in the backend as
necessary.

### DB API

This collection of modules provides an service interface for the Mimus server
to request permanent data storage, without having to deal with the
implementation details of the underlying storage engine. It queues requests
using Cloud Pub/Sub, and waits for the results to show up in Redis. It
then returns those results to the server.

### DB worker

The DB worker process is an endless loop that polls the Cloud Pub/Sub topic and
runs queries it receives against the database.  It puts the results in Redis.

#### Running

Application configuration is loaded from `mimus_cfg.py` and database connection
configuration is loaded from `db_config.py`.

Provided the correct python modules are installed, the worker can be run
with `python ./db_worker.py`.  By default, it logs to `db_worker.log`.


## Deployment

> **Note**: It is HIGHLY recommended that all systems running a single Mimus
> simulation all have the same system time (use of NTP is recommended).  If they
> are not, message processing errors can occur!

Both applications (`mimus_client.py` and `db_worker.py`) can be deployed on a
single machine.  It is recommended that a container orchestration tool is used
if attempting to do a full-scale stress test. Example `Dockerfile`s for the two
executables are included in the `docker` directory.

Before running either application for the first time, you should have stood up a
Redis instance and the MySQL database, populated the connection details into the
appropriate config file or environment variables, and installed the prerequisite
python modules listed above.

### MySQL setup

It is recommended that you use a non-privledged MySQL user for this application.
Example SQL to create a non-privledged MySQL user can be found in `db_api/create_dbuser.sql`
- It is suggested that you choose a new password.  The credentials you create
should then be put in the `db_config.py`.

### Redis setup

A Redis instance must be running and reachable from the hosts running both the
mimus client and the database worker.
- The Redis hostname and port can either be specified explicitly in the config file, or by using the environment variables specified in the config file.
- Mimus assumes you have [AUTH set up on your Redis instance](http://redis.io/commands/AUTH). Be sure to update the password in `mimus_cfg.py`.
