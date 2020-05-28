#!/usr/bin/python3

import time
from postgresql import postgresql
from mysql import mysql
from extras import log, get_file_contents, write_state_dump
from extras import PREFIX

log('info', 'Start quota')
while True:
    config = get_file_contents("quota_conf")
    try:
        f = open(PREFIX + "/state.yml")
    except IOError:
        f = open(PREFIX + "/state.yml", 'a+')
        log('info', 'Create state_file')
        f.close()
    state = get_file_contents("state")
    if not "databases" in config:
        log('error', 'No database found in config')
        break
    if config["databases"] is None:
        log('error', 'No database found in config')
        break
    for database in config["databases"]:
        if state is None:
            state = {database["database_name"]: {'Info': False, 'Lock': False, 'Date': '', 'Size': ''}}
        elif not database["database_name"] in state:
            state[database["database_name"]] = {'Info': False, 'Lock': False, 'Date': '', 'Size': ''}
        write_state_dump(state)
    if config["main"]["database_system"] == "postgresql":
        for database in config["databases"]:
            postgresql(database, state, config)
    elif config["main"]["database_system"] == "mysql":
        for database in config["databases"]:
            mysql(database, state, config)
    write_state_dump(state)
    time.sleep(5)
