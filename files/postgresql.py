# postgresql
import psycopg2
from extras import convert_from_human_to_byte, log, send_mail, handle_soft_limit, handle_less_than_quota_soft


def handle_hard_limit(state, database, list_of_schema, list_of_tables, config):
    try:
        state[database["database_name"]]["Lock"] = True
        conn = psycopg2.connect(
            "dbname='%s' user='%s' password='%s'" % (
                database["database_name"], config["main"]["root_user"],
                config["main"]["root_user_password"]))
        cur = conn.cursor()
        cur.execute("""revoke create on database %s from %s;""" % (
            database["database_name"], database["database_user"]))
        for schema in list_of_schema:
            cur.execute(
                """revoke create on schema "%s" from %s;""" % (schema[0], database["database_user"]))
        for table in list_of_tables:
            table_name = table[0].split('".')[0] + '"."' + table[0].split('".')[1] + '"'
            cur.execute(
                """revoke insert on table %s from %s;""" % (table_name, database["database_user"]))
        cur.execute("""select pg_terminate_backend(pid) from pg_stat_activity where pid <> 
                                    pg_backend_pid() and datname = '%s';""" % (database["database_name"]))
        conn.commit()
        conn.close()
        log('warning', 'database %s is now locked' % database["database_name"])
        send_mail(config["main"]["send_mail_from"], database["database_user_mail"],
                  database["quota_soft"], database["quota_hard"],
                  database["database_name"], True, config["main"]["smtp_server"])
    except Exception as e:
        log("error", "Unable to lock database %s: %s" % (database["database_name"], e))


def handle_less_than_quota_hard(state, database, list_of_schema, list_of_tables, config):
    try:
        if state[database["database_name"]]["Lock"]:
            state[database["database_name"]]["Lock"] = False
            conn = psycopg2.connect(
                "dbname='%s' user='%s' password='%s'" % (
                    database["database_name"], config["main"]["root_user"],
                    config["main"]["root_user_password"]))
            cur = conn.cursor()
            cur.execute(
                """grant create on database %s to %s;""" % (database["database_name"], database["database_user"]))
            for schema in list_of_schema:
                cur.execute("""grant create on schema "%s" to %s;""" % (schema[0], database["database_user"]))
            for table in list_of_tables:
                table_name = table[0].split('".')[0] + '"."' + table[0].split('".')[1] + '"'
                cur.execute("""grant insert on table %s to %s;""" % (table_name, database["database_user"]))
            cur.execute("""select pg_terminate_backend(pid) from pg_stat_activity where pid <> 
                                            pg_backend_pid() and datname = '%s';""" % (database["database_name"]))
            conn.commit()
            conn.close()
            log('info', 'database %s is now unlocked' % database["database_name"])
    except Exception as e:
        log("error", "Unable to unlock database %s: %s" % (database["database_name"], e))


def postgresql(database, state, config):
    try:
        conn = psycopg2.connect(
            "dbname='%s' user='%s' password='%s'" % (
                database["database_name"], config["main"]["root_user"],
                config["main"]["root_user_password"]))
        cur = conn.cursor()
        cur.execute("""select pg_database_size('%s');""" % database["database_name"])
        database_size = cur.fetchall()
        database_size = database_size[0][0]
        state[database["database_name"]]["Size"] = database_size
        cur.execute("""select '"'||nspname||'".'||relname from pg_class join pg_namespace on relnamespace = 
                    pg_namespace.oid where relkind='r' and relname !~ '^(pg_|sql_)';""")
        list_of_tables = cur.fetchall()
        cur.execute("""SELECT n.nspname AS "Name" FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND 
                    n.nspname <> 'information_schema' ORDER BY 1;""")
        list_of_schema = cur.fetchall()
        database_limit = convert_from_human_to_byte(database["quota_hard"])
        database_soft_limit = convert_from_human_to_byte(database["quota_soft"])
        conn.close()
        if database_soft_limit <= database_size:
            if database_limit <= database_size:
                if not state[database["database_name"]]["Lock"]:
                    handle_hard_limit(state, database, list_of_schema, list_of_tables, config)

            else:
                handle_soft_limit(state, database, config)
                handle_less_than_quota_hard(state, database, list_of_schema, list_of_tables, config)
        else:
            handle_less_than_quota_soft(state, database)
            handle_less_than_quota_hard(state, database, list_of_schema, list_of_tables, config)
    except Exception as e:
        log("error", "Unable to connect to database %s: %s" % (database["database_name"], e))
