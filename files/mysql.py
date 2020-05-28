# mysql
import pymysql
from extras import convert_from_human_to_byte, log, send_mail, handle_soft_limit, handle_less_than_quota_soft


def handle_hard_limit(state, database, config):
    try:
        state[database["database_name"]]["Lock"] = True
        conn = pymysql.connect(
            database=database["database_name"], user=config["main"]["root_user"],
            password=config["main"]["root_user_password"])
        cur = conn.cursor()
        cur.execute("""revoke create on %s.* from %s;""" % (
            database["database_name"], database["database_user"]))
        cur.execute(
            """revoke insert on %s.* from %s;""" % (database["database_name"], database["database_user"]))
        log('warning', 'database %s is now locked' % database["database_name"])
        send_mail(config["main"]["send_mail_from"], database["database_user_mail"],
                  database["quota_soft"], database["quota_hard"],
                  database["database_name"], True, config["main"]["smtp_server"])
    except Exception as e:
        log("error", "Unable to lock Database %s: %s" % (database["database_name"], e))


def handle_less_than_quota_hard(state, database, config):
    try:
        if state[database["database_name"]]["Lock"]:
            state[database["database_name"]]["Lock"] = False
            conn = pymysql.connect(
                database=database["database_name"], user=config["main"]["root_user"],
                password=config["main"]["root_user_password"])
            cur = conn.cursor()
            cur.execute(
                """grant create on %s.* to %s;""" % (database["database_name"], database["database_user"]))
            cur.execute("""grant insert on %s.* to %s;""" % (database["database_name"], database["database_user"]))
            log('info', 'database %s is now unlocked' % database["database_name"])
    except Exception as e:
        log("error", "Unable to unlock Database %s: %s" % (database["database_name"], e))


def mysql(database, state, config):
    try:
        conn = pymysql.connect(
            database=database["database_name"], user=config["main"]["root_user"],
            password=config["main"]["root_user_password"])
        cur = conn.cursor()
        database_size = cur.execute("""SELECT table_schema AS 'DB Name', SUM(data_length + index_length) AS 'DB Size in 
        B' FROM information_schema.tables WHERE table_schema = '%s' GROUP BY table_schema;""" % database[
            "database_name"])
        database_size = cur.fetchall()
        database_size = database_size[0][1]
        state[database["database_name"]]["Size"] = database_size
        database_limit = convert_from_human_to_byte(database["quota_hard"])
        database_soft_limit = convert_from_human_to_byte(database["quota_soft"])
        conn.close()
        if database_soft_limit <= database_size:
            if database_limit <= database_size:
                if not state[database["database_name"]]["Lock"]:
                    handle_hard_limit(state, database, config)
            else:
                handle_soft_limit(state, database, config)
                handle_less_than_quota_hard(state, database, config)
        else:
            handle_less_than_quota_soft(state, database)
            handle_less_than_quota_hard(state, database, config)
    except Exception as e:
        log("error", "Unabel to connect to database %s: %s" % (database["database_name"], e))
