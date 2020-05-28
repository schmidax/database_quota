# extras
import smtplib
import logging
import yaml
import datetime
from config import PREFIX, CONFIG_DIR


def log_dir():
    try:
        with open(CONFIG_DIR + "/quota_conf.yml", "r") as ymlfile:
            config = yaml.load(ymlfile)
            ymlfile.close()
    except:
        log_path = PREFIX + "/database_quota_error.log"
        logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', filename=log_path, level=logging.DEBUG)
        logging.error(CONFIG_DIR + "/quota_conf.yml does not exist")
        raise SystemExit
    try:
        path = config["main"]["log_path"]
    except:
        if config["main"]["database_system"] == "postgresql":
            path = "/var/log/postgresql"
        elif config["main"]["database_system"] == "mysql":
            path = "/var/log/mysql"
        else:
            log("error", "No valid Database_System selected")
            raise SystemExit
    return path


def get_file_contents(filename):
    try:
        if filename is "email_quota_hard":
            with open(CONFIG_DIR + "/email_quota_hard.txt", "r") as mail_file:
                mail = mail_file.read()
                mail_file.close()
                return mail
        elif filename is "email_quota_soft":
            with open(CONFIG_DIR + "/email_quota_soft.txt", "r") as mail_file:
                mail = mail_file.read()
                mail_file.close()
                return mail
        elif filename is "quota_conf":
            with open(CONFIG_DIR + "/quota_conf.yml", "r") as ymlfile:
                config = yaml.load(ymlfile)
                ymlfile.close()
                return config
        elif filename is "state":
            with open(PREFIX + "/state.yml", "r") as statefile:
                state = yaml.load(statefile)
                statefile.close()
                return state
        else:
            log("error", "file not found!")
    except:
        log("error", "%s does not exist!" % filename)
        raise SystemExit


def write_state_dump(current_state):
    try:
        with open(PREFIX + "/state.yml", "w") as statefile:
            yaml.dump(current_state, statefile, default_flow_style=False)
            statefile.close()
    except:
        log("error", PREFIX + "/state.yml does not exist")
        raise SystemExit


def log(log_level, message):
    log_path = log_dir() + "/database_quota.log"
    logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', filename=log_path, level=logging.DEBUG)
    if log_level == 'debug':
        logging.debug(message)
    elif log_level == 'info':
        logging.info(message)
    elif log_level == 'warning':
        logging.warning(message)
    elif log_level == 'error':
        logging.error(message)
    else:
        logging.error("No valid log_level selected")


def convert_from_human_to_byte(size):
    if size[-1] == 'B':
        size = size[:-1]
    if size.isdigit():
        bytes = int(size)
    else:
        bytes = size[:-1]
        unit = size[-1]
        if bytes.isdigit():
            bytes = int(bytes)
            if unit == 'G':
                bytes *= 1073741824
            elif unit == 'M':
                bytes *= 1048576
            elif unit == 'K':
                bytes *= 1024
            else:
                bytes = 0
        else:
            bytes = 0
    return bytes


def send_mail(sender, address, limit_soft, limit_hard, database_name, critical, smtp):
    receivers = []
    for each_address in address.split(", "):
        receivers.append(each_address)
    db_data = {'address': address, 'limit_soft': limit_soft, 'limit_hard': limit_hard, 'database_name': database_name, 'sender': sender}
    if critical:
        mail = get_file_contents("email_quota_hard")
    else:
        mail = get_file_contents("email_quota_soft")

    message = mail.format(**db_data)
    try:
        smtpObj = smtplib.SMTP(smtp)
        smtpObj.sendmail(sender, receivers, message)
    except smtplib.SMTPException:
        log("error", "Unable to send email for database %s" % database_name)


def time_actual():
    return str((datetime.datetime.now()).day) + '-' + str(
        (datetime.datetime.now()).month) + '-' + str((datetime.datetime.now()).year) + '-' + str(
        (datetime.datetime.now()).hour)


def time_tomorrow():
    return str(
        (datetime.datetime.now() + datetime.timedelta(days=1)).day) + '-' + str(
        (datetime.datetime.now() + datetime.timedelta(days=1)).month) + '-' + str(
        (datetime.datetime.now() + datetime.timedelta(days=1)).year) + '-' + str(
        (datetime.datetime.now()).hour)


def handle_soft_limit(state, database, config):
    try:
        if not state[database["database_name"]]["Lock"]:
            if not state[database["database_name"]]["Info"]:
                send_mail(config["main"]["send_mail_from"], database["database_user_mail"],
                          database["quota_soft"], database["quota_hard"],
                          database["database_name"], False, config["main"]["smtp_server"])
                state[database["database_name"]]["Info"] = True
                state[database["database_name"]]["Date"] = time_tomorrow()
                log('info', 'database %s have reached quota_soft' % database["database_name"])
            elif state[database["database_name"]]["Date"] == time_actual():
                # Hir geht kein else, da die E-Mail sonst für jeden Durchlauf gesendet wird
                send_mail(config["main"]["send_mail_from"], database["database_user_mail"],
                          database["quota_soft"], database["quota_hard"],
                          database["database_name"], False, config["main"]["smtp_server"])
                state[database["database_name"]]["Date"] = time_tomorrow()
    except Exception as e:
        log('error', '%s' % e)


def handle_less_than_quota_soft(state, database):
    try:
        if state[database["database_name"]]["Info"]:
            state[database["database_name"]]["Info"] = False
            log('info', 'database %s is now less than quota_soft' % database["database_name"])
    except Exception as e:
        log('error', '%s' % e)
