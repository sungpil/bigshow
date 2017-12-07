import logging
from daemonize import Daemonize
import time
from com.sundaytoz import bigshow
import pymysql.cursors
from pymysql.converters import conversions, through
from pymysql.constants import FIELD_TYPE
from datetime import datetime

pid = "/tmp/com.sundaytoz.schedule.pid"
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.FileHandler("/tmp/com.sundaytoz.schedule.log", "a")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
keep_fds = [fh.stream.fileno()]


def main():
    while True:
        check_schedule()
        time.sleep(60)

def check_schedule():
    now = datetime.now()

    time_minute = now.minute
    time_hour = now.hour
    time_month = now.month
    time_day = now.day
    time_date = now.weekday()

    sql = "SELECT * FROM schedules WHERE " \
          "(time_minute={minute} and time_hour<0 and time_day<0 and time_month<0 and time_date<0)" \
          "OR (time_minute={minute} and time_hour={hour} and time_day<0 and time_month<0 and time_date<0) " \
          "OR (time_minute={minute} and time_hour={hour} and time_day={day} and time_month<0 and time_date<0) " \
          "OR (time_minute={minute} and time_hour={hour} and time_day={day} and time_month={month} and time_date<0) " \
          "OR (time_minute={minute} and time_hour={hour} and time_day<0 and time_month<0 and time_date={date}) "\
        .format(minute=time_minute, hour=time_hour, day=time_day, month=time_month, date=time_date)

    connection = get_db()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            schedules = cursor.fetchall()
            if schedules:
                for schedule in schedules:
                    chart_id = "schedule-{schedule_id}".format(schedule_id=schedule['id'])
                    results, error = bigshow.Chart.query_sync(chart_id=chart_id,
                                                              resource=schedule['resource'],
                                                              query_type=schedule['query_type'],
                                                              query=schedule['query'],
                                                              query_params=schedule['query_params'])
                    logger.debug("{schedule_id} : error={error}".format(time=datetime.now().strftime("%y%m%d %H%M%S"), schedule_id=schedule['id'], error={error}))
    finally:
        connection.close()

def get_db():
    conversions[FIELD_TYPE.TIMESTAMP] = through
    from config.dev import config
    db_config = config['db']['default']
    return pymysql.connect(host=db_config["host"],
                           user=db_config["user"],
                           password=db_config["password"],
                           db=db_config["db"],
                           charset=db_config["charset"],
                           cursorclass=pymysql.cursors.DictCursor,
                           conv=conversions)

daemon = Daemonize(app="schedule", pid=pid, action=main, keep_fds=keep_fds, logger=logger)
daemon.start()
