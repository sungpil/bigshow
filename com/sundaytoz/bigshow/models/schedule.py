from com.sundaytoz.logger import Logger
import pymysql.cursors
from pymysql.converters import conversions, through
from pymysql.constants import FIELD_TYPE


class Schedule:

    __db = None

    __schema = ['id', 'title', 'resource', 'query_type', 'query', 'query_params', 'time_minute', 'time_hour',
                'time_day', 'time_month', 'time_date', 'created']

    @classmethod
    def get_all(cls):
        Logger.info("get_all")
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = 'SELECT * FROM schedules'
                cursor.execute(sql)
                return cursor.fetchall()
        finally:
            connection.close()

    @classmethod
    def get(cls, schedule_id):
        Logger.info('get: schedule_id:{schedule_id}'.format(schedule_id=schedule_id))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = 'SELECT * FROM schedules where id=%s'
                cursor.execute(sql, (schedule_id,))
                return cursor.fetchone()
        finally:
            connection.close()

    @classmethod
    def add(cls, schedule):
        Logger.error("add: schedule={schedule}".format(schedule=schedule))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                keys = list(set(cls.__schema).intersection(set(schedule.keys())) - {'id'})
                sql = "INSERT INTO schedules({keys}, created) " \
                      "VALUES({columns}, now())".format(keys=','.join(keys), columns=','.join(map(lambda x: "%s", keys)))
                values = []
                for key in keys:
                    values.append(schedule[key])
                Logger.debug("sql={sql}, values={values}".format(sql=sql, values=values))
                cursor.execute(sql, values)
                insert_id = connection.insert_id()
            connection.commit()
            return insert_id
        finally:
            connection.close()

    @classmethod
    def update(cls, schedule):
        Logger.error("update: schedule={schedule}".format(schedule=schedule))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                keys = list(set(cls.__schema).intersection(set(schedule.keys())))
                sql = "UPDATE schedules SET {key_val} WHERE id={id}".format(key_val=','.join(map(lambda x: "{key}=%s".format(key=x), keys)), id=schedule['id'])
                values = []
                for key in keys:
                    values.append(schedule[key])
                Logger.debug("sql={sql}, values={values}".format(sql=sql, values=values))
                cursor.execute(sql, values)
                insert_id = connection.insert_id()
            connection.commit()
            return insert_id
        finally:
            connection.close()

    @classmethod
    def delete(cls, schedule_id):
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "DELETE FROM schedules WHERE id=%s"
                cursor.execute(sql, (schedule_id,))
            connection.commit()
            return True
        finally:
            connection.close()

    @classmethod
    def __get_db(cls):
        if not cls.__db:
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
        return cls.__db
