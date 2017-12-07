from com.sundaytoz.logger import Logger
import pymysql.cursors
from pymysql.converters import conversions, through
from pymysql.constants import FIELD_TYPE


class Note:

    __db = None

    @classmethod
    def get_all(cls, note_ids=None):
        Logger.info("get_all, node_ids={note_ids}".format(note_ids=note_ids))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM notes"
                cursor.execute(sql)
                return cursor.fetchall()
        finally:
            connection.close()

    @classmethod
    def get(cls, note_id):
        Logger.info("get: note_id={note_id}".format(note_id=note_id))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM notes WHERE id=%s"
                cursor.execute(sql, (note_id,))
                return cursor.fetchone()
        finally:
            connection.close()

    @classmethod
    def add(cls, title):
        Logger.error("add: title={title}".format(title=title))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO notes(title, created) VALUES(%s, now())"
                cursor.execute(sql, (title,))
                insert_id = connection.insert_id()
            connection.commit()
            return insert_id
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
