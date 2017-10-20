from com.sundaytoz.logger import Logger
import pymysql.cursors
from pymysql.converters import conversions, through
from pymysql.constants import FIELD_TYPE


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Note(metaclass=Singleton):
    pass

    __db = None

    def get_all(self, note_ids=None):
        Logger.info("get_all")
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM notes"
                cursor.execute(sql)
                return cursor.fetchall()
        finally:
            connection.close()

    def get(self, note_id):
        Logger.info("get: note_id={note_id}".format(note_id=note_id))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM notes WHERE id=%s"
                cursor.execute(sql, (note_id,))
                return cursor.fetchone()
        finally:
            connection.close()

    def add(self, title):
        Logger.error("add: title={title}".format(title=title))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO notes(title, created) VALUES(%s, now())"
                cursor.execute(sql, (title,))
            connection.commit()
            return connection.insert_id()
        finally:
            connection.close()

    def __get_db(self):
        if not self.__db:
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
        return self.__db
