from com.sundaytoz.logger import Logger
import pymysql.cursors

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Admin(metaclass=Singleton):
    pass

    __db = None

    def get(self, email):
        Logger.info("get: email={email}".format(email=email))
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT idx, email, name, lv, dept, permission, time_register FROM usr WHERE email=%s"
                cursor.execute(sql, (email,))
                row = cursor.fetchone()
            connection.commit()
        finally:
            connection.close()
        return row

    def __getDB(self):
        if not self.__db:
            from config.dev import config
            Logger.debug("__getDB: config=" + str(config))
            db_config = config['db']['admin']
            return pymysql.connect(host=db_config["host"],
                                   user=db_config["user"],
                                   password=db_config["password"],
                                   db=db_config["db"],
                                   charset=db_config["charset"],
                                   cursorclass=pymysql.cursors.DictCursor)
        return self.__db