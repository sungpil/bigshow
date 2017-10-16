from com.sundaytoz.logger import Logger
import pymysql.cursors
import json

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Chart(metaclass=Singleton):
    pass

    __db = None
    __schema = ['id','title','type','width','header','options','query']

    def add(self, chart):
        Logger.error("add: chart={chart}".format(chart=chart))
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO charts(title, type, width, options, header, query) VALUES(%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (
                chart['title'], chart['type'], chart['width'], chart['options'], json.dumps(chart['header']),
                chart['query'],))
            connection.commit()
        finally:
            connection.close()
        return True

    def get_query(self, chart_id):
        Logger.info("get: chart_id={chart_id}".format(chart_id=chart_id))
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT query FROM charts WHERE id=%s"
                cursor.execute(sql, (chart_id,))
                row = cursor.fetchone()
            connection.commit()
        finally:
            connection.close()
        return row['query']

    def get_all(self):
        Logger.info("get_all")
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM charts"
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    row['created'] = str(row['created'])
                    if row['header']:
                        row['header'] = json.loads(row['header'])
                    if row['options']:
                        row['options'] = json.loads(row['options'])
        finally:
            connection.close()
        return rows

    def delete(self, chart_id):
        Logger.info("delete: chart_id={chart_id}".format(chart_id=chart_id))
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                sql = "DELETE FROM charts WHERE id=%s"
                cursor.execute(sql, (chart_id,))
            connection.commit()
        finally:
            connection.close()
        return True

    def update(self, chart):
        schema = set(self.__schema) - {'id'}
        targets = list(schema & chart.keys())
        columns = ','.join(map(lambda x: "{x}=%s".format(x=x), targets))
        values = []
        for key in targets:
            if isinstance(chart[key], (list, dict)):
                values.append("{x}".format(x=json.dumps(chart[key])))
            else:
                values.append(chart[key])
        sql = "UPDATE charts SET {columns} WHERE id={chart_id}".format(columns=columns,chart_id=chart['id'])
        Logger.debug("columns={columns},sql={sql},values={values}".format(columns=columns,sql=sql,values=values))
        connection = self.__getDB()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(values))
            connection.commit()
        finally:
            connection.close()
        return True


    def __getDB(self):
        if not self.__db:
            from config.dev import config
            db_config = config['db']['default']
            return pymysql.connect(host=db_config["host"],
                                   user=db_config["user"],
                                   password=db_config["password"],
                                   db=db_config["db"],
                                   charset=db_config["charset"],
                                   cursorclass=pymysql.cursors.DictCursor)
        return self.__db