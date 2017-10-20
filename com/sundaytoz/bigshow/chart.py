from com.sundaytoz.logger import Logger
import pymysql.cursors
from pymysql.converters import conversions, through, FIELD_TYPE
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
    __schema = ['id', 'title', 'chart_type', 'query_type', 'type', 'width', 'header', 'options', 'query']

    def add(self, chart):
        Logger.error("add: chart={chart}".format(chart=chart))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO charts(title, chart_type, query_type, type, width, options, header, query) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (chart['title'], chart['chart_type'], chart['query_type'], chart['type'], chart['width'], chart['options'], json.dumps(chart['header']), chart['query'],))
            connection.commit()
            return connection.insert_id()
        finally:
            connection.close()

    def get(self, chart_id, columns=None):
        Logger.info("get: chart_id={chart_id},columns={columns}".format(chart_id=chart_id, columns=columns))
        if not columns:
            columns = ['*']
        if not isinstance(columns, list):
            columns = [columns]
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT {0} FROM charts WHERE id=%s".format(','.join(columns))
                cursor.execute(sql, (chart_id,))
                return cursor.fetchone()
        finally:
            connection.close()

    def get_query(self, chart_id):
        Logger.info("get: chart_id={chart_id}".format(chart_id=chart_id))
        row = self.get(chart_id, ['query'])
        if row:
            return row['query']
        else:
            return None

    def get_all(self, chart_ids=None):
        Logger.info("get_all chart_ids={0}".format(chart_ids))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM charts"
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    if row['header']:
                        row['header'] = json.loads(row['header'])
                    if row['options']:
                        row['options'] = json.loads(row['options'])
                return rows
        finally:
            connection.close()

    def delete(self, chart_id):
        Logger.info("delete: chart_id={chart_id}".format(chart_id=chart_id))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "DELETE FROM charts WHERE id=%s"
                cursor.execute(sql, (chart_id,))
            connection.commit()
            return True
        finally:
            connection.close()

    def update(self, chart_id, chart):
        schema = set(self.__schema) - {'id'}
        targets = list(schema & chart.keys())
        columns = ','.join(map(lambda x: "{x}=%s".format(x=x), targets))
        values = []
        for key in targets:
            if isinstance(chart[key], (list, dict)):
                values.append("{x}".format(x=json.dumps(chart[key])))
            else:
                values.append(chart[key])
        sql = "UPDATE charts SET {columns} WHERE id={chart_id}".format(columns=columns, chart_id=chart_id)
        Logger.debug("columns={columns},sql={sql},values={values}".format(columns=columns, sql=sql, values=values))
        connection = self.__get_db()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(values))
            connection.commit()
            return True
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
                                   cursorclass=pymysql.cursors.DictCursor)
        return self.__db
