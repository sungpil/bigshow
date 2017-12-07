from com.sundaytoz.logger import Logger
import pymysql.cursors
from pymysql.converters import conversions, through, FIELD_TYPE
import json


class Chart:

    __db = None
    __schema = ['id', 'note', 'title', 'resource', 'graph', 'query_type', 'query', 'query_params', 'created']

    @classmethod
    def add(cls, chart):
        Logger.error("add: chart={chart}".format(chart=chart))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO charts(note, title, resource, graph, query_type, query, query_params) VALUES(%s, " \
                      "%s, %s, %s, %s, %s, %s) "
                cursor.execute(sql, (chart['note'], chart['title'], chart['resource'], json.dumps(chart['graph']),
                                     chart['query_type'], chart['query'], json.dumps(chart['query_params']),))
                insert_id = connection.insert_id()
            connection.commit()
            return insert_id
        finally:
            connection.close()

    @classmethod
    def get(cls, chart_id, columns=None):
        Logger.info("get: chart_id={chart_id}, columns={columns}".format(chart_id=chart_id, columns=columns))
        if not columns:
            columns = ['*']
        if not isinstance(columns, list):
            columns = [columns]
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT {0} FROM charts WHERE id=%s".format(','.join(columns))
                cursor.execute(sql, (chart_id,))
                return cursor.fetchone()
        finally:
            connection.close()

    @classmethod
    def get_query(cls, chart_id):
        Logger.info("get: chart_id={chart_id}".format(chart_id=chart_id))
        row = cls.get(chart_id, ['query'])
        if row:
            return row['query']
        else:
            return None

    @classmethod
    def get_all(cls, note_id=None, chart_ids=None):
        Logger.info("get_all note_id={note_id}, chart_ids={chart_ids}".format(note_id=note_id, chart_ids=chart_ids))
        connection = cls.__get_db()
        try:
            wheres = []
            sql = "SELECT * FROM charts"
            if note_id:
                wheres.append('note={note_id}'.format(note_id=note_id))
            if chart_ids:
                wheres.append('ids in ({chart_ids})'.format(chart_ids=','.join(chart_ids)))
            if wheres:
                sql += ' WHERE {wheres}'.format(wheres=' and '.join(wheres))
            Logger.info("get_all sql={sql}".format(sql=sql))
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    for column in ['graph', 'query_params']:
                        if row[column]:
                            row[column] = json.loads(row[column])
                return rows
        finally:
            connection.close()

    @classmethod
    def delete(cls, chart_id):
        Logger.info("delete: chart_id={chart_id}".format(chart_id=chart_id))
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                sql = "DELETE FROM charts WHERE id=%s"
                cursor.execute(sql, (chart_id,))
            connection.commit()
            return True
        finally:
            connection.close()

    @classmethod
    def update(cls, chart_id, chart):
        schema = set(cls.__schema) - {'id'}
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
        connection = cls.__get_db()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(values))
            connection.commit()
            return True
        finally:
            connection.close()

    @classmethod
    def __get_db(cls):
        if not Chart.__db:
            conversions[FIELD_TYPE.TIMESTAMP] = through
            from config.dev import config
            db_config = config['db']['default']
            Logger.debug("new connection - CHART")
            return pymysql.connect(host=db_config["host"],
                                   user=db_config["user"],
                                   password=db_config["password"],
                                   db=db_config["db"],
                                   charset=db_config["charset"],
                                   cursorclass=pymysql.cursors.DictCursor)
        return Chart.__db
