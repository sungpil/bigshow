import time
import ast
from com.sundaytoz.logger import Logger
from com.sundaytoz.cache import Cache
from com.sundaytoz.bigshow.chart import Chart
from com.sundaytoz.bigshow.querybuilder import QueryBuilder
from com.sundaytoz.bigshow import CHART_TYPE


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ChartManager:
    pass

    @staticmethod
    def query(chart_id, chart_type, query_type, query):
        Logger.debug('chart_id={0}, chart_type={1}, query_type={2}, query={3}'.format(chart_id, chart_type, query_type, query))
        adapter = ChartManager.get_adapter(chart_type=chart_type)
        if not adapter:
            return None, '{"message":"fail to find adapter}'
        else:
            return adapter.query(chart_id=ChartManager.get_job_id(chart_id), query=QueryBuilder.get_query(query_type=query_type, query=query))

    @staticmethod
    def get_result(chart_id, from_cache=True):
        Logger().debug("get_result: chart_id={chart_id}, from_cache={from_cache}"
                       .format(chart_id=chart_id, from_cache=from_cache))
        last_job_key = ChartManager.get_job_key(chart_id=chart_id)
        if from_cache is True:
            last_job = Cache().get(last_job_key)
        else:
            last_job = None
        if not last_job:
            chart = Chart().get(chart_id, ['chart_type,query_type,query'])
            new_job = {'id': ChartManager.get_job_id(chart_id), 'type': chart['chart_type']}
            adapter = ChartManager.get_adapter(chart_type=chart['chart_type'])
            adapter.query_async(new_job['id'], QueryBuilder.get_query(chart['query_type'], chart['query']))
            Cache().set(last_job_key, new_job)
            return 'RUNNING', None, None
        else:
            last_job = ast.literal_eval(last_job)
            last_job_id = last_job['id']
            last_job_result = Cache().get(last_job_id)
            if last_job_result:
                last_job_result = ast.literal_eval(last_job_result)
                return 'DONE', last_job_result['result'], last_job_result['error']
            else:
                adapter = ChartManager.get_adapter(last_job['type'])
                if not adapter.exists(last_job_id):
                    chart = Chart().get(chart_id, ['chart_type,query_type,query'])
                    new_job = {'id': last_job_id, 'type': chart['chart_type']}
                    adapter = ChartManager.get_adapter(new_job['type'])
                    adapter.query_async(new_job['id'], QueryBuilder.get_query(chart['query_type'], chart['query']))
                    Cache().set(last_job_key, new_job)
                    return 'RUNNING', None, None
                else:
                    status, results, error = adapter.get_result(last_job_id)
                    if 'DONE' == status:
                        Cache().set(last_job_id, {'result': results, 'error': error})
                    return status, results, error

    @staticmethod
    def del_cache(chart_id):
        Cache().delete(ChartManager.get_job_key(chart_id=chart_id))

    @staticmethod
    def get_adapter(chart_type):
        if CHART_TYPE.BIGQUERY == int(chart_type):
            from com.sundaytoz.bigshow.adapter.bigqueryadapter import BigQueryAdapter
            return BigQueryAdapter
        elif CHART_TYPE.LIVY == int(chart_type):
            from com.sundaytoz.bigshow.adapter.livyadapter import LivyAdapter
            return LivyAdapter
        else:
            return None

    @staticmethod
    def get_cached_result(last_job_key):
        last_job_id = Cache().get(last_job_key)
        if last_job_id:
            return last_job_id, Cache().get(last_job_id)
        else:
            return None, None

    @staticmethod
    def get_job_id(chart_id):
        return "chart-{chart_id}-{time}".format(chart_id=chart_id, time=int(time.time()))

    @staticmethod
    def get_job_key(chart_id):
        return "last_job:{chart_id}".format(chart_id=chart_id)
