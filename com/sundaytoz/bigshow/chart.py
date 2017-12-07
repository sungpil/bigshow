import ast
import time

from com.sundaytoz.bigshow import models
from com.sundaytoz.bigshow.resources import Resource
from com.sundaytoz.cache import Cache
from com.sundaytoz.logger import Logger


class Chart:
    pass

    TTL_LAST_JOB = 3600
    TTL_LAST_RESULT = 2592000

    __data_adapters = {}

    @staticmethod
    def query(chart_id, resource, query_type, query, query_params):
        Logger.debug('chart_id={0}, resource={1}, query_type={2}, query={3}, query_params={4}'
                     .format(chart_id, resource, query_type, query, query_params))
        adapter = Resource.get(resource_id=resource)
        if not adapter:
            return None
        else:
            job_id = Chart.get_job_id(chart_id)
            adapter.query(job_id=job_id, query_type=query_type, query=query, query_params=query_params)
            return job_id

    @staticmethod
    def query_sync(chart_id, resource, query_type, query, query_params):
        job_id = Chart.query(chart_id=chart_id, resource=resource,
                             query_type=query_type, query=query, query_params=query_params)
        if not job_id:
            return None, {'message': 'fail to initialize job'}
        adapter = Resource.get(resource_id=resource)
        if not adapter:
            return None, {'message': 'fail to initialize resources'}
        retry_count = 100
        while retry_count > 0:
            status, results, error = adapter.get_result(job_id)
            if 'DONE' == status:
                return results, error
            else:
                time.sleep(10)

    @staticmethod
    def get_result(chart_id, from_cache=True):
        Logger().debug("get_result: chart_id={chart_id}, from_cache={from_cache}"
                       .format(chart_id=chart_id, from_cache=from_cache))
        last_job_key = Chart.get_job_key(chart_id=chart_id)
        if from_cache is True:
            last_job = Cache().get(last_job_key)
        else:
            last_job = None
        if not last_job:
            chart = models.Chart.get(chart_id, ['resource,query_type,query,query_params'])
            new_job = {'id': Chart.get_job_id(chart_id), 'resource': chart['resource']}
            adapter = Resource.get(resource_id=chart['resource'])
            adapter.query(job_id=new_job['id'], query_type=chart['query_type'],
                          query=chart['query'], query_params=chart['query_params'])
            Cache().set(last_job_key, new_job, Chart.TTL_LAST_JOB)
            return 'RUNNING', None, None
        else:
            last_job = ast.literal_eval(last_job)
            last_job_id = last_job['id']
            last_job_result = Cache().get(last_job_id)
            if last_job_result:
                last_job_result = ast.literal_eval(last_job_result)
                return 'DONE', last_job_result['result'], last_job_result['error']
            else:
                adapter = Resource.get(resource_id=last_job['resource'])
                if not adapter.exists(job_id=last_job_id):
                    chart = models.Chart.get(chart_id, ['resource,query_type,query,query_params'])
                    adapter.query_async(job_id=last_job['id'], query_type=chart['query_type'],
                                        query=chart['query'], query_params=chart['query_params'])
                    Cache().set(last_job_key, last_job, Chart.TTL_LAST_JOB)
                    return 'RUNNING', None, None
                else:
                    status, results, error = adapter.get_result(last_job_id)
                    if 'DONE' == status:
                        Cache().set(last_job_id, {'result': results, 'error': error}, Chart.TTL_LAST_RESULT)
                    return status, results, error

    @staticmethod
    def del_cache(chart_id):
        Cache().delete(Chart.get_job_key(chart_id=chart_id))

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
