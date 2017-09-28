from com.sundaytoz.logger import Logger
from com.sundaytoz.cache import Cache
from com.sundaytoz.chart import Chart
from com.google.jobmanager import JobManager
import ast, time

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ChartManager(metaclass=Singleton):
    pass

    def get_result(chart_id, from_cache=True):
        status_key = "last_job:{chart_id}".format(chart_id=chart_id)
        last_job_id = Cache().get(status_key)
        Logger.debug("get_result: status_key={status_key}, last_job_id={last_job_id}".format(status_key=status_key, last_job_id=last_job_id))
        if not last_job_id:
            last_job_id = "chart-{chart_id}-{time}".format(chart_id=chart_id, time=int(time.time()))
        if from_cache:
            results = Cache().get(last_job_id)
        else:
            results = None
        if not results:
            Logger.debug("from BIGQUERY")
            if JobManager().exists(last_job_id):
                results = JobManager().get_result(last_job_id)
            else:
                query = Chart().get_query(chart_id)
                JobManager().query_async(query, last_job_id)
                Cache().set(status_key, last_job_id, 3600)
            if results and from_cache:
                results = list(map(lambda x: list(x), results))
                Cache().set(last_job_id, results)
            return results
        else:
            Logger.debug("from CACHE")
            return ast.literal_eval(results)