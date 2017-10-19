from com.sundaytoz.logger import Logger
from com.sundaytoz.cache import Cache
from com.sundaytoz.chart import Chart
from com.google.jobmanager import JobManager
from com.sundaytoz.customquery import CustomQuery
import ast, time, json, datetime


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ChartManager(metaclass=Singleton):
    pass

    CHART_TYPE_BIGQUERY = 0
    CHART_TYPE_CUSTOM = 1

    def get_result(self, chart_id, from_cache=True):
        Logger().debug("get_result: chart_id={chart_id}, from_cache={from_cache}".format(chart_id=chart_id, from_cache=from_cache))
        status_key = "last_job:{chart_id}".format(chart_id=chart_id)
        error = None
        results = None
        if from_cache:
            last_job_id = Cache().get(status_key)
            results = Cache().get(last_job_id)
        else:
            last_job_id = self._get_job_id(chart_id)
        Logger.debug("get_result: status_key={status_key}, last_job_id={last_job_id}".format(status_key=status_key, last_job_id=last_job_id))
        if not results:
            Logger.debug("from BIGQUERY")
            if last_job_id and JobManager().exists(last_job_id):
                status, results, error = JobManager().get_result(last_job_id)
            else:
                if not last_job_id:
                    last_job_id = self._get_job_id(chart_id)
                chart = Chart().get(chart_id,['chart_type, query'])
                query = self.get_query(chart['chart_type'], chart['query'])
                JobManager().query_async(query, last_job_id)
                Cache().set(status_key, last_job_id, 3600)
                status = 'RUNNING'
            if 'DONE' == status:
                if results:
                    results = list(map(lambda x: list(x), results))
                Cache().set(last_job_id, {'error':error, 'results':results})
            return status, results, error
        else:
            Logger.debug("from CACHE")
            status = ast.literal_eval(results)
            return 'DONE', status['results'], status['error']

    def del_cache(self, chart_id):
        Logger.debug("del_cache:{0}".format(chart_id))
        status_key = "last_job:{chart_id}".format(chart_id=chart_id)
        return Cache().delete(status_key)


    def get_query(self, chart_type, query):
        if self.CHART_TYPE_BIGQUERY == int(chart_type):
            return query
        elif self.CHART_TYPE_CUSTOM == int(chart_type):
            option = json.loads(query)
            range = option['range']
            interval = option['interval']
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=interval)
            return CustomQuery().retention(start_date, end_date, range)
        else:
            return None

    def _get_job_id(self, chart_id):
        return "chart-{chart_id}-{time}".format(chart_id=chart_id, time=int(time.time()))