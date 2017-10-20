from com.google.jobmanager import JobManager
from com.sundaytoz.logger import Logger
from com.sundaytoz.bigshow.adapter.dataadapter import DataAdapter


class BigQueryAdapter(DataAdapter):
    pass

    @staticmethod
    def exists(chart_id):
        Logger().debug("exists: chart_id={chart_id}".format(chart_id=chart_id))
        return JobManager().exists(job_name=chart_id)

    @staticmethod
    def query(chart_id, query):
        Logger().debug("query: chart_id={chart_id}".format(chart_id=chart_id))
        data = None
        error = None
        job = JobManager().query(query=query, job_name=chart_id)
        if job.error_result:
            error = job.error_result
        else:
            data = job.result().fetch_data()
            if data:
                data = list(map(lambda x: list(x), data))
        return data, error

    @staticmethod
    def query_async(chart_id, query):
        Logger().debug("query_async: chart_id={chart_id}".format(chart_id=chart_id))
        return JobManager().query_async(query=query, job_name=chart_id)

    @staticmethod
    def get_result(chart_id):
        Logger().debug("get_result: chart_id={chart_id}".format(chart_id=chart_id))
        status, results, error = JobManager().get_result(chart_id)
        if 'DONE' == status and results:
            results = list(map(lambda x: list(x), results))
        return status, results, error
