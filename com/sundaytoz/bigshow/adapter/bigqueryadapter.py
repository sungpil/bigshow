from com.google.jobmanager import JobManager
from com.sundaytoz.logger import Logger
from com.sundaytoz.bigshow.adapter.dataadapter import DataAdapter


class BigQueryAdapter(DataAdapter):
    pass

    @staticmethod
    def exists(job_id):
        Logger().debug("exists: job_id={job_id}".format(job_id=job_id))
        return JobManager().exists(job_name=job_id)

    @staticmethod
    def query(job_id, query):
        Logger().debug("query: job_id={job_id}".format(job_id=job_id))
        data = None
        error = None
        job = JobManager().query(query=query, job_name=job_id)
        if job.error_result:
            error = job.error_result
        else:
            data = job.result().fetch_data()
            if data:
                data = list(map(lambda x: list(x), data))
        return data, error

    @staticmethod
    def query_async(job_id, query):
        Logger().debug("query_async: job_id={job_id}".format(job_id=job_id))
        return JobManager().query_async(query=query, job_name=job_id)

    @staticmethod
    def get_result(job_id):
        Logger().debug("get_result: job_id={job_id}".format(job_id=job_id))
        status, results, error = JobManager().get_result(job_id)
        if 'DONE' == status and results:
            results = list(map(lambda x: list(x), results))
        return status, results, error
