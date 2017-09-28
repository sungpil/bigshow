from google.cloud import bigquery
import time
from com.sundaytoz.logger import Logger

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class JobManager(metaclass=Singleton):
    pass

    __client = None

    def get_result(self, job_name):
        Logger.debug("get_result: job_name={job_name}".format(job_name=job_name))
        savedJob = self.__get(job_name)
        if None != savedJob and 'DONE' == savedJob.state:
            Logger.debug("state={state} ended={ended}".format(state=savedJob.state,ended=savedJob.ended))
            result = savedJob.result()
            if result:
                return result.fetch_data()
        return None

    def query(self, query, job_name):
        Logger.debug("query: job_name={job_name}".format(job_name=job_name))
        job = self.query_async(query, job_name)
        retry_count = 100
        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            time.sleep(10)
            job.reload()
        results = job.result()
        return results.fetch_data()

    def query_async(self, query, job_name):
        Logger.debug("query_async: job_name={job_name}".format(job_name=job_name))
        job = self.__get(job_name)
        if not job:
            job = self.__create(job_name, query)
            job.use_legacy_sql = False
            job.begin()
        return job

    def exists(self, job_name):
        job = self.__get(job_name)
        Logger.error("exists job={job}".format(job=job))
        return job and job.exists()

    def __get(self, job_name):
        Logger.debug("__get: job_name={job_name}".format(job_name=job_name))
        try:
            job = self.__getClient().get_job(job_name)
        except :
            import sys
            Logger.error("__get.except {error_msg}".format(error_msg=sys.exc_info()[0]))
            job = None
        return job

    def __create(self, job_name, query):
        Logger.debug("__create: job_name={job_name}".format(job_name=job_name))
        return self.__getClient().run_async_query(job_name, query)

    def __getClient(self):
        if not self.__client:
            self.__client = bigquery.Client(project='puzzle-art-41863118')
        self.__client.list_jobs()
        return self.__client