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
        saved_job = self.__get(job_name)
        if None is saved_job:
            return 'DONE', None, {'message': 'Invalid Job'}
        if 'DONE' == saved_job.state:
            if not saved_job.error_result:
                Logger.debug("state={state} ended={ended}".format(state=saved_job.state, ended=saved_job.ended))
                result = saved_job.result()
                if result:
                    return 'DONE', result.fetch_data(), None
                else:
                    return 'DONE', None, {'message': 'Result not exists'}
            else:
                Logger.debug("state={state} error={error}".format(state=saved_job.state, error=saved_job.error_result))
                return 'DONE', None, saved_job.error_result
        else:
            return saved_job.state, None, None

    def query(self, query, job_name):
        Logger.debug("query: job_name={job_name}".format(job_name=job_name))
        job = self.query_async(query, job_name)
        retry_count = 100
        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            time.sleep(10)
            job.reload()
        return job

    def query_async(self, query, job_name):
        Logger.debug("query_async: job_name={job_name}".format(job_name=job_name))
        job = self.__get(job_name)
        if not job:
            job = self.__create(job_name, query)
            job.use_legacy_sql = False
            job.begin()
        return job

    def exists(self, job_name):
        if not job_name:
            return False
        job = self.__get(job_name)
        Logger.error("exists job={job}".format(job=job))
        return job and job.exists()

    def __get(self, job_name):
        Logger.debug("__get: job_name={job_name}".format(job_name=job_name))
        try:
            job = self.__get_client().get_job(job_name)
        except:
            import sys
            Logger.error("__get.except {error_msg}".format(error_msg=sys.exc_info()[0]))
            job = None
        return job

    def __create(self, job_name, query):
        Logger.debug("__create: job_name={job_name}".format(job_name=job_name))
        return self.__get_client().run_async_query(job_name, query)

    def __get_client(self):
        if not self.__client:
            from config.dev import config
            self.__client = bigquery.Client(project=config['bigquery']['project_id'])
        self.__client.list_jobs()
        return self.__client
