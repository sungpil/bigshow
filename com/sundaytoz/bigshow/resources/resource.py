from config.dev import config
from abc import ABCMeta, abstractmethod


class ResourceType:
    BIGQUERY = 0
    LIVY = 1
    DB = 2


class Resource:

    __resources = {}

    @staticmethod
    def get(resource_id):
        if resource_id not in Resource.__resources:
            if ResourceType.BIGQUERY == int(resource_id):
                from com.sundaytoz.bigshow.resources.bigqueryadapter import BigQueryAdapter
                Resource.__resources[resource_id] = BigQueryAdapter(config=config['bigquery'])
            elif ResourceType.LIVY == int(resource_id):
                from com.sundaytoz.bigshow.resources.livyadapter import LivyAdapter
                Resource.__resources[resource_id] = LivyAdapter()
        return Resource.__resources[resource_id]


class ResourceAdapter(metaclass=ABCMeta):

    @abstractmethod
    def query(self, job_id, query_type, query, query_params=None):
        pass

    @abstractmethod
    def get_result(self, job_id):
        pass
