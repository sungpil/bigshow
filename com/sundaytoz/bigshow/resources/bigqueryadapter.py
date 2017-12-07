import datetime
import json
import re
from dateutil.relativedelta import relativedelta

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from com.sundaytoz.bigshow.resources.resource import ResourceAdapter
from com.sundaytoz.logger import Logger


class QueryType:
    STANDARD = 0
    RETENTION = 1


class BigQueryAdapter(ResourceAdapter):
    pass

    __config = None
    __client = None

    def __init__(self, config):
        self.__config = config

    def exists(self, job_id):
        Logger().debug("exists: job_id={job_id}".format(job_id=job_id))
        return self.__get_job(job_id=job_id)

    def query(self, job_id, query_type, query, query_params=None):
        Logger().debug("query_async: job_id={job_id}, query_type={query_type}, query_params={query_params}"
                       .format(job_id=job_id, query_type=query_type, query_params=query_params))
        job = self.__get_job(job_id=job_id)
        if not job:
            job = self.__get_client().run_async_query(
                job_name=job_id,
                query=self.QueryBuilder.get_query(
                    config=self.__config, query_type=query_type, query=query, query_params=query_params))
            job.use_legacy_sql = False
            try:
                query_params = json.loads(BigQueryAdapter.QueryBuilder.replace_patterns(query=query_params))
                Logger().debug("query_params={query_params}".format(query_params=query_params))
                if 'destinationTable' in query_params:
                    client = self.__get_client()
                    dataset = client.dataset(dataset_name=query_params['destinationTable']['dataset'])
                    job.destination = dataset.table(name=query_params['destinationTable']['name'])
                    if 'write_disposition' in query_params['destinationTable']:
                        job.write_disposition = query_params['destinationTable']['write_disposition']
            except:
                Logger.error("invalid query params:{query_params}".format(query_params=query_params))

            job.begin()
        return job

    def get_result(self, job_id):
        Logger().debug("get_result: job_id={job_id}".format(job_id=job_id))
        job = self.__get_job(job_id=job_id)
        if None is job:
            return 'DONE', None, {'message': 'Job Not Exists'}
        if 'DONE' == job.state.upper():
            if not job.error_result:
                result = job.result()
                if result:
                    return 'DONE', list(map(lambda x: list(x), result.fetch_data())), None
                else:
                    return 'DONE', None, {'message': 'Result not exists'}
            else:
                return 'DONE', None, job.error_result
        else:
            return job.state, None, None

    def __get_job(self, job_id):
        Logger.debug("__get_job: job_name={job_id}".format(job_id=job_id))
        try:
            job = self.__get_client().get_job(job_id)
        except NotFound:
            job = None
        return job

    def __get_client(self):
        if not self.__client:
            self.__client = bigquery.Client(project=self.__config['project_id'])
        return self.__client

    class QueryBuilder:
        pass

        @staticmethod
        def get_query(config, query_type, query, query_params=None):
            Logger().debug("get_query: query_type={query_type}, query_params={query_params}"
                           .format(query_type=query_type, query_params=query_params))
            if QueryType.RETENTION == int(query_type):
                option = json.loads(query)
                date_range = option['range']
                interval = date_range - 1
                region = option['region'] if 'region' in option else None
                platform = BigQueryAdapter.QueryBuilder.__get_platform(option['platform']) if 'platform' in option else None
                end_date = datetime.datetime.now() - datetime.timedelta(days=2)
                start_date = end_date - datetime.timedelta(days=interval)
                query = BigQueryAdapter.QueryBuilder.retention(config, start_date, end_date, date_range, region, platform)
                ##Logger().debug(query)
            query = BigQueryAdapter.QueryBuilder.replace_patterns(query)
            Logger().debug(query)
            return query

        @staticmethod
        def date_replacer(format):
            groups = re.findall(r"%(?P<token>[Y|m|d])(?P<val>[+|-]\d+)*", format)
            delta = {'Y': 0, 'm': 0, 'd': 0}
            format = ''
            for group in groups:
                if len(group[1]) > 0:
                    delta[group[0]] += int(group[1])
                format += '%'+group[0]
            date = datetime.datetime.now() + relativedelta(years=delta['Y'], months=delta['m'], days=delta['d'])
            return date.strftime(format)

        @staticmethod
        def today_replacer(format, delta=None):
            date = datetime.datetime.now() + relativedelta(years=delta['Y'], months=delta['m'], days=delta['d'])
            return date.strftime(format)

        @staticmethod
        def today_1_replacer(format):
            return BigQueryAdapter.QueryBuilder.today_replacer(format=format, delta={'Y': 0,'m': 0, 'd': -1})

        @staticmethod
        def today_2_replacer(format):
            return BigQueryAdapter.QueryBuilder.today_replacer(format=format, delta={'Y': 0, 'm': 0, 'd': -2})

        @staticmethod
        def replace_patterns(query):
            replacers = {'DATE':BigQueryAdapter.QueryBuilder.date_replacer,
                         'TODAY':BigQueryAdapter.QueryBuilder.today_replacer,
                         'TODAY-1':BigQueryAdapter.QueryBuilder.today_1_replacer,
                         'TODAY-2':BigQueryAdapter.QueryBuilder.today_2_replacer}
            groups = re.findall(r"\${(?P<pattern>.+)::(?P<type>[^}]+)}", query)
            for group in list(set(groups)):
                target = "${{{pattern}::{type}}}".format(pattern=group[0], type=group[1])
                if group[1] in replacers:
                    query = query.replace(target, replacers.get(group[1])(group[0]))
            return query

        @staticmethod
        def retention(config, start_date, end_date, interval, region, platform):
            project_id = config['project_id']
            dataset_nru = "{0}.{1}".format(project_id, config['dataset']['nru'])
            dataset_dau = "{0}.{1}".format(project_id, config['dataset']['dau'])
            today = datetime.datetime.now() - datetime.timedelta(days=2)
            if end_date > today:
                end_date = today
            if start_date > end_date:
                start_date = end_date
            where_nru = BigQueryAdapter.QueryBuilder.__get_where_region_platform(region=region, platform=platform, table_alias=None)
            where_dru = BigQueryAdapter.QueryBuilder.__get_where_region_platform(region=region, platform=platform, table_alias='t1')
            unions = []
            Logger().debug("retention: start_date={0}, end_date={1}".format(start_date, end_date))
            # make union
            while start_date <= end_date:
                target_date_list = [start_date + datetime.timedelta(days=x) for x in range(0, interval)]
                cnt = 0
                date_str = None
                query_list = ["'{0}' dt".format(start_date.strftime('%y%m%d'))]
                for target_date in target_date_list:
                    if target_date <= today:
                        if cnt == 0:
                            date_str = target_date.strftime('%y%m%d')
                            query_list.append(
                                "(SELECT count(distinct(uid)) FROM `{dataset_nru}.{date_str}` {where}) r{cnt} ".format(
                                    dataset_nru=dataset_nru, date_str=date_str, cnt=cnt, where=where_nru))
                        else:
                            query_list.append(
                                "(SELECT count(distinct(t1.uid)) FROM `{dataset_nru}.{date_str}` t1 "
                                "inner join `{dataset_dau}.{dau_date_str}` t2 "
                                "ON t1.uid = t2.uid AND t1.region = t2.region {where}) r{cnt}".format(
                                    dataset_nru=dataset_nru, date_str=date_str, dataset_dau=dataset_dau,
                                    dau_date_str=target_date.strftime('%y%m%d'), cnt=cnt, where=where_dru))
                    else:
                        query_list.append("0 r{0}".format(cnt))
                    cnt += 1
                if query_list:
                    unions.append("(SELECT {0})".format(','.join(query_list)))
                start_date = start_date + datetime.timedelta(days=1)
            if unions:
                return "SELECT * FROM {0} ORDER BY dt".format(" union all ".join(unions))
            else:
                return None

        @staticmethod
        def __get_interval(date_range, interval):
            if interval is None:
                interval = date_range - 1
            return max(date_range - 1, interval)

        @staticmethod
        def __get_platform(platform):
            if platform.lower() in ['android', 'a', 'aos', 'google']:
                return 'A'
            elif platform.lower() in ['apple', 'i', 'ios']:
                return 'I'
            else:
                return None

        @staticmethod
        def __get_where_region_platform(region, platform, table_alias):
            where_list = []
            if table_alias is not None:
                table_alias += '.'
            else:
                table_alias = ''
            if region is not None:
                where_list.append("{table_alias}region='{region}'".format(table_alias=table_alias, region=region))
            if platform is not None:
                where_list.append(
                    "{table_alias}platform='{platform}'".format(table_alias=table_alias, platform=platform))
            if len(where_list) > 0:
                return "WHERE {where_list}".format(where_list=' AND '.join(where_list))
            else:
                return ''
