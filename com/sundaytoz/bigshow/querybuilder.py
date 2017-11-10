import datetime
import json
from com.sundaytoz.logger import Logger
from com.sundaytoz.bigshow import QUERY_TYPE
from config.dev import config


class QueryBuilder:
    pass

    @staticmethod
    def get_query(query_type, query):
        Logger().debug("get_query: query_type={query_type}".format(query_type=query_type))
        if QUERY_TYPE.CUSTOM == int(query_type):
            option = json.loads(query)
            date_range = option['range']
            interval = date_range-1
            region = option['region'] if 'region' in option else None
            platform = QueryBuilder._getPlatform(option['platform']) if 'platform' in option else None
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=interval)
            query = QueryBuilder.retention(start_date, end_date, date_range, region, platform)
            Logger().debug(query)
            return query
        else:
            return query

    @staticmethod
    def retention(start_date, end_date, interval, region, platform):
        project_id = config['bigquery']['project_id']
        dataset_nru = "{0}.{1}".format(project_id, config['bigquery']['dataset']['nru'])
        dataset_dau = "{0}.{1}".format(project_id, config['bigquery']['dataset']['dau'])
        today = datetime.datetime.now() - datetime.timedelta(days=1)
        if end_date > today:
            end_date = today
        if start_date > end_date:
            start_date = end_date
        where_nru = QueryBuilder._getWhereRegionPlatform(region=region, platform=platform, table_alias=None)
        where_dru = QueryBuilder._getWhereRegionPlatform(region=region, platform=platform, table_alias='t1')
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
                            "(SELECT count(*) FROM `{dataset_nru}.{date_str}` {where}) r{cnt} ".format(
                                dataset_nru=dataset_nru, date_str=date_str, cnt=cnt, where=where_nru))
                    else:
                        query_list.append(
                            "(SELECT count(*) FROM `{dataset_nru}.{date_str}` t1 inner join `{dataset_dau}.{dau_date_str}` t2 ON t1.uid = t2.uid AND t1.region = t2.region {where}) r{cnt}".format(
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
    def _getInterval(range, interval):
        if interval is None:
            interval = range-1
        return max(range-1, interval)

    @staticmethod
    def _getPlatform(platform):
        if platform.lower() in ['android', 'a', 'aos', 'google']:
            return 'A'
        elif platform.lower() in ['apple', 'i', 'ios']:
            return 'I'
        else:
            return None

    @staticmethod
    def _getWhereRegionPlatform(region, platform, table_alias):
        where_list = []
        if table_alias is not None:
            table_alias += '.'
        else:
            table_alias = ''
        if region is not None:
            where_list.append("{table_alias}region='{region}'".format(table_alias=table_alias,region=region))
        if platform.upper() is not None:
            where_list.append("{table_alias}platform='{platform}'".format(table_alias=table_alias,platform=platform))
        if len(where_list) > 0:
            return "WHERE {where_list}".format(where_list=' AND '.join(where_list))
        else:
            return ''
