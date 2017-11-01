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
            interval = option['interval']
            region = option['region']
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=interval)
            return QueryBuilder.retention(start_date, end_date, date_range, region)
        else:
            return query

    @staticmethod
    def retention(start_date, end_date, interval, region):
        project_id = config['bigquery']['project_id']
        dataset_nru = "{0}.{1}".format(project_id, config['bigquery']['dataset']['nru'])
        dataset_dau = "{0}.{1}".format(project_id, config['bigquery']['dataset']['dau'])
        today = datetime.datetime.now() - datetime.timedelta(days=1)
        if end_date > today:
            end_date = today
        if start_date > end_date:
            start_date = end_date
        unions = []
        Logger().debug("retention: start_date={0}, end_date={1}".format(start_date, end_date))
        while start_date <= end_date:
            target_date_list = [start_date + datetime.timedelta(days=x) for x in range(0, interval)]
            cnt = 0
            date_str = None
            query_list = ["'{0}' dt".format(start_date.strftime('%y%m%d'))]
            for target_date in target_date_list:
                if target_date <= today:
                    if cnt == 0:
                        date_str = target_date.strftime('%y%m%d')
                        query_list.append("(SELECT count(*) FROM `{0}.{1} WHERE region = {2}`) r{3} ".format(dataset_nru, date_str, region, cnt))
                    else:
                        query_list.append(
                            "(SELECT count(*) FROM `{0}.{1}` t1 inner join `{2}.{3}` t2 ON t1.resettable_device_id = t2.resettable_device_id AND t1.region = t2.region AND t1.region = '{4}') r{5}".format(
                                dataset_nru, date_str, dataset_dau, target_date.strftime('%y%m%d'), region, cnt))
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
