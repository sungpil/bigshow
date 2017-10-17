import datetime
from com.sundaytoz.logger import Logger
from config.dev import config

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class CustomQuery(metaclass=Singleton):
    pass

    def retention(self, start_date, end_date, interval):
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
            query_list = ["'{0}' dt".format(start_date.strftime('%y%m%d'))]
            for target_date in target_date_list:
                if target_date <= today:
                    if cnt == 0:
                        date_str = target_date.strftime('%y%m%d')
                        query_list.append("(SELECT count(*) FROM `{0}.{1}`) r{2} ".format(dataset_nru, date_str, cnt))
                    else:
                        query_list.append("(SELECT count(*) FROM `{0}.{1}` t1 inner join `{2}.{3}` t2 ON t1.resettable_device_id = t2.resettable_device_id) r{4}".format(dataset_nru, date_str, dataset_dau, target_date.strftime('%y%m%d'), cnt))
                else:
                    query_list.append("0 r{0}".format(cnt))
                cnt = cnt+1
            if query_list:
                unions.append("(SELECT {0})".format(','.join(query_list)))
            start_date = start_date + datetime.timedelta(days=1)
        if unions:
            return "SELECT * FROM {0} ORDER BY dt".format(" union all ".join(unions))
        else:
            return None