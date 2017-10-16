from com.sundaytoz.logger import Logger
from pymemcache.client.base import PooledClient

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Cache(metaclass=Singleton):
    pass

    __cache = None

    def get(self, key):
        ret = self.__get_cache().get(str(key))
        if ret:
            ret = ret.decode('utf-8')
        return ret

    def set(self, key, val, ttl=86400):
        return self.__get_cache().set(str(key), val, ttl)

    def delete(self, key):
        return self.__get_cache().delete(key)

    def __get_cache(self):
        if not self.__cache:
            Logger.debug("__get_cache")
            from config.dev import config
            self.__cache = PooledClient(server=config['cache']['server'])
        return self.__cache