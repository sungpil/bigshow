from com.sundaytoz.logger import Logger

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
        ret = self.__get_cache().get(key.encode('utf-8'))
        if ret:
            ret = ret.decode('utf-8')
        return ret

    def set(self, key, val, ttl=86400):
        return self.__get_cache().set(key.encode('utf-8'), val, ttl)

    def __get_cache(self):
        if not self.__cache:
            Logger.debug("__get_cache")
            from pymemcache.client.base import Client
            from config.dev import config
            self.__cache = Client(server=config['cache']['server'])
        return self.__cache