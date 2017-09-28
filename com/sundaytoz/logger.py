import inspect

class Logger(object):

    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    FATAL = 5

    _level_ = 1

    @classmethod
    def setLevel(cls, level):
        cls._level_ = level

    @classmethod
    def level(cls):
        return cls._level_

    @classmethod
    def __log(cls, level, level_tag, msg, caller):
        if cls._level_ > level:
            return
        if not caller:
            caller = cls.__get_class_name()
        print("{level_tag}/{caller}: {msg}".format(level_tag = level_tag, caller=caller, msg = msg))

    @classmethod
    def debug(cls, msg):
        cls.__log(Logger.DEBUG, 'D', msg, cls.__get_class_name())

    @classmethod
    def info(cls, msg):
        cls.__log(Logger.INFO, 'I', msg, cls.__get_class_name())

    @classmethod
    def warn(cls, msg):
        cls.__log(Logger.WARN, 'W', msg, cls.__get_class_name())

    @classmethod
    def error(cls, msg):
        cls.__log(Logger.ERROR, 'E', msg, cls.__get_class_name())

    @classmethod
    def fatal(cls, msg):
        cls.__log(Logger.FATAL, 'F', msg, cls.__get_class_name())

    @classmethod
    def __get_class_name(cls):
        stack = inspect.stack(0)
        if 'self' in stack[2][0].f_locals.keys():
            return stack[2][0].f_locals["self"].__class__.__name__
        else:
            return 'root'