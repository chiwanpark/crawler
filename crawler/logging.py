import logging


class LogMixin(object):
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '[%(levelname)-8s] %(asctime)s [%(filename)s] [%(funcName)s:%(lineno)d] %(message)s',
            '%Y-%m-%d %H:%M:%S')

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)

        self._logger.addHandler(handler)

    def log_info(self, msg, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    def log_warning(self, msg, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

    def log_error(self, msg, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def log_debug(self, msg, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)

    def log_exception(self, msg, *args, **kwargs):
        self._logger.exception(msg, *args, **kwargs)

    def log_critical(self, msg, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)
