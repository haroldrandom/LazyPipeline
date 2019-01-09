import time
from datetime import timedelta
from abc import ABCMeta
from abc import abstractmethod
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from LazyPipeline import celery_app


logger = get_task_logger(__name__)


class BaseTask(celery_app.Task):
    """ Common task services"""

    @property
    def name(self):
        return '.'.join([self.__module__, self.__name__])


class ControllerBaseTask(BaseTask):
    """ Parse config file and dispatch them to worker"""

    ignore_result = True
    retry = False
    eta = timedelta(seconds=4)
    expires = 7200
    soft_time_limit = 3600
    time_limit = soft_time_limit + 2

    def __init__(self):
        super(BaseTask, self).__init__()

    def _parse_config(self, config):
        pass

    def run(self, *args, **kwargs):
        self._parse_config()


class WorkerBaseTask(BaseTask, metaclass=ABCMeta):
    """ Parse config and run script"""

    ignore_result = True
    retry = False
    eta = timedelta(seconds=4)
    expires = 3600
    soft_time_limit = 3600
    time_limit = soft_time_limit + 2

    @abstractmethod
    def config(self, node):
        pass

    @abstractmethod
    def pull_data_from_upstream(self):
        pass


class MultiUpstreamWorkerTask(WorkerBaseTask):
    """ Worker which can receive data from multiple uptreams """

    def config(self, node):
        self.upstream_data = {}

    def pull_data_from_upstream(self):
        if self.configed is False:
            raise Exception("Worker hasn't been configed. Call config() first.")

        upstream_data = {}
        for up in self.upstreams:
            upstream_data[up] = {}

        while True:
            msg = ''
            if msg['is_finished']


def run_job(base=ControllerBaseTask):
    pass


@celery_app.task(base=MultiUpstreamWorkerTask)
def run_script(worker_conf):
    self = run_script

    try:
        self.config(worker_conf)
    except:
        return

    try:
        script_args = self.pull_data_from_upstream()
    except SoftTimeLimitExceeded:
        logger.error('SoftTimeLimitExceeded !')
    finally:
        logger.info('run script task finished')
