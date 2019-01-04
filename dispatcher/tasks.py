import time
from datetime import timedelta
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


class WorkerBaseTask(BaseTask):
    """ Parse config and run script"""

    ignore_result = True
    retry = False
    eta = timedelta(seconds=4)
    expires = 3600
    soft_time_limit = 3600
    time_limit = soft_time_limit + 2

    def __init__(self):
        super(BaseTask, self).__init__()

    def fetch_data_from_queue(self):
        time.sleep(8)
        print('fetch_data_from worker queue')


# celery_app.register_task(ControllerBaseTask())
# celery_app.register_task(WorkerBaseTask())


def run_job(base=ControllerBaseTask):
    pass


@celery_app.task(base=WorkerBaseTask)
def run_script(*args, **kwargs):
    self = run_script

    try:
        print('run script')
        self.fetch_data_from_queue()
    except SoftTimeLimitExceeded:
        logger.error('SoftTimeLimitExceeded !')
    finally:
        logger.info('run script task finished')
