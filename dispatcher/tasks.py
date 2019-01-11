import uuid
from datetime import timedelta

from django_redis import get_redis_connection
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
    time_limit = soft_time_limit + 5

    def config(self, node):
        # parse node config to fill up self.upstreams
        self.upstreams = []

        # parse node config to fill up self.downstreams
        self.downstreams = []

        self.my_name = str(uuid.uuid4())

        self.finished_flag_cnt = 0
        self.timeout_flag_cnt = 0

        # hold a connection to upstream message queue for reading
        if not hasattr(self, '_input_queue_conn'):
            self._input_queue_conn = get_redis_connection('LazyPipeline')

        self.configed = False

    @staticmethod
    def valid_msg(msg):
        try:
            if (
                (not msg) or
                (not hasattr(msg, 'tag')) or
                (not hasattr(msg, 'status')) or
                (not hasattr(msg, 'ttye'))
            ):
                raise

            msg['status']['is_finished'], msg['status']['is_timeout']
        except Exception:
            logger.info('received invalid message %s' % str(msg))
            return False
        return True

    def get_message(self):
        print('polling data from queue: {0}'.format(self.my_name))

        msg = self._input_queue_conn.brpop(self.my_name, timeout=10)

        while not self.valid_msg(msg):
            print('receive msg: {0}'.format(msg))
            msg = self._input_queue_conn.brpop(self.my_name, timeout=10)

        return msg

    def pull_data_from_upstream(self):
        raise NotImplementedError("this method hasn't been implemented")


class MultiUpstreamWorkerTask(WorkerBaseTask):
    """ Worker which can receive data from multiple uptreams
    in the same time. Could be useful if you want ot join/merge
    """

    def config(self, node):
        super(MultiUpstreamWorkerTask, self).config(node)

        self.upstream_data = {}
        for up in self.upstreams:
            self.upstream_data[up] = {}

        self.configed = True

    def pull_data_from_upstream(self):
        """ Invoked once and return all data from upstreams """

        if not hasattr(self, 'configed') or not self.configed:
            raise Exception("Invoke config() first")

        while True:
            self.get_message()


def run_job(base=ControllerBaseTask):
    pass


@celery_app.task(base=MultiUpstreamWorkerTask)
def run_script(worker_conf):
    self = run_script

    worker_conf = {}

    try:
        self.config(worker_conf)
    except Exception:
        return

    try:
        self.pull_data_from_upstream()
    except SoftTimeLimitExceeded:
        logger.error(self.name + ' Timeout !')
    finally:
        logger.info(self.name + ' task finished')
