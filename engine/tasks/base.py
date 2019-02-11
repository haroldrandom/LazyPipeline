import os
import json
import shutil
import stat
from datetime import timedelta
from collections import deque
from collections import OrderedDict

from django.conf import settings
from django_redis import get_redis_connection
from celery.utils.log import get_task_logger

from LazyPipeline import celery_app
from engine.tasks.config import WorkerConfig
from engine.tasks.message import MessageType
from engine.tasks.signal import FinishSignal
from engine.tasks.utils import UniqueKeySerialCounter


logger = get_task_logger(__name__)


class BaseTask(celery_app.Task):
    """ Common task services"""

    @property
    def name(self):
        return '.'.join([self.__module__, self.__name__])


class ControllerBaseTask(BaseTask):
    """ Parse config file and dispatch them to worker
    """

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
    """ Parse config and run script
    """

    ignore_result = True
    retry = False
    eta = timedelta(seconds=4)
    soft_time_limit = 3600
    time_limit = soft_time_limit + 5
    expires = 300

    prefix = settings.BASE_DIR

    def config(self, node_conf):
        self.worker_conf = WorkerConfig.config_from_object(node_conf)

        self.worker_dir = '{base}/worker_containers/{job}/{node}'.format(
            base=self.prefix, job=self.job_id, node=self.node_id)
        self.worker_file = self.worker_dir + '/{node_id}.py'.format(
            node_id=self.node_id)

        self.finished_ups = UniqueKeySerialCounter(allowed_keys=self.upstreams)

        self.timeout_ups = UniqueKeySerialCounter(allowed_keys=self.upstreams)

        # set a connection to upstream message queue for reading
        if not hasattr(self, '_mq_conn'):
            self._mq_conn = get_redis_connection('LazyPipeline')

        self._configured = True

    def init(self, node_conf):
        # set worker runtime configuration
        self.config(node_conf)

        # prepare temporary executable dir and file
        try:
            os.makedirs(self.worker_dir, mode=0o700, exist_ok=True)

            with open(self.worker_file, 'w') as fd:
                fd.write(self.script)
            os.chmod(self.worker_file, stat.S_IRWXU)
        except Exception as e:
            logger.warn('can not create worker script: %s' % str(e))
            self.destroy()

    def destroy(self):
        """ delete temporary executable file and dir """
        shutil.rmtree(self.worker_dir, ignore_errors=True)

    @property
    def job_id(self):
        return self.worker_conf.job_id

    @property
    def node_id(self):
        return self.worker_conf.node_id

    @property
    def upstreams(self):
        return self.worker_conf.upstreams

    @property
    def downstreams(self):
        return self.worker_conf.downstreams

    @property
    def script(self):
        return self.worker_conf.script

    @property
    def script_file(self):
        return self.worker_file

    def _recv_message(self):
        if not hasattr(self, '_configured') or not self._configured:
            raise Exception("Invoke config() first")

        msg = self._mq_conn.brpop(self.node_id, timeout=self.expires)
        msg = json.loads(msg[1])    # msg is tuple(node_id, message)

        while not self._validate_message(msg):
            logger.warn('received invalid msg: {0}'.format(msg))
            msg = self._mq_conn.brpop(self.node_id, timeout=self.expires)

        return msg

    def _pack_message(self, message_body,
                      type_=MessageType.DATA,
                      is_finished=False, is_timeout=False):
        c = {
            'sender': self.node_id,
            'data': message_body,
            'type': type_,
            'status': {'is_finished': is_finished, 'is_timeout': is_timeout}
        }
        return json.dumps(c)

    def _send_message(self, downstream, message):
        if not hasattr(self, '_configured') or not self._configured:
            raise Exception("Invoke config() first")

        self._mq_conn.lpush(downstream, message)
        self._mq_conn.expire(downstream, self.expires)

    def _validate_message(self, msg):
        try:
            if (
                (not msg) or
                ('sender' not in msg) or ('data' not in msg) or
                ('type' not in msg) or ('status' not in msg)
            ):
                raise Exception('invalid message')

            # check existence
            msg['status']['is_finished'], msg['status']['is_timeout']
        except Exception as e:
            logger.warn(e)
            logger.info('received invalid message %s' % str(msg))
            return False

        try:
            if msg['sender'] not in self.upstreams:
                raise Exception('receive data from an uninvited sender')
        except Exception as e:
            logger.warn(e)
            return False

        return True

    def pull_data(self):
        raise NotImplementedError("Subcalss not implemented yet")

    def push_data(self, message_body):
        raise NotImplementedError("Subclass not implemented yet")

    def send_timeout_message(self):
        for down in self.downstreams:
            msg = self._pack_message(
                [], MessageType.CTRL, is_finished=True, is_timeout=True)
            self._send_message(down, msg)

    def send_finished_message(self):
        for down in self.downstreams:
            msg = self._pack_message(
                [], MessageType.CTRL, is_finished=True, is_timeout=False)
            self._send_message(down, msg)


class BatchDataWorker(WorkerBaseTask):
    """ Worker that can receive all data from uptream(s) in the same time.
    Could be useful if you want ot join/merge/convergence.
    """

    def config(self, node_conf):
        super().config(node_conf)

        self.upstream_data = OrderedDict()
        for up in self.upstreams:
            self.upstream_data[up] = {'sender': up, 'data': []}

    def pull_data(self):
        """ Invoke once and return all data from upstreams.

        return self.upstreams_data if self.upstreams exist,
        otherwise None
        """

        while len(self.upstreams) > 0:
            msg = self._recv_message()

            up = msg['sender']

            if msg['type'] == MessageType.CTRL:
                if msg['status']['is_finished'] is True:
                    self.finished_ups[up] += 1
                if msg['status']['is_timeout'] is True:
                    self.timeout_ups[up] += 1

                complete_cnt = len(self.finished_ups) + len(self.timeout_ups)
                if complete_cnt >= len(self.upstreams):
                    return self.upstream_data
            else:
                self.upstream_data[up]['data'].append(msg['data'])

        return self.upstream_data

    def push_data(self, message_body):
        msg = self._pack_message(message_body)

        for down in self.downstreams:
            self._send_message(down, msg)


class StreamDataWorker(WorkerBaseTask):
    """ Worker that receive one line of data from upstream(s) at a time.
    Could be useful in stream processing.
    """

    def config(self, node_conf):
        super().config(node_conf)

        self.upstream_data = {}
        for up in self.upstreams:
            self.upstream_data[up] = {'data': deque()}
            self.ret_data[up] = {'data': []}

    def pull_data(self):
        """ Invoke once and return one line of data at a time.
        raise Finished signal if all upstreams are finished
        """
        while True:
            msg = self._recv_message()

            up = msg['sender']

            if msg['type'] == MessageType.CTRL:
                if msg['status']['is_finished'] is True:
                    self.finished_ups[up] += 1
                if msg['status']['is_timeout'] is True:
                    self.timeout_ups[up] += 1
            else:
                self.upstream_data[up]['data'].append(msg)

            complete_cnt = len(self.finished_ups) + len(self.timeout_ups)
            if complete_cnt >= len(self.upstreams):
                raise FinishSignal()  # raise finished signal

            useable_ups = list(filter(lambda up: len(self.upstream_data[up]['data']) > 0,
                                      self.upstream_data))
            if len(useable_ups) + complete_cnt < len(self.upstreams):
                continue

            for up in useable_ups:
                try:
                    self.ret_data[up]['data'].append(self.upstream_data[up]['data'].popleft())
                except Exception:
                    self.ret_data[up]['data'] = []
            return self.ret_data


class ConvergenceStreamDataWorker(StreamDataWorker):
    """ Worker that receive data from upstream(s) at a time.
    Could be useful in common filter processing
    """

    def config(self, node_conf):
        pass

    def pull_data(self):
        pass
