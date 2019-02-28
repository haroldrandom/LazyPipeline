import os
import json
import shutil
import stat
from datetime import timedelta
from collections import deque

from django.conf import settings
from django_redis import get_redis_connection
from celery.utils.log import get_task_logger

from LazyPipeline import celery_app
from engine.tasks import worker_states
from engine.tasks.config import WorkerConfig
from engine.tasks.message import MessageType
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

    ignore_result = False

    retry = False

    track_started = True

    eta = timedelta(seconds=4)

    soft_time_limit = 3600

    time_limit = soft_time_limit + 5

    expires = 300

    worker_home = settings.BASE_DIR

    worker_state = worker_states.INIT

    def config(self, node_conf):
        self.worker_conf = WorkerConfig.config_from_object(node_conf)

        self.worker_dir = '{base}/worker_containers/{job}/{node}'.format(
            base=self.worker_home, job=self.job_id, node=self.node_id)
        self.worker_file = self.worker_dir + '/{node_id}.py'.format(
            node_id=self.node_id)

        self.finished_ups = UniqueKeySerialCounter(allowed_keys=self.upstreams)

        self.timeout_ups = UniqueKeySerialCounter(allowed_keys=self.upstreams)

        self._recv_message_count = 0
        self._recv_ctrl_message_count = 0
        self._recv_data_message_count = 0

        self._sent_message_count = 0
        self._sent_ctrl_message_count = 0
        self._sent_data_message_count = 0

        # about sender config
        self._sender_buffer_size = self.worker_conf.sender_buffer_size
        self._sender_separator = self.worker_conf.sender_separator
        self._sender_buffer = deque()

        # set a connection to upstream message queue for reading
        if not hasattr(self, '_mq_conn'):
            self._mq_conn = get_redis_connection('LazyPipeline')

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

        self.worker_state = worker_states.READY

    def destroy(self):
        """ delete temporary executable file and dir """
        shutil.rmtree(self.worker_dir, ignore_errors=True)

    @property
    def statistics(self):
        s = {
            'job_id': self.job_id,
            'node_id': self.node_id,
            'recv_message_count': self._recv_message_count,
            'sent_message_count': self._sent_message_count,
            'recv_ctrl_message_count': self._recv_ctrl_message_count,
            'recv_data_message_count': self._recv_data_message_count,
            'sent_ctrl_message_count': self._sent_ctrl_message_count,
            'sent_data_message_count': self._sent_data_message_count,
            'state': str(self.worker_state)
        }
        return s

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
        if self.worker_state < worker_states.READY:
            raise Exception("Invoke init() first")

        msg = self._mq_conn.brpop(self.node_id, timeout=self.expires)
        msg = json.loads(msg[1])    # msg is tuple(node_id, message)

        while not self._validate_message(msg):
            logger.warn('received invalid msg: {0}'.format(msg))
            msg = self._mq_conn.brpop(self.node_id, timeout=self.expires)

        self._recv_message_count += 1

        if self.node_id in ['33', '44']:
            from pprint import pprint
            print('-' * 30 + 'recv_msg' + '-' * 30)
            pprint(str(msg))

        return msg

    def _pack_message(self, message_body, type_=MessageType.DATA):
        c = {
            'sender': self.node_id,
            'data': message_body,
            'type': type_,
            'state': self.worker_state
        }
        return json.dumps(c)

    def _send_message(self, downstream, message, message_type):
        if self.worker_state < worker_states.READY:
            raise Exception("Invoke init() first")

        message = self._pack_message(message, message_type)

        if message_type == MessageType.DATA:
            self._sent_data_message_count += 1
        elif message_type == MessageType.CTRL:
            self._sent_ctrl_message_count += 1
        else:
            raise Exception('Unsupported message type: ' + message_type)

        self._sent_message_count += 1

        if self.node_id == str(33):
            print('sent=' + str(message))

        self._mq_conn.lpush(downstream, message)
        self._mq_conn.expire(downstream, self.expires)

    def _clear_buffer(self):
        while len(self._sender_buffer) > 0:
            msg = self._sender_buffer.popleft()

            for down in self.downstreams:
                self._send_message(down, msg, MessageType.DATA)

    def _validate_message(self, msg):
        try:
            if (
                (not msg) or
                ('sender' not in msg) or ('data' not in msg) or
                ('type' not in msg) or ('state' not in msg)
            ):
                raise Exception('invalid message')

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
        raise NotImplementedError("pull_data() subcalss not implemented yet")

    def push_data(self, message_body):
        if message_body is None:
            raise AttributeError('message_body must not be None')

        if isinstance(self._sender_separator, str):
            tmp = message_body.strip().split(self._sender_separator)
            self._sender_buffer.extend(tmp)
        else:
            self._sender_buffer.append(message_body)

        if (
            self._sender_buffer_size > 0 and
            len(self._sender_buffer) >= self._sender_buffer_size
        ):
            t = self._sender_buffer_size

            while t > 0 and len(self.downstreams) > 0:
                msg = self._sender_buffer.popleft()
                for down in self.downstreams:
                    self._send_message(down, msg, MessageType.DATA)
                t -= 1

    def send_finished_message(self):
        self._clear_buffer()

        if self.worker_state == worker_states.RUNNING:
            self.worker_state = worker_states.FINISHED

        for down in self.downstreams:
            self._send_message(down, [], MessageType.CTRL)


# class ConvergenceStreamDataWorker(StreamDataWorker):
#     """ Worker that receive data from upstream(s) at a time.
#     Could be useful in common filter processing
#     """

#     def config(self, node_conf):
#         pass

#     def pull_data(self):
#         pass
