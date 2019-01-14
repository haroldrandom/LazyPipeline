import json
from datetime import timedelta

from django_redis import get_redis_connection
from celery.utils.log import get_task_logger

from LazyPipeline import celery_app
from dispatcher.tasks.message import MessageType


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
    soft_time_limit = 3600
    time_limit = soft_time_limit + 5
    expires = soft_time_limit

    def config(self, node):
        self.upstreams = frozenset(node.get('upstreams')) or set()

        self.downstreams = frozenset(node.get('downstreams')) or set()

        self.node_id = node['node_id']

        self.finished_up_cnt = 0

        self.timeout_up_cnt = 0

        # set a connection to upstream message queue for reading
        if not hasattr(self, '_mq_conn'):
            self._mq_conn = get_redis_connection('LazyPipeline')

        self.configured = True

    def _recv_message(self):
        if not hasattr(self, 'configured') or not self.configured:
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
        if not hasattr(self, 'configured') or not self.configured:
            raise Exception("Invoke config() first")

        self._mq_conn.lpush(downstream, message)
        self._mq_conn.expire(downstream, self.expires)

    def pull_data(self):
        raise NotImplementedError("Not implemented yet")

    def push_data(self):
        raise NotImplementedError("Not implemented yet")

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

    @staticmethod
    def _validate_message(msg):
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
        return True


class MessageEmitterWorker(WorkerBaseTask):
    """ """

    def config(self, node_conf):
        super(MessageEmitterWorker, self).config(node_conf)

    def pull_data(self):
        """ Invoke and get nothing """
        pass

    def push_data(self, message_body):
        msg = self._pack_message(message_body)

        for down in self.downstreams:
            self._send_message(down, msg)


class MultiUpstreamWorkerTask(WorkerBaseTask):
    """ Worker which can receive data from multiple uptreams
    in the same time. Could be useful if you want ot join/merge
    """

    def config(self, node):
        super(MultiUpstreamWorkerTask, self).config(node)

        self.upstream_data = {}
        for up in self.upstreams:
            self.upstream_data[up] = {'data': []}

    def pull_data(self):
        """ Invoked once and return all data from upstreams """

        while True:
            msg = self._recv_message()

            if msg['sender'] in self.upstreams:
                up = msg['sender']
                self.upstream_data[up]['data'].append(msg['data'])

                if msg['status']['is_finished'] is True:
                    self.finished_up_cnt += 1
                if msg['status']['is_finished'] is True:
                    self.timeout_up_cnt += 1

            complete_cnt = self.finished_up_cnt + self.timeout_up_cnt
            if complete_cnt >= len(self.upstreams):
                return self.upstream_data



