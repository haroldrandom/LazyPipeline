import os
import shlex
import json
import copy
import subprocess

from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from LazyPipeline import celery_app
from engine.tasks.base import MessageEmitterWorker
from engine.tasks.base import BatchDataWorker
from engine.tasks.base import StreamDataWorker


logger = get_task_logger(__name__)


@celery_app.task(base=MessageEmitterWorker)
def run_message_emitter_worker(conf):
    self = run_message_emitter_worker

    try:
        self.config(conf)
    except Exception:
        return

    try:
        for i in range(5):
            self.push_data(i)
    except SoftTimeLimitExceeded:
        logger.error('TASK [%s] [id=%s]- %s' % (self.name, self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('TASK [%s] [id=%s] - %s' % (self.name, self.node_id, 'FINISHED'))
        self.send_finished_message()


@celery_app.task(base=BatchDataWorker)
def run_batch_data_worker(conf):
    self = run_batch_data_worker

    try:
        self.config(conf)
    except Exception:
        self.send_finished_message()
        return

    try:
        messages = self.pull_data()

        arg_seq = 1

        new_env = copy.deepcopy(dict(os.environ))   # TODO env must be manicured

        for sender, body in messages.items():
            k = 'ARG_{0}'.format(arg_seq)
            new_env[k] = json.dumps(body['data'])
            arg_seq += 1

        cmd = shlex.split('echo 123')
        stdoutput = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, env=new_env)
        stdoutput = stdoutput.decode('utf-8')

        self.push_data(stdoutput)
    except subprocess.CalledProcessError:
        logger.error('TASK [%s] [id=%s] %s' % (
            self.name, self.node_id, 'RET_CODE ERROR'))
    except SoftTimeLimitExceeded:
        logger.error('TASK [%s] [id=%s] %s' % (
            self.name, self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('TASK [%s] [id=%s] %s' % (
            self.name, self.node_id, 'FINISHED'))
        self.send_finished_message()


@celery_app.task(base=StreamDataWorker)
def run_stream_data_worker(conf):
    self = run_stream_data_worker

    try:
        self.config(conf)
    except Exception:
        self.send_finished_message()
        return

    try:
        # while True:
        #     msg = self.pull_data()
        pass
    except SoftTimeLimitExceeded:
        logger.error('TASK [%s] [id=%s]- %s' % (self.name, self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('TASK [%s] [id=%s] - %s' % (self.name, self.node_id, 'FINISHED'))
        self.send_finished_message()
