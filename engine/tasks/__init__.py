import os
import shlex
import copy
import subprocess

from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from LazyPipeline import celery_app
from engine.tasks.base import BatchDataWorker
from engine.tasks.base import StreamDataWorker


logger = get_task_logger(__name__)


@celery_app.task(base=BatchDataWorker)
def run_batch_data_worker(conf):
    self = run_batch_data_worker

    # init this worker
    try:
        self.init(conf)
    except Exception:
        logger.error('[TASK_ID=%s]- %s' % (self.node_id, 'CONFIG ERROR'))
        self.destroy()
        return

    try:
        messages = self.pull_data() or {}

        new_env = copy.deepcopy(dict(os.environ))   # TODO env must be manicured
        import json

        for idx, body in enumerate(messages.items()):
            k = 'ARG_{0}'.format(idx + 1)
            new_env[k] = json.dumps(body[1]['data'])

        cmd = shlex.split('python {0}'.format(self.script_file))
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, env=new_env)
        output = output.decode('utf-8')

        self.push_data(output)
    except subprocess.CalledProcessError:
        logger.error('[TASK_ID=%s]- %s' % (self.node_id, 'RET_CODE ERROR'))
    except SoftTimeLimitExceeded:
        logger.error('[TASK_ID=%s]- %s' % (self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('[TASK_ID=%s]- %s' % (self.node_id, 'FINISHED'))
        self.destroy()  # delete external resource
        self.send_finished_message()    # send finished message


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
        logger.error('TASK [%s] [id=%s]- %s' % (
            self.name, self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('TASK [%s] [id=%s] - %s' % (
            self.name, self.node_id, 'FINISHED'))
        self.send_finished_message()
