import os
import json
import shlex
import copy
import subprocess

from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from LazyPipeline import celery_app
from engine.tasks.base import BatchDataWorker
from engine.tasks.base import StreamDataWorker
from engine.tasks.signal import FinishedSignal


logger = get_task_logger(__name__)


@celery_app.task(base=BatchDataWorker)
def batch_data_worker(conf, reserve_output=False):
    self = batch_data_worker

    try:
        self.init(conf)  # init this worker
    except Exception:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'CONFIG ERROR'))
        self.destroy()
        return

    output = None
    try:
        messages = self.pull_data() or {}

        new_env = copy.deepcopy(dict(os.environ))   # TODO env must be manicured

        for idx, body in enumerate(messages.items()):
            k = 'ARG_{0}'.format(idx + 1)
            new_env[k] = json.dumps(body[1]['data'])

        cmd = shlex.split('python {0}'.format(self.script_file))
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, env=new_env)
        output = output.decode('utf-8') or None

        self.push_data(output)
        self.send_finished_message()    # send finished message
    except subprocess.CalledProcessError as exc:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'RET_CODE ERROR'))
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, exc.output.decode('utf-8')))
    except SoftTimeLimitExceeded:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    except Exception as e:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'UNHANLDE ERROR'))
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, str(e)))
    finally:
        logger.info('[TASK_ID=%s] - %s' % (self.node_id, 'FINISHED'))
        self.destroy()  # delete external resource

        reserve_output = output if reserve_output is True else None
        return {'job_id': self.job_id,
                'node_id': self.node_id,
                'output': reserve_output,
                'state': self.worker_state}


@celery_app.task(base=StreamDataWorker)
def stream_data_worker(conf, reserve_output=False):
    self = stream_data_worker

    try:
        self.init(conf)  # init this worker
    except Exception:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'CONFIG ERROR'))
        self.destroy()
        return

    message_puller = self.pull_data()
    try:
        while True:
            messages = next(message_puller) or {}
            print('message=', messages)

            new_env = copy.deepcopy(dict(os.environ))   # TODO env must be manicured

            for idx, body in enumerate(messages.items()):
                k = 'ARG_{0}'.format(idx + 1)
                new_env[k] = json.dumps(body[1]['data'])

            cmd = shlex.split('python {0}'.format(self.script_file))
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, env=new_env)
            output = output.decode('utf-8') or None

            print('output=', output)

            self.push_data(output)
    except FinishedSignal:
        self.send_finished_message()
    except subprocess.CalledProcessError as exc:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'RET_CODE ERROR'))
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, exc.output.decode('utf-8')))
    except SoftTimeLimitExceeded:
        logger.error('TASK [%s] [id=%s]- %s' % (
            self.name, self.node_id, 'TIMEOUT'))
        self.send_timeout_message()
    except Exception as e:
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, 'UNHANLDE ERROR'))
        logger.error('[TASK_ID=%s] - %s' % (self.node_id, str(e)))
    finally:
        logger.info('[TASK_ID=%s] - %s' % (self.node_id, 'FINISHED'))
        self.destroy()  # delete external resource

        return self.statistics
