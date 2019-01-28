import os
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
        logger.error(self.node_id + ' Timeout !')
        self.send_timeout_message()
    else:
        logger.info(self.node_id + ' task finished')
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

        new_env = copy.deepcopy(dict(os.environ))
        for msg in messages:
            new_env

        # subprocess.check_output(['python'], stderr=subprocess.STDOUT, env=new_env)
    except subprocess.CalledProcessError:
        logger.error('%s - %s' % (self.name, 'RETURN CODE ERROR'))
    except SoftTimeLimitExceeded:
        logger.error('%s - %s' % (self.name, 'TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('%s - %s' % (self.name, 'TASK FINISHED'))
        self.send_finished_message()


@celery_app.task(base=StreamDataWorker)
def run_stream_data_worker(conf):
    self = run_stream_data_worker

    try:
        self.config(conf)
    except Exception:
        logger.error('%s - %s' % (self.name, 'CONFIG ERROR'))
        self.send_finished_message()
        return

    try:
        while True:
            msg = self.pull_data()
    except SoftTimeLimitExceeded:
        logger.error('%s - %s' % (self.name, ' TIMEOUT'))
        self.send_timeout_message()
    finally:
        logger.info('%s - %s' % (self.name, ' TASK FINISHED'))
        self.send_finished_message()
