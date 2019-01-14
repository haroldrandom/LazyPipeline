from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from LazyPipeline import celery_app
from dispatcher.tasks.base import MessageEmitterWorker
from dispatcher.tasks.base import MultiUpstreamWorkerTask


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
            self.push_data([i])
    except SoftTimeLimitExceeded:
        logger.error(self.node_id + ' Timeout !')
        self.send_timeout_message()
    else:
        logger.info(self.node_id + ' task finished')
        self.send_finished_message()


@celery_app.task(base=MultiUpstreamWorkerTask)
def run_multi_upstream_worker(conf):
    self = run_multi_upstream_worker

    try:
        self.config(conf)
    except Exception:
        return

    try:
        msg = self.pull_data()
        logger.info(msg)
    except SoftTimeLimitExceeded:
        logger.error(self.name + ' Timeout !')
        self.send_timeout_message()
    finally:
        logger.info(self.name + ' task finished')
        self.send_finished_message()