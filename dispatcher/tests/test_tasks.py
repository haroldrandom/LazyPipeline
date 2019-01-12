import uuid
from django.test import TestCase

from dispatcher.tasks import run_message_emitter_worker
from dispatcher.tasks import run_multi_upstream_worker


class MultiUpstreamWorkerTest(TestCase):
    def setUp(self):
        super(MultiUpstreamWorkerTest, self).setUp()

    def test1(self):
        job_id = str(uuid.uuid1())

        e_conf_1 = {
            'job_id': job_id,
            'node_id': str(uuid.uuid1()),
            'upstreams': [],
            'downstreams': []
        }

        worker_1 = {
            'job_id': job_id,
            'node_id': str(uuid.uuid1()),
            'upstreams': [],
            'downstreams': [],
        }

        e_conf_1['downstreams'].append(worker_1['node_id'])

        worker_1['upstreams'].append(e_conf_1['node_id'])

        run_message_emitter_worker.apply_async(args=[e_conf_1])

        run_multi_upstream_worker.soft_time_limit = 20
        run_multi_upstream_worker.apply_async(args=[worker_1])
