import uuid
from django.test import TestCase

from dispatcher.tasks import run_message_emitter_worker
from dispatcher.tasks import run_batch_data_worker


class MultiUpstreamWorkerTest(TestCase):
    def setUp(self):
        super(MultiUpstreamWorkerTest, self).setUp()

    def test1(self):
        """ Test with only one upstream"""

        job_id = str(uuid.uuid4())

        w_conf_1 = {
            'job_id': job_id,
            'node_id': str(uuid.uuid4()),
            'upstreams': [],
            'downstreams': []
        }

        w_conf_2 = {
            'job_id': job_id,
            'node_id': str(uuid.uuid4()),
            'upstreams': [],
            'downstreams': [],
        }

        w_conf_1['downstreams'].append(w_conf_2['node_id'])
        w_conf_2['upstreams'].append(w_conf_1['node_id'])

        run_message_emitter_worker.apply_async(args=[w_conf_1])
        run_batch_data_worker.apply_async(args=[w_conf_2])

    def test2(self):
        """ Test with two upstreams"""

        job_id = str(uuid.uuid4())

        w_conf_1 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [],
                    'downstreams': []}
        w_conf_2 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [],
                    'downstreams': []}
        w_conf_3 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [],
                    'downstreams': []}

        w_conf_1['downstreams'].append(w_conf_3['node_id'])
        w_conf_2['downstreams'].append(w_conf_3['node_id'])

        w_conf_3['upstreams'].append(w_conf_1['node_id'])
        w_conf_3['upstreams'].append(w_conf_2['node_id'])

        print(w_conf_1)
        print(w_conf_2)
        print(w_conf_3)

        run_message_emitter_worker.apply_async(args=[w_conf_1])
        run_message_emitter_worker.apply_async(args=[w_conf_2])
        run_batch_data_worker.apply_async(args=[w_conf_3])
