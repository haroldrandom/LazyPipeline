import uuid
from django.test import TestCase

from engine.tasks import run_message_emitter_worker
from engine.tasks import run_batch_data_worker


class MultiUpstreamWorkerTest(TestCase):
    def setUp(self):
        super(MultiUpstreamWorkerTest, self).setUp()

    def test1(self):
        """ Test worker with one upstream and no downstream"""

        job_id = str(uuid.uuid4())

        worker_1 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}

        worker_2 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}

        worker_1['downstreams'].append(worker_2['node_id'])
        worker_2['upstreams'].append(worker_1['node_id'])

        run_message_emitter_worker.apply_async(args=[worker_1])
        run_batch_data_worker.apply_async(args=[worker_2])

    def test2(self):
        """ Test worker with two upstreams and no downstream"""

        job_id = str(uuid.uuid4())

        worker_1 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_2 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_3 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}

        worker_1['downstreams'].append(worker_3['node_id'])
        worker_2['downstreams'].append(worker_3['node_id'])

        worker_3['upstreams'].append(worker_1['node_id'])
        worker_3['upstreams'].append(worker_2['node_id'])

        run_message_emitter_worker.apply_async(args=[worker_1])
        run_message_emitter_worker.apply_async(args=[worker_2])
        run_batch_data_worker.apply_async(args=[worker_3])

    def test3(self):
        """ Test worker with three upstreams and one downstream"""

        job_id = str(uuid.uuid4())

        worker_1 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_2 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_3 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_4 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}

        worker_1['downstreams'].append(worker_4['node_id'])
        worker_2['downstreams'].append(worker_4['node_id'])
        worker_3['downstreams'].append(worker_4['node_id'])

        worker_4['upstreams'].append(worker_1['node_id'])
        worker_4['upstreams'].append(worker_2['node_id'])
        worker_4['upstreams'].append(worker_3['node_id'])
        worker_4['downstreams'].append(
            'test3_worker_4_dummy_downstream_1 ' + str(uuid.uuid4()))

        run_message_emitter_worker.apply_async(args=[worker_1])
        run_message_emitter_worker.apply_async(args=[worker_2])
        run_message_emitter_worker.apply_async(args=[worker_3])
        run_batch_data_worker.apply_async(args=[worker_4])

    def test4(self):
        """ Test worker with three upstreams and two downstream"""

        job_id = str(uuid.uuid4())

        worker_1 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_2 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_3 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}
        worker_4 = {'job_id': job_id, 'node_id': str(uuid.uuid4()),
                    'upstreams': [], 'downstreams': []}

        worker_1['downstreams'].append(worker_4['node_id'])
        worker_2['downstreams'].append(worker_4['node_id'])
        worker_3['downstreams'].append(worker_4['node_id'])

        worker_4['upstreams'].append(worker_1['node_id'])
        worker_4['upstreams'].append(worker_2['node_id'])
        worker_4['upstreams'].append(worker_3['node_id'])
        worker_4['downstreams'].append(
            'test4_worker_4_dummy_downstream_1 ' + str(uuid.uuid4()))
        worker_4['downstreams'].append(
            'test4_worker_4_dummy_downstream_2 ' + str(uuid.uuid4()))

        run_message_emitter_worker.apply_async(args=[worker_1])
        run_message_emitter_worker.apply_async(args=[worker_2])
        run_message_emitter_worker.apply_async(args=[worker_3])
        run_batch_data_worker.apply_async(args=[worker_4])
