import uuid

from django.test import TestCase
from django.conf import settings

from engine.tasks import run_batch_data_worker
from engine.tasks.config import WorkerConfig


class BatchDataWorkerTest(TestCase):
    def setUp(self):
        super(BatchDataWorkerTest, self).setUp()

        self.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

        with open(self.scripts_home + 'ts_emitter_every_3s.py') as fd:
            self.ts_emitter_3s_script = fd.read()

        with open(self.scripts_home + 'ts_emitter_every_5s.py') as fd:
            self.ts_emitter_5s_script = fd.read()

        with open(self.scripts_home + 'ts_emitter_every_10s.py') as fd:
            self.ts_emitter_10s_script = fd.read()

        with open(self.scripts_home + 'ts_emitter_every_30s.py') as fd:
            self.ts_emitter_30s_script = fd.read()

        with open(self.scripts_home + 'batch_data_worker_1ups.py') as fd:
            self.batch_data_worker_1ups_script = fd.read()

        with open(self.scripts_home + 'batch_data_worker_2ups.py') as fd:
            self.batch_data_worker_2ups_script = fd.read()

        with open(self.scripts_home + 'batch_data_worker_3ups.py') as fd:
            self.batch_data_worker_3ups_script = fd.read()

    def test_1up_0down(self):
        """
        Test worker with 1 upstream and no downstream:

        (worker1) --> (worker2) --> discard output

        Besides, worker2 script is not interested to handle worker1's output
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_downstream(worker2_conf)

        worker2_conf.add_upstream(worker1_conf)

        worker_task1 = run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker_task2 = run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])

        r1 = worker_task1.get()
        self.assertEqual(worker_task1.state, 'SUCCESS')
        self.assertEqual(r1['state'], 'FINISHED')
        self.assertEqual(r1['job_id'], worker1_conf.job_id)
        self.assertEqual(r1['node_id'], worker1_conf.node_id)

        r2 = worker_task2.get()
        self.assertEqual(worker_task2.state, 'SUCCESS')
        self.assertEqual(r2['state'], 'FINISHED')
        self.assertEqual(r2['job_id'], worker2_conf.job_id)
        self.assertEqual(r2['node_id'], worker2_conf.node_id)

    def test_2ups_0down(self):
        """
        Test worker with 2 upstreams and no downstream:

        (worker1) \
                   (worker3) --> discard output
        (worker2) /

        Besides, worker3 script handles upstreams' output
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_5s_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstream(worker1_conf)
        worker3_conf.add_upstream(worker2_conf)

        worker1_task = run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, 'SUCCESS')
        self.assertEqual(r1['state'], 'FINISHED')

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, 'SUCCESS')
        self.assertEqual(r2['state'], 'FINISHED')

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, 'SUCCESS')
        self.assertEqual(r3['state'], 'FINISHED')

    def test_2ups_1down(self):
        """
        Test worker with two upstreams and one downstream:

        (worker1) \
                   —- (worker3) - (worker4) -> discard output
        (worker2) /
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_10s_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_2ups_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_1ups_script)

        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstream(worker3_conf)

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = run_batch_data_worker.apply_async(args=[worker4_conf.to_dict])

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, 'SUCCESS')
        self.assertEqual(r3['state'], 'FINISHED')

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, 'SUCCESS')
        self.assertEqual(r4['state'], 'FINISHED')

    def test_3ups_2downs(self):
        """
        Test worker with three upstreams and two downstream:

        (worker1) \
                   \               /  (dummy-worker1)
        (worker2)   -> (worker4) ->
                   /               \  (dummy-worker2)
        (worker3) /
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_5s_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_3ups_script)
        worker5_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_1ups_script)
        worker6_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_1ups_script)

        worker1_conf.add_downstream(worker4_conf)
        worker2_conf.add_downstream(worker4_conf)
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstreams([worker1_conf, worker2_conf, worker3_conf])
        worker4_conf.add_downstreams([worker5_conf, worker6_conf])

        worker5_conf.add_upstream(worker4_conf)
        worker6_conf.add_upstream(worker4_conf)

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker4_conf.to_dict])
        worker5_task = run_batch_data_worker.apply_async(args=[worker5_conf.to_dict])
        worker6_task = run_batch_data_worker.apply_async(args=[worker6_conf.to_dict])

        r5 = worker5_task.get()
        self.assertEqual(worker5_task.state, 'SUCCESS')
        self.assertTrue(r5['state'], 'FINISHED')

        r6 = worker6_task.get()
        self.assertEqual(worker6_task.state, 'SUCCESS')
        self.assertTrue(r6['state'], 'FINISHED')
