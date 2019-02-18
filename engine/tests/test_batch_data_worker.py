import uuid

from django.test import TestCase
from django.conf import settings

from engine.tasks import run_batch_data_worker
from engine.tasks.config import WorkerConfig


class BatchDataWorkerTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

        with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
            cls.ts_emitter_3s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
            cls.ts_emitter_5s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
            cls.ts_emitter_10s_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_1ups.py') as fd:
            cls.batch_data_worker_1ups_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_2ups.py') as fd:
            cls.batch_data_worker_2ups_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_3ups.py') as fd:
            cls.batch_data_worker_3ups_script = fd.read()

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
        self.assertEqual(r5['state'], 'FINISHED')

        r6 = worker6_task.get()
        self.assertEqual(worker6_task.state, 'SUCCESS')
        self.assertEqual(r6['state'], 'FINISHED')


class BatchDataWorkerTimeoutTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

        with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
            cls.ts_emitter_3s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
            cls.ts_emitter_5s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
            cls.ts_emitter_10s_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_1ups.py') as fd:
            cls.batch_data_worker_1ups_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_2ups.py') as fd:
            cls.batch_data_worker_2ups_script = fd.read()

        with open(cls.scripts_home + 'batch_data_worker_3ups.py') as fd:
            cls.batch_data_worker_3ups_script = fd.read()

    def test_timeout_0up_0down(self):
        """
        Test single worker timeout, and without downstream.
        The output will be left behind until timeout

        (worker1)
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_10s_script)

        worker1_task = run_batch_data_worker.apply_async(
            args=[worker1_conf.to_dict],
            kwargs={'reserve_output': True},
            soft_time_limit=2)

        r1 = worker1_task.get()

        self.assertEqual(worker1_task.state, 'SUCCESS')
        self.assertEqual(r1['state'], 'TIMEOUT')
        self.assertIsNone(r1['output'])

    def test_timeout_1up_0down(self):
        """
        Test worker when worker2 timeout but worker1 doesn't,
        so worker1's ouput will be left behind in Message Queue until messgae timeout

        (worker1) --> (worker2)
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_downstream(worker2_conf)
        worker2_conf.add_upstream(worker1_conf)

        worker1_task = run_batch_data_worker.apply_async(
            args=[worker1_conf.to_dict],
            kwargs={'reserve_output': True})

        worker2_task = run_batch_data_worker.apply_async(
            args=[worker2_conf.to_dict],
            kwargs={'reserve_output': True},
            soft_time_limit=2)

        r1 = worker1_task.get()
        r2 = worker2_task.get()

        self.assertEqual(r1['state'], 'FINISHED')
        self.assertEqual(r2['state'], 'TIMEOUT')
        self.assertIsNotNone(r1['output'])
        self.assertIsNone(r2['output'])

    def test_timeout_1up_0down_2(self):
        """
        Test worker when worker1 timeout but worker2 doesn't

        (worker1) --> (worker2)
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_5s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.batch_data_worker_1ups_script)

        worker1_conf.add_downstream(worker2_conf)
        worker2_conf.add_upstream(worker1_conf)

        worker1_task = run_batch_data_worker.apply_async(
            args=[worker1_conf.to_dict],
            kwargs={'reserve_output': True},
            soft_time_limit=2)

        worker2_task = run_batch_data_worker.apply_async(
            args=[worker2_conf.to_dict],
            kwargs={'reserve_output': True})

        r1 = worker1_task.get()
        r2 = worker2_task.get()

        self.assertEqual(r1['state'], 'TIMEOUT')
        self.assertIsNone(r1['output'])

        self.assertEqual(r2['state'], 'FINISHED')
        self.assertIsNone(r2['output'])

    def test_timeout_2ups_1down(self):
        """
        Test worker with 2ups and 1down

        (worker1) \
                   —> (worker3) -> (worker4) -> discard output
        (worker2) /

        worker3 will discard worker2's output because it's timeout,
        and only worker1's result pass down
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

        worker1_task = run_batch_data_worker.apply_async(
            args=[worker1_conf.to_dict],
            kwargs={'reserve_output': True})
        worker2_task = run_batch_data_worker.apply_async(
            args=[worker2_conf.to_dict],
            kwargs={'reserve_output': True},
            soft_time_limit=2)
        worker3_task = run_batch_data_worker.apply_async(
            args=[worker3_conf.to_dict],
            kwargs={'reserve_output': True})
        worker4_task = run_batch_data_worker.apply_async(args=[worker4_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(r1['state'], 'FINISHED')
        r1_out = r1['output'].split('\n')

        r2 = worker2_task.get()
        self.assertEqual(r2['state'], 'TIMEOUT')
        self.assertIsNone(r2['output'])

        r3 = worker3_task.get()
        self.assertEqual(r3['state'], 'FINISHED')
        r3_out = r3['output'].split('\n')

        # worker3 output should contains only worker1's output
        self.assertEqual(len(r1_out), len(r3_out))
        for line1 in r1_out:
            f = False
            for line3 in r3_out:
                if line1 in line3:
                    f = True
                    break
            self.assertTrue(f)

        r4 = worker4_task.get()
        self.assertEqual(r4['state'], 'FINISHED')

    def test_timeout_2ups_1down_2(self):
        """
        Test worker with 2ups and 1down

        (worker1) \
                   —> (worker3) -> (worker4) -> discard output
        (worker2) /

        worker3 will timeout before any upstreams' output arrive
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

        worker1_task = run_batch_data_worker.apply_async(
            args=[worker1_conf.to_dict],
            kwargs={'reserve_output': True})
        worker2_task = run_batch_data_worker.apply_async(
            args=[worker2_conf.to_dict])
        worker3_task = run_batch_data_worker.apply_async(
            args=[worker3_conf.to_dict],
            kwargs={'reserve_output': True},
            soft_time_limit=2)
        worker4_task = run_batch_data_worker.apply_async(
            args=[worker4_conf.to_dict],
            kwargs={'reserve_output': True})

        r1 = worker1_task.get()
        self.assertEqual(r1['state'], 'FINISHED')
        self.assertIsNotNone(r1['output'])

        r2 = worker2_task.get()
        self.assertEqual(r2['state'], 'FINISHED')

        r3 = worker3_task.get()
        self.assertEqual(r3['state'], 'TIMEOUT')
        self.assertIsNone(r3['output'])

        r4 = worker4_task.get()
        print(r4)
        self.assertEqual(r4['state'], 'FINISHED')
        self.assertIsNone(r4['output'])


class BatchDataWorkerExpiresTest(TestCase):
    def setUp(self):
        pass
