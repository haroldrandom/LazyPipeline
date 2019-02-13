import uuid

from django.conf import settings
from django.test import TestCase

from engine.tasks.config import WorkerConfig


class WorkerConfigTest(TestCase):
    def setUp(self):
        super(WorkerConfigTest, self).setUp()

        self.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

        with open(self.scripts_home + 'ts_emitter_every_3s.py') as fd:
            self.ts_emitter_3s_script = fd.read()

    def test1(self):
        job_id = str(uuid.uuid4())
        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_upstream(worker2_conf)
        worker2_conf.add_downstream(worker1_conf)

        self.assertEqual(len(worker1_conf.upstreams), 1)
        self.assertTrue(worker2_conf.node_id in worker1_conf.upstreams)

        self.assertEqual(len(worker2_conf.downstreams), 1)
        self.assertTrue(worker1_conf.node_id in worker2_conf.downstreams)

    def test2(self):
        job_id = str(uuid.uuid4())
        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_upstream(worker2_conf.node_id)
        worker2_conf.add_downstream(worker1_conf.node_id)

        self.assertEqual(len(worker1_conf.upstreams), 1)
        self.assertTrue(worker2_conf.node_id in worker1_conf.upstreams)

        self.assertEqual(len(worker2_conf.downstreams), 1)
        self.assertTrue(worker1_conf.node_id in worker2_conf.downstreams)

    def test3(self):
        job_id = str(uuid.uuid4())
        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstreams([worker1_conf, worker2_conf])

        self.assertEqual(len(worker3_conf.upstreams), 2)
        self.assertTrue(worker1_conf.node_id in worker3_conf.upstreams)
        self.assertTrue(worker2_conf.node_id in worker3_conf.upstreams)

        self.assertTrue(worker3_conf.node_id in worker1_conf.downstreams)
        self.assertTrue(worker3_conf.node_id in worker2_conf.downstreams)
