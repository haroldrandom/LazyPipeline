import uuid

from django.test import TestCase
from django.conf import settings

from engine.tasks import run_batch_data_worker
from engine.tasks.config import WorkerConfig


class BatchDataWorkerTest(TestCase):
    def setUp(self):
        super(BatchDataWorkerTest, self).setUp()

        self.scripts_home = settings.BASE_DIR + '/engine/tests/test_scripts/'

        with open(self.scripts_home + 'ts_x10_emitter.py') as fd:
            self.worker_script_1 = fd.read()

        with open(self.scripts_home + 'batch_data_worker_with_2_ups.py') as fd:
            self.worker_script_2 = fd.read()

        with open(self.scripts_home + 'batch_data_worker_with_3_ups.py') as fd:
            self.worker_script_3 = fd.read()

    def test1(self):
        """ Test worker with one upstream and no downstream:

        worker1 -- worker2

        worker2's script is not interested to handle worker1's output
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)

        worker1_conf.add_downstream(worker2_conf.node_id)

        worker2_conf.add_upstream(worker1_conf.node_id)

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])

    def test2(self):
        """ Test worker with two upstreams and no downstream:

        worker1 \
                  worker3
        worker2 /

        worker3's script handles upstreams' output
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_2)

        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstream(worker1_conf)
        worker3_conf.add_upstream(worker2_conf)

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])

    def test3(self):
        """ Test worker with three upstreams and one downstream:
        worker1 \
        worker2  — worker4 - dummy-worker
        worker3 /
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_3)

        worker1_conf.add_downstream(worker4_conf)
        worker2_conf.add_downstream(worker4_conf)
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstreams([worker1_conf, worker2_conf, worker3_conf])
        worker4_conf.add_downstream('test3_dummy-worker4_downstream_1_of_[' + str(worker4_conf.node_id) + ']')

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker4_conf.to_dict])

    def test4(self):
        """ Test worker with three upstreams and two downstream:
        worker1 \
                 \            dummy-worker1
        worker2   -- worker4 /
                 /           \ dummy-worker2
        worker3 /
        """

        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_1)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.worker_script_3)

        worker1_conf.add_downstream(worker4_conf)
        worker2_conf.add_downstream(worker4_conf)
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstreams(
            [worker1_conf.node_id, worker2_conf.node_id, worker3_conf.node_id])
        worker4_conf.add_downstreams(
            ['test4_dummy-worker4_downstream_1_of_[' + str(worker4_conf.node_id) + ']',
             'test4_dummy-worker4_downstream_2_of_[' + str(worker4_conf.node_id) + ']'])

        run_batch_data_worker.apply_async(args=[worker1_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker2_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker3_conf.to_dict])
        run_batch_data_worker.apply_async(args=[worker4_conf.to_dict])
