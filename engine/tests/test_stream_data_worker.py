import uuid

from celery import states
from django.test import TestCase
from django.conf import settings

from engine.tasks import stream_data_worker
from engine.tasks import worker_states
from engine.tasks.config import WorkerConfig


class StreamDataWorker(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

        with open(cls.scripts_home + 'ts_emitter_x1.py') as fd:
            cls.ts_emitter_x1_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_x3.py') as fd:
            cls.ts_emitter_x3_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_x5.py') as fd:
            cls.ts_emitter_x5_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_x10.py') as fd:
            cls.ts_emitter_x10_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
            cls.ts_emitter_3s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
            cls.ts_emitter_5s_script = fd.read()

        with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
            cls.ts_emitter_10s_script = fd.read()

        with open(cls.scripts_home + 'data_worker_1ups.py') as fd:
            cls.data_worker_1ups_script = fd.read()

        with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
            cls.data_worker_2ups_script = fd.read()

        with open(cls.scripts_home + 'data_worker_2ups_join.py') as fd:
            cls.data_worker_2ups_join_script = fd.read()

        with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
            cls.data_worker_3ups_script = fd.read()

    def test_0up_0down(self):
        """
        Test worker with 0 upstream and 0 downstream

        (worker1)
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['job_id'], worker1_conf.job_id)
        self.assertEqual(r1['node_id'], worker1_conf.node_id)
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 0)
        self.assertEqual(r1['sent_data_message_count'], 0)

    def test_1up_0down(self):
        """
        Test worker(worker2) with 1 upstream and 0 downstream

        (worker1) -> (worker2)

        worker2 has no interest to handle upstream's output
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

        worker1_conf.add_downstream(worker2_conf)

        worker2_conf.add_upstream(worker1_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 2)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 2)
        self.assertEqual(r2['recv_data_message_count'], 1)

    def test_1up_0down_2(self):
        """
        Test worker(worker2) with 1 upstream and 0 downstream

        (worker1) -> (worker2)

        worker2 has no interest to handle upstream's output
        worker1 sends its data in 3 pieces
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

        worker1_conf.set_sender(buffer_size=1, separator='\n')
        worker1_conf.add_downstream(worker2_conf)

        worker2_conf.add_upstream(worker1_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 4)
        self.assertEqual(r2['recv_data_message_count'], 3)

    def test_1up_1down(self):
        """
        Test worker(worker2) with 1 upstream and 0 downstream

        (worker1) -> (worker2) -> (worker3)

        worker2 will handle upstream's output, and worker1 sends its data in 3 pieces
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.set_sender(buffer_size=1, separator='\n')
        worker1_conf.add_downstream(worker2_conf)

        worker2_conf.add_upstream(worker1_conf)
        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstream(worker2_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 4)
        self.assertEqual(r2['recv_data_message_count'], 3)
        self.assertEqual(r2['sent_data_message_count'], 3)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r3['state'])
        # why 4 ?
        # cause worker2 receive 3 pieces of data and process 3 times
        # so sent data 3 times
        self.assertEqual(r3['recv_message_count'], 4)
        self.assertEqual(r3['recv_data_message_count'], 3)

    def test_1up_1down_2(self):
        """
        Test worker(worker2) with 1 upstream and 0 downstream

        (worker1) -> (worker2) -> (worker3)

        worker2 will handle upstream's output, and worker1 sends its data in 3 pieces
        according to sender config
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.set_sender(buffer_size=1, separator='\n')
        worker1_conf.add_downstream(worker2_conf)

        worker2_conf.set_sender(buffer_size=1, separator='\n')
        worker2_conf.add_upstream(worker1_conf)
        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstream(worker2_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 4)
        self.assertEqual(r2['recv_data_message_count'], 3)
        self.assertEqual(r2['sent_data_message_count'], 3)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 4)
        self.assertEqual(r3['recv_data_message_count'], 3)

    def test_2ups_0down(self):
        """
        Test worker(worker3) with 2 upstreams and 1 downstream

        (worker1) \
                   —> (worker3) -> discard output
        (worker2) /

        worker1 and worker2 have the same amount output
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)

        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstreams([worker1_conf, worker2_conf])

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 2)
        self.assertEqual(r1['sent_data_message_count'], 1)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, 'SUCCESS')
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 4)
        self.assertEqual(r3['recv_data_message_count'], 2)

    def test_2ups_1down(self):
        """
        Test worker(worker3) with 2 upstreams and 1 downstream

        (worker1) \
                   —> (worker3) -> (worker4)
        (worker2) /

        1. Worker1 and worker2 have the different amount output.
        2. Worker1 sends its output separately, worker2 sends its output at once.
        3. Worker3 will receive 4 pieces of data and can be formed in 3 line,
            but send them in batch
        4. Worker4 will receive 3 piece of data cause worker3's output
            sent separately
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.add_downstream(worker3_conf)
        worker1_conf.set_sender(buffer_size=1, separator='\n')

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstream(worker3_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 6)
        self.assertEqual(r3['recv_data_message_count'], 4)
        self.assertEqual(r3['recv_ctrl_message_count'], 2)

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r4['state'])
        self.assertEqual(r4['sent_message_count'], 0)
        self.assertEqual(r4['recv_message_count'], 4)
        self.assertEqual(r4['recv_data_message_count'], 3)

    def test_2ups_1down_2(self):
        """
        Test worker(worker3) with 2 upstreams and 1 downstream

        (worker1) \
                   —> (worker3) -> (worker4)
        (worker2) /

        A bit different with test_2up_0down_2.
        Worker3 will send its output separately, so the result might be weird.
        It performs an union-like operation.
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
        worker3_conf = WorkerConfig(job_id, str(33), self.data_worker_2ups_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.add_downstream(worker3_conf)
        worker1_conf.set_sender(buffer_size=1, separator='\n')

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.set_sender(buffer_size=1, separator='\n')
        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstream(worker3_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 6)
        self.assertEqual(r3['recv_data_message_count'], 4)
        self.assertEqual(r3['recv_ctrl_message_count'], 2)

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r4['state'])
        self.assertEqual(r4['sent_message_count'], 0)
        # the result might be confused, but 11 is the result
        # It's (1 + 5) + (1 + 1) + (1 + 1) + 1
        self.assertEqual(r4['recv_message_count'], 11)
        self.assertEqual(r4['recv_data_message_count'], 10)

    def test_2ups_1down_3(self):
        """
        Test worker(worker3) with 2 upstreams and 1 downstream

        (worker1) \
                   —> (worker3) -> (worker4)
        (worker2) /

        Different with test_2up_0down_3 cause worker3 hold a different script
        that perform join-like operation.
        Worker3 will send its output separately in pack
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
        worker3_conf = WorkerConfig(job_id, str(33), self.data_worker_2ups_join_script)
        worker4_conf = WorkerConfig(job_id, str(44), self.data_worker_1ups_script)

        worker1_conf.add_downstream(worker3_conf)
        worker1_conf.set_sender(buffer_size=1, separator='\n')

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.set_sender(buffer_size=1, separator='\n')
        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstream(worker4_conf)

        worker4_conf.add_upstream(worker3_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 6)
        self.assertEqual(r3['recv_data_message_count'], 4)
        self.assertEqual(r3['recv_ctrl_message_count'], 2)

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r4['state'])
        self.assertEqual(r4['sent_message_count'], 0)
        # 4 = (1 data) + (1 data) + (1 data) + (1 ctrl)
        self.assertEqual(r4['recv_message_count'], 4)
        self.assertEqual(r4['recv_data_message_count'], 3)

    def test_2ups_2down(self):
        """
        Test worker(worker3) with 2 upstreams and 2 downstreams

        (worker1) \              / (worker4)
                   —> (worker3) -
        (worker2) /              \ (worker5)

        worker3 with default sender config
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)
        worker5_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.set_sender(buffer_size=1, separator='\n')
        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstreams([worker4_conf, worker5_conf])

        worker4_conf.add_upstream(worker3_conf)

        worker5_conf.add_upstream(worker3_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])
        worker5_task = stream_data_worker.apply_async(args=[worker5_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 6)
        self.assertEqual(r3['recv_data_message_count'], 4)
        self.assertEqual(r3['recv_ctrl_message_count'], 2)
        self.assertEqual(r3['sent_message_count'], 8)
        self.assertEqual(r3['sent_data_message_count'], 6)

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r4['state'])
        self.assertEqual(r4['sent_message_count'], 0)
        self.assertEqual(r4['recv_message_count'], 4)
        self.assertEqual(r4['recv_data_message_count'], 3)

        r5 = worker5_task.get()
        self.assertEqual(worker5_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r5['state'])
        self.assertEqual(r5['sent_message_count'], 0)
        self.assertEqual(r5['recv_message_count'], 4)
        self.assertEqual(r5['recv_data_message_count'], 3)

    def test_2ups_2down_2(self):
        """
        Test worker(worker3) with 2 upstreams and 2 downstreams

        (worker1) \              / (worker4)
                   —> (worker3) -
        (worker2) /              \ (worker5)

        worker3 with different sender config
        """
        job_id = str(uuid.uuid4())

        worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
        worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_join_script)
        worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)
        worker5_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

        worker1_conf.set_sender(buffer_size=1, separator='\n')
        worker1_conf.add_downstream(worker3_conf)

        worker2_conf.add_downstream(worker3_conf)

        worker3_conf.set_sender(buffer_size=1, separator='\n')
        worker3_conf.add_upstreams([worker1_conf, worker2_conf])
        worker3_conf.add_downstreams([worker4_conf, worker5_conf])

        worker4_conf.add_upstream(worker3_conf)

        worker5_conf.add_upstream(worker3_conf)

        worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
        worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
        worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
        worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])
        worker5_task = stream_data_worker.apply_async(args=[worker5_conf.to_dict])

        r1 = worker1_task.get()
        self.assertEqual(worker1_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r1['state'])
        self.assertEqual(r1['recv_message_count'], 0)
        self.assertEqual(r1['sent_message_count'], 4)
        self.assertEqual(r1['sent_data_message_count'], 3)

        r2 = worker2_task.get()
        self.assertEqual(worker2_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r2['state'])
        self.assertEqual(r2['recv_message_count'], 0)
        self.assertEqual(r2['sent_message_count'], 2)
        self.assertEqual(r2['sent_data_message_count'], 1)

        r3 = worker3_task.get()
        self.assertEqual(worker3_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r3['state'])
        self.assertEqual(r3['recv_message_count'], 6)
        self.assertEqual(r3['recv_data_message_count'], 4)
        self.assertEqual(r3['recv_ctrl_message_count'], 2)
        self.assertEqual(r3['sent_message_count'], 8)
        self.assertEqual(r3['sent_data_message_count'], 6)

        r4 = worker4_task.get()
        self.assertEqual(worker4_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r4['state'])
        self.assertEqual(r4['sent_message_count'], 0)
        self.assertEqual(r4['recv_message_count'], 4)
        self.assertEqual(r4['recv_data_message_count'], 3)

        r5 = worker5_task.get()
        self.assertEqual(worker5_task.state, states.SUCCESS)
        self.assertEqual(worker_states.FINISHED, r5['state'])
        self.assertEqual(r5['sent_message_count'], 0)
        self.assertEqual(r5['recv_message_count'], 4)
        self.assertEqual(r5['recv_data_message_count'], 3)

    # def test_3ups_0down(self):
    #     """
    #     Test worker(worker4) with 2 upstreams and 0 downstream

    #     (worker1) \
    #                \
    #     (worker2)   —> (worker4) -> discard output
    #                /
    #     (worker3) /

    #     worker1, worker2 and worker3 has the same amount output
    #     """
    #     job_id = str(uuid.uuid4())

    #     worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
    #     worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
    #     worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
    #     worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_3ups_script)

    #     worker1_conf.add_downstream(worker4_conf)

    #     worker2_conf.add_downstream(worker4_conf)

    #     worker3_conf.add_downstream(worker4_conf)

    #     worker4_conf.add_upstreams([worker1_conf, worker2_conf, worker3_conf])

    #     worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
    #     worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
    #     worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
    #     worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])
