# import uuid

# from django.test import TestCase
# from django.conf import settings

# from engine.tasks import stream_data_worker
# from engine.tasks.config import WorkerConfig


# class StreamDataWorker(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_x1.py') as fd:
#             cls.ts_emitter_x1_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x3.py') as fd:
#             cls.ts_emitter_x3_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x5.py') as fd:
#             cls.ts_emitter_x5_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x10.py') as fd:
#             cls.ts_emitter_x10_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
#             cls.ts_emitter_3s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
#             cls.ts_emitter_5s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1ups.py') as fd:
#             cls.data_worker_1ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_0up_0down(self):
#         """
#         Test worker with 0 upstream and 0 downstream

#         (worker1)
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['preprocessed_message_count'], 0)
#         self.assertEqual(r1['postprocessed_message_count'], 0)

#     def test_0up_0down_2(self):
#         """
#         Test worker with 0 upstream and 0 downstream

#         (worker1)
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)
#         worker1_conf.add_downstream(str(uuid.uuid4()))

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['preprocessed_message_count'], 0)
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#     def test_1up_0down(self):
#         """
#         Test worker(worker2) with 1 upstream and 0 downstream

#         (worker1) -> (worker2)

#         But worker2 has no interest to handle upstream's output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['preprocessed_message_count'], 0)
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, 'SUCCESS')
#         self.assertEqual(r2['state'], 'FINISHED')
#         self.assertEqual(r2['preprocessed_message_count'], 1)
#         self.assertEqual(r2['postprocessed_message_count'], 0)

#     def test_1up_0down_2(self):
#         """
#         Test worker(worker2) with 1 upstream and 0 downstream

#         (worker1) -> (worker2)

#         worker1's output size is different to worker2'.
#         But worker2 has no interest to handle upstream's output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x1_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['preprocessed_message_count'], 0)
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, 'SUCCESS')
#         self.assertEqual(r2['state'], 'FINISHED')
#         self.assertEqual(r2['preprocessed_message_count'], 1)
#         self.assertEqual(r2['postprocessed_message_count'], 0)

#     def test_1up_0down_3(self):
#         """
#         Test worker(worker2) with 1 upstream and 0 downstream

#         (worker1) -> (worker2)

#         worker1's output size is different to worker2'.
#         But worker2 is going to handle worker1's output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1ups_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['preprocessed_message_count'], 0)
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, 'SUCCESS')
#         self.assertEqual(r2['state'], 'FINISHED')
#         self.assertEqual(r2['preprocessed_message_count'], 1)
#         self.assertEqual(r2['postprocessed_message_count'], 0)

#     def test_2ups_0down(self):
#         """
#         Test worker(worker3) with 2 upstreams and 1 downstream

#         (worker1) \
#                    —> (worker3) -> discard output
#         (worker2) /

#         worker1 and worker2 has the same amount output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)

#         worker1_conf.add_downstream(worker3_conf)

#         worker2_conf.add_downstream(worker3_conf)

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
#         worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, 'SUCCESS')
#         self.assertEqual(r2['state'], 'FINISHED')
#         self.assertEqual(r2['postprocessed_message_count'], 1)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, 'SUCCESS')
#         self.assertEqual(r3['state'], 'FINISHED')
#         self.assertEqual(r3['preprocessed_message_count'], 1)

#     def test_3ups_0down(self):
#         """
#         Test worker(worker4) with 2 upstreams and 0 downstream

#         (worker1) \
#                    \
#         (worker2)   —> (worker4) -> discard output
#                    /
#         (worker3) /

#         worker1, worker2 and worker3 has the same amount output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_3ups_script)

#         worker1_conf.add_downstream(worker4_conf)

#         worker2_conf.add_downstream(worker4_conf)

#         worker3_conf.add_downstream(worker4_conf)

#         worker4_conf.add_upstreams([worker1_conf, worker2_conf, worker3_conf])

#         worker1_task = stream_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = stream_data_worker.apply_async(args=[worker2_conf.to_dict])
#         worker3_task = stream_data_worker.apply_async(args=[worker3_conf.to_dict])
#         worker4_task = stream_data_worker.apply_async(args=[worker4_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, 'SUCCESS')
#         self.assertEqual(r1['state'], 'FINISHED')
#         self.assertEqual(r1['postprocessed_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, 'SUCCESS')
#         self.assertEqual(r2['state'], 'FINISHED')
#         self.assertEqual(r2['postprocessed_message_count'], 1)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, 'SUCCESS')
#         self.assertEqual(r3['state'], 'FINISHED')
#         self.assertEqual(r3['postprocessed_message_count'], 1)

#         r4 = worker4_task.get()
#         self.assertEqual(worker4_task.state, 'SUCCESS')
#         self.assertEqual(r4['state'], 'FINISHED')
#         self.assertEqual(r4['preprocessed_message_count'], 1)
