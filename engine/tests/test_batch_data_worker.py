# import uuid
# import celery

# from celery import states
# from django.test import TestCase
# from django.conf import settings

# from engine.tasks import batch_data_worker
# from engine.tasks import worker_states
# from engine.tasks.config import WorkerConfig


# class BatchDataWorkerTest(TestCase):
#     """
#     Test worker with default sender settings
#     """
#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_x3.py') as fd:
#             cls.ts_emitter_x3_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x5.py') as fd:
#             cls.ts_emitter_x5_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x10.py') as fd:
#             cls.ts_emitter_x10_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1up.py') as fd:
#             cls.data_worker_1up_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_0up_0down(self):
#         """
#         Test worker with 0 upstream and no downstream:

#         (worker1)

#         worker1 won't send anything
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker_task1 = batch_data_worker.apply_async(args=[worker1_conf.to_dict])

#         r1 = worker_task1.get()
#         self.assertEqual(worker_task1.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 0)

#     def test_1up_0down(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         Besides, worker2 script is not interested to handle worker1's output
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker_task1 = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker_task1.get()
#         self.assertEqual(worker_task1.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 2)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 2)
#         self.assertEqual(r2['recv_data_message_count'], 1)
#         self.assertEqual(r2['sent_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 0)

#     def test_2ups_0down(self):
#         """
#         Test worker with 2 upstreams and no downstream:

#         (worker1) \
#                    (worker3) --> discard output
#         (worker2) /

#         Besides, worker3 script handles upstreams' output
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.add_downstream(worker3_conf)

#         worker2_conf.add_downstream(worker3_conf)

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = batch_data_worker.apply_async(args=[worker2_conf.to_dict])
#         worker3_task = batch_data_worker.apply_async(args=[worker3_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 2)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 0)
#         self.assertEqual(r2['sent_message_count'], 2)
#         self.assertEqual(r2['sent_data_message_count'], 1)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r3['state'])
#         self.assertEqual(r3['recv_message_count'], 4)
#         self.assertEqual(r3['recv_data_message_count'], 2)

#     def test_2ups_1down(self):
#         """
#         Test worker with two upstreams and one downstream:

#         (worker1) \
#                    —- (worker3) - (worker4) -> discard output
#         (worker2) /
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x10_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)
#         worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker3_conf)

#         worker2_conf.add_downstream(worker3_conf)

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])
#         worker3_conf.add_downstream(worker4_conf)

#         worker4_conf.add_upstream(worker3_conf)

#         batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         worker3_task = batch_data_worker.apply_async(args=[worker3_conf.to_dict])

#         worker4_task = batch_data_worker.apply_async(args=[worker4_conf.to_dict])

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r3['state'])

#         r4 = worker4_task.get()
#         self.assertEqual(worker4_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r4['state'])

#     def test_1up_0down_2(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         Besides, worker2 script is not interested to handle worker1's output.
#         Worker2 will receive 3 pieces of data.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 2)
#         self.assertEqual(r2['recv_data_message_count'], 1)

#     def test_1up_0down_3(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         worker2 will not handle worker1's output.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 2)
#         self.assertEqual(r2['recv_data_message_count'], 1)

#     def test_3ups_2downs(self):
#         """
#         Test worker with three upstreams and two downstream:

#         (worker1) \                  (worker5)
#                    \               /
#         (worker2)   -> (worker4) ->
#                    /               \
#         (worker3) /                  (worker6)
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_3ups_script)
#         worker5_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)
#         worker6_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker4_conf)
#         worker2_conf.add_downstream(worker4_conf)
#         worker3_conf.add_downstream(worker4_conf)

#         worker4_conf.add_upstreams([worker1_conf, worker2_conf, worker3_conf])
#         worker4_conf.add_downstreams([worker5_conf, worker6_conf])

#         worker5_conf.add_upstream(worker4_conf)
#         worker6_conf.add_upstream(worker4_conf)

#         batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         batch_data_worker.apply_async(args=[worker2_conf.to_dict])
#         batch_data_worker.apply_async(args=[worker3_conf.to_dict])
#         batch_data_worker.apply_async(args=[worker4_conf.to_dict])
#         worker5_task = batch_data_worker.apply_async(args=[worker5_conf.to_dict])
#         worker6_task = batch_data_worker.apply_async(args=[worker6_conf.to_dict])

#         r5 = worker5_task.get()
#         self.assertEqual(worker5_task.state, 'SUCCESS')
#         self.assertEqual(worker_states.FINISHED, r5['state'])
#         self.assertEqual(r5['job_id'], worker5_conf.job_id)
#         self.assertEqual(r5['node_id'], worker5_conf.node_id)
#         self.assertEqual(r5['recv_message_count'], 2)
#         self.assertEqual(r5['recv_data_message_count'], 1)

#         r6 = worker6_task.get()
#         self.assertEqual(worker6_task.state, 'SUCCESS')
#         self.assertEqual(r6['job_id'], worker6_conf.job_id)
#         self.assertEqual(r6['node_id'], worker6_conf.node_id)
#         self.assertEqual(r6['recv_message_count'], 2)
#         self.assertEqual(r6['recv_data_message_count'], 1)


# class BatchDataWorkerSenderTest(TestCase):
#     """
#     Test worker with different sender config
#     """
#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_x3.py') as fd:
#             cls.ts_emitter_x3_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x5.py') as fd:
#             cls.ts_emitter_x5_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x10.py') as fd:
#             cls.ts_emitter_x10_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1up.py') as fd:
#             cls.data_worker_1up_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_1up_0down(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         Besides, worker2 script is not interested to handle worker1's output
#         But, worker2 will receive 3 pieces of data
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.set_sender(buffer_size=1, separator='\n')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 4)
#         self.assertEqual(r2['recv_data_message_count'], 3)

#     def test_1up_0down_2(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         Besides, worker2 script is not interested to handle worker1's output.
#         Worker2 will receive 3 pieces of data.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.set_sender(buffer_size=2, separator='\n')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 4)
#         self.assertEqual(r2['recv_data_message_count'], 3)

#     def test_1up_0down_3(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         Besides, worker2 script is not interested to handle worker1's output.
#         Worker2 will receive 3 pieces of data even though
#         worker1's buffer size is greater than its output result
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.set_sender(buffer_size=4, separator='\n')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 4)
#         self.assertEqual(r2['recv_data_message_count'], 3)

#     def test_1up_0down_4(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         worker2 will handle worker1's output.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.set_sender(buffer_size=1, separator='\n')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 4)
#         self.assertEqual(r2['recv_data_message_count'], 3)

#     def test_1up_0down_5(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         worker2 will handle worker1's output.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.set_sender(separator='\n')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 4)
#         self.assertEqual(r2['recv_data_message_count'], 3)

#     def test_1up_0down_6(self):
#         """
#         Test worker with 1 upstream and no downstream:

#         (worker1) --> (worker2) --> discard output

#         worker1 will apply a different separator
#         worker2 will handle worker1's output.
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.set_sender(buffer_size=1, separator='#')
#         worker1_conf.add_downstream(worker2_conf)

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker_task2 = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(r1['state'], str(worker_states.FINISHED))
#         self.assertEqual(r1['job_id'], worker1_conf.job_id)
#         self.assertEqual(r1['node_id'], worker1_conf.node_id)
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker_task2.get()
#         self.assertEqual(worker_task2.state, states.SUCCESS)
#         self.assertEqual(r2['state'], str(worker_states.FINISHED))
#         self.assertEqual(r2['job_id'], worker2_conf.job_id)
#         self.assertEqual(r2['node_id'], worker2_conf.node_id)
#         self.assertEqual(r2['recv_message_count'], 2)
#         self.assertEqual(r2['recv_data_message_count'], 1)

#     def test_2ups_0down(self):
#         """
#         Test worker with 2 upstreams and no downstream:

#         (worker1) \
#                    (worker3) --> discard output
#         (worker2) /

#         worker3 will not handle upstreams' output but receive and process
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)

#         worker1_conf.add_downstream(worker3_conf)
#         worker1_conf.set_sender(buffer_size=1, separator='\n')

#         worker2_conf.add_downstream(worker3_conf)
#         worker2_conf.set_sender(buffer_size=1, separator='\n')

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = batch_data_worker.apply_async(args=[worker2_conf.to_dict])
#         worker3_task = batch_data_worker.apply_async(args=[worker3_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 5)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r3['state'])
#         self.assertEqual(r3['recv_message_count'], 10)
#         self.assertEqual(r3['recv_data_message_count'], 8)

#     def test_2ups_0down_2(self):
#         """
#         Test worker with 2 upstreams and no downstream:

#         (worker1) \
#                    (worker3) --> discard output
#         (worker2) /

#         worker3 will handle upstreams' output
#         """

#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x3_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_x5_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)

#         worker1_conf.add_downstream(worker3_conf)
#         worker1_conf.set_sender(buffer_size=1, separator='\n')

#         worker2_conf.add_downstream(worker3_conf)
#         worker2_conf.set_sender(buffer_size=1, separator='\n')

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = batch_data_worker.apply_async(args=[worker2_conf.to_dict])
#         worker3_task = batch_data_worker.apply_async(args=[worker3_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 5)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r3['state'])
#         self.assertEqual(r3['recv_message_count'], 10)
#         self.assertEqual(r3['recv_data_message_count'], 8)


# class BatchDataWorkerTimeoutTest(TestCase):
#     """
#     Test timeout situation with default sender settings
#     """

#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
#             cls.ts_emitter_3s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
#             cls.ts_emitter_5s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1up.py') as fd:
#             cls.data_worker_1up_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_timeout_0up_0down(self):
#         """
#         Test single worker timeout, and without downstream.
#         The output will be left behind until timeout

#         (worker1)
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_10s_script)

#         worker1_task = batch_data_worker.apply_async(
#             args=[worker1_conf.to_dict],
#             soft_time_limit=2)

#         r1 = worker1_task.get()

#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r1['state'])

#     def test_timeout_1up_0down(self):
#         """
#         Test worker when worker2 timeout but worker1 doesn't,
#         so worker1's ouput will be left behind in Message Queue until messgae timeout

#         (worker1) --> (worker2)

#         worekr2 will timeout and exit before recevice anything
#         because worker1 haven't prepare to send output
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])

#         worker2_task = batch_data_worker.apply_async(
#             args=[worker2_conf.to_dict],
#             soft_time_limit=2)

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 2)
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 0)
#         self.assertEqual(r2['sent_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 0)

#     def test_timeout_1up_0down_2(self):
#         """
#         Test worker when worker1 timeout but worker2 doesn't

#         (worker1) -> (worker2)

#         worker2 will receive timeout signal from worker1,
#         so worker2 can not handle the process propertly and
#         lef RUNTIME_ERROR state
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_5s_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(
#             args=[worker1_conf.to_dict],
#             soft_time_limit=2)

#         worker2_task = batch_data_worker.apply_async(args=[worker2_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 1)
#         self.assertEqual(r1['sent_data_message_count'], 0)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.RUNTIME_ERROR, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 1)
#         self.assertEqual(r2['sent_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 0)

#     def test_timeout_2ups_1down(self):
#         """
#         Test worker with 2ups and 1down

#         (worker1) \
#                    —> (worker3) -> (worker4) -> discard output
#         (worker2) /

#         worker3 will discard worker2's output because it's timeout,
#         and only worker1's result pass down
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_10s_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)
#         worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker3_conf)

#         worker2_conf.add_downstream(worker3_conf)

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])
#         worker3_conf.add_downstream(worker4_conf)

#         worker4_conf.add_upstream(worker3_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = batch_data_worker.apply_async(
#             args=[worker2_conf.to_dict],
#             soft_time_limit=2)
#         worker3_task = batch_data_worker.apply_async(
#             args=[worker3_conf.to_dict])
#         worker4_task = batch_data_worker.apply_async(args=[worker4_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 2)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r2['state'])
#         self.assertEqual(r2['sent_message_count'], 1)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r3['state'])
#         self.assertEqual(r3['recv_data_message_count'], 1)
#         self.assertEqual(r3['sent_message_count'], 2)

#         r4 = worker4_task.get()
#         self.assertEqual(worker4_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r4['state'])
#         self.assertEqual(r3['recv_data_message_count'], 1)

#     def test_timeout_2ups_1down_2(self):
#         """
#         Test worker with 2ups and 1down

#         (worker1) \
#                    —> (worker3) -> (worker4) -> discard output
#         (worker2) /

#         worker3 will timeout and exit before any upstreams' output arrive
#         therefor, worker4 will get nothing from upstream and its script
#         can not behave propertly as expected
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_10s_script)
#         worker3_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_2ups_script)
#         worker4_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker3_conf)

#         worker2_conf.add_downstream(worker3_conf)

#         worker3_conf.add_upstreams([worker1_conf, worker2_conf])
#         worker3_conf.add_downstream(worker4_conf)

#         worker4_conf.add_upstream(worker3_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])
#         worker2_task = batch_data_worker.apply_async(
#             args=[worker2_conf.to_dict])
#         worker3_task = batch_data_worker.apply_async(
#             args=[worker3_conf.to_dict],
#             soft_time_limit=2)
#         worker4_task = batch_data_worker.apply_async(args=[worker4_conf.to_dict])

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r2['state'])
#         self.assertEqual(r1['sent_data_message_count'], 1)

#         r3 = worker3_task.get()
#         self.assertEqual(worker3_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r3['state'])
#         self.assertEqual(r3['recv_data_message_count'], 0)

#         r4 = worker4_task.get()
#         self.assertEqual(worker4_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.RUNTIME_ERROR, r4['state'])
#         self.assertEqual(r4['recv_data_message_count'], 0)


# class BatchDataWorkerTimeoutSenderTest(TestCase):
#     """
#     Test timeout situation with different sender settings
#     """

#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_every_1s.py') as fd:
#             cls.ts_emitter_1s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
#             cls.ts_emitter_3s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
#             cls.ts_emitter_5s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x1.py') as fd:
#             cls.ts_emitter_x1_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x3.py') as fd:
#             cls.ts_emitter_x3_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x5.py') as fd:
#             cls.ts_emitter_x5_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_x10.py') as fd:
#             cls.ts_emitter_x10_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1up.py') as fd:
#             cls.data_worker_1up_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_timeout_1up_0down(self):
#         """
#         Test worker when worker2 timeout but worker1 doesn't

#         (worker1) --> (worker2)

#         worekr2 will timeout and exit in 2s
#         but worker1 will sent its output after 3s so, worker2 will get nothing
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)
#         worker2_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.data_worker_1up_script)

#         worker1_conf.add_downstream(worker2_conf)
#         worker1_conf.set_sender(buffer_size=1, separator='\n')

#         worker2_conf.add_upstream(worker1_conf)

#         worker1_task = batch_data_worker.apply_async(args=[worker1_conf.to_dict])

#         worker2_task = batch_data_worker.apply_async(
#             args=[worker2_conf.to_dict],
#             soft_time_limit=2)

#         r1 = worker1_task.get()
#         self.assertEqual(worker1_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.FINISHED, r1['state'])
#         self.assertEqual(r1['recv_message_count'], 0)
#         self.assertEqual(r1['sent_message_count'], 4)
#         self.assertEqual(r1['sent_data_message_count'], 3)

#         r2 = worker2_task.get()
#         self.assertEqual(worker2_task.state, states.SUCCESS)
#         self.assertEqual(worker_states.TIMEOUT, r2['state'])
#         self.assertEqual(r2['recv_message_count'], 0)
#         self.assertEqual(r2['sent_message_count'], 0)
#         self.assertEqual(r2['sent_data_message_count'], 0)


# class BatchDataWorkerExpiresTest(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         super().setUpClass()

#         cls.scripts_home = settings.BASE_DIR + '/engine/tests/test_worker_scripts/'

#         with open(cls.scripts_home + 'ts_emitter_every_3s.py') as fd:
#             cls.ts_emitter_3s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_5s.py') as fd:
#             cls.ts_emitter_5s_script = fd.read()

#         with open(cls.scripts_home + 'ts_emitter_every_10s.py') as fd:
#             cls.ts_emitter_10s_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_1up.py') as fd:
#             cls.data_worker_1up_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_2ups.py') as fd:
#             cls.data_worker_2ups_script = fd.read()

#         with open(cls.scripts_home + 'data_worker_3ups.py') as fd:
#             cls.data_worker_3ups_script = fd.read()

#     def test_expire(self):
#         """
#         Test worker with expiration
#         """
#         job_id = str(uuid.uuid4())

#         worker1_conf = WorkerConfig(job_id, str(uuid.uuid4()), self.ts_emitter_3s_script)

#         worker1_task = batch_data_worker.apply_async(
#             args=[worker1_conf.to_dict],
#             countdown=2,
#             expires=1)

#         with self.assertRaises(celery.exceptions.TaskRevokedError):
#             worker1_task.get()

#         self.assertEqual(worker1_task.state, 'REVOKED')
