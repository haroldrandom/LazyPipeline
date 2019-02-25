from collections import OrderedDict

from engine.tasks.base import WorkerBaseTask
from engine.tasks import worker_states
from engine.tasks.message import MessageType


class BatchDataWorker(WorkerBaseTask):
    """ Worker that can receive all data from uptream(s) in the same time.
    Could be useful if you want ot join/merge/convergence.
    """

    def config(self, node_conf):
        super().config(node_conf)

        self.upstream_data = OrderedDict()
        for up in self.upstreams:
            self.upstream_data[up] = {'sender': up, 'data': []}

    def pull_data(self):
        """ Invoke once and return all data from upstreams.

        return self.upstreams_data if self.upstreams exist,
        otherwise None
        """

        self.worker_state = worker_states.RUNNING

        while len(self.upstreams) > 0:
            msg = self._recv_message()

            up = msg['sender']

            if msg['type'] == MessageType.CTRL:
                if worker_states.TIMEOUT == msg['state']:
                    self.timeout_ups[up] += 1
                elif worker_states.FINISHED == msg['state']:
                    self.finished_ups[up] += 1
                else:
                    continue

                self._recv_ctrl_message_count += 1

                complete_cnt = len(self.finished_ups) + len(self.timeout_ups)
                if complete_cnt >= len(self.upstreams):
                    break
            else:
                self._recv_data_message_count += 1

                self.upstream_data[up]['data'].append(msg['data'])

        self.preprocessed_message_count += 1

        return self.upstream_data
