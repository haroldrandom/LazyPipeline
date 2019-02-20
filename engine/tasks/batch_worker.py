from collections import OrderedDict

from engine.tasks.base import WorkerBaseTask
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

        while len(self.upstreams) > 0:
            msg = self._recv_message()

            up = msg['sender']

            if msg['type'] == MessageType.CTRL:
                if msg['status']['is_timeout'] is True:
                    self.timeout_ups[up] += 1
                elif msg['status']['is_finished'] is True:
                    self.finished_ups[up] += 1
                else:
                    continue

                complete_cnt = len(self.finished_ups) + len(self.timeout_ups)
                if complete_cnt >= len(self.upstreams):
                    return self.upstream_data
            else:
                self.upstream_data[up]['data'].append(msg['data'])

        return self.upstream_data

    def push_data(self, message_body):
        msg = self._pack_message(message_body)

        for down in self.downstreams:
            self._send_message(down, msg)
