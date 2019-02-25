from collections import deque
from collections import OrderedDict

from engine.tasks.base import WorkerBaseTask
from engine.tasks.message import MessageType
from engine.tasks.signal import FinishedSignal


class StreamDataWorker(WorkerBaseTask):
    """ Worker that receive one line of data from upstream(s) at a time.
    Could be useful in stream processing.
    """

    def config(self, node_conf):
        super().config(node_conf)

        self.upstream_data = OrderedDict()
        self.ret_data = OrderedDict()
        for up in self.upstreams:
            self.upstream_data[up] = {'sender': up, 'data': deque()}
            self.ret_data[up] = {'data': []}

    def pull_data(self):
        """ Invoke once and return one line of data at a time.
        raise Finished signal if all upstreams are finished
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

                self._recv_ctrl_message_count += 1
            else:
                self._recv_data_message_count += 1

                self.upstream_data[up]['data'].append(msg['data'])

            complete_cnt = len(self.finished_ups) + len(self.timeout_ups)

            if complete_cnt >= len(self.upstreams):
                raise FinishedSignal()  # raise finished signal

            useable_ups = []

            for up, body in self.upstream_data.items():
                if self.finished_ups[up] != 0:
                    useable_ups.append(up)
                elif self.timeout_ups[up] != 0:
                    useable_ups.append(up)
                elif len(body['data']) > 0:
                    useable_ups.append(up)
                else:
                    continue

            if len(useable_ups) < len(self.upstreams):
                continue

            for up in useable_ups:
                try:
                    self.ret_data[up]['data'].append(self.upstream_data[up]['data'].popleft())
                except Exception:
                    self.ret_data[up]['data'] = []

            self.preprocessed_message_count += 1

            yield self.ret_data

        yield self.ret_data
        raise FinishedSignal()  # raise finished signal

    def push_data(self, message_body):
        msg = self._pack_message(message_body)

        for down in self.downstreams:
            self._send_message(down, msg)

        if len(self.downstreams) > 0:
            self.postprocessed_message_count += 1
