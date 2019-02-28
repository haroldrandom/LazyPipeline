from collections import deque
from collections import OrderedDict

from engine.tasks.base import WorkerBaseTask
from engine.tasks import worker_states
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
            else:
                self._recv_data_message_count += 1

                self.upstream_data[up]['data'].append(msg['data'])

            # all data is arrived
            complete_cnt = len(self.finished_ups) + len(self.timeout_ups)
            if complete_cnt >= len(self.upstreams):
                break

            # yield data if available
            if self._prepare_data() is True:
                yield self.ret_data
                self._reset_ret_data()

        # clear the rest of data
        while self._prepare_data() is True:
            yield self.ret_data
            self._reset_ret_data()

        if len(self.upstreams) == 0:
            yield self.ret_data

        raise FinishedSignal()  # raise finished signal

    def _prepare_data(self):
        drained_ups, ready_ups = 0, 0

        if len(self.upstreams) == 0:
            return False

        if self.node_id == '33':
            print('-' * 30 + 'before' + '-' * 30)
            print(self.ret_data)
            print(self.upstream_data)

        for up in self.upstreams:
            if (
                (self.finished_ups[up] != 0 or self.timeout_ups[up] != 0) and
                len(self.upstream_data[up]['data']) == 0
            ):
                drained_ups += 1

            elif len(self.ret_data[up]['data']) > 0:    # already popleft

                ready_ups += 1

            else:
                if len(self.upstream_data[up]['data']) == 0:  # no ready
                    continue
                else:
                    self.ret_data[up]['data'] = [self.upstream_data[up]['data'].popleft()]
                    ready_ups += 1

        if self.node_id == '33':
            print('drained_ups=%d ready_ups=%d' % (drained_ups, ready_ups))

        if ready_ups == len(self.upstreams):
            if self.node_id == '33':
                print('-' * 30 + 'after' + '-' * 30)
                print(self.ret_data)
                print(self.upstream_data)
            return True
        elif (
            drained_ups != 0 and ready_ups != 0 and
            ready_ups + drained_ups == len(self.upstreams)
        ):
            return True
        elif drained_ups == len(self.upstreams):
            return False

        return False

    def _reset_ret_data(self):
        for up in self.ret_data:
            self.ret_data[up]['data'] = []
