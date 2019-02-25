import json


class WorkerConfig():
    def __init__(self, job_id, node_id, script):
        self._job_id = job_id
        self._node_id = node_id

        if not self._job_id:
            raise AttributeError('job_id is must not be empty')
        if not self._node_id:
            raise AttributeError('node_id must not be empty')
        if not script:
            raise AttributeError('script must not be empty')

        self._upstreams = []
        self._downstreams = []
        self._script = script

        # sender config

        # send buffer at most buffer_size when buffer size is equal to buffer_size,
        # -1 means send buffer at once
        self._sender_buffer_size = -1
        # split worker's output using separator if set,
        # otherwise, append it to the buffer intactly
        self._sender_separator = None

    @property
    def job_id(self):
        return self._job_id

    @property
    def node_id(self):
        return self._node_id

    @property
    def upstreams(self):
        return self._upstreams

    @property
    def downstreams(self):
        return self._downstreams

    @property
    def script(self):
        return self._script

    @property
    def sender_buffer_size(self):
        return self._sender_buffer_size

    @property
    def sender_separator(self):
        return self._sender_separator

    @property
    def to_dict(self):
        c = {
            'job_id': self._job_id,
            'node_id': self._node_id,
            'script': self._script,
            'upstreams': self._upstreams,
            'downstreams': self._downstreams,
            'sender': {
                'buffer_size': self._sender_buffer_size,
                'separator': self._sender_separator
            }
        }
        return c

    def __str__(self):
        return json.dumps(self.to_dict)

    @staticmethod
    def config_from_object(config):
        if 'job_id' not in config:
            raise AttributeError('key job_id must not be empty')
        if 'node_id' not in config:
            raise AttributeError('key node_id must not be empty')
        if 'script' not in config:
            raise AttributeError('key script must not be empty')

        script = config.get('script')

        wc = WorkerConfig(config.get('job_id'), config.get('node_id'), script)

        ups = config.get('upstreams') or []
        wc.add_upstreams(ups)

        dws = config.get('downstreams') or []
        wc.add_downstreams(dws)

        if config.get('sender'):
            wc.set_sender(config['sender']['buffer_size'],
                          config['sender']['separator'])

        return wc

    def set_sender(self, buffer_size=-1, separator=None):
        """
        @param buffer_size,
            send buffer at most buffer_size
            wwhen buffer size is equal to buffer_size.
            -1 means send buffer at last one
        @param separator
            split worker's output using separator if set.
            otherwise, append it to the buffer intactly
        """

        if buffer_size > 0:
            assert separator is not None
            assert isinstance(separator, str)

        self._sender_buffer_size = buffer_size
        self._sender_separator = separator

    def add_upstream(self, up):
        if isinstance(up, WorkerConfig):
            up = up.node_id
        elif isinstance(up, str):
            up = up
        else:
            raise AttributeError(
                'up must be either instance of WorkerConfig or str')

        if up not in self._upstreams:
            self._upstreams.append(up)

    def add_upstreams(self, ups):
        for up in ups:
            self.add_upstream(up)

    def add_downstream(self, down):
        if isinstance(down, WorkerConfig):
            down = down.node_id
        elif isinstance(down, str):
            down = down
        else:
            raise AttributeError(
                'down must be either instance of WorkerConfig or str')

        if down not in self._downstreams:
            self._downstreams.append(down)

    def add_downstreams(self, downs):
        for down in downs:
            self.add_downstream(down)
