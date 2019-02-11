class WorkerConfig():
    def __init__(self, job_id, node_id):
        self._job_id = job_id
        self._node_id = node_id

        if not self._job_id:
            raise AttributeError('job_id is must not be empty')
        if not self._node_id:
            raise AttributeError('node_id must not be empty')

        self._upstreams = []
        self._downstreams = []

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
    def to_dict(self):
        c = {'job_id': self._job_id,
             'node_id': self._node_id,
             'upstreams': self._upstreams,
             'downstreams': self._downstreams}
        return c

    @staticmethod
    def config_from_object(config):
        if 'job_id' not in config:
            raise AttributeError('key job_id must exist')
        if 'node_id' not in config:
            raise AttributeError('key node_id must exist')

        wc = WorkerConfig(config.get('job_id'), config.get('node_id'))

        ups = config.get('upstreams') or []
        wc.add_upstreams(ups)

        dws = config.get('downstreams') or []
        wc.add_downstreams(dws)

        return wc

    def add_upstream(self, up):
        if isinstance(up, WorkerConfig):
            self._upstreams.append(up.node_id)
        elif isinstance(up, str):
            self._upstreams.append(up)
        else:
            raise AttributeError(
                'down must be either instance of WorkerConfig or str')

    def add_upstreams(self, ups):
        for up in ups:
            self.add_upstream(up)

    def add_downstream(self, down):
        if isinstance(down, WorkerConfig):
            self._downstreams.append(down.node_id)
        elif isinstance(down, str):
            self._downstreams.append(down)
        else:
            raise AttributeError(
                'down must be either instance of WorkerConfig or str')

    def add_downstreams(self, downs):
        for down in downs:
            self.add_downstream(down)
