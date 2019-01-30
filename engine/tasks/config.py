class WorkerConfig():
    def __init__(self, job_id, node_id):
        self._job_id = job_id
        self._node_id = node_id

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
        return

    def add_upstreams(self, up1, *ups):
        self._upstreams.append(up1)

        if ups:
            for up in ups:
                self._upstreams.append(up)

    def add_downstreams(self, down1, *downs):
        self._downstreams.append(down1)

        if downs:
            for down in downs:
                self._downstreams.append(down)
