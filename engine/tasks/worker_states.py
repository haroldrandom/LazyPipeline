"""
The worker state hierarchy in LazyPipeline:

    (celery)        ->  PENDING
                            |
                        RECEIVED
                            |
                        STARTED (Task was received by a celery worker)
                            |
    (lazypipeline)  ->  INIT    (not configed yet)
                            |
                        CONFIG_ERROR / READY
                            |
                        RUNNING
                            |
                        RUNTIME_ERROR / TIMEOUT / FINISHED
                            |
    (celery)        ->  SUCCESS / FAILURE / REVOKED

"""

_worker_states = {
    'INIT': 0,
    'CONFIG_ERROR': 1,
    'READY': 2,
    'RUNNING': 4,
    'RUNTIME_ERROR': 8,
    'TIMEOUT': 16,
    'FINISHED': 32,
}


def worker_state(state):
    try:
        return _worker_states[state]
    except KeyError:
        raise AttributeError('unsupport state: ' + str(state))


class WorkerState(str):

    def __lt__(self, other):
        return worker_state(self) < worker_state(other)

    def __le__(self, other):
        return worker_state(self) <= worker_state(other)

    def __gt__(self, other):
        return worker_state(self) > worker_state(other)

    def __ge__(self, other):
        return worker_state(self) >= worker_state(other)


INIT = WorkerState('INIT')

CONFIG_ERROR = WorkerState('CONFIG_ERROR')

READY = WorkerState('READY')

RUNNING = WorkerState('RUNNING')

TIMEOUT = WorkerState('TIMEOUT')

RUNTIME_ERROR = WorkerState('RUNTIME_ERROR')

FINISHED = WorkerState('FINISHED')
