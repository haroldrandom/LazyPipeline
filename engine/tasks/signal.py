class PipelineSignal(Exception):
    """ """


class FinishedSignal(PipelineSignal):
    """ Indicate task is finished """


class TimeoutSignal(PipelineSignal):
    """ Indicate task is timeout """
