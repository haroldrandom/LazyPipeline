class PipelineSignal(Exception):
    """ """


class FinishSignal(PipelineSignal):
    """ Indicate task is finished """


class TimeoutSignal(PipelineSignal):
    """ Indicate task is timeout """
