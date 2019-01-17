import sys


__platform__ = sys.platform


if __platform__ == 'darwin':
    pass
elif __platform__ == 'linux':
    pass
else:
    raise


class Container:
    """
    A minimum container to run user's script with limited isolation.

    It's a simple mutilated container. Be careful used in production
    Think about mature Container product like Docker.
    """

    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self):
        pass
