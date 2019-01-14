from collections import UserDict


class KeyLimitedDict(UserDict):
    def __init__(self, *args, allowed_keys=None, **kwargs):
        self._allowed_keys = frozenset(allowed_keys)

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if key not in self._allowed_keys:
            raise KeyError('key: {!r} is not allowed'.format(key))

        super().__setitem__(key, value)

    def __getitem__(self, key):
        if key not in self._allowed_keys:
            raise KeyError('key: {!r} is not allowed'.format(key))

        return super().__getitem__(key)


class UniqueKeySerialCounter(KeyLimitedDict):
    def __init__(self, allowed_keys=None, starter=0):
        self._serial_starter = starter

        super().__init__(allowed_keys=allowed_keys)

    def __getitem__(self, key):
        return self._serial_starter if key not in self else super().__getitem__(key)
