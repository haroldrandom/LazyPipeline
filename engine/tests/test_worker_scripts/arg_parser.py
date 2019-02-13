#!/usr/bin/env python3

import os
import json


def parse_arg(arg_name):
    arg_values = os.environ.get(arg_name)

    arg_values = json.loads(arg_values)

    ret = []

    for values in arg_values:
        for v in values.split('\n'):
            new_line = '1up - '.format(v)
            ret.append(new_line)

    return ret
