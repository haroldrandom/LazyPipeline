#!/usr/bin/env python3

"""
Deal with 1 upstreams at a time.

The sperator for each line is '\n' here
"""

import os
import json


def parse_arg(arg_name):
    arg_values = os.environ.get(arg_name)

    arg_values = json.loads(arg_values)

    ret = []

    for values in arg_values:
        for v in values.split('\n'):
            if not v:
                continue
            new_line = '1up - [{0}]'.format(v)
            ret.append(new_line)

    return ret


def main():
    values = parse_arg('ARG_1')

    for v in values:
        print(v)


if __name__ == '__main__':
    main()
