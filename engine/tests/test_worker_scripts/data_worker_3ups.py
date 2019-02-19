#!/usr/bin/env python3

"""
Deal with three upstreams at the same time
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
            ret.append(v)

    return ret


def main():
    values1 = parse_arg('ARG_1')
    values2 = parse_arg('ARG_2')
    values3 = parse_arg('ARG_3')

    # union
    vls = values1 + values2 + values3

    for v in vls:
        v = 'with 3ups - [{0}]'.format(v)
        print(v)


if __name__ == '__main__':
    main()
