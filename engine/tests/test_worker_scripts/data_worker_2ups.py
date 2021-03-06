#!/usr/bin/env python3

"""
Union two upstreams at the same time
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
            ret.append(arg_name + ' = ' + v)

    if len(ret) == 0:
        return [arg_name + ' = []']
    return ret


def main():
    values1 = parse_arg('ARG_1')
    values2 = parse_arg('ARG_2')

    # union
    vls = values1 + values2

    for v in vls:
        v = 'with 2ups - [{0}]'.format(v)
        print(v)


if __name__ == '__main__':
    main()
