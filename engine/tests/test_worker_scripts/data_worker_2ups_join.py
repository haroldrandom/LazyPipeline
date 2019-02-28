#!/usr/bin/env python3

"""
Join two upstreams at the same time
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
            v = v.replace('\n', ' ')
            ret.append(v)

    if len(ret) == 0:
        return [arg_name + ' = []']
    else:
        return [arg_name + '=' + '# '.join(ret)]

    return ret


def main():
    values1 = parse_arg('ARG_1')
    values2 = parse_arg('ARG_2')

    if len(values1) < len(values2):
        shorter = values1
        longer = values2
    else:
        shorter = values2
        longer = values1

    for idx, _ in enumerate(longer):
        if idx >= len(shorter):
            line = longer[idx] + ' <=> shorter=None'
        else:
            line = longer[idx] + ' <=> ' + shorter[idx]

        print(line)


if __name__ == '__main__':
    main()
