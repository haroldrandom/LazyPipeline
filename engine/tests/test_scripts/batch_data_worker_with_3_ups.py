#!/usr/bin/env python3

"""
Deal with three upstreams at the same time
"""

import os
import json


def main():
    arg1 = os.environ.get('ARG_1')
    arg2 = os.environ.get('ARG_2')
    arg3 = os.environ.get('ARG_3')

    arg1 = json.loads(arg1)
    line1 = 'ARG_1.size={0}'.format(len(arg1))
    print(line1)

    arg2 = json.loads(arg2)
    for arg in arg2:
        arr2 = arg.split('\n')
        line2 = 'ARG_2.each_element.size={0}'.format(len(arr2))
        print(line2)

    print('-' * 30 + 'ARG_3' + '-' * 30)

    arg3 = json.loads(arg3)
    for arg in arg3:
        arr = arg.split('\n')
        for item in arr:
            print(item)


if __name__ == '__main__':
    main()
