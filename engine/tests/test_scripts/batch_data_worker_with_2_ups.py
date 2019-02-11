#!/usr/bin/env python3

"""
Deal with two upstreams at the same time
"""

import os


def main():
    arg1 = os.environ.get('ARG_1')
    arg2 = os.environ.get('ARG_2')

    print('ARG_1')
    print(arg1)

    print('ARG_2')
    print(arg2)


if __name__ == '__main__':
    main()
