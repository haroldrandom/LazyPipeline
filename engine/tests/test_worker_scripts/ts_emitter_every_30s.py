#!/usr/bin/env python3

"""
Simply emit current timestamp ten times every 1 second
"""

import time
from datetime import datetime


def main():30
    for i in range(10):
        time.sleep(1)

        n = datetime.now()

        data = 'ID:[{0}] [{1}]'.format(i + 1, n.isoformat())
        print(data)


if __name__ == '__main__':
    main()
