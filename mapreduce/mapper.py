#!/usr/bin/env python
"""mapper.py"""

import sys

payment_dic = [
    'Credit card',
    'Cash',
    'No charge',
    'Dispute',
    'Unknown',
    'Voided trip'
]

def perform_map():
    for line in sys.stdin:
        line = line.strip()
        rows = line.split(',')
        try:
            tips = float(rows[13])
            payment_type = payment_dic[int(rows[9]) - 1]
        except ValueError:
            continue
        split_date = rows[1].split('-')
        if split_date[0] != '2020':
            continue
        month = str('{}-{}'.format(split_date[0], split_date[1]))
        print('{},{}\t{}'.format(payment_type, month, tips))


if __name__ == '__main__':
    perform_map()