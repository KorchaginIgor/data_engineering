#!/usr/bin/env python
"""reducer.py"""

import sys


def perform_reduce():
    current_payment = None
    current_count = 0
    current_tips = 0

    for line in sys.stdin:
        line = line.strip()
        payment_date, tips = line.split('\t', 1)
        payment, date = payment_date.split(',', 1)

        try:
            tips = float(tips)
        except ValueError:
            continue

        if current_payment == payment and current_date == date:
                current_tips += tips
                current_count += 1
        else:
            if current_payment and current_date:
                print('{},{},{}'.format(current_payment, current_date, current_tips))
            current_payment = payment
            current_date = date
            current_tips = tips
            current_count = 1

    if current_payment == payment and current_date == date:
        print('{},{},{}'.format(current_payment, current_date, current_tips))


if __name__ == '__main__':
    perform_reduce()
