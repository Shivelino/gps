#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse

from modules import run_gps, cal_seed_accuracy


def script_run(args):
    if args.seed_accuracy:
        accuracy = cal_seed_accuracy()
        print(f"Seed Accuracy: {accuracy:.6%}")
    else:
        accuracy = run_gps(args.amount)
        print(f"Accuracy: {accuracy:.6%}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-accuracy", help="calculate seed accuracy", action="store_true")
    parser.add_argument("-a", "--amount", type=int, default=200, help="data amount")

    script_run(parser.parse_args())
