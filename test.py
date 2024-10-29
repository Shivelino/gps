#!/usr/bin/env python
# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt

from modules import run_gps


def run_tests():
    def plot_results(data_list):
        plt.plot(range(0, len(data_list)), data_list, marker='o')
        plt.title("Result")
        plt.show()

    acc = []
    for amount in range(500, 10001, 500):
        accuracy = run_gps(amount)
        acc.append(accuracy)
        print(f"Accuracy: {accuracy:.6%}")

    plot_results(acc)


if __name__ == "__main__":
    run_tests()
