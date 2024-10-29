#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import json


def split_seed(amount: int, folder: str):
    """
    Split seed file with custom amount
    """
    rf = open(f"{folder}/lzr_seed_april2021_filt.json", 'r', encoding='utf-8')
    wf = open(f"{folder}/data-{amount}.json", 'w', encoding='utf-8')

    for i in range(amount):
        line = rf.readline()
        wf.write(line)

    rf.close()
    wf.close()


def load_seed_file(filepath: str):
    """
    Load seed file as pandas frame
    """
    data_list = []

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            json_data = json.loads(line.strip())
            data_list.append(json_data)

    df = pd.DataFrame(data_list)
    return df


def get_seed_ipp(filepath: str):
    """
    get [ip, port] from seed file
    """
    df = load_seed_file(filepath)
    return df[['ip', 'p']].drop_duplicates()
