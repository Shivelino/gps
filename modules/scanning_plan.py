#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .utils import truncate_ip


def collect_gps_priors(df):
    df['slash'] = df['ip'].apply(lambda x: f"{truncate_ip(x, 16)}/{16}")
    grouped = df.groupby(['neededP', 'slash']).size().reset_index(name='c')
    grouped['condition'] = grouped['c'] / (2 ** (32 - 16))
    filtered = grouped[grouped['condition'] > 0.00001].sort_values(by='c', ascending=False)
    result = filtered[['neededP', 'slash']]
    return result.reset_index(drop=True)

