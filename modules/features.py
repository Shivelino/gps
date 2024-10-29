#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

from .utils import truncate_ip


def extract_data_features(df):
    """
    extract data features
    """
    http_split_df = df.assign(http_split=df['data'].str.split("\n")).explode('http_split')
    http_line_df = http_split_df[['ip', 'p']].copy()
    http_line_df['server'] = "http line: " + http_split_df['http_split']

    fingerprint_df = df[['ip', 'p', 'fingerprint']].copy()
    fingerprint_df['server'] = "fingerprint: " + fingerprint_df['fingerprint']

    window_df = df[['ip', 'p', 'w']].copy()
    window_df['server'] = "window: " + window_df['w'].astype(str)

    ssh_banner_df = df[df['fingerprint'] == "ssh"][['ip', 'p', 'data']].copy()
    ssh_banner_df['server'] = "ssh banner: " + ssh_banner_df['data']

    result_df = pd.concat([http_line_df, fingerprint_df, window_df, ssh_banner_df], ignore_index=True)

    result_df = result_df[['ip', 'p', 'server']].drop_duplicates()
    return result_df


def extract_spatial_features(df):
    """
    extract spatial feature
    """
    spatial_features = []

    for bits in range(16, 23):
        truncated_df = df[['ip', 'p']].copy()
        truncated_df['server'] = df['ip'].apply(lambda ip: f"s{bits}: {truncate_ip(ip, bits)}")
        spatial_features.append(truncated_df)

    l4_df = df[['ip', 'p']].copy()
    l4_df['server'] = "L4"
    spatial_features.append(l4_df)

    asn_df = df[['ip', 'p', 'asn']].copy()
    asn_df['server'] = "asn: " + asn_df['asn'].astype(str)
    spatial_features.append(asn_df)

    result_df = pd.concat(spatial_features, ignore_index=True)
    return result_df


def cross_join_features(data_features_df, full_df):
    """
    get cross-join feature(with data feature)
    """
    # Create subnet-16 feature (similar to t2 in SQL)
    subnet_16_df = full_df[['ip']].drop_duplicates().copy()
    subnet_16_df['server'] = subnet_16_df['ip'].apply(lambda ip: f"{truncate_ip(ip, 16)}")

    join_subnet_16_df = pd.merge(data_features_df, subnet_16_df, on='ip', suffixes=('_t1', '_t2'))
    join_subnet_16_df['server'] = "16: " + join_subnet_16_df['server_t1'] + "-" + join_subnet_16_df['server_t2']

    asn_df = full_df[['ip', 'asn']].drop_duplicates().copy()
    asn_df['server'] = "asn: " + asn_df['asn'].astype(str)

    join_asn_df = pd.merge(data_features_df, asn_df, on='ip', suffixes=('_t1', '_t2'))
    join_asn_df['server'] = "asn: " + join_asn_df['server_t1'] + "-" + join_asn_df['server_t2']

    result_df = pd.concat([join_subnet_16_df[['ip', 'p', 'server']], join_asn_df[['ip', 'p', 'server']]],
                          ignore_index=True)
    return result_df


def get_features(seed_table):
    """
    get all features of seed_table
    """
    dataFeatures = extract_data_features(seed_table)
    spatialFeatures = extract_spatial_features(seed_table)
    crossJoinFeatures = cross_join_features(dataFeatures, seed_table)
    return dataFeatures, spatialFeatures, crossJoinFeatures
