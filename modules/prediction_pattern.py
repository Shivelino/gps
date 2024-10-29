#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .features import *


def filter_hosts_with_ports(df, data_features_df, spatial_features_df, cross_join_features_df):
    """
    filter hosts with ports
    """
    ip_port_count = df.groupby('ip')['p'].nunique().reset_index()
    filtered_ips = ip_port_count[(ip_port_count['p'] > 1) & (ip_port_count['p'] < 11)][['ip']]
    merged_data_features = pd.merge(filtered_ips, data_features_df, on='ip')
    merged_spatial_features = pd.merge(filtered_ips, spatial_features_df, on='ip')
    merged_cross_join_features = pd.merge(filtered_ips, cross_join_features_df, on='ip')
    result_df = pd.concat([merged_data_features, merged_spatial_features, merged_cross_join_features],
                          ignore_index=True)
    return result_df


def generate_service_pairs(base_df):
    """
    generate pairs
    """
    pairs_df = pd.merge(base_df, base_df, on='ip', suffixes=('_t1', '_t2'))
    pairs_df = pairs_df[pairs_df['p_t1'] != pairs_df['p_t2']]
    pairs_df = pairs_df[['ip', 'p_t1', 'server_t1', 'p_t2']].drop_duplicates()
    pairs_df.columns = ['ip', 'p1', 'p1_server', 'p2']
    return pairs_df


def calculate_hit_rates(pairs_df, base_df):
    """
    calculate hit rates
    """
    count_p2_df = pairs_df.groupby(['p1', 'p1_server', 'p2']).size().reset_index(name='count_p2')
    cp_server_df = base_df.groupby(['p', 'server']).size().reset_index(name='cp_server')
    found_corr_df = pd.merge(count_p2_df, cp_server_df, left_on=['p1', 'p1_server'], right_on=['p', 'server'])
    found_corr_df['hitrate'] = found_corr_df['count_p2'] / found_corr_df['cp_server']
    found_corr_df = found_corr_df[found_corr_df['count_p2'] > 2]
    found_corr_df = found_corr_df[['p1', 'p1_server', 'p2', 'hitrate', 'count_p2', 'cp_server']]
    return found_corr_df


def calculate_meta(pairs_df, found_corr_df, base_df):
    """
    calculate meta table
    """
    t1 = pairs_df[['ip', 'p1', 'p2', 'p1_server']]
    t1 = t1.merge(found_corr_df[['p1', 'p2', 'hitrate', 'p1_server', 'count_p2', 'cp_server']],
                  how='left', left_on=['p1', 'p2', 'p1_server'], right_on=['p1', 'p2', 'p1_server'])
    t2 = base_df.groupby('ip').agg(c=('p', 'nunique')).reset_index()
    meta_df = t1.merge(t2, how='inner', on='ip')
    meta_df.rename(columns={'p2': 'p', 'p1': 'neededP'}, inplace=True)
    return meta_df


def combine_correlative_features(meta_df, hitrate_threshold):
    """
    combine correlative features
    """
    filtered_meta = meta_df[meta_df['c'] > 1]
    aggregated = (filtered_meta.groupby(['ip', 'p']).agg(neededP=('neededP', 'last'), count_p2=('count_p2', 'last'),
                                                         hitrate=('hitrate', 'last'), cp_server=('cp_server', 'last'),
                                                         server=('p1_server', 'last')).reset_index())
    filtered_aggregated = aggregated[aggregated['hitrate'] > hitrate_threshold]
    single_port_ips = (meta_df[['ip', 'p']].drop_duplicates())
    port_counts = (meta_df.groupby('ip').agg(c=('p', 'nunique')).reset_index())
    single_port_ips = single_port_ips.merge(port_counts[port_counts['c'] == 1][['ip']], on='ip')

    # Prepare final result DataFrame
    single_port_ips['neededP'] = None
    single_port_ips['count_p2'] = None
    single_port_ips['hitrate'] = None
    single_port_ips['cp_server'] = None
    single_port_ips['server'] = None

    # Combine both DataFrames
    final_result = pd.concat(
        [filtered_aggregated, single_port_ips[['ip', 'neededP', 'p', 'count_p2', 'hitrate', 'cp_server', 'server']]]
    ).reset_index(drop=True)
    return final_result


def get_predictive_pattern(seed_table, dataFeatures, spatialFeatures, crossJoinFeatures):
    """
    get predictive pattern
    """
    base = filter_hosts_with_ports(seed_table, dataFeatures, spatialFeatures, crossJoinFeatures)
    pairs = generate_service_pairs(base)
    found_corr = calculate_hit_rates(pairs, base)
    meta = calculate_meta(pairs, found_corr, base)
    pattern = combine_correlative_features(meta, 0.00001)
    return pattern
