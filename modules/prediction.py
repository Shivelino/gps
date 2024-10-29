#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .features import *


def combine_features(data_features, spatial_features, cross_join_features):
    """
    Combine all features
    """
    combined_df = pd.concat(
        [data_features, spatial_features[['ip', 'p', 'server']], cross_join_features[['ip', 'p', 'server']]],
        ignore_index=True)
    return combined_df


def get_predictions(base_df, pred_table_df):
    """
    get predicted [ip, port]
    """
    distinct_base = base_df[['ip', 'p', 'server']].drop_duplicates()
    filtered_pred_table = pred_table_df[pred_table_df['p'].notnull()][['neededP', 'p', 'server']].drop_duplicates()
    preds = pd.merge(distinct_base, filtered_pred_table, left_on=['p', 'server'], right_on=['neededP', 'server'],
                     how='inner')
    preds.rename(columns={'p_y': 'p'}, inplace=True)
    return preds[['ip', 'p']].reset_index(drop=True)


def filter_prior_scans(preds_df, prior_table_df, step):
    """
    filter pairs in prior scan list
    """
    preds_df['slash'] = preds_df['ip'].apply(lambda ip: f"{ip}/{step}")
    distinct_preds = preds_df[['slash', 'p']].drop_duplicates()

    # Create the slash column for the prior_table_df
    prior_table_df['slash'] = prior_table_df['neededP'].apply(lambda neededP: f"{neededP}/{step}")
    filt_prior = distinct_preds[~distinct_preds.set_index(['slash', 'p']).index.isin(
        prior_table_df.set_index(['slash', 'neededP']).index
    )].reset_index(drop=True)

    result = pd.merge(filt_prior, preds_df[['ip', 'p', 'slash']], on=['slash', 'p'], how='inner')
    return result[['ip', 'p']].reset_index(drop=True)


def filter_seed_services(filtpriors_df, seed_table_df):
    """
    filter pairs in seed table
    """
    filt_seed = filtpriors_df[['ip', 'p']].drop_duplicates()
    seed_services = seed_table_df[['ip', 'p']].drop_duplicates()

    filt_seed = filt_seed[~filt_seed.set_index(['ip', 'p']).index.isin(seed_services.set_index(['ip', 'p']).index
                                                                       )].reset_index(drop=True)

    filt_seed = filt_seed[['ip', 'p']].drop_duplicates().reset_index(drop=True)
    return filt_seed[['ip', 'p']].drop_duplicates().reset_index(drop=True)
