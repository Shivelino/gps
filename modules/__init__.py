#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path as op

from .dataloader import *
from .features import *
from .prediction_pattern import *
from .prediction import *
from .scanning_plan import *
from .check import *


def cal_seed_accuracy():
    """
    calculate accuracy of seed-table(front-10000)
    """
    df = get_seed_ipp(f"data/data-{10000}.json")
    acc = check_result(df)
    return acc


def stage1(seed_table):
    """
    stage1 of GPS
    """
    dataFeatures, spatialFeatures, crossJoinFeatures = get_features(seed_table)
    predictive_pattern = get_predictive_pattern(seed_table, dataFeatures, spatialFeatures, crossJoinFeatures)
    s1_scanning_plan = collect_gps_priors(predictive_pattern)
    return dataFeatures, spatialFeatures, crossJoinFeatures, predictive_pattern, s1_scanning_plan


def stage2(seed_table, dataFeatures, spatialFeatures, crossJoinFeatures, predictive_pattern, s1_scanning_plan):
    """
    stage2 of GPS
    """
    s2_base = combine_features(dataFeatures, spatialFeatures, crossJoinFeatures)
    s2_pairs = get_predictions(s2_base, predictive_pattern)
    s2_filtpriors = filter_prior_scans(s2_pairs, s1_scanning_plan, 16)
    prediction_result = filter_seed_services(s2_filtpriors, seed_table)
    return prediction_result


def gps(seed_table):
    """
    execute GPS
    """
    dataFeatures, spatialFeatures, crossJoinFeatures, predictive_pattern, s1_scanning_plan = stage1(seed_table)  # first
    prediction_result = stage2(seed_table, dataFeatures, spatialFeatures, crossJoinFeatures, predictive_pattern,
                               s1_scanning_plan)  # second
    return prediction_result


def run_gps(amount: int):
    """
    run GPS test
    """
    result_file = f"data/results-{amount}.csv"

    if not op.exists(result_file):
        seed_file_path = f"data/data-{amount}.json"
        if not op.exists(seed_file_path):
            split_seed(amount, "data")

        seed_table = load_seed_file(seed_file_path)
        result = gps(seed_table)
        result.to_csv(result_file, index=False)

    return check_result_file(result_file)
