#!/usr/bin/env python
# -*- coding: utf-8 -*-
import socket
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

from modules import *


def is_port_open(ip: str, port: int, timeout: float = 2) -> bool:
    """
    Check if a port is open
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False


def check_single_port(row):
    """
    Check if a port is open with a pandas frame row
    """
    ip, port = row["ip"], int(row["p"])
    return is_port_open(ip, port)


def check_result(result_df):
    """
    Check result dataframe
    """
    with Pool(cpu_count()) as pool:
        results = list(
            tqdm(pool.imap(check_single_port, [row for _, row in result_df.iterrows()]), total=len(result_df)))
    open_cnt = sum(results)
    accuracy = round(open_cnt / result_df.shape[0], 6)

    # print(f"Accuracy: {accuracy:.6%}")
    return accuracy


def check_result_file(result_file: str):
    """
    Check result file
    """
    result_df = pd.read_csv(result_file)
    return check_result(result_df)
