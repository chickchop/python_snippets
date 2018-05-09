"""
시스템 간 분석을 위해 프로 디스커버리를 적용할 때 대용량 log file을 조작하기 위한 모듈
Created on 2018.02.14
@author : adam ko
"""
import csv
import datetime
import os
import sys
from multiprocessing import Pool
from operator import itemgetter

import numpy as np
import pandas as pd


def progress_bar(value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()


def remove_duplicate_activity_by_pandas(df, case_id, timestamp, activity):
    """
    케이스당 연속되는 엑티비티를 중복으로 보고 삭제하는 함수. pandas를 이용
    parameters
    -------------------------------
    df : DataFrame/   / 처리해야할 데이터 프레임
    case_id : string /   / case id 열 위치
    activity : string /    / activity 열 위치
    timestamp : string /    / timestamp 열 위치
    returns
    -------------------------------
    df : DataFrame/     / 처리 완료된 데이터 프레임
    """
    df.sort_values(by=[case_id, timestamp], inplace=True)
    drop_list = list()
    k = 1
    while k < len(df):
        if df[case_id].iloc[k] == df[case_id].iloc[k - 1]:
            if df[activity].iloc[k] == df[activity].iloc[k - 1]:
                if df[activity].iloc[k] == df[activity].iloc[k + 1]:
                    pass
                else:
                    drop_list.append(df.index[k])

        k = k + 1
    df.drop(drop_list, inplace=True)

    return df
