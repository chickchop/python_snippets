# coding=utf-8
"""
Puzzle Data

Created on 2018.02.14
@author : adam ko

시스템 간 분석을 위해 프로 디스커버리를 적용할 때 대용량 log file 을 조작하기 위한 모듈

"""
import csv
import datetime
from tqdm import tqdm, trange
from operator import itemgetter


def remove_duplicate_activity_(df, case_id, timestamp, activity):
    """
    케이스당 연속되는 엑티비티를 중복으로 보고 삭제하는 함수. pandas 를 이용
    -------------------------------
    :param df : DataFrame/   / 처리해야할 데이터 프레임
    :param case_id : string /   / case id 열 위치
    :param activity : string /    / activity 열 위치
    :param timestamp : string /    / timestamp 열 위치
    :return df : DataFrame/     / 처리 완료된 데이터 프레임
    """
    print("step 1")
    df.sort_values(by=[case_id, timestamp], inplace=True)
    drop_list = list()
    k = 1
    print("step 2")
    while k < len(df):
        if k <= 1 or \
                k == len(df) - 1 or \
                df[case_id].iloc[k] != df[case_id].iloc[k - 1] or \
                df[activity].iloc[k] != df[activity].iloc[k - 1] or \
                df[case_id].iloc[k] != df[case_id].iloc[k + 1]:
            pass
        else:
            drop_list.append(df.index[k])
        k = k + 1
    print("step3")
    df.drop(drop_list, inplace=True)

    return df


def insert_start_end_time(in_file, out_file, case_id_idx, activity_idx, timestamp_idx, encoding='utf-8'):
    """
    케이스에 timestamp 한가지만 존재하는 경우 시작시간과 종료시간 timestamp 를 만들어주는 함수
    -------------------------------
    :param in_file : string / header 와 reader 의 형태로 존재하는 raw log file 형태 / raw log file 경로
    :param out_file : string / header 와 reader 의 형태로 존재하는 raw log file 형태 / 새로 작성될 raw log file 경로
    :param case_id_idx : int /   / case id 열 위치
    :param activity_idx : int /    / activity 열 위치
    :param timestamp_idx : int /    / timestamp 열 위치
    :param encoding : string /    / raw log file을 읽어드릴 때 encoding 방법
    """
    fw = csv.writer(open(out_file, "w", newline=''))

    with open(in_file, "r", encoding=encoding) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        k = 0
        prev_row = ''
        prev_prev_row = ''
        for row in reader:
            if k == 0:
                row.append("StartAt")
                row.append("EndAt")
                fw.writerow(row)
                k = k + 1
                prev_row = row
                continue

            if k > 1 and (
                    prev_row[case_id_idx] != row[case_id_idx] or prev_row[case_id_idx] == row[case_id_idx] and prev_row[
                activity_idx] != row[activity_idx]):
                if k == 2 or prev_prev_row[case_id_idx] != prev_row[case_id_idx]:
                    startat = prev_row[timestamp_idx]
                else:
                    startat = prev_prev_row[timestamp_idx]
                endat = prev_row[timestamp_idx]
                prev_row.append(startat)
                prev_row.append(endat)
                if k > 1:
                    fw.writerow(prev_row)

            if k > 0:
                prev_prev_row = prev_row
            prev_row = row
            k = k + 1

        if prev_prev_row[case_id_idx] != prev_row[case_id_idx]:
            startat = prev_row[timestamp_idx]
        else:
            startat = prev_prev_row[timestamp_idx]
        endat = prev_row[timestamp_idx]
        prev_row.append(startat)
        prev_row.append(endat)
        fw.writerow(prev_row)


def fill_null_activity_(df, activity, fill_data):
    """
    케이스당 null 엑티비티가 있을 경우, 특정 칼럼의 데이터를 복사해와 null 에 넣어주는 함수
    pandas 를 이용
    -------------------------------
    :param df : DataFrame /    / 처리해야할 데이터 프레임
    :param activity : string / / activity 컬럼 명
    :param fill_data : string /  / activity 컬럼에서 빈 row를 채워주기 위한 data column
    :return df : DataFrame /       / 처리 완료된 데이터 프레임
    """
    df[activity].fillna(df[fill_data], inplace=True)

    return df


def case_modeling_(df, old_case_id, new_case_id, checking_col, timestamp, regex_var):
    """
    다른 case 이지만 log 상에서 구별이 되지 않아 하나의 case 로 취급될 경우, log 를 이용하여 서로 다른 case 로 구분되도록 모델링 하는 함수
    pandas 이용
    -------------------------------
    :param df : DataFrame /         / 처리되어야 할 데이터 프레임
    :param old_case_id : string /   / case id 컬럼 명
    :param new_case_id : string /   / 새로 작성될 case id 컬럼 명
    :param checking_col : string /    / case를 나누기 위한 log 데이터 컬럼 명
    :param timestamp : string /    / timestamp 컬럼 명
    :param regex_var : string / r"(.*FxApp)|(.*InitApp)" / checking_col column 의 log 중 case 를 구별 할 수 있는 log 를 찾아내기 위한 정규 표현식
    :return df : DataFrame /      / case modeling 이 완료된 데이터 프레임
    """
    print("step 1")
    # data import and sort
    df[new_case_id] = " "
    df.sort_values(by=[old_case_id, timestamp], inplace=True)
    old_case_id_idx = df.columns.get_loc(old_case_id)
    new_case_id_idx = df.columns.get_loc(new_case_id)
    timestamp_idx = df.columns.get_loc(timestamp)

    print("step 2")
    # temp data set
    tmp_var_time = df.iloc[0, timestamp_idx]
    tmp_var_id = df.iloc[0, old_case_id_idx] + '_' + tmp_var_time.strftime('%Y%m%d %H:%M:%S')

    # reference time set
    dt1 = datetime.datetime(1988, 9, 16, 0, 0, 0)
    dt2 = datetime.datetime(1988, 9, 16, 8, 0, 0)
    ref_time = dt2 - dt1

    # find checking data
    check_list = df[checking_col].str.match(regex_var)

    print("step 3")
    # new case insert
    flag = 0
    for k in tqdm(check_list):
        if k:
            if df.iloc[flag, timestamp_idx] - tmp_var_time > ref_time and \
                    df.iloc[flag, old_case_id_idx] == df.iloc[flag - 1, old_case_id_idx]:
                tmp_var_time = df.iloc[flag, timestamp_idx]
                tmp_var_id = df.iloc[flag, old_case_id_idx] + '_' + tmp_var_time.strftime('%Y%m%d %H:%M:%S')
                df.iloc[flag, new_case_id_idx] = tmp_var_id
            else:
                df.iloc[flag, new_case_id_idx] = tmp_var_id
        if df.iloc[flag, old_case_id_idx] != df.iloc[flag - 1, old_case_id_idx]:
            tmp_var_time = df.iloc[flag, timestamp_idx]
            tmp_var_id = df.iloc[flag, old_case_id_idx] + '_' + tmp_var_time.strftime('%Y%m%d %H:%M:%S')
            df.iloc[flag, new_case_id_idx] = tmp_var_id
        else:
            df.iloc[flag, new_case_id_idx] = tmp_var_id
        flag += 1

    return df


def drop_certain_event_(df, checking_col, regex_var):
    """
    case 에서 필요 없는 activity(e.g. sample app, 의미 없는 log)를 제거하는 함수
    -------------------------------
    :param df : DataFrame /         / 처리되어야 할 데이터 프레임
    :param checking_col : string /    / check 를 하기위한 column 명
    :param regex_var : string / r("") 형태의 정규 표현식 / checking_col column 의 log 중 case 를 구별 할 수 있는 log 를 찾아내기 위한 정규 표현식
    :return df : DataFrame /      / 처리가 완료된 데이터 프레임
    """
    check_list = df[checking_col].str.match(regex_var)
    del_list = list()
    flag = 0
    for k in tqdm(check_list):
        if k:
            del_list.append(flag)
        flag += 1

    df.drop(del_list, 0, inplace=True)

    return df
