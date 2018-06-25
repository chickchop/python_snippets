"""
시스템 간 분석을 위해 프로 디스커버리를 적용할 때 2 gb 이상의 대용량 log file을 조작하기 위한 모듈
Created on 2018.06.25
@author : adam ko
"""
import csv
from operator import itemgetter
import datetime

def remove_duplicate_activity_(in_file, out_file, case_id_idx, activity_idx, timestamp_idx, encoding='utf-8'):
    """
    케이스당 연속되는 엑티비티를 중복으로 보고 삭제하는 함수.pandas를 이용할 경우 낭비되는 메모리와 느린 속도 개선.
    parameters
    -------------------------------
    in_file : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    case_id_idx : int /   / case id 열 위치
    activity_idx : int /    / activity 열 위치
    timestamp_idx : int /    / timestamp 열 위치
    encoding : string /    / raw log file을 읽어드릴 때 encoding 방법
    """
    df = list()
    rows = list()
    with open(in_file, "r", encoding=encoding) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        header = next(reader)
        for row in reader:
            rows.append(row)

        rows.sort(key=itemgetter(case_id_idx, timestamp_idx))

        df.append(header)
        df.extend(rows)

        fw = csv.writer(open(out_file, "w", newline='', encoding=encoding))

        k = 0
        while k < len(df):
            if k <= 1 or \
                    k == len(df) - 1 or \
                    df[k][case_id_idx] != df[k - 1][case_id_idx] or \
                    df[k][activity_idx] != df[k - 1][activity_idx] or \
                    df[k][case_id_idx] != df[k + 1][case_id_idx]:
                fw.writerow(df[k])
            k = k + 1


def case_modeling_(df, old_case_id, new_case_id, checking_col, timestamp, regex_var):
    """
    다른 case이지만 log상에서 구별이 되지 않아 하나의 case로 취급될 경우, log를 이용하여 서로 다른 case로 구분되도록 모델링 하는 함수
    pandas이용
    parameters
    -------------------------------
    df : DataFrame /         / 처리되어야 할 데이터 프레임
    old_case_id : string /   / case id 컬럼 명
    new_case_id : string /   / 새로 작성될 case id 컬럼 명
    checking_col : string /    / case를 나누기 위한 log 데이터 컬럼 명
    timestamp : string /    / timestamp 컬럼 명
    regex_var : string / r"(.*FxApp)|(.*InitApp)" / checking_col column의 log중 case를 구별 할 수 있는 log를 찾아내기 위한 정규 표현식
    
    returns
    --------------------------------
    df : DataFrame /      / case modeling이 완료된 데이터 프레임
    """
    # data import and sort
    df[new_case_id] = " "
    df.sort_values(by=[old_case_id, timestamp], inplace=True)
    old_case_id_idx = df.columns.get_loc(old_case_id)
    new_case_id_idx = df.columns.get_loc(new_case_id)
    timestamp_idx = df.columns.get_loc(timestamp)
    print(1)
    # temp data set
    tmp_var_time = df.iloc[0, timestamp_idx]
    tmp_var_id = df.iloc[0, old_case_id_idx] + '_' + tmp_var_time.strftime('%Y%m%d %H:%M:%S')
    print(2)
    # reference time set
    dt1 = datetime.datetime(1988, 9, 16, 0, 0, 0)
    dt2 = datetime.datetime(1988, 9, 16, 8, 0, 0)
    ref_time = dt2 - dt1
    print(3)
    # find checking data
    check_list = df[checking_col].str.match(regex_var)

    # new case insert
    flag = 0
    for k in check_list:
        print("processing... %d" % flag)
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
        
        
    print(4)
    return df
