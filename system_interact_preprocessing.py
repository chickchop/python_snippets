'''
시스템 간 분석을 위해 프로 디스커버리를 적용할 때 대용량 log file을 조작하기 위한 모듈

Created on 2018.02.14

@author : adam ko
'''
import numpy as np
import pandas as pd
import sys
import os
import time
import csv
from operator import itemgetter,attrgetter
import datetime


def progress_bar(value, endvalue, bar_length=20):

    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length)-1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()


def remove_duplicate_activity_by_pandas(in_file_path, case_id, timestamp, activity, out_file_path, encoding='utf-8'):
    '''
    케이스당 연속되는 엑티비티를 중복으로 보고 삭제하는 함수. pandas를 이용
    
    parameters
    -------------------------------
    in_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    case_id : string /   / case id 열 위치
    activity : string /    / activity 열 위치
    timestamp : string /    / timestamp 열 위치
    encoding : stirng /    / raw log file을 읽어드릴 때 encoding 방법
    '''
    
    df = pd.read_csv(in_file_path, encoding= encoding)
    df.sort_values(by=[case_id,timestamp], inplace= True)
    drop_list = list()
    k = 1
    while k < len(df):
        if df[case_id].iloc[k] == df[case_id].iloc[k-1]:
            if df[activity].iloc[k] == df[activity].iloc[k-1]:
                if df[activity].iloc[k] == df[activity].iloc[k+1]:
                    pass
                else:
                    drop_list.append(df.index[k])
                    
        k = k + 1
    df.drop(drop_list, inplace=True)

    df.to_csv(out_file_path, encoding = encoding, index=False)   


def remove_duplicate_activity(in_file_path, out_file_path, case_id_idx, activity_idx, timestamp_idx, encoding='utf-8'):
    '''
    케이스당 연속되는 엑티비티를 중복으로 보고 삭제하는 함수.pandas를 이용할 경우 낭비되는 메모리와 느린 속도 개선.
    
    parameters
    -------------------------------
    in_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    case_id_idx : int /   / case id 열 위치
    activity_idx : int /    / activity 열 위치
    timestamp_idx : int /    / timestamp 열 위치
    encoding : stirng /    / raw log file을 읽어드릴 때 encoding 방법
    '''
    df = list()
    rows = list()
    with open(in_file_path, "r", encoding=encoding) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        header = next(reader)
        for row in reader:
            rows.append(row)
        
        rows.sort(key=itemgetter(case_id_idx, timestamp_idx))
        
        df.append(header)
        df.extend(rows)
        
        fw = csv.writer(open(out_file_path, "w",newline=''))

        k = 0
        while k < len(df):
            if k <= 1 or \
                k == len(df) -1 or \
                df[k][case_id_idx] != df[k-1][case_id_idx] or \
                   df[k][activity_idx] != df[k-1][activity_idx] or \
                        df[k][case_id_idx] != df[k+1][case_id_idx]:
                    fw.writerow(df[k])
            k = k + 1


def insert_start_end_time(in_file_path, out_file_path, case_id_idx, activity_idx, timestamp_idx, encoding='utf-8'):
    '''
    케이스에 timestamp 한가지만 존재하는 경우 시작시간과 종료시간 timestamp를 만들어주는 함수

    parameters
    -------------------------------
    in_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    case_id_idx : int /   / case id 열 위치
    activity_idx : int /    / activity 열 위치
    timestamp_idx : int /    / timestamp 열 위치
    encoding : stirng /    / raw log file을 읽어드릴 때 encoding 방법
    '''
    fw = csv.writer(open(out_file_path, "w", newline=''))

    with open(in_file_path, "r", encoding=encoding) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        k = 0
        prev_row = ''
        prev_prev_row = ''
        for row in reader:
            if k == 0 :
                row.append("StartAt")
                row.append("EndAt")
                fw.writerow(row)
                k = k + 1
                prev_row = row
                continue

            if k > 1 and  ( prev_row[case_id_idx] != row[case_id_idx] or prev_row[case_id_idx] == row[case_id_idx] and prev_row[activity_idx] != row[activity_idx]) :
                if k == 2 or prev_prev_row[case_id_idx] != prev_row[case_id_idx] :
                    startat = prev_row[timestamp_idx]
                else : 
                    startat = prev_prev_row[timestamp_idx]
                endat = prev_row[timestamp_idx]
                prev_row.append(startat)
                prev_row.append(endat)
                if k > 1 :
                    fw.writerow(prev_row)

            if k > 0 :
                prev_prev_row = prev_row
            prev_row = row
            k = k + 1

        if prev_prev_row[case_id_idx] != prev_row[case_id_idx] :
            startat = prev_row[timestamp_idx]
        else :
            startat = prev_prev_row[timestamp_idx]
        endat = prev_row[timestamp_idx]
        prev_row.append(startat)
        prev_row.append(endat)
        fw.writerow(prev_row)


def fill_null_activity_(file_path, activity, fill_data, encoding='utf-8'):
    '''
    케이스당 null 엑티비티가 있을 경우, 특정 칼럼의 데이터를 복사해와 null에 넣어주는 함수
    pandas를 이용

    parameters
    -------------------------------
    file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    activity : string / / activity 컬럼 명
    fill_data : string /  / activity 컬럼에서 빈 row를 채워주기 위한 data column
    encoding : stirng / / raw log file을 읽어드릴 때 encoding 방법
    '''

    df = pd.read_csv(file_path, encoding= encoding)
    df[activity].fillna(df[fill_data], inplace=True)

    df.to_csv(file_path, encoding = encoding, index=False)


def case_modeling_(in_file_path, out_file_path, old_case_id, new_case_id, checking_data, timestamp, regex_var, encoding = "utf-8"):
    '''
    다른 case이지만 log상에서 구별이 되지 않아 하나의 case로 취급될 경우, log를 이용하여 서로 다른 case로 구분되도록 모델링 하는 함수
    pandas이용

    parameters
    -------------------------------
    in_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    old_case_id : string /   / case id 컬럼 명
    new_case_id : string /   / 새로 작성될 case id 컬럼 명
    checking_data : string /    / case를 나누기 위한 log 데이터 컬럼 명
    timestamp : string /    / timestamp 컬럼 명
    regex_var : string / r("") 형태의 정규 표현식 / checking_data column의 log중 case를 구별 할 수 있는 log를 찾아내기 위한 정규 표현식
    encoding : stirng /    / raw log file을 읽어드릴 때 encoding 방법
    '''
    # data import and sort
    date_parser = lambda x: pd.datetime.strptime(x, '%Y%m%d %H:%M:%S')
    df = pd.read_csv(in_file_path, encoding= encoding, dtype = {old_case_id : str},parse_dates=[timestamp], date_parser=date_parser)
    df.sort_values(by=[old_case_id, timestamp], inplace = True)
    old_case_id_idx = df.columns.get_loc(old_case_id)
    new_case_id_idx = df.columns.get_loc(new_case_id)
    timestamp_idx = df.columns.get_loc(timestamp)

    # temp data set
    tmp_var_time = df.iloc[0, timestamp_idx]
    tmp_var_id = df.iloc[0, old_case_id_idx]+ '_'+tmp_var_time.strftime('%Y%m%d %H:%M:%S')

    # reference time set
    dt1 = datetime.datetime(1988, 9, 16, 0, 0 ,0)
    dt2 = datetime.datetime(1988, 9, 16, 6, 0, 0)
    ref_time = dt2 - dt1

    # find checking data
    # check_list = df[checking_data].str.match(r'(.*FxApp)|(.*InitApp)')
    check_list = df[checking_data].str.match(regex_var)

    # new case insert
    flag = 0
    for k in check_list:
        if k:
            if df.iloc[flag, timestamp_idx] - tmp_var_time < ref_time:
                df.iloc[flag, new_case_id_idx] = tmp_var_id
            else:
                tmp_var_time = df.iloc[flag, timestamp_idx]
                tmp_var_id = df.iloc[flag, old_case_id_idx]+ '_'+tmp_var_time.strftime('%Y%m%d %H:%M:%S')
                df.iloc[flag, new_case_id_idx] = tmp_var_id
        else:
            df.iloc[flag, new_case_id_idx] = tmp_var_id
        flag += 1

    # export
    df.to_csv(out_file_path, encoding = encoding, index=False)


if __name__ == '__main__':
    remove_duplicate_activity("C:\\Users\\ko\\Desktop\\hynix analyze\\data\\random_data10.csv", "C:\\Users\\ko\\Desktop\\hynix analyze\\data\\sorted_out.csv",0,1,3,'utf-8')
    # insert_start_end_time("C:\\Users\\ko\\Desktop\\hynix analyze\\data\\sorted_out.csv", "C:\\Users\\ko\\Desktop\\hynix analyze\\data\\start_end_out.csv",0,1,3,'utf-8')
