'''
프로 디스커버리를 적용할 때 대용량 log file을 조작하기 위한 모듈

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

def remove_duplicate_activity_by_pandas(in_file_path, out_file_path, encoding='utf-8', case_id='CaseID', timestamp='Timestamp', activity='Activity'):
    '''
    케이스당 중복되는 엑티비티를 삭제하는 함수. pandas를 이용
    
    parameters
    -------------------------------
    in_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / raw log file 경로
    out_file_path : string / header와 reader의 형태로 존재하는 raw log file형태 / 새로 작성될 raw log file 경로
    case_id_idx : int /   / case id 열 위치
    activity_idx : int /    / activity 열 위치
    timestamp_idx : int /    / timestamp 열 위치
    encoding : stirng /    / raw log file을 읽어드릴 때 encoding 방법
    '''
    
    df = pd.read_csv(in_file_path, encoding= encoding)
    df.sort_values(by=[case_id,timestamp]).drop_duplicates(keep=False)
    k = 1
    while k < len(df):
        if df[case_id].iloc[k] == df[case_id].iloc[k-1]:
            if df[activity].iloc[k] == df[activity].iloc[k-1]:
                if df[activity].iloc[k] == df[activity].iloc[k+1]:
                    pass
                else:
                    df.drop(df.index[k], inplace=True)
                    k = k - 1
        k = k + 1

    df.to_csv(out_file_path, encoding = 'utf-8', index=False)   

def progressBar(value, endvalue, bar_length=20):
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length)-1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()


def remove_duplicate_activity(in_file_path, out_file_path, case_id_idx, activity_idx, timestamp_idx, encoding='utf-8'):
    '''
    케이스당 중복되는 엑티비티를 삭제하는 함수.pandas를 이용할 경우 낭비되는 메모리와 느린 속도 개선.
    
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


if __name__ == '__main__':
    remove_duplicate_activity("./random_data10.csv", "sorted_out.csv")
    insert_start_end_time("./sorted_out.csv", "start_end_out.csv")
