# coding=utf-8
"""
Puzzle Data

Created on 2018.07.11
@author : adam ko

머신 러닝을 적용할 때 기본적으로 적용되는 데이터 처리 함수 모듈
"""


def activity_to_symbol(acti_series):
    """
    activity 데이터를 symbol 형태로 변환하는 함수
    ----------------------------------------------------------------------------------------
    :param acti_series: list / [smartMES^P92, smartMES^P2300,...] / 엑티비티 log 의 리스트 형태
    :return symbol_list: list / [a,b,...] / symbol 의 리스트 형태
    :return acti_dic: dic / {a: smartMES^P92, b: smartMES^P2300,...} / symbol 과 activity 의 변환 표.
    """
    acti_key = list(set(acti_series))
    end = 33 + len(acti_key)
    acti_value = [chr(i) for i in range(33, end)]
    acti_dic = {k: v for (k, v) in zip(acti_key, acti_value)}

    symbol_list = []
    for i in acti_series:
        j = acti_dic[i]
        symbol_list.append(j)

    return symbol_list, acti_dic


def symbol_to_activity(symbol_list, acti_dic):
    """
    symbol 형태의 데이터를 다시 activity 데이터로 변환하는 함수
    --------------------------------------------------------------------------------------
    :param symbol_list: list / [a,b,,...] / symbol 의 리스트 형태
    :param acti_dic: dic / {a: smartMES^P92, b: smartMES^P2300,...} / symbol 과 activity 의 변환 표.
    :return acti_list: list / [smartMES^P92, smartMES^P2300,...] / 엑티비티 log 의 리스트 형태
    """
    symbol_dic = {v: k for k, v in acti_dic.items()}

    acti_list = []
    for i in symbol_list:
        j = symbol_dic[i]
        acti_list.append(j)

    return acti_list
