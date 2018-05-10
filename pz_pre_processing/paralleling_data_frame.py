import os
from multiprocessing import Pool

import numpy as np
import pandas as pd


def _apply_df(args):
    df, func, kwargs = args
    para_list = list(kwargs.values())

    return func(df, **kwargs)
    #return df.apply(func, **kwargs, broadcast=True)


def paralleling_data_frame(df, func, **kwargs):
    """
    병렬처리를 멀티코어 방식으로 활용하는 함수
    유의할 점 : func에 전달되는 변수는 오직 df 뿐이다. func에 다른 param이 필요하다면
    -------------------------------------------------------
    :param df: DataFrame /       /병렬 처리를 하기 위한 데이터 프레임
    :param func: function /       / 병렬 처리를 적용하기 위한 로직이 있는 함수
    :return df : DataFrame /       / 병렬 처리가 완료된 후 하나로 합쳐진 데이터 프레임
    """
    num_cores = os.cpu_count() - 1
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    results = pool.map(_apply_df, [(_df, func,  kwargs) for _df in df_split])
    pool.close()
    pool.join()
    df = pd.concat(results)

    return df
