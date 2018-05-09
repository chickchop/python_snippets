import pandas as pd
import numpy as np
import os

def paralleling_data_frame(df, func):
    """
    병렬처리를 멀티코어 방식으로 활용하는 함수
    -------------------------------------------------------
    :param df: DataFrame /       /병렬 처리를 하기 위한 데이터 프레임
    :param func: function /       / 병렬 처리를 적용하기 위한 로직이 있는 함수
    :return df : DataFrame /       / 병렬 처리가 완료된 후 하나로 합쳐진 데이터 프레임
    """
    num_cores = os.cpu_count()
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()

    return df
