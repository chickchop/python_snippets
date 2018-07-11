# coding=utf-8
"""
csv 파일 형태의 log 데이터를 조작하기 위한 함수 모듈
Created on 2018.07.11
@author : adam ko
"""
import glob
import os
from shutil import move
from tempfile import mkstemp


def csv_header_add(in_file, add_line, encoding="UTF-8"):
    """
    header 가 없는 CSV 파일에 header 를 추가시키는 함수
    --------------------------------------------------------------------------
    :param in_file: str / "D:\\test\\log_data000.csv" / 파일 위치
    :param encoding: str / 'utf-8' / 인코딩 방식
    :param add_line: str / "sys_id,user_id,exec_typ,prog_id,app_id,log_timekey\n" / 추가할 헤드 내용
    """
    with open(in_file, "r+", encoding=encoding) as f:
        s = f.read()
        f.seek(0)

        f.write(add_line + s)


def csv_split(n_div_cnt, file_path, file_name, div_file_folder, file_exe=".csv", encoding="utf-8"):
    """
    대용량 파일을 분할하기 위한 함수.
    분할된 파일은 따로 분할 폴더를 만들어 그 안에 숫자가 붙어서 저장된다.
    -------------------------------------------------------
    :param n_div_cnt: int / 1000000 / 분할된 파일에 저장될 row수
    :param file_path: str / "D:\\" / 분할할 파일이 저장될 폴더 경로
    :param file_name: str / "log_data" / 분할하기 위한 대용량 파일 명
    :param div_file_folder: str / "div_file\\" / 분할된 파일이 저장될 폴더 명
    :param file_exe: str / ".csv" / 분할할 파일의 확장자
    :param encoding: str / "utf-8" / 인코딩 방식
    """
    dir_name = file_path + div_file_folder
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

    file_idx = 0
    n_line_cnt = 0
    with open(file_path + file_name + file_exe, "r", encoding=encoding) as f:
        g = open(dir_name + file_name + "_" + str(file_idx) + file_exe, 'w', encoding=encoding)
        while True:
            line = f.readline()
            if not line:
                break
            if n_line_cnt == n_div_cnt:
                g.close()
                file_idx += 1
                n_line_cnt = 0
                g = open(dir_name + file_name + "_" + str(file_idx) + file_exe, 'w', encoding=encoding)
            n_line_cnt += 1
            g.write(line)
        g.close()


def csv_merge(file_path, out_file_name, regex_var, file_exe=".csv", encoding="UTF-8"):
    """
    분할된 파일을 하나로 합치기 위한 함수.
    분할 폴더의 분할된 csv 파일이 하나로 합쳐진다.
    -------------------------------------------------------
    :param file_path: str / "D:\\div_file\\" / 분할된 파일이 저장된 폴더 경로
    :param out_file_name: str / "log_data_201804" / 합쳐진 파일 명
    :param regex_var: str / "*.csv" / 합칠 파일을 찾아내기 위한 정규표현식
    :param file_exe: str / ".csv" / 합쳐진 파일 확장자 명
    :param encoding: str / "utf-8" / 인코딩 방식
    """
    files_ = file_path + regex_var
    flag = True
    for file_ in glob.glob(files_):
        with open(file_, "r", encoding=encoding) as f:
            if flag:
                lines = f.read()
                flag = False
                with open(file_path + out_file_name + ".backup", 'w', encoding=encoding) as g:
                    g.write(lines)
            else:
                lines = f.readlines()[1:]
                with open(file_path + out_file_name + ".backup", 'a', encoding=encoding) as g:
                    for line in lines:
                        g.write(line)

    for file_ in glob.glob(files_):
        os.remove(file_)

    for file_ in glob.glob(file_path + "*.backup"):
        new_name = file_.replace(".backup", "")
        os.rename(file_, new_name + file_exe)


def csv_text_replace(file_name, pattern, substr, encoding="utf-8"):
    """
    text file 내부의 내용을 한줄 당 바꾸기 위한 함수
    -------------------------------------------------------
    :param file_name: str / "E:\\t.txt" /  파일 경로
    :param pattern: str / "\n" / 바뀌어야 할 문자열.
    :param substr: str /"',\n'" / 바뀔 내용. 찾아낸 문자열을 이 형태로 바꾼다.
    :param encoding: str / "utf-8" / 인코딩 방식
    """
    # create temp file 
    tmp_file, abs_path = mkstemp()

    # replace data
    with open(file_name, 'r', encoding=encoding) as f:
        with open(tmp_file, 'w', encoding=encoding) as g:
            while True:
                line = f.readline()

                if not line:
                    break

                g.write(line.replace(pattern, substr))

    # del old file and move new file
    os.remove(file_name)
    move(abs_path, file_name)


if __name__ == '__main__':
    csv_text_replace("E:\\t.txt", "\n", "',\n'")
