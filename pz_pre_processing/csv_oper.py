import os


def csv_header_add(in_file, encoding, add_line):
    """
    header가 없는 CSV파일에 header를 추가시키는 함수
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
    :param file_path: str / "D:\\" / 분할할 파일이 저장된 파일이 위치한 폴더 경로
    :param file_name: str / "log_data" / 분할하기 위한 대용량 파일 명
    :param div_file_folder: str / "div_file1\\" / 분할된 파일이 저장될 폴더 명
    :param file_exe: str / ".csv" / 분할할 파일의 확장자
    :param encoding: str / "utf-8" / 인코딩 방식
    """
    dir_name = file_path + div_file_folder
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

    file_idx = 0
    n_line_cnt = 0
    with open(file_path + file_name + file_exe, "r", encoding=encoding) as f:
        g = open(dir_name + file_name + str(file_idx) + file_exe, 'w', encoding=encoding)
        while True:
            line = f.readline()
            if not line:
                break
            if n_line_cnt == n_div_cnt:
                g.close()
                file_idx += 1
                n_line_cnt = 0
                g = open(dir_name + file_name + str(file_idx) + file_exe, 'w', encoding=encoding)
            n_line_cnt += 1
            g.write(line)
        g.close()
