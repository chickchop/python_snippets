import glob
import os

def file_name_change(file_path):
    for file_path in glob.glob(file_path):
        file_path_list = file_path.split('.')
        tmp = file_path_list[1]
        file_path_list[1] = 'csv'
        file_path_list[0] = file_path_list[0] + tmp
        t = '.'
        file_path_r = t.join(file_path_list)
        os.rename(file_path,file_path_r)

if __name__ == '__main__':
    file_name_change("D:\\test\\log_data.*")
