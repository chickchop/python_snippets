import glob
import os
import binascii


def change_file_name(file_path_name,split_point,str_to_change):
    """
    파일명을 바꾸기 위한 함수
    구조적으로 저장된 파일이지만 파일명을 알아보기 힘들 때 사용한다.
    -------------------------------------------------------
    :param file_path_name: str / "D:\\test\\log_data.*" / 바꾸고 싶은 파일 명
    :param split_point: str / '.' / 바꿀 파일 명과 바뀌지 않을 파일 명을 분리하는 포인트
    :param str_to_change: str / "csv" / 바꾸자 하는 문자열 
    """
    for file_path_name in glob.glob(file_path_name):
        file_path_name_ls = file_path_name.split(split_point)
        tmp = file_path_name_ls[1]
        file_path_name_ls[1] = str_to_change
        file_path_name_ls[0] = file_path_name_ls[0] + tmp
        t = split_point
        file_path_name_r = t.join(file_path_name_ls)
        os.rename(file_path_name,file_path_name_r)


def find_flag_in_dat_file():
    flag2_dat_data = []
    #find floder list
    for dir in os.listdir("C:\\Users\\ko\\Desktop\\180825"):
        for file_name in os.listdir("C:\\Users\\ko\\Desktop\\180825\\"+dir):
            #open file
            f = open("C:\\Users\\ko\\Desktop\\180825\\" +dir+"\\"+file_name,'r', encoding="utf-8")
            print(dir, file_name)
            lines = f.readlines()
            ##find 2 tag and transform data(list -> string) and save data
            for line in lines:
                dat_data_list = line.split('\t')
                if dat_data_list[0] == '2':
                    dat_data = dir + file_name + str(dat_data_list)
                    flag2_dat_data.append(dat_data)
                    
                    
            f.close()
    
    print(flag2_dat_data)    
    #write file_name and data
    w = open("C:\\Users\\ko\\Desktop\\find2_dat_file.txt",'w')
    #transform list -> str and write
    for write_list in flag2_dat_data:
        write_line = str(write_list)
        w.write(write_line)
        w.write('\n')
    w.close()