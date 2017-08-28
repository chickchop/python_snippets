'''
Created on 2017. 8. 25.

@author: ko
'''
import os


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


if __name__ == '__main__':
    find_flag_in_dat_file()