def csv_split():
    n_div_cnt = 200000

    file_path = "D:\\"
    file_name = "log_data"
    file_exe = ".csv"

    file_folder = "div_file\\"
    
    dir_name = file_path+file_folder
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

    file_idx = 0
    n_line_cnt = 0

    with open(file_path + file_name+file_exe, "r", encoding='utf-8') as f:
        g = open(dir_name + file_name+str(file_idx)+file_exe,'w', encoding='utf-8')
        while True:
            line = f.readline()

            if not line:
                break

            if n_line_cnt == n_div_cnt:
                g.close()
                file_idx += 1
                n_line_cnt = 0
                g = open(dir_name + file_name+str(file_idx)+file_exe,'w', encoding='utf-8') 

            n_line_cnt += 1
            g.write(line)
        g.close()
