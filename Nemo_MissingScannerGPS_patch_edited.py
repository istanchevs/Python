import csv
import easygui as gui
import os



def add_gps_data(raw_file, gps_data_array):
    gps_array = gps_data_array.copy()
    output_file = raw_file
    with open(input_dir+"\\"+raw_file, "r", newline='') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_NONE)
        with open(output_dir+"\\"+output_file, "w", newline='') as w:
            writer = csv.writer(w, quoting=csv.QUOTE_MINIMAL, quotechar = "|")
            prev_row_time = '00:00:00.000'
            for row in reader:
                if row[0][0] != '#':
                    if len(gps_array) > 1:
                        # row_time = 60*float(row[1][-9:-7]) + float(row[1][-6:])
                        # gps_time = 60*float(gps_array[0][1][-9:-7]) + float(gps_array[0][1][-6:])
                        # print(row_time, gps_time, prev_row_time)
                        row_time = row[1]
                        gps_time = gps_array[0][1]
                        if row_time >= gps_time:
                            if gps_time >= prev_row_time:
                                writer.writerow(gps_array[0])
                                gps_array.pop(0)
                            else:
                                while gps_time < prev_row_time:
                                    if len(gps_array) > 1:
                                        gps_array.pop(0)
                                        gps_time = gps_array[0][1]
                                        # gps_time = 60 * float(gps_array[0][1][-9:-7]) + float(gps_array[0][1][-6:])
                                    else:
                                        break
                                writer.writerow(gps_array[0])
                                gps_array.pop(0)
                        prev_row_time = row_time
                writer.writerow(row)


def parse(nemo_file):
    gps_data = []
    err = []
    print(nemo_file)
    with open(input_dir+"\\"+nemo_file, "r", newline='') as f:
        reader = csv.reader(f)
        
        for row in reader:
            try:
                if row[0] == "GPS":
                    gps_data.append(row)
            except Exception as e:
                if empty_output["errors"] is True:
                    write_headers(headers_error, out_file_errors) # create Errors file
                    empty_output["errors"] = False  # Errors file is not empty
                for i in (nemo_file, row[0], row[1], type(e), e.args):
                    err.append(i)
                print(err)
                with open(out_file_errors, 'a',  newline='') as w:
                    writer = csv.writer(w)
                    writer.writerow(err)
                err = []
    return gps_data
# _main_

nemo_list_gps = []
input_dir = gui.diropenbox("Nemo GPS Copy", "Select directory with Nemo files with missing scanner GPS data")

if input_dir is not None:

    files = os.listdir(input_dir)

    if files is not None:

        lst = list(files)
        # print(lst)
        for file in lst:
            if file[-6:] == ".1.nmf":
                nemo_list_gps.append(file)
        # print(nemo_list_gps)

        while 1:
            output_dir = gui.diropenbox("Nemo GPS Copy", "Select output directory for patched Nemo files")
            if output_dir != input_dir:
                break
            print("Please, select different output directory!")
            gui.msgbox("Please, select different output directory!", "Nemo GPS Copy")

        if output_dir is not None:
            for nemo_file_gps in nemo_list_gps:
                gps_data = parse(nemo_file_gps)
                for i in (13,14):
                    no_gps_file = nemo_file_gps[:-5]+str(i)+".nmf"
                    print(no_gps_file)
                    #print(no_gps_file, gps_data)
                    add_gps_data(no_gps_file, gps_data)
                    #print(gps_data)

            message = "%i files patched."  % (len(nemo_list_gps)*2)
            print(message)
            gui.msgbox(message, "Nemo copy GPS data")

