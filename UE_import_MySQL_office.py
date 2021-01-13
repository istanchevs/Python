import easygui as gui
import os
import MySQLdb
import datetime


mysql_dir = ''
mtable_fields = ["Name", "Date", "Technology", "Concat", "U9_UARFCN", "L21_EARFCN"]
mtable_data = {}

template_uelabels = "(`UE` VARCHAR(5) NULL, `UE_Label` VARCHAR(255) NULL, `Test_script` VARCHAR(255) NULL)"

template_scan_gsm = "(`GPS_Time` VARCHAR(255) NULL, " \
                    "`Lon` DOUBLE NULL, INDEX `Lon` (`Lon`), " \
                    "`Lat` DOUBLE NULL, INDEX `Lat` (`Lat`), " \
                    "`Time` VARCHAR(255) NULL, " \
                    "`ARFCN` INT(10) NULL, " \
                    "`BSIC` INT(10) NULL, " \
                    "`Scanned_level_dBm` DOUBLE NULL, " \
                    "`Date` Date  NULL, " \
                    "`Filename` VARCHAR(255) NULL)"

template_scan_umts = "(`GPS_Time` VARCHAR(255) NULL, " \
                    "`Lon` DOUBLE NULL, INDEX `Lon` (`Lon`), " \
                    "`Lat` DOUBLE NULL, INDEX `Lat` (`Lat`), " \
                    "`Time` VARCHAR(255) NULL, " \
                    "`UARFCN` INT(10) NULL, " \
                    "`PSC` INT(10) NULL, " \
                    "`EcIo_dB` DOUBLE NULL, " \
                    "`RSCP_dBm` DOUBLE NULL," \
                    "`Date` Date  NULL, " \
                    "`Filename` VARCHAR(255) NULL)"

template_scan_lte = "(`GPS_Time` VARCHAR(255) NULL, " \
                    "`Lon` DOUBLE NULL, INDEX `Lon` (`Lon`), " \
                    "`Lat` DOUBLE NULL, INDEX `Lat` (`Lat`), " \
                    "`Time` VARCHAR(255) NULL, " \
                    "`EARFCN` INT(10) NULL," \
                    "`DL Bandwidth` INT(10) NULL, " \
                    "`PCI` INT(10) NULL, " \
                    "`Antenna Ports` VARCHAR(10) NULL, " \
                    "`RSRP` DOUBLE NULL, " \
                    "`RSRQ` DOUBLE NULL, " \
                    "`CINR` DOUBLE NULL, " \
                    "`MIMO mode` INT(10) NULL, " \
                    "`Date` Date  NULL, " \
                    "`Filename` VARCHAR(255) NULL)"

template_ue = "(" \
        "`Time` VARCHAR(20)  NULL," \
        "`Lat` DOUBLE NULL, INDEX `Lat` (`Lat`)," \
        "`Lon` DOUBLE NULL, INDEX `Lon` (`Lon`)," \
        "`GCell_type` VARCHAR(10)  NULL," \
        "`BCCH` VARCHAR(4)  NULL," \
        "`BSIC` VARCHAR(2)  NULL," \
        "`RxLev_full` VARCHAR(7)  NULL," \
        "`RxLev_sub` VARCHAR(7)  NULL," \
        "`C1` VARCHAR(4)  NULL," \
        "`C2` VARCHAR(4)  NULL," \
        "`CellID` VARCHAR(5)  NULL," \
        "`LAC` VARCHAR(5)  NULL," \
        "`RAC` VARCHAR(5)  NULL," \
        "`GCell_type_1` VARCHAR(10)  NULL," \
        "`BCCH_1` VARCHAR(4)  NULL," \
        "`BSIC_1` VARCHAR(2)  NULL," \
        "`RxLev_full_1` VARCHAR(7)  NULL," \
        "`C1_1` VARCHAR(4)  NULL," \
        "`C2_1` VARCHAR(4)  NULL," \
        "`GCell_type_2` VARCHAR(10)  NULL," \
        "`BCCH_2` VARCHAR(4)  NULL," \
        "`BSIC_2` VARCHAR(2)  NULL," \
        "`RxLev_full_2` VARCHAR(7)  NULL," \
        "`C1_2` VARCHAR(4)  NULL," \
        "`C2_2` VARCHAR(4)  NULL," \
        "`RxQual_full` VARCHAR(1)  NULL," \
        "`RxQual_sub` VARCHAR(1)  NULL," \
        "`FER_full` VARCHAR(5)  NULL," \
        "`FER_sub` VARCHAR(5)  NULL," \
        "`DTX_DL` VARCHAR(1)  NULL," \
        "`RLT` VARCHAR(3)  NULL," \
        "`TA` VARCHAR(3)  NULL," \
        "`MSPower` VARCHAR(3)  NULL," \
        "`MSPower_Band` VARCHAR(7)  NULL," \
        "`AMR_mode_UL` VARCHAR(10)  NULL," \
        "`AMR_mode_DL` VARCHAR(10)  NULL," \
        "`AMR_channel_type` VARCHAR(6)  NULL," \
        "`CI_AMR` VARCHAR(5)  NULL," \
        "`CI_average` VARCHAR(5)  NULL," \
        "`ARFCN_1` VARCHAR(4)  NULL," \
        "`CI_1` VARCHAR(5)  NULL," \
        "`ARFCN_2` VARCHAR(4)  NULL," \
        "`CI_2` VARCHAR(5)  NULL," \
        "`ARFCN_3` VARCHAR(4)  NULL," \
        "`CI_3` VARCHAR(5)  NULL," \
        "`ActiveSet_UARFCN_1` VARCHAR(5)  NULL," \
        "`ActiveSet_PSC_1` VARCHAR(3)  NULL," \
        "`ActiveSet_EcNo_1` VARCHAR(6)  NULL," \
        "`ActiveSet_RSCP_1` VARCHAR(7)  NULL," \
        "`ActiveSet_UARFCN_2` VARCHAR(5)  NULL," \
        "`ActiveSet_PSC_2` VARCHAR(3)  NULL," \
        "`ActiveSet_EcNo_2` VARCHAR(6)  NULL," \
        "`ActiveSet_RSCP_2` VARCHAR(7)  NULL," \
        "`ActiveSet_UARFCN_3` VARCHAR(5)  NULL," \
        "`ActiveSet_PSC_3` VARCHAR(3)  NULL," \
        "`ActiveSet_EcNo_3` VARCHAR(6)  NULL," \
        "`ActiveSet_RSCP_3` VARCHAR(7)  NULL," \
        "`MonitoredSet_UARFCN_1` VARCHAR(5)  NULL," \
        "`MonitoredSet_PSC_1` VARCHAR(3)  NULL," \
        "`MonitoredSet_EcNo_1` VARCHAR(6)  NULL," \
        "`MonitoredSet_RSCP_1` VARCHAR(7)  NULL," \
        "`MonitoredSet_UARFCN_2` VARCHAR(5)  NULL," \
        "`MonitoredSet_PSC_2` VARCHAR(3)  NULL," \
        "`MonitoredSet_EcNo_2` VARCHAR(6)  NULL," \
        "`MonitoredSet_RSCP_2` VARCHAR(7)  NULL," \
        "`DetectedSet_UARFCN_1` VARCHAR(5)  NULL," \
        "`DetectedSet_PSC_1` VARCHAR(3)  NULL," \
        "`DetectedSet_EcNo_1` VARCHAR(6)  NULL," \
        "`DetectedSet_RSCP_1` VARCHAR(7)  NULL," \
        "`UndetectedSet_UARFCN_1` VARCHAR(5)  NULL," \
        "`UndetectedSet_PSC_1` VARCHAR(3)  NULL," \
        "`UndetectedSet_EcNo_1` VARCHAR(6)  NULL," \
        "`UndetectedSet_RSCP_1` VARCHAR(7)  NULL," \
        "`UMTS_TXPower` VARCHAR(6)  NULL," \
        "`UMTS_CompressMode` VARCHAR(1)  NULL," \
        "`HSDPA_Ph_Req_rate_bps` VARCHAR(10)  NULL," \
        "`HSDPA_CQI_Average` VARCHAR(5)  NULL," \
        "`Serving_EARFCN` VARCHAR(5)  NULL," \
        "`Serving_PCI` VARCHAR(3)  NULL," \
        "`Serving_RSRP` VARCHAR(7)  NULL," \
        "`Serving_RSRQ` VARCHAR(6)  NULL," \
        "`Listed_EARFCN_1` VARCHAR(5)  NULL," \
        "`Listed_PCI_1` VARCHAR(3)  NULL," \
        "`Listed_RSRP_1` VARCHAR(7)  NULL," \
        "`Listed_RSRQ_1` VARCHAR(6)  NULL," \
        "`Detected_EARFCN_1` VARCHAR(5)  NULL," \
        "`Detected_PCI_1` VARCHAR(3)  NULL," \
        "`Detected_RSRP_1` VARCHAR(7)  NULL," \
        "`Detected_RSRQ_1` VARCHAR(6)  NULL," \
        "`Detected_EARFCN_2` VARCHAR(5)  NULL," \
        "`Detected_PCI_2` VARCHAR(3)  NULL," \
        "`Detected_RSRP_2` VARCHAR(7)  NULL," \
        "`Detected_RSRQ_2` VARCHAR(6)  NULL," \
        "`Detected_EARFCN_3` VARCHAR(5)  NULL," \
        "`Detected_PCI_3` VARCHAR(3)  NULL," \
        "`Detected_RSRP_3` VARCHAR(7)  NULL," \
        "`Detected_RSRQ_3` VARCHAR(6)  NULL," \
        "`SCell_1_EARFCN` VARCHAR(5)  NULL," \
        "`SCell_1_PCI` VARCHAR(3)  NULL," \
        "`SCell_1_RSRP` VARCHAR(7)  NULL," \
        "`SCell_1_RSRQ` VARCHAR(6)  NULL," \
        "`SCell_2_EARFCN` VARCHAR(5)  NULL," \
        "`SCell_2_PCI` VARCHAR(3)  NULL," \
        "`SCell_2_RSRP` VARCHAR(7)  NULL," \
        "`SCell_2_RSRQ` VARCHAR(6)  NULL," \
        "`SCell_3_EARFCN` VARCHAR(5)  NULL," \
        "`SCell_3_PCI` VARCHAR(3)  NULL," \
        "`SCell_3_RSRP` VARCHAR(7)  NULL," \
        "`SCell_3_RSRQ` VARCHAR(6)  NULL," \
        "`LTE_Cell_type` VARCHAR(8)  NULL," \
        "`LTE_SNR_PCell` VARCHAR(5) NULL," \
        "`LTE_SNR_SCell_1` VARCHAR(5) NULL," \
        "`LTE_SNR_SCell_2` VARCHAR(5) NULL," \
        "`LTE_SNR_SCell_3` VARCHAR(5) NULL," \
        "`LTE_SNR_Port_0` VARCHAR(5) NULL," \
        "`LTE_SNR_Port_1` VARCHAR(5) NULL," \
        "`LTE_SNR_Port_2` VARCHAR(5) NULL," \
        "`LTE_SNR_Port_3` VARCHAR(5) NULL," \
        "`LTE_Req_rate_bps` VARCHAR(10)  NULL," \
        "`LTE_WB_CQI_Codeword0` VARCHAR(5)  NULL," \
        "`LTE_WB_CQI_Codeword1` VARCHAR(5)  NULL," \
        "`LTE_SB_CQI_Codeword0` VARCHAR(5)  NULL," \
        "`LTE_SB_CQI_Codeword1` VARCHAR(5)  NULL," \
        "`LTE_WB_PMI` VARCHAR(5)  NULL," \
        "`LTE_Req_Rank_1` VARCHAR(5)  NULL," \
        "`LTE_Rank_1` VARCHAR(2)  NULL," \
        "`LTE_Req_Rank_2` VARCHAR(5)  NULL," \
        "`LTE_Rank_2` VARCHAR(2)  NULL," \
        "`LTE_Req_Rank_3` VARCHAR(5)  NULL," \
        "`LTE_Rank_3` VARCHAR(2)  NULL," \
        "`LTE_Req_Rank_4` VARCHAR(5)  NULL," \
        "`LTE_Rank_4` VARCHAR(2)  NULL," \
        "`LTE_TA` VARCHAR(3)  NULL," \
        "`LTE_TXPower_PUSCH` VARCHAR(6)  NULL," \
        "`LTE_TXPower_PUCCH` VARCHAR(6)  NULL," \
        "`LTE_TXPower_HeadRoom_PUSCH` VARCHAR(6)  NULL," \
        "`LTE_Max_TXPower_UL` VARCHAR(6) NULL,"\
        "`LTE_Bandwidth` VARCHAR(6) NULL,"\
        "`LTE_Transmission_Mode` VARCHAR(6) NULL,"\
        "`LTE_TX_Antennas` VARCHAR(6) NULL,"\
        "`LTE_SCells` VARCHAR(2) NULL,"\
        "`LTE_Bandwidth_SCell_1` VARCHAR(6) NULL,"\
        "`Channel_SCell_1` VARCHAR(6) NULL,"\
        "`PCI_SCell_1` VARCHAR(6) NULL,"\
        "`LTE_Transmission_Mode_SCell_1` VARCHAR(6) NULL," \
        "`LTE_TX_Antennas_SCell_1` VARCHAR(6) NULL," \
        "`LTE_Bandwidth_SCell_2` VARCHAR(6) NULL," \
        "`Channel_SCell_2` VARCHAR(6) NULL," \
        "`PCI_SCell_2` VARCHAR(6) NULL," \
        "`LTE_Transmission_Mode_SCell_2` VARCHAR(6) NULL," \
        "`LTE_TX_Antennas_SCell_2` VARCHAR(6) NULL," \
        "`LTE_PRB_utilization_UL` VARCHAR(6) NULL," \
        "`LTE_PRB_utilization_DL` VARCHAR(6) NULL," \
        "`LTE_Total_PRB_usage` VARCHAR(6) NULL," \
        "`LTE_Total_PRB_utilization` VARCHAR(6) NULL," \
        "`Channel` VARCHAR(6) NULL,"\
        "`Call_id` VARCHAR(5)  NULL," \
        "`System` VARCHAR(10)  NULL," \
        "`Call_type` VARCHAR(25)  NULL," \
        "`Call_status` VARCHAR(50)  NULL," \
        "`Phone_number` VARCHAR(15)  NULL," \
        "`Call_timeout` VARCHAR(10)  NULL," \
        "`HO_id` VARCHAR(5)  NULL," \
        "`HO_status` TEXT  NULL," \
        "`RRC_state` VARCHAR(10)  NULL," \
        "`RRC_Direction` VARCHAR(5)  NULL," \
        "`RRC_Msg` VARCHAR(50)  NULL," \
        "`RRC_Channel` VARCHAR(20)  NULL," \
        "`RRC_String` TEXT  NULL," \
        "`RRC_parsed` TEXT NULL," \
        "`PDSCH_rate_Codeword0` VARCHAR(20)  NULL," \
        "`PDSCH_rate_Codeword1` VARCHAR(20)  NULL," \
        "`PDSCH_rate` VARCHAR(20)  NULL," \
        "`PUSCH_rate` VARCHAR(20)  NULL," \
        "`Data_call_status` VARCHAR(50)  NULL," \
        "`Data_connection_id` VARCHAR(6)  NULL," \
        "`Data_call_id` VARCHAR(6)  NULL," \
        "`Host_address` TEXT  NULL," \
        "`Data_connection_timeout` VARCHAR(6)  NULL," \
        "`Service_access_time` VARCHAR(6)  NULL," \
        "`IP_term_time` VARCHAR(6)  NULL," \
        "`Bytes_UL` VARCHAR(20)  NULL," \
        "`Bytes_DL` VARCHAR(20)  NULL," \
        "`App_protocol` VARCHAR(20)  NULL," \
        "`App_rate_UL` VARCHAR(20)  NULL," \
        "`App_rate_DL` VARCHAR(20)  NULL," \
        "`App_bytes_UL` VARCHAR(20)  NULL," \
        "`App_bytes_DL` VARCHAR(20)  NULL," \
        "`Date` Date  NULL," \
        "`Filename` VARCHAR(255) NULL)"


def master_table_update(field_values):
    sql = ("INSERT INTO operator_scanners.master_table (`Name`, `Date`, `Technology`, `Concat`, `U9_UARFCN`, "
           "`L21_EARFCN`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');"
           % (field_values["Name"], field_values["Date"], field_values["Technology"], field_values["Concat"],
              field_values["U9_UARFCN"], field_values["L21_EARFCN"]))
    try:
        cursor.execute(sql)
        print("master_table update done: affected rows = {} \n".format(cursor.rowcount))
    except MySQLdb.Error as e:
        print("master_table update: " + str(e))

    sql = "CALL operator_scanners.`setIndexAndCellref`"
    cursor.execute(sql)
    print("Stored procedure operator_scanners.`setIndexAndCellref` executed")


def mysql_import_files(selected_files):

    for j in range(len(selected_files)):
        label = selected_files[j]

        if label == "DeviceLabels.txt":
            name = "operator_ues." + mtable_data["Name"] + "_ue_lb"

            sql = ("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s ENGINE = MYISAM;"
                   % (name, name, template_uelabels))
            cursor.execute(sql)

            sql = ("LOAD DATA INFILE '%s' INTO TABLE %s "
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
                   % (mysql_dir + "/" + label, name))
            try:
                cursor.execute(sql)
                print(label + " import done")
                # print(cursor._last_executed)

            except MySQLdb.Error as e:
                print(label + " import:" + str(e))

            # print("affected rows = {}".format(cursor.rowcount))

        elif label == "gsm_scanner.txt":
            name = "operator_scanners." + mtable_data["Name"] + "_gsm"

            sql = ("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s ENGINE = MYISAM;"
                   % (name, name, template_scan_gsm))
            cursor.execute(sql)

            sql = ("LOAD DATA INFILE '%s' INTO TABLE %s "
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
                   % (mysql_dir + "/" + label, name))
            try:
                cursor.execute(sql)
                print(label + " import done")

            except MySQLdb.Error as e:
                print(label + " import:" + str(e))

        elif label == "umts_scanner.txt":
            name = "operator_scanners." + mtable_data["Name"] + "_umts"

            sql = ("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s ENGINE = MYISAM;"
                   % (name, name, template_scan_umts))
            cursor.execute(sql)

            sql = ("LOAD DATA INFILE '%s' INTO TABLE %s "
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
                   % (mysql_dir + "/" + label, name))
            try:
                cursor.execute(sql)
                print(label + " import done")

            except MySQLdb.Error as e:
                print(label + " import:" + str(e))

        elif label == "lte_scanner.txt":
            name = "operator_scanners." + mtable_data["Name"] + "_lte"

            sql = ("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s ENGINE = MYISAM;"
                   % (name, name, template_scan_lte))
            cursor.execute(sql)

            sql = ("LOAD DATA INFILE '%s' INTO TABLE %s "
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
                   % (mysql_dir + "/" + label, name))
            try:
                cursor.execute(sql)
                print(label + " import done")

            except MySQLdb.Error as e:
                print(label + " import:" + str(e))

        elif label[0:2] == "UE":
            if label[4] == ".":
                ue_label = "_ue_" + label[3]
            else:
                ue_label = "_ue_" + label[3:5]
            name = "operator_ues." + mtable_data["Name"] + ue_label

            sql = ("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s ENGINE = MYISAM;"
                   % (name, name, template_ue))
            cursor.execute(sql)

            sql = ("LOAD DATA INFILE '%s' INTO TABLE %s "
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
                   % (mysql_dir + "/" + label, name))
            try:
                cursor.execute(sql)
                print(label + " import done")
                # print(cursor._last_executed)

            except MySQLdb.Error as e:
                print(label + " import:" + str(e))


def mysql_import(selected_files):
    if selected_files is not None:

        master_table_data = master_table_data_get()

        if master_table_data is not None:
            i = 0
            for mpointer in mtable_fields:
                mtable_data[mpointer] = master_table_data[i]
                i += 1
            sql = ("SELECT `Name` FROM operator_scanners.master_table WHERE `Name` = '%s'" % mtable_data["Name"])
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is not None:
                message = ("Duplicate entry '%s' for field `Name` in Master_table!\n"
                      % mtable_data["Name"])
                print(message)
                choice = gui.ynbox(message+"Do you want to skip Master_table update?", "Master_table duplicated entry")
                if choice:
                    mysql_import_files(selected_files)

                    message = "Master_table update skipped!\n%i files processed." % len(selected_files)
                    print(message)
                    gui.msgbox(message, "UE Import to MySQL DB")
            else:
                mysql_import_files(selected_files)
                master_table_update(mtable_data)

                message = "%i files processed." % len(selected_files)
                print(message)
                gui.msgbox(message, "UE Import to MySQL DB")

            # gui.msgbox(message, "Import to MySQL")

        else:
            message = "No files selected!"
            print(message)
            gui.msgbox(message, "UE Import to MySQL DB")


def master_table_data_get():

    msg = "Enter needed data for Master Table"
    title = "Master Table Info"
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    initial_values = ["", date,"GSM/UMTS/LTE", "", "00", "00"]  # we start with blanks for the values
    field_values = gui.multenterbox(msg, title, mtable_fields, initial_values)

    # make sure that none of the fields was left blank
    while 1:
        if field_values is None:
            break
        errmsg = ""
        if len(field_values[0])>28:
            errmsg = '"Name" field must be less than 29 symbols.\n\n'
        field_values[0] = field_values[0].lower()
        for i in range(len(mtable_fields)):
            if field_values[i].strip() == "":
                errmsg += ('"%s" is a required field.\n\n' % mtable_fields[i])
        if errmsg == "":
            break  # no problems found
        field_values = gui.multenterbox(errmsg, title, mtable_fields, field_values)

    # print("Reply was: %s" % str(field_values))
    return field_values


def parsed_files_selection():

    global mysql_dir
    ue_list = []

    input_dir = gui.diropenbox(msg="Select directory with parsed Nemo UE files for import",
                               title="Import parsed Nemo UE files to MySQL")

    if input_dir is not None:
        mysql_dir = input_dir.replace(os.sep, '/')
        files = os.listdir(input_dir)

        if files is not None:

            lst = list(files)

            for file in lst:
                if file[-3:] == "txt" or file[-3:] == "csv":
                    ue_list.append(file)

            selected_parsed_files = gui.multchoicebox(msg='Please select parsed text files for import',
                                                      title='File selection', choices=ue_list, preselect=0,
                                                      callback=None, run=True)
            return selected_parsed_files


mydb = MySQLdb.connect(host='192.168.1.1',
                       port=3306,
                       user='user',
                       passwd='password',
                       db='')

cursor = mydb.cursor()

selection = parsed_files_selection()
mysql_import(selection)

cursor.close()
mydb.close()

