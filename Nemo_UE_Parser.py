import csv
import easygui as gui
import os
from datetime import datetime
from pycrate_asn1dir import RRC3G
from pycrate_asn1dir import RRCLTE
from binascii import unhexlify

headers_ue = ["Time", "Lat", "Lon",
               "GCell_type", "BCCH", "BSIC", "RxLev_full", "RxLev_sub", "C1", "C2", "CellID", "LAC", "RAC",
               "GCell_type_1", "BCCH_1", "BSIC_1", "RxLev_full_1", "C1_1", "C2_1",
               "GCell_type_2", "BCCH_2", "BSIC_2", "RxLev_full_2", "C1_2", "C2_2",
               "RxQual_full", "RxQual_sub", "FER_full", "FER_sub", "DTX_DL", "RLT", "TA", "MSPower", "MSPower_Band",
               "AMR_mode_UL", "AMR_mode_DL", "AMR_channel_type", "CI_AMR", "CI_average",
               "ARFCN_1", "CI_1", "ARFCN_2", "CI_2", "ARFCN_3", "CI_3",
               "ActiveSet_UARFCN_1", "ActiveSet_PSC_1", "ActiveSet_EcNo_1", "ActiveSet_RSCP_1",
               "ActiveSet_UARFCN_2", "ActiveSet_PSC_2", "ActiveSet_EcNo_2", "ActiveSet_RSCP_2",
               "ActiveSet_UARFCN_3", "ActiveSet_PSC_3", "ActiveSet_EcNo_3", "ActiveSet_RSCP_3",
               "MonitoredSet_UARFCN_1", "MonitoredSet_PSC_1", "MonitoredSet_EcNo_1", "MonitoredSet_RSCP_1",
               "MonitoredSet_UARFCN_2", "MonitoredSet_PSC_2", "MonitoredSet_EcNo_2", "MonitoredSet_RSCP_2",
               "DetectedSet_UARFCN_1", "DetectedSet_PSC_1", "DetectedSet_EcNo_1", "DetectedSet_RSCP_1",
               "UndetectedSet_UARFCN_1", "UndetectedSet_PSC_1", "UndetectedSet_EcNo_1", "UndetectedSet_RSCP_1",
               "UMTS_TXPower", "UMTS_CompressMode", "HSDPA_Ph_Req_rate_bps", "HSDPA_CQI_Average",
               "Serving_EARFCN", "Serving_PCI", "Serving_RSRP", "Serving_RSRQ",
               "Listed_EARFCN_1", "Listed_PCI_1", "Listed_RSRP_1", "Listed_RSRQ_1",
               "Detected_EARFCN_1", "Detected_PCI_1", "Detected_RSRP_1", "Detected_RSRQ_1",
               "Detected_EARFCN_2", "Detected_PCI_2", "Detected_RSRP_2", "Detected_RSRQ_2",
               "Detected_EARFCN_3", "Detected_PCI_3", "Detected_RSRP_3", "Detected_RSRQ_3",
               "SCell_1_EARFCN", "SCell_1_PCI", "SCell_1_RSRP", "SCell_1_RSRQ",
               "SCell_2_EARFCN", "SCell_2_PCI", "SCell_2_RSRP", "SCell_2_RSRQ",
               "SCell_3_EARFCN", "SCell_3_PCI", "SCell_3_RSRP", "SCell_3_RSRQ",
               "LTE_Cell_type", "LTE_SNR_PCell", "LTE_SNR_SCell_1", "LTE_SNR_SCell_2", "LTE_SNR_SCell_3",
               "LTE_SNR_Port_0", "LTE_SNR_Port_1", "LTE_SNR_Port_2", "LTE_SNR_Port_3",
               "LTE_Req_rate_bps", "LTE_WB_CQI_Codeword0", "LTE_WB_CQI_Codeword1",
               "LTE_SB_CQI_Codeword0", "LTE_SB_CQI_Codeword1", "LTE_WB_PMI", "LTE_Req_Rank_1", "LTE_Rank_1",
               "LTE_Req_Rank_2", "LTE_Rank_2", "LTE_Req_Rank_3", "LTE_Rank_3", "LTE_Req_Rank_4", "LTE_Rank_4",
               "LTE_TA", "LTE_TXPower_PUSCH", "LTE_TXPower_PUCCH", "LTE_TXPower_HeadRoom_PUSCH", "LTE_Max_TXPower_UL",
               "LTE_Bandwidth", "LTE_Transmission_Mode", "LTE_TX_Antennas", "LTE_SCells",
               "LTE_Bandwidth_SCell_1", "Channel_SCell_1", "PCI_SCell_1", "LTE_Transmission_Mode_SCell_1",
               "LTE_TX_Antennas_SCell_1",
               "LTE_Bandwidth_SCell_2", "Channel_SCell_2", "PCI_SCell_2", "LTE_Transmission_Mode_SCell_2",
               "LTE_TX_Antennas_SCell_2",
               "LTE_PRB_utilization_UL", "LTE_PRB_utilization_DL", "LTE_Total_PRB_usage", "LTE_Total_PRB_utilization",
               "Channel", "Call_id", "System", "Call_type", "Call_status", "Phone_number", "Call_timeout",
               "HO_id", "HO_status", "RRC_state", "RRC_Direction", "RRC_Msg", "RRC_Channel", "RRC_String", "RRC_parsed",
               "PDSCH_rate_Codeword0", "PDSCH_rate_Codeword1", "PDSCH_rate", "PUSCH_rate",
               "Data_call_status", "Data_connection_id", "Data_call_id", "Host_address", "Data_connection_timeout",
               "Service_access_time", "IP_term_time", "Bytes_UL", "Bytes_DL",
               "App_protocol", "App_rate_UL", "App_rate_DL", "App_bytes_UL", "App_bytes_DL", "Date", "Filename"]

headers_ue_label = ["UE", "UE_label", "Test_script"]

headers_scan_gsm = ["GPS_Time", "Lon", "Lat", "Time", "ARFCN", "BSIC", "Scanned_level_dBm", "Date", "Filename"]

headers_scan_umts = ["GPS_Time", "Lon", "Lat", "Time", "UARFCN", "PSC", "EcIo_dB", "RSCP_dBm", "Date", "Filename"]

headers_scan_lte = ["GPS_Time", "Lon", "Lat", "Time", "EARFCN", "DL Bandwidth", "PCI", "Antenna Ports", "RSRP", "RSRQ",
                    "CINR", "MIMO mode", "Date", "Filename"]

headers_error = ["Parsed_file", "EventID", "Timestamp", "Error_type", "Error_arguments"]

gsm_param_list = {"RxQual_full": "", "RxQual_sub": "", "FER_full": "", "FER_sub": "", "FER_TCH": "", "DTX_DL": "",
                  "RLT": "", "TA": "", "MSPower": "", "MSPower_Band": "", "CI_AMR": "", "AMR_mode_UL": "",
                  "AMR_mode_DL": "", "AMR_channel_type": ""}

umts_param_list = {"Channel":"", "RRC_state": ""}

lte_param_list = {"LTE_Bandwidth": "", "Serving_PCI": "", "Channel": "", "LTE_Transmission_Mode": "", "LTE_TX_Antennas": "",
                  "LTE_SCells":"", "LTE_Bandwidth_SCell": "", "Channel_SCell": "", "PCI_SCell": "",
                  "LTE_Transmission_Mode_SCell": "", "LTE_TX_Antennas_SCell": "", "RRC_state": ""}

info_list = {"Date": "", "Filename": "", "IMEI": "", "IMSI": "", "Handler_ver": "", "Device_HW_ver": "",
             "Device_Manufacturer": "", "Device_Model": "", "Device_Chipset": ""}

system = {"1": "GSM", "2": "TETRA", "5": "UMTS_FDD", "6": "UMTS_TDD", "7": "LTE_FDD", "8": "LTE_TDD",
          "10": "cdmaOne", "11": "CDMA_1x", "12": "EVDO",
          "20": "WLAN", "21": "GAN_WLAN", "25": "WiMAX",
          "50": "NMT", "51": "AMPS", "52": "NAMPS", "53": "DAMPS", "54": "ETACS", "55": "iDEN",
          "60": "PSTN", "61": "ISDN", "62": "IP", "65": "DVB-H"}

parsed_system = {"GSM": "1", "UMTS": "5", "LTE": "7"}

band = {"10850": "GSM850", "10900": "GSM900", "11800": "GSM1800", "11900": "GSM1900", "19999": "GSM"}

call_type = {"1": "Voice call", "2": "Markov call", "3": "Data call", "4": "Fax call", "5": "Dial-up data call",
             "6": "Loopback call (CDMA)", "7": "Video call", "8": "Push-to-talk", "9": "Push-to-talk (TETRA)",
             "10": "VoIP", "11": "Skype", "12": "QChat", "13": "Kodiak", "14": "LTE IMS voice",
             "15": "iDEN push-to-talk", "16": "LTE IMS video", "17": "WLAN IMS voice", "18": "WLAN IMS video",
             "19": "IP IMS voice", "20": "WhatsApp voice", "21": "WhatsApp video", "22": "Viber voice",
             "23": "Viber video"}

call_state = ""
rrc_state = ""

gps = {"Time": "", "Lon": "", "Lat": "", "Height": "", "Velocity": ""}

direction = {"1": "UL", "2": "DL"}

lte_cell_type = {"0": "PCell", "1": "SCell_1", "2": "SCell_2", "3": "SCell_3", "4": "SCell_4", "5": "SCell_5",
                 "6": "SCell_6", "7": "SCell_7"}

app_protocols = {"0": "Nemo Modem", "1": "Nemo TCP", "2": "Nemo UDP", "3": "FTP", "4": "HTTP", "5": "SMTP",
                 "6": "POP3", "7": "MMS", "8": "WAP 1.0", "9": "Streaming", "10": "WAP 2.0", "11": "HTTP browsing",
                 "12": "ICMP ping", "13": "IP_TCP", "14": "IP_UDP", "15": "Trace_route", "16": "SFTP", "17": "IMAP",
                 "18": "Facebook", "19": "Twitter", "20": "Instagram", "21": "LinkedIn", "22": "PEVQ_S", "23": "Dropbox",
                 "24": "Speedtest", "25": "mScore", "26": "Netflix", "27": "WhatsApp", "28": "UDP Echo", "29": "Viber"}

empty_output = {"ue": True, "scan_gsm": True, "scan_umts": True, "scan_lte": True, "labels": True, "errors": True}

nemo_list = []
ue_list = []
label_data = {}
label_array = []

def parse_ofdmscan(row):

    if row[3] == "7": # LTE scanning

        headers = ["PCI", "CP", "Antenna Ports", "RSRP", "RSRQ", "CINR", "Time offset", "Delay_spread", "Indication",
                   "MIMO mode", "RSSI", "CFO"]

        tx = {"0": "1TX", "1": "2TX", "3": "4TX"}

        data = {}
        array = []

        if row[6] == "1": # RS scanning

            head_param = int(row[4])
            data["EARFCN"] = row[5]
            data["DL Bandwidth"] = row[7]

            cells = int(row[5+head_param])
            params = int(row[5+head_param+1])
            length = cells * params

            data["Date"] = info_list["Date"]
            data["Filename"] = info_list["Filename"]
            data["Time"] = row[1]
            data["GPS_Time"] = gps["Time"]
            data["Lat"] = gps["Lat"]
            data["Lon"] = gps["Lon"]

            # loop over measured cells and their parameters
            for cell_pointer in range(7+head_param, length+7+head_param, params):
                for i in range(params):
                    j = headers[i]
                    if j == "Antenna Ports":
                        data[j] = tx[row[cell_pointer + i]]
                    else:
                        data[j] = row[cell_pointer + i]
                array.append(data)
                if data["PCI"] != "" and data["Lat"] != "" and data["Lon"] != "" :
                    write_to_file(array, headers_scan_lte, out_file_ltescan)
                array = []


def parse_pilotscan(row):

    if row[3] == "5": # UMTS scanning

        headers = ["PSC", "EcIo_dB", "RSCP_dBm", "SIR", "Delay", "Delay_spread"]

        data = {}
        array = []

        if row[6] == "1" or row[6] == "4": # PCPICH or PCPICH TX diversity scanning

            head_param = int(row[4])
            data["UARFCN"] = row[5]

            cells = int(row[5+head_param])
            params = int(row[6+head_param])
            length = cells * params

            data["Date"] = info_list["Date"]
            data["Filename"] = info_list["Filename"]
            data["Time"] = row[1]
            data["GPS_Time"] = gps["Time"]
            data["Lat"] = gps["Lat"]
            data["Lon"] = gps["Lon"]

            # loop over measured cells and their parameters
            for cell_pointer in range(7+head_param, length+7+head_param, params):
                for i in range(params):
                    j = headers[i]
                    data[j] = row[cell_pointer + i]
                array.append(data)
                if data["PSC"] != "" and data["Lat"] != "" and data["Lon"] != "" :
                    write_to_file(array, headers_scan_umts, out_file_umtsscan)
                array = []


def parse_freqscan(row):

    if row[3] == "1": # GSM scanning
        headers = ["ARFCN", "BSIC", "Scanned_level_dBm", "CI", "SCH_level"]

        data = {}
        array = []


        cells = int(row[6])
        params = int(row[7])
        length = cells * params

        data["Date"] = info_list["Date"]
        data["Filename"] = info_list["Filename"]
        data["Time"] = row[1]
        data["GPS_Time"] = gps["Time"]
        data["Lat"] = gps["Lat"]
        data["Lon"] = gps["Lon"]

        # loop over measured cells and their parameters
        for cell_pointer in range(8, length+8, params):
            for i in range(params):
                j = headers[i]
                data[j] = row[cell_pointer + i]
            array.append(data)
            if data["BSIC"] != "" and data["Lat"] != "" and data["Lon"] != "" :
                write_to_file(array, headers_scan_gsm, out_file_gsmscan)
            array = []


def parse_plaiu(row):

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    if row[3] == "7": # if system is LTE
        headers = ["Sample_duration", "LTE_PRB_utilization_UL", "LTE_PUSCH_TBS_average", "LTE_PUSCH_TBS_max",
                   "LTE_Cell_type"]
        j = 5
        for i in headers:
            if i == "LTE_Cell_type":
                if row[j] in lte_cell_type:
                    data[i] = lte_cell_type[row[j]]
                else:
                    data[i] = row[j]
            else:
                data[i] = row[j]
            j += 1

        array.append(data)

        if data["Lat"] != "" and data["Lon"] != "":
            write_to_file(array, headers_ue, out_file)


def parse_plaid(row):

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    if row[3] == "7": # if system is LTE
        headers = ["Sample_duration", "LTE_PRB_utilization_DL", "LTE_PDSCH_TBS_average", "LTE_PDSCH_TBS_max",
                   "LTE_Cell_type"]
        j = 5
        for i in headers:
            if i == "LTE_Cell_type":
                if row[j] in lte_cell_type:
                    data[i] = lte_cell_type[row[j]]
                else:
                    data[i] = row[j]
            else:
                data[i] = row[j]
            j += 1

        array.append(data)

        if data["Lat"] != "" and data["Lon"] != "":
            write_to_file(array, headers_ue, out_file)


def parse_plaisum(row):

    headers = ["LTE_Total_PRB_usage", "LTE_Total_PRB_utilization", "LTE_DL_streams", "LTE_Scheduled_cells"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    if row[3] == "7":
        j = 4
        for i in headers:
            data[i] = row[j]
            j += 1

        array.append(data)
        if data["Lat"] != "" and data["Lon"] != "":
            write_to_file(array, headers_ue, out_file)



def parse_dcomp(row):

    headers = ["Data_connection_id", "App_protocol", "Data_call_status"]

    ending_headers = ["Service_access_time", "IP_term_time", "Bytes_UL", "Bytes_DL", "Header_transfer_time",
                      "TCP_handshake_time", "Redirect_address", "Payload_access_time", "Processing_delay",
                      "Connection_processing_delay"]

    transfer_status = {"1": "Transfer_success", "2": "Socket error", "3": "Protocol error or timeout",
                       "4": "Test system failure", "5": "User abort", "6": "Partial success"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]


    j = 3
    for i in headers:
        key = row[j]
        if i == "App_protocol":
            if key in app_protocols:
                data[i] = app_protocols[key]
            else:
                data[i] = key
        elif i == "Data_call_status":
            if key in transfer_status:
                data[i] = transfer_status[key]
            else:
                data[i] = key

        else:
            data[i] = key
        j += 1

    # parse the headers from the end of the row
    j = -10
    for i in ending_headers:
        data[i] = row[j]
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_dreq(row):

    headers = ["Data_transfer_id", "Data_connection_id", "App_protocol", "Data_transfer_direction"]

    transfer_direction = {"1": "UL", "2": "DL", "3": "UL&DL"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Data_call_status"] = "Transfer_request"

    j = 3
    for i in headers:
        key = row[j]
        if i == "App_protocol":
            if key in app_protocols:
                data[i] = app_protocols[key]
            else:
                data[i] = key
        elif i == "Data_transfer_direction":
            if key in transfer_direction:
                data[i] = transfer_direction[key]
            else:
                data[i] = key

        else:
            data[i] = key
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_dad(row):

    headers = ["Data_connection_id", "App_protocol", "Data_call_status"]

    call_disc_status = {"1": "Normal data disconnect", "2": "Socket error", "3": "Protocol error or timeout",
                        "4": "Test system failure"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    j = 3
    for i in headers:
        key = row[j]
        if i == "App_protocol":
            if key in app_protocols:
                data[i] = app_protocols[key]
            else:
                data[i] = key
        elif i == "Data_call_status":
            if key in call_disc_status:
                data[i] = call_disc_status[key]
            else:
                data[i] = key

        else:
            data[i] = key
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_daf(row):

    headers = ["Data_connection_id", "App_protocol", "Data_call_status"]

    call_fail_status = {"1": "User abort", "2": "Socket error", "3": "Protocol error or timeout",
                        "4": "Test system failure"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    j = 3
    for i in headers:
        key = row[j]
        if i == "App_protocol":
            if key in app_protocols:
                data[i] = app_protocols[key]
            else:
                data[i] = key
        elif i == "Data_call_status":
            if key in call_fail_status:
                data[i] = call_fail_status[key]
            else:
                data[i] = key

        else:
            data[i] = key
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_dac(row):

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Data_call_status"] = "Connection_success"
    data["Data_connection_id"] = row[3]

    key = row[4]
    if key in app_protocols:
        data["App_protocol"] = app_protocols[key]
    else:
        data["App_protocol"] = key

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)

def parse_daa(row):
    headers = ["Data_connection_id", "Packet_session_id", "Data_call_id", "App_protocol", "Host_address", "Host_port",
               "Data_connection_timeout", "Data_security_protocol", "Data_authentication_scheme"]

    security_protocols = {"0": "None", "1": "SSL", "2": "SSH"}

    authentication_schemes = {"0": "Basic", "1": "Digest", "3": "None", "4": "NTLM", "5": "Negotiate"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Data_call_status"] = "Connection_attempt"

    j = 3
    for i in headers:
        key = row[j]
        if i == "App_protocol":
            if key in app_protocols:
                data[i] = app_protocols[key]
            else:
                data[i] = key
        elif i == "Data_security_protocol":
            if key in app_protocols:
                data[i] = security_protocols[key]
            else:
                data[i] = key
        elif i == "Data_authentication_scheme":
            if key in app_protocols:
                data[i] = authentication_schemes[key]
            else:
                data[i] = key
        else:
            data[i] = key
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_phrate(row):

    headers_pbch = ["PBCH_block_rate", "PBCH_BLER", "LTE_Cell_type"]

    headers_pdsch = ["PDSCH_rate_Codeword0", "PDSCH_rate_Codeword1", "PDSCH_block_rate", "PDSCH_BLER",
                     "PDSCH_scheduled_rate_per_PRB", "PDSCH_rate", "PDCCH_BLER_estimation", "LTE_Cell_type",
                     "PDCCH_CFI1_perc", "PDCCH_CFI2_perc", "PDCCH_CFI3_perc", "PDSCH_BLER_Codeword0",
                     "PDSCH_BLER_Codeword1", "PDCCH_Chanel_formats"]

    headers_pusch = ["PUSCH_rate", "LTE_Cell_type"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # if system is LTE
    if row[3] == "7":
        channel = row[4]
        # if channel is PBCH
        if channel == "1":
            j = 5
            for i in headers_pbch:
                if i == "LTE_Cell_type":
                    if row[j] in lte_cell_type:
                        data[i] = lte_cell_type[row[j]]
                    else:
                        data[i] = row[j]
                else:
                    data[i] = row[j]
                j += 1

        # if channel is PDSCH
        elif channel == "2":
            j = 5
            for i in headers_pdsch:
                if i == "LTE_Cell_type":
                    if row[j] in lte_cell_type:
                        data[i] = lte_cell_type[row[j]]
                    else:
                        data[i] = row[j]
                else:
                    data[i] = row[j]
                j += 1
        # if channel is PUSCH
        elif channel == "3":
            j = 5
            for i in headers_pusch:
                if i == "LTE_Cell_type":
                    if row[j] in lte_cell_type:
                        data[i] = lte_cell_type[row[j]]
                    else:
                        data[i] = row[j]
                else:
                    data[i] = row[j]
                j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_chi(row):

    global lte_param_list, rrc_state

    headers = ["Band", "RRC_state", "LTE_Bandwidth", "Channel", "Serving_PCI", "Cell_Identification",
               "TAC", "LTE_Max_TXPower_UL", "LTE_Transmission_Mode", "LTE_TX_Antennas", "TDD_ULDL_Config",
               "LTE_Cyclic_Prefix_UL", "Root_Sequence_Index", "C-RNTI", "LTE_SCells"]

    scell_headers = ["SCell_Type", "Band_SCell", "LTE_Bandwidth_SCell", "Channel_SCell", "PCI_SCell", "LTE_Transmission_Mode_SCell",
                     "LTE_TX_Antennas_SCell", "TDD_ULDL_Config", "LTE_Cyclic_Prefix_UL", "TDD_Special_subframe_config",
                     "LTE_CA_Mode_SCell"]

    bandwidth = {"1": "180kHz", "6": "1.4MHz", "15": "3MHz", "25": "5MHz", "50": "10MHz", "75": "15MHz", "100": "20MHz"}

    tx = {"0": "1TX", "1": "2TX", "3": "4TX"}

    lte_rrc_state = {"1": "Idle", "2": "Connected"}

    data = {}
    array = []
    # clear the old values for CA in lte_param_list
    for i in lte_param_list:
        if i not in ("Channel", "Serving_PCI", "RRC_state"):
            lte_param_list[i] = ""

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    j = 4
    for i in headers:
        if i == "LTE_Bandwidth":
            if row[j] in bandwidth:
                if i in lte_param_list:
                    lte_param_list[i] = bandwidth[row[j]]
                data[i] = bandwidth[row[j]]
            else:
                data[i] = row[j]
        elif i == "LTE_Transmission_Mode":
            if i in lte_param_list:
                lte_param_list[i] = "TM"+row[j]
            data[i] = "TM"+row[j]
        elif i == "LTE_TX_Antennas":
            if row[j] in tx:
                if i in lte_param_list:
                    lte_param_list[i] = tx[row[j]]
                data[i] = tx[row[j]]
            else:
                data[i] = row[j]
        elif i == "RRC_state":
            if row[j] in lte_rrc_state:
                if i in lte_param_list:
                    lte_param_list[i] = lte_rrc_state[row[j]]
                data[i] = lte_rrc_state[row[j]]
            else:
                if i in lte_param_list:
                    lte_param_list[i] = row[j]
                data[i] = row[j]
        else:
            if i in lte_param_list:
                lte_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    scell_num = row[18]
    if scell_num != '':
        scell_num = int(scell_num)
        if scell_num > 0:
            scell_param = int(row[19])
            s = 1
            for a in range(20, 20+scell_num*scell_param, scell_param):
                j = 0
                for i in scell_headers:
                    col_pointer = a+j
                    if i == "LTE_Bandwidth_SCell":
                        if row[col_pointer] in bandwidth:
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = bandwidth[row[col_pointer]]
                            data[i + "_" + str(s)] = bandwidth[row[col_pointer]]
                        else:
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = row[col_pointer]
                            data[i + "_" + str(s)] = row[col_pointer]
                    elif i == "LTE_Transmission_Mode_SCell":
                        if i in lte_param_list:
                            lte_param_list[i + "_" + str(s)] = "TM"+row[col_pointer]
                        data[i + "_" + str(s)] = "TM"+row[col_pointer]
                    elif i == "LTE_TX_Antennas_SCell":
                        if row[col_pointer] in tx:
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = tx[row[col_pointer]]
                            data[i + "_" + str(s)] = tx[row[col_pointer]]
                        else:
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = row[col_pointer]
                            data[i + "_" + str(s)] = row[col_pointer]
                    elif i == "LTE_CA_Mode_SCell":
                        if row[col_pointer] == "1":
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = "DL"
                            data[i + "_" + str(s)] = "DL"
                        elif row[col_pointer] == "2":
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = "UL+DL"
                            data[i + "_" + str(s)] = "UL+DL"
                        else:
                            if i in lte_param_list:
                                lte_param_list[i + "_" + str(s)] = row[col_pointer]
                            data[i + "_" + str(s)] = row[col_pointer]
                    else:
                        if i in lte_param_list:
                            lte_param_list[i + "_" + str(s)] = row[col_pointer]
                        data[i + "_" + str(s)] = row[col_pointer]
                    j += 1
                s += 1
    data["Serving_EARFCN"] = data["Channel"]
    rrc_state = data["RRC_state"]
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_umts_chi(row):

    global umts_param_list, rrc_state

    headers = ["Band", "RRC_state", "Channel", "Cell_Identification", "LAC", "Add_Window_1A", "TimeToTrig_1A",
               "Drop_Window_1B", "TimeToTrig_1B", "Replace_Window_1C", "TimeToTrig_1C", "SF_DL", "Min_SF_UL",
               "DRX_Cycle_Length", "UMTS_Max_TXPower_UL", "Treselection"]

    umts_rrc_state = {"1": "Idle", "2": "URA_PCH", "3": "Cell_PCH", "4": "Cell_FACH", "5": "Cell_DCH"}

    data = {}
    array = []

    for i in umts_param_list:
        if i not in ("Channel", "RRC_state"):
            umts_param_list[i] = ""

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    j = 4
    for i in headers:
        if i == "RRC_state":
            if row[j] in umts_rrc_state:
                if i in umts_param_list:
                    umts_param_list[i] = umts_rrc_state[row[j]]
                data[i] = umts_rrc_state[row[j]]
            else:
                if i in umts_param_list:
                    umts_param_list[i] = row[j]
                data[i] = row[j]
        else:
            if i in umts_param_list:
                umts_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    rrc_state = data["RRC_state"]
    data["ActiveSet_UARFCN_1"] = data["Channel"]
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_chi(row):

    global gsm_param_list

    headers = ["Band", "Channel_Type", "Channel", "Cell_Identification", "LAC", "DTX_UL", "RLT_Max",
               "Ext_Channel_Type", "TS", "BCCH", "BSIC", "BCCH_Band"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    j = 4
    for i in headers:
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_ci(row):

    # headers = ["LTE_SNR", "LTE_Cell_type"]

    # ant_headers = ["LTE_Port_SNR", "LTE_Port"]

    port = {"0": "0", "1": "1", "2": "2", "3": "3",
            "100": "TX0_RX0", "101": "TX0_RX1", "102": "TX0_RX2", "103": "TX0_RX3",
            "110": "TX1_RX0", "111": "TX1_RX1", "112": "TX1_RX2", "113": "TX1_RX3",
            "120": "TX2_RX0", "121": "TX2_RX1", "122": "TX2_RX2", "123": "TX2_RX3",
            "130": "TX3_RX0", "131": "TX3_RX1", "132": "TX3_RX2", "133": "TX3_RX3"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    snr_average = row[5]
    cell_type = row[6]
    data["LTE_Cell_type"] = lte_cell_type[cell_type]
    data["LTE_SNR_" + lte_cell_type[cell_type]] = snr_average

    # loop over antennas and their parameters

    ant_pos = 5+int(row[4])
    ant_num = int(row[7])
    ant_params = int(row[8])
    ant_length = ant_num*ant_params

    port_type = "99"

    for ant_pointer in range(ant_pos+2, ant_pos + 2 + ant_length, ant_params):
        for i in reversed(range(ant_params)):
            if i == 1:
                port_type = port[row[ant_pointer + i]]
            elif i == 0:
                data["LTE_SNR_Port_" + port_type] = row[ant_pointer + i]

    array.append(data)
    # print(array)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_txpc(row):

    headers = ["LTE_TXPower_PUSCH", "LTE_TXPower_PUCCH", "LTE_TXPower_HeadRoom_PUSCH", "LTE_TXPower_Adj_PUSCH",
               "LTE_TXPower_Adj_PUCCH"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["LTE_Cell_type"] = lte_cell_type[row[-1]]
    j = 4
    for i in headers:
        data[i] = row[j]
        j += 1

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_umts_txpc(row):

    headers = ["UMTS_TXPower", "UMTS_PC_Algorithm", "UMTS_PC_StepSize", "UMTS_CompressMode", "UMTS_UL_PwrUp",
               "UMTS_UL_PwrDwn", "UMTS_UL_PwrUp_perc"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_tad(row):

    headers = ["LTE_TA", "LTE_Cell_type"]

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:
        if i == "LTE_Cell_type":
            data[i] = lte_cell_type[row[j]]
        else:
            data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_cqi(row):

    headers = ["LTE_CQI_Sample_duration_ms", "LTE_Req_rate_bps", "LTE_WB_CQI_Codeword0", "LTE_WB_CQI_Codeword1",
               "LTE_SB_CQI_Codeword0", "LTE_SB_CQI_Codeword1", "LTE_WB_PMI", "LTE_Cell_type"]

    rank_headers = ["LTE_Req_Rank", "LTE_Rank"]

    cqi_headers = ["LTE_CQI_SB_index", "LTE_CQI_Codeword0_SB", "LTE_CQI_Codeword1_SB"]



    data = {}
    array = []

    head_params = int(row[4])

    rank_pos = 4+head_params+1
    rank_values = int(row[rank_pos])
    rank_params = int(row[rank_pos+1])
    rank_length = rank_values*rank_params

    cqi_pos = rank_pos+rank_length+2
    cqi_values = int(row[cqi_pos])
    cqi_params = int(row[cqi_pos+1])
    length = cqi_values*cqi_params

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # loop over header parameters
    i = 0
    for head_pointer in range(5, 5+head_params):
        j = headers[i]
        if i == 7:
            data[j] = lte_cell_type[row[head_pointer]]
        else:
            data[j] = row[head_pointer]
        i += 1

    # loop over rank values and their parameters
    k = 1
    for rank_pointer in range(rank_pos+2, rank_pos+2 + rank_length, rank_params):
        for i in range(rank_params):
            j = rank_headers[i] + "_" + str(k)
            data[j] = row[rank_pointer + i]
        k += 1
    # loop over measured CQI values and their parameters
    k = 0
    for cqi_pointer in range(cqi_pos+2, cqi_pos+2 + length, cqi_params):
        for i in range(cqi_params):
                j = cqi_headers[i] + "_" + str(k)
                data[j] = row[cqi_pointer + i]
        k += 1

    array.append(data)
    # print(array)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_umts_cqi(row):

    headers = ["HSDPA_CQI_Sample_duration_ms", "HSDPA_Ph_Req_rate_bps", "HSDPA_CQI_repetitions", "HSDPA_CQI_cycle",
               "HSDPA_MIMO_Rank2_req_ratio"]

    cqi_headers = ["HSDPA_CQI_percent", "HSDPA_CQI", "HSDPA_CQI_type", "HSDPA_CQI2", "HSDPA_Cell_type"]

    hsdpa_cell_type = {"1": "Primary", "2": "Secondary"}
    hsdpa_cqi_type = {"1": "A", "2": "B"}

    data = {}
    array = []

    cqi_average = 0

    head_params = int(row[4])
    cqi_pos = 4+head_params+1
    cqi_values = int(row[cqi_pos])
    cqi_params = int(row[cqi_pos+1])
    length = cqi_values*cqi_params

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # loop over header parameters
    i = 0
    for head_pointer in range(5, 5+head_params):
        j = headers[i]
        data[j] = row[head_pointer]
        i += 1

    # loop over measured CQI values and their parameters
    k = 0
    for cqi_pointer in range(cqi_pos+2, cqi_pos+2 + length, cqi_params):
        for i in range(cqi_params):
            if k > 0:
                j = cqi_headers[i] + "_" + str(k)
            else:
                j = cqi_headers[i]
            if i == 1:
                cqi_average = cqi_average + float(row[cqi_pointer+i])
                data[j] = row[cqi_pointer + i]
            elif i == 2:
                data[j] = hsdpa_cqi_type[row[cqi_pointer+i]]
            elif i == 4:
                data[j] = hsdpa_cell_type[row[cqi_pointer+i]]
            else:
                data[j] = row[cqi_pointer+i]
        k += 1
        if cqi_values > 0:
            data["HSDPA_CQI_Average"] = str(round(cqi_average/cqi_values, 1))
        else:
            data["HSDPA_CQI_Average"] = ""
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_lte_cellmeas(row):

    # cell_type = {"0": "Active", "1": "Monitored", "2": "Detected", "3": "Undetected"}

    headers = ["Lcell_type", "Band", "EARFCN", "PCI", "RSSI", "RSRP", "RSRQ", "Timing", "Pathloss", "Srxlev", "SINR"]

    data = {}
    array = []
    ctype = ""

    cells = int(row[5])
    params = int(row[6])
    length = cells * params

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # loop over measured cells and their parameters
    l = d = s = 1
    for cell_pointer in range(7, 7 + length, params):
        for i in range(params):

            if i == 0:
                ctype = row[cell_pointer + i]
            if ctype == "0":
                j = headers[i]
                data["Serving_"+j] = row[cell_pointer + i]
            elif ctype == "1":
                j = headers[i] + "_" + str(l)
                data["Listed_" + j] = row[cell_pointer + i]
            elif ctype == "2":
                j = headers[i] + "_" + str(d)
                data["Detected_" + j] = row[cell_pointer + i]
            elif ctype == "10":
                j = str(s) + "_" + headers[i]
                data["SCell_" + j] = row[cell_pointer + i]
        if ctype == "1":
            l += 1
        elif ctype == "2":
            d += 1
        elif ctype =="10":
            s += 1
    if rrc_state != "Idle": #rrc_state is not idle
        for l in lte_param_list:
            data[l] = lte_param_list[l]
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_umts_cellmeas(row):

    # cell_type = {"0": "Active", "1": "Monitored", "2": "Detected", "3": "Undetected"}

    headers = ["Ucell_type", "Band", "UARFCN", "PSC", "EcNo", "STTD", "RSCP", "SecondarySC", "Squal", "Srxlev",
               "Hqual", "Hrxlev", "Rqual", "Rrxlev", "OFF", "Tm", "Pathloss"]

    ch_headers = ["Uchannel", "RSSI", "UBand"]

    data = {}
    array = []
    ctype = ""

    channels = int(row[5])
    ch_params = int(row[6])
    ch_length = channels * ch_params

    cells_pos = 7+ch_length
    cells = int(row[cells_pos])
    params = int(row[cells_pos+1])
    length = cells*params

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # loop over measured channels and their parameters
    k = 1
    for ch_pointer in range(7, 7+ch_length, ch_params):
        for i in range(ch_params):
            j = ch_headers[i] + "_" + str(k)
            data[j] = row[ch_pointer + i]
        k += 1

    # loop over measured cells and their parameters

    a = m = d = u = 1
    for cell_pointer in range(cells_pos + 2, cells_pos + 2 + length, params):
        for i in range(params):

            if i == 0:
                ctype = row[cell_pointer + i]
            if ctype == "0":
                j = headers[i] + "_" + str(a)
                data["ActiveSet_"+j] = row[cell_pointer + i]
            elif ctype == "1":
                j = headers[i] + "_" + str(m)
                data["MonitoredSet_" + j] = row[cell_pointer + i]
            elif ctype == "2":
                j = headers[i] + "_" + str(d)
                data["DetectedSet_" + j] = row[cell_pointer + i]
            else:
                j = headers[i] + "_" + str(u)
                data["UndetectedSet_" + j] = row[cell_pointer + i]
        if ctype == "0":
            a += 1
        elif ctype == "1":
            m += 1
        elif ctype == "2":
            d += 1
        else:
            u += 1
#    k = 0
#    for cell_pointer in range(cells_pos+2, cells_pos+2+length, params):
#
#        for i in range(params):
#            j = headers[i] + "_" + str(k)
#            if j == ("Ucell_type" + "_" + str(k)):
#                # Active cell
#                if row[cell_pointer + i] == "0":
#                    data[j] = cell_type["0"]
#
#                    if call_state == "Dedicated":
#
#                        for l in umts_param_list:
#                            data[l] = umts_param_list[l]
#                # Monitored cell
#                elif row[cell_pointer + i] == "1":
#                    data[j] = cell_type["1"]
#                # Detected cell
#                elif row[cell_pointer + i] == "2":
#                    data[j] = cell_type["2"]
#                # Undetected cell
#                elif row[cell_pointer + i] == "3":
#                    data[j] = cell_type["3"]
#
#            else:
#                data[j] = row[cell_pointer + i]
#        k += 1
    if rrc_state != "Idle": #rrc_state is not idle
        for l in umts_param_list:
            data[l] = umts_param_list[l]
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_rrcsm(row):
    global gsm_param_list

    # 3G RRC
    # bcch = RRC3G.Class_definitions.BCCH_BCH2_Message
    bcch_bch = RRC3G.Class_definitions.BCCH_BCH_Message
    pcch = RRC3G.Class_definitions.PCCH_Message
    ccch_ul = RRC3G.Class_definitions.UL_CCCH_Message
    ccch_dl = RRC3G.Class_definitions.DL_CCCH_Message
    dcch_dl = RRC3G.Class_definitions.DL_DCCH_Message
    dcch_ul = RRC3G.Class_definitions.UL_DCCH_Message

    # LTE RRC
    lte_bcch = RRCLTE.EUTRA_RRC_Definitions.BCCH_BCH_Message
    lte_sch = RRCLTE.EUTRA_RRC_Definitions.BCCH_DL_SCH_Message
    lte_pcch = RRCLTE.EUTRA_RRC_Definitions.PCCH_Message
    lte_ccch_ul = RRCLTE.EUTRA_RRC_Definitions.UL_CCCH_Message
    lte_ccch_dl = RRCLTE.EUTRA_RRC_Definitions.DL_CCCH_Message
    lte_dcch_dl = RRCLTE.EUTRA_RRC_Definitions.DL_DCCH_Message
    lte_dcch_ul = RRCLTE.EUTRA_RRC_Definitions.UL_DCCH_Message

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]

    # if RRC message is UMTS
    if row[3] == "5":
        data["System"] = system[row[3]]
        data["RRC_Direction"] = direction[row[4]]
        data["RRC_Msg"] = row[5]
        data["RRC_Channel"] = row[6]
        data["RRC_String"] = rrc_string = row[9]
        # rrc_string_bytes = bytes.fromhex(rrc_string)

        if data["RRC_Msg"] != "UNKNOWN" and data["RRC_Msg"] != "":
            if data["RRC_Channel"] == "DCCH" and data["RRC_Direction"] == "DL":
                try:
                    dcch_dl.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = dcch_dl()
                    # print(dcch_dl.to_asn1())
                    # print(dcch_dl())
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "DCCH" and data["RRC_Direction"] == "UL":
                try:
                    dcch_ul.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = dcch_ul()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "PCCH":
                try:
                    pcch.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = pcch()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "CCCH" and data["RRC_Direction"] == "DL":
                try:
                    ccch_dl.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = ccch_dl()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "CCCH" and data["RRC_Direction"] == "UL":
                try:
                    ccch_ul.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = ccch_ul()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "BCCH_BCH" and data["RRC_Msg"] == 'SYSTEM_INFORMATION_BCH':
                try:
                    bcch_bch.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = bcch_bch()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            # elif data["RRC_Channel"] == "BCCH" and data["RRC_Msg"] != 'SYSTEM_INFORMATION_BLOCK_TYPE_2'
            # and data["RRC_Msg"] != 'SYSTEM_INFORMATION_BLOCK_TYPE_7' and data["RRC_Msg"] != 'SCHEDULING_BLOCK_1':
            #    print(row[1])
            #    bcch.from_uper(unhexlify(rrc_string))
            #    data["RRC_parsed"] = bcch()
        # add lte_param_list values to the array for writing
        for l in umts_param_list:
            data[l] = umts_param_list[l]

    # if RRC message is LTE
    elif row[3] == "7":
        data["System"] = system[row[3]]
        data["RRC_Direction"] = direction[row[4]]
        data["RRC_Msg"] = row[5]
        data["RRC_Channel"] = row[6]
        data["RRC_String"] = rrc_string = row[9]
        # rrc_string_bytes = bytes.fromhex(rrc_string)

        if data["RRC_Msg"] != "UNKNOWN" and data["RRC_Msg"] != "":
            if data["RRC_Channel"] == "DCCH" and data["RRC_Direction"] == "DL":
                try:
                    lte_dcch_dl.from_uper(unhexlify(rrc_string))
                    #print(lte_dcch_dl.to_asn1(rrc_string))
                    #print(dcch_dl())
                    data["RRC_parsed"] = lte_dcch_dl()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "DCCH" and data["RRC_Direction"] == "UL":
                try:
                    lte_dcch_ul.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_dcch_ul()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "PCCH":
                try:
                    lte_pcch.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_pcch()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "CCCH" and data["RRC_Direction"] == "DL":
                try:
                    lte_ccch_dl.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_ccch_dl()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "CCCH" and data["RRC_Direction"] == "UL":
                try:
                    lte_ccch_ul.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_ccch_ul()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "BCCH-BCH":
                try:
                    lte_bcch.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_bcch()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
            elif data["RRC_Channel"] == "BCCH-SCH":
                try:
                    lte_sch.from_uper(unhexlify(rrc_string))
                    data["RRC_parsed"] = lte_sch()
                except Exception as e:
                    msg = "--- RRC decoding error: " + str(e)+" ---"
                    print(msg)
                    data["RRC_parsed"] = msg
        # add lte_param_list values to the array for writing
        for l in lte_param_list:
            data[l] = lte_param_list[l]
    array.append(data)

    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_sho(row):
    global gsm_param_list

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    sho_status = ""

    # if system is UMTS FDD
    if row[3] == "5":
        # SHO status
        if row[4] == "1":
            sho_status = "Success"
        elif row[4] == "2":
            sho_status = "Fail"
        else:
            sho_status = "n/a"

    # number of cells added/removed
    sho_add = row[6]
    sho_rmv = row[7]

    # PSC of added/removed cells
    added_psc = []
    removed_psc = []

    # SHO addition
    if sho_add > "0" and sho_rmv == "0":
        for j in range(8, 8+int(sho_add)):
            added_psc.append(row[j])
        added_psc_str = '_'.join(added_psc)
        data["HO_status"] = "SHO Add " + sho_status + " PSC" + added_psc_str

    # SHO removal
    elif sho_add == "0" and sho_rmv > "0":
        for j in range(8, 8+int(sho_rmv)):
            removed_psc.append(row[j])
        removed_psc_str = '_'.join(removed_psc)
        data["HO_status"] = "SHO Rmv " + sho_status + " PSC" + removed_psc_str

    # SHO replacement
    elif sho_add > "0" and sho_rmv > "0":
        for j in range(8, 8+int(sho_add)):
            added_psc.append(row[j])
        added_psc_str = '_'.join(added_psc)
        for j in range(8+int(sho_add), 8+int(sho_add)+int(sho_rmv)):
            removed_psc.append(row[j])
        removed_psc_str = '_'.join(removed_psc)
        data["HO_status"] = "SHO Rep " + sho_status + " AddPSC" + added_psc_str + " RmvPSC" + removed_psc_str
    else:
        data["HO_status"] = "n/a"
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_hof(row):
    global gsm_param_list

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["HO_id"] = row[3]
    data["HO_status"] = "HO Fail"

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_hos(row):
    global gsm_param_list

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["HO_id"] = row[3]
    data["HO_status"] = "HO Success"

    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_hoa(row):
    global gsm_param_list
    # headers = ["ChanNo", "Code"]
    headers_gsm = ["ARFCN", "TS"]
    headers_umts = ["UARFCN", "PSC"]
    headers_lte = ["EARFCN", "PCI"]

    ho_type = {"101": "GSM intracell HO Att", "102": "GSM intercell HO Att", "103": "GSM intersystem HO Att",
               "104": "GSM interband HO Att", "105": "GSM intracell interband HO Att",
               "401": "UMTS hard HO Att", "403": "UMTS intersystem HO Att",
               "901": "LTE intercell HO Att", "902": "LTE interfreq HO Att", "903": "LTE interband HO Att",
               "904": "LTE intersystem HO Att"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["HO_id"] = row[3]
    ho = row[5]

    # Current system parameters
    cur_sys = row[6]
    cur_sys_params = int(row[7])

    # Attempted system parameters - depends of current system length
    atmpt_pos = 7+cur_sys_params+1
    atmpt_sys = row[atmpt_pos]
    # atmpt_sys_params = row[atmpt_pos+1]
    if cur_sys != '' and atmpt_sys != '':
        if ho in ho_type:
            ho_status = ho_type[ho]
        else:
            ho_status = ho

        current_sys_list = []
        atmpt_sys_list = []

        # Parsing current system parameters
        j = 8

        # Checking if current system is GSM, UMTS or LTE
        if cur_sys == "1":
            for i in headers_gsm:
                current_sys_list.append(i+row[j])
                j += 1
        elif cur_sys == "5":
            for i in headers_umts:
                current_sys_list.append(i+row[j])
                j += 1
        elif cur_sys == "7":
            for i in headers_lte:
                current_sys_list.append(i+row[j])
                j += 1
        else:
            current_sys_list.append(system[cur_sys])

        # Parsing attempted system parameters
        j = atmpt_pos+2

        # Checking if attempted system is GSM, UMTS or LTE
        if atmpt_sys == "1":
            for i in headers_gsm:
                atmpt_sys_list.append(i+row[j])
                j += 1
        elif atmpt_sys == "5":
            for i in headers_umts:
                atmpt_sys_list.append(i+row[j])
                j += 1
        elif atmpt_sys == "7":
            for i in headers_lte:
                atmpt_sys_list.append(i+row[j])
                j += 1
        else:
            atmpt_sys_list.append(system[atmpt_sys])

        current_sys_list_str = '_'.join(current_sys_list)
        atmpt_sys_list_str = '_'.join(atmpt_sys_list)

        data["HO_status"] = ho_status + " from " + current_sys_list_str + " to " + atmpt_sys_list_str
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_caf(row):
    global gsm_param_list, call_state

    headers = ["Call_type", "Call_status", "Call_disc_cause"]

    call_disc_status = {"1": "Timeout before connection", "2": "Call was released before connection",
                        "3": "Service not available", "4": "Incoming call rejected",
                        "5": "Test system failure", "6": "SDCCH blocking", "7": "TCH blocking",
                        "8": "RRC connection failed", "9": "Radio bearer setup failed",
                        "10": "SDCCH release", "11": "SDCCH drop", "12": "TCH assignment failure",
                        "13": "Incoming call not received", "20": "PPP error"}

    data = {}
    array = []

    call_state = "Idle"

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Call_id"] = row[3]
    sys = row[4]
    if sys in system:
        data["System"] = system[sys]
    else:
        data["System"] = sys

    j = 5
    for i in headers:
        key = row[j]
        if i == headers[0]:
            if key in call_type:
                # gsm_param_list[i] = call_type[key]
                data[i] = call_type[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        elif i == headers[1]:
            if key in call_disc_status:
                # gsm_param_list[i] = call_disc_status[key]
                data[i] = call_disc_status[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        else:
            # gsm_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    array.append(data)
    # print(array)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_cad(row):
    
    global gsm_param_list, call_state
    
    headers = ["Call_type", "Call_status", "Call_disc_cause"]
    
    call_disc_status = {"1": "Normal disconnect", "2": "Dropped call", "3": "Dropped out of service", "4": "Dropped HO",
                        "5": "Test system failure", "6": "Timeout", "7": "Voice quality sync lost",
                        "12": "TCH assignment fail", "13": "Early release", "20": "PPP error"}
    
    data = {}
    array = []
    
    call_state = "Idle"

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Call_id"] = row[3]
    sys = row[4]
    if sys in system:
        data["System"] = system[sys]
    else:
        data["System"] = sys
    
    j = 5
    for i in headers:
        key = row[j]
        if i == headers[0]:
            if key in call_type:
                # gsm_param_list[i] = call_type[key]
                data[i] = call_type[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        elif i == headers[1]:
            if key in call_disc_status:        
                # gsm_param_list[i] = call_disc_status[key]
                data[i] = call_disc_status[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        else:
            # gsm_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    array.append(data)
    # print(array)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_cac(row):
    
    global gsm_param_list, call_state
    headers = ["Call_type", "Call_status", "Params"]

    call_status = {"1": "Traffic channel allocated", "2": "Alerting", "3": "Connected",
                   "4": "Dial-up connection established"}
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Call_id"] = row[3]
    sys = row[4]
    if sys in system:
        data["System"] = system[sys]
    else:
        data["System"] = sys

    j = 5
    for i in headers:
        key = row[j]
        if i == headers[0]:
            if key in call_type:
                # gsm_param_list[i] = call_type[key]
                data[i] = call_type[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        elif i == headers[1]:
            if key == "1":
                call_state = "Dedicated"
            if key in call_status:
                # gsm_param_list[i] = call_status[key]
                data[i] = call_status[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        else:
            # gsm_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_caa(row):
    global gsm_param_list, call_state
    headers = ["Call_type", "Call_status", "Phone_number", "Own_phone_number", "Call_timeout", "Unique_ID",
               "CAA_time_correction"]

    call_direction = {"1": "Originated call", "2": "Terminated call"}

    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["Call_id"] = row[3]
    sys = row[4]
    if sys in system:
        data["System"] = system[sys]
    else:
        data["System"] = sys

    j = 5
    for i in headers:
        key = row[j]
        if i == headers[0]:
            if key in call_type:
                # gsm_param_list[i] = call_type[key]
                data[i] = call_type[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        elif i == headers[1]:
            if key in call_direction:
                # gsm_param_list[i] = call_status[key]
                data[i] = call_direction[key]
            else:
                # gsm_param_list[i] = row[j]
                data[i] = row[j]
        else:
            # gsm_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_amrs(row):
    
    global gsm_param_list
    headers = ["AMR_mode_UL", "AMR_mode_DL", "AMR_mode_cmd", "AMR_mode_req", "AMR_channel_type"]

    amr_codec_set = {"0": "NB_4.75", "1": "NB_5.15", "2": "NB_5.9", "3": "NB_6.7", "4": "NB_7.4", "5": "NB_7.95",
                     "6": "NB_10.2", "7": "NB_12.2", "100": "WB_6.6", "101": "WB_8.85", "102": "WB_12.65",
                     "103": "WB_14.25", "104": "WB_15.85", "105": "WB_18.25", "106": "WB_19.85", "107": "WB_23.05",
                     "108": "WB_23.85"}
    amr_chan_type = {"1": "AMR_HR", "2": "AMR_FR"}
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:
        key = row[j]
        if i == headers[-1]:
            if key in amr_chan_type:
                gsm_param_list[i] = amr_chan_type[key]
                data[i] = amr_chan_type[key]
            else:
                gsm_param_list[i] = row[j]
                data[i] = row[j]
        else:
            if key in amr_codec_set:
                gsm_param_list[i] = amr_codec_set[key]
                data[i] = amr_codec_set[key]
            else:
                gsm_param_list[i] = row[j]
                data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_drate(row):
    
    global gsm_param_list
    headers = ["Transfer_ID", "App_protocol", "App_rate_UL", "App_rate_DL", "App_bytes_UL", "App_bytes_DL"]


    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 3
    for i in headers:        
        if i == headers[1]:
            key = row[j]
            if key in app_protocols:
                gsm_param_list[i] = app_protocols[key]
                data[i] = app_protocols[key]
        else:
            gsm_param_list[i] = row[j]
            data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_ppp(row):
    
    global gsm_param_list
    headers = ["PPP_rate_UL", "PPP_rate_DL", "PPP_bytes_UL", "PPP_bytes_UL"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 3
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_amrq(row):
    
    global gsm_param_list
    headers = ["CI_AMR"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_ci(row):

    headers = ["ARFCN", "CI", "RSSI"]
    
    data = {}
    array = []

    # number of gprs timeslot C/I results
    gprs = int(row[5])
    
    if gprs != 0:
        gprs_ci_last = 5+gprs
        channels = int(row[gprs_ci_last+1])
        params = int(row[gprs_ci_last+2])
        length = channels*params
        start = gprs_ci_last+3                                                                         
    else:
        gprs_ci_last = 5
        channels = int(row[6])
        params = int(row[7])
        length = channels*params
        start = gprs_ci_last+3

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    data["CI_average"] = row[4]
    # loop over measured ARFCNs and their parameters
    k = 1
    for pointer in range(start, start+length, params):
        for i in range(params):
            j = headers[i] + "_" + str(k)
            data[j] = row[pointer+i]
        k += 1
    array.append(data)
    # data = {}
    # print(array)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)
        

def parse_gsm_msp(row):
    
    global gsm_param_list
    headers = ["MSPower", "MSPower_Band"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        if i == "MSPower_Band":
            key = row[j]
            if key in band:
                gsm_param_list[i] = data[i] = band[key]
            else:
                gsm_param_list[i] = data[i] = band["19999"]
        else:
            gsm_param_list[i] = data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_tad(row):
    
    global gsm_param_list
    headers = ["TA"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_rlt(row):
    
    global gsm_param_list
    headers = ["RLT"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_fer(row):
    
    global gsm_param_list
    headers = ["FER_full", "FER_sub", "FER_TCH", "DTX_DL"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_rxq(row):

    global gsm_param_list
    headers = ["RxQual_full", "RxQual_sub"]
    
    data = {}
    array = []

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    j = 4
    for i in headers:     
        gsm_param_list[i] = row[j]
        data[i] = row[j]
        j += 1
    array.append(data)
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_gsm_cellmeas(row):

    cell_type = {"0": "Neighbor", "1": "Serving"}

    headers = ["GCell_type", "Band", "BCCH", "BSIC", "RxLev_full", "RxLev_sub", "C1", "C2", "C31", "C32",
               "HCS_prio", "HCS_thr", "CellID", "LAC", "RAC", "Srxlev"]

    data = {}
    array = []

    cells = int(row[5])
    params = int(row[6])
    length = cells*params

    data["Date"] = info_list["Date"]
    data["Filename"] = info_list["Filename"]
    data["Time"] = row[1]
    data["Lat"] = gps["Lat"]
    data["Lon"] = gps["Lon"]
    
    k = 0
    # loop over measured cells and their parameters
    for cell_pointer in range(7, length, params):
        
        for i in range(params):
            if k == 0:
                j = headers[i]
            else:
                j = headers[i] + "_" + str(k)
            if j == "GCell_type" or j == ("GCell_type" + "_" + str(k)):
                if row[cell_pointer+i] == "1":
                    # Serving cell
                    data[j] = cell_type["1"]
                    if call_state == "Dedicated":
                        # add gsm_param_list values to the array for writing
                        for l in gsm_param_list:
                            data[l] = gsm_param_list[l]
                else:
                    # Neighbor cell
                    data[j] = cell_type["0"]
            elif j == "Band" or j == ("Band" + "_" + str(k)):
                key = row[cell_pointer+i]
                if key in band:
                    data[j] = band[key]
                else:
                    data[j] = band["19999"]
            else:
                data[j] = row[cell_pointer+i]
        k += 1
    array.append(data)
    # print(array)
    # data = {}
    if data["Lat"] != "" and data["Lon"] != "":
        write_to_file(array, headers_ue, out_file)


def parse_info(row):

    global info_list

    if row[0] == "#START":
        info_list["Date"] = datetime.date(datetime.strptime(row[3], '%d.%m.%Y')) #get only the date
    elif row[0] == "#EI":
        info_list["IMEI"] = row[3]
    elif row[0] == "#SI":
        info_list["IMSI"] = row[3]
    elif row[0] == "#HV":
        info_list["Handler_ver"] = row[3]
    elif row[0] == "#HW":
        headers = ["Device_HW_ver", "Device_Manufacturer", "Device_Model", "Device_Chipset"]
        j = 0
        for i in range(3,len(row)):
            info_list[headers[j]] = row[i]
            j += 1
#        info_list["Device_HW_ver"] = row[3]
#        info_list["Device_Manufacturer"] = row[4]
#        info_list["Device_Model"] = row[5]
#        info_list["Device_Chipset"] = row[6]


def device_label(row):
    if row[0] == "#DL":
        label = row[3]
        device = "UE_" + ue
        label_data["UE"] = device
        label_data["UE_label"] = label
    elif row[0] == "#TS":
        label_data["Test_script"] = row[3]
    return label_data


def parse_gps(row):

    global gps
    
    gps["Time"] = row[1]
    gps["Lon"] = row[3]
    gps["Lat"] = row[4]
    gps["Height"] = row[5]
    gps["Velocity"] = row[9]


def write_to_file(array, headers, output_file):
    with open(output_file, "a", newline='') as w:
        writer = csv.DictWriter(w, fieldnames=headers, extrasaction='ignore')
        writer.writerows(array)   


def write_headers(headers, output_file):
        with open(output_file, "w", newline='') as g:
            writer = csv.writer(g)
            writer.writerow(headers)


def parse(nemo_file):

    err = []
    print(nemo_file)
    info_list["Filename"] = nemo_file
    with open(input_dir+"\\"+nemo_file, "r", newline='') as f:
        reader = csv.reader(f)
        
        for row in reader:
            try:
                if row[0] == "GPS":
                    parse_gps(row)
                elif row[0] == "#DL" or row[0] == "#TS":
                    device_label(row)
                elif row[0] in ("#START", "#EI", "#SI", "#HV", "#HW"):
                    parse_info(row)
                elif row[0] == "CELLMEAS":
                    empty_output["ue"] = False  # something was parsed from this logfile
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_cellmeas(row)
                    elif row[3] == parsed_system["UMTS"]:
                        parse_umts_cellmeas(row)
                    elif row[3] == parsed_system["LTE"]:
                        parse_lte_cellmeas(row)
                elif row[0] == "RXQ":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_rxq(row)
                elif row[0] == "FER":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_fer(row)
                elif row[0] == "RLT":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_rlt(row)
                elif row[0] == "TAD":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_tad(row)
                    elif row[3] == parsed_system["LTE"]:
                        parse_lte_tad(row)
                elif row[0] == "MSP":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_msp(row)
                elif row[0] == "CI":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_ci(row)
                    if row[3] == parsed_system["LTE"]:
                        parse_lte_ci(row)
                elif row[0] == "AMRQ":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_amrq(row)
                # elif row[0] == "PPPRATE":
                #    if row[3] == parsed_system["GSM"]:
                #        parse_gsm_ppp(row)
                elif row[0] == "DRATE":
                    parse_drate(row)
                    empty_output["ue"] = False  # something was parsed from this logfile
                    # if row[3] == parsed_system["GSM"]:
                elif row[0] == "AMRS":
                    if row[3] == parsed_system["GSM"]:
                        parse_gsm_amrs(row)
                elif row[0] == "CAA":
                    parse_caa(row)
                    empty_output["ue"] = False  # something was parsed from this logfile
                elif row[0] == "CAC":
                    parse_cac(row)
                    empty_output["ue"] = False  # something was parsed from this logfile
                elif row[0] == "CAD":
                    parse_cad(row)
                elif row[0] == "CAF":
                    parse_caf(row)
                elif row[0] == "HOA":
                    parse_hoa(row)
                    empty_output["ue"] = False  # something was parsed from this logfile
                elif row[0] == "HOS":
                    parse_hos(row)
                elif row[0] == "HOF":
                    parse_hof(row)
                elif row[0] == "SHO":
                    parse_sho(row)
                    empty_output["ue"] = False  # something was parsed from this logfile
                elif row[0] == "RRCSM":
                    parse_rrcsm(row)
                    empty_output["ue"] = False # something was parsed from this logfile
                elif row[0] == "CQI":
                    if row[3] == parsed_system["UMTS"]:
                        parse_umts_cqi(row)
                    elif row[3] == parsed_system["LTE"]:
                        parse_lte_cqi(row)
                elif row[0] == "TXPC":
                    if row[3] == parsed_system["UMTS"]:
                        parse_umts_txpc(row)
                    elif row[3] == parsed_system["LTE"]:
                        parse_lte_txpc(row)
                elif row[0] == "CHI":
                    #if row[3] == parsed_system["GSM"]:
                    #    parse_gsm_chi(row)
                    if row[3] == parsed_system["UMTS"]:
                        parse_umts_chi(row)
                    elif row[3] == parsed_system["LTE"]:
                       parse_lte_chi(row)
                elif row[0] == "PHRATE":
                    parse_phrate(row)
                elif row[0] == "DAA":
                    parse_daa(row)
                elif row[0] == "DAC":
                    parse_dac(row)
                elif row[0] == "DAF":
                    parse_daf(row)
                elif row[0] == "DAD":
                    parse_dad(row)
                elif row[0] == "DREQ":
                    parse_dreq(row)
                elif row[0] == "DCOMP":
                    parse_dcomp(row)
                elif row[0] == "PLAISUM":
                    parse_plaisum(row)
                elif row[0] == "PLAID":
                    parse_plaid(row)
                elif row[0] == "PLAIU":
                    parse_plaiu(row)
                elif row[0] == "FREQSCAN":
                    if empty_output["scan_gsm"] is True:
                        write_headers(headers_scan_gsm, out_file_gsmscan)
                        empty_output["scan_gsm"] = False  # something was parsed from this logfile
                    parse_freqscan(row)
                elif row[0] == "PILOTSCAN":
                    if empty_output["scan_umts"] is True:
                        write_headers(headers_scan_umts, out_file_umtsscan)
                        empty_output["scan_umts"] = False  # something was parsed from this logfile
                    parse_pilotscan(row)
                elif row[0] == "OFDMSCAN":
                    if empty_output["scan_lte"] is True:
                        write_headers(headers_scan_lte, out_file_ltescan)
                        empty_output["scan_lte"] = False  # something was parsed from this logfile
                    parse_ofdmscan(row)

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
# _main_
input_dir = gui.diropenbox("Nemo UE Parser", "Select directory with Nemo files to parse")

if input_dir is not None:

    files = os.listdir(input_dir)

    if files is not None:

        lst = list(files)

        for file in lst:
            if file[-3:] == "nmf" and file != "superstream.nmf":
                nemo_list.append(file)

        for nemo_file in nemo_list:
            ue = nemo_file[-6:-4]

            if ue[0] == ".":
                ue = int(ue[1])
            else:
                ue = int(ue)
            if ue not in ue_list:
                ue_list.append(ue)
        ue_list.sort()
        print("UE list:", ue_list)

        output_dir = gui.diropenbox("Nemo UE Parser", "Select output directory")

        if output_dir is not None:

            out_file_devlabels = output_dir + "\\DeviceLabels.txt"
            out_file_gsmscan = output_dir + "\\gsm_scanner.txt"
            out_file_umtsscan = output_dir + "\\umts_scanner.txt"
            out_file_ltescan = output_dir + "\\lte_scanner.txt"
            out_file_errors = output_dir + "\\Errors.csv"

            write_headers(headers_ue_label, out_file_devlabels)
            for ue in ue_list:
                ue = str(ue)
                out_file = output_dir + "\\UE_" + ue + ".txt"
                write_headers(headers_ue, out_file)

                print("UE:", ue)
                for nemo_file in nemo_list:
                    if nemo_file[-6] == "." and nemo_file[-5] == ue:
                        parse(nemo_file)
                    elif nemo_file[-6:-4] == ue:
                        parse(nemo_file)

                if empty_output["ue"] is False:
                    label_array.append(label_data)
                    write_to_file(label_array, headers_ue_label, out_file_devlabels)
                    empty_output["labels"] = False
                else:
                    os.remove(out_file)

                empty_output["ue"] = True
                
                label_data = {}
                label_array = []

            if empty_output["labels"] is True: # DeviceLabels file is empty
                os.remove(out_file_devlabels)
            if empty_output["errors"] is True: # Errors file is empty
                message = "%i files parsed successfully!" % len(nemo_list)
            else:
                message = "%i files parsed successfully with parsing errors.\n " \
                          "Check Errors.csv file in the output directory!" % len(nemo_list)
            print(message)
            gui.msgbox(message, "Nemo UE Parser")
