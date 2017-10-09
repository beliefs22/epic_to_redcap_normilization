import createtables_subsequent
import datapull_subsequent_sql
from collections import OrderedDict
import sqlite3
import os
import csv
from datapull_subsequent_functions import *


class OrderedDefaultDict(OrderedDict):

    def __init__(self, *a, **kw):
        default_factory = kw.pop('default_factory', self.__class__)
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __missing__(self, key):
        self[key] = value = self.default_factory()
        return value


def edvisit(subject_id, csn, visitnum, conn):
    """Gets available ED visit data from CEIRS Tables

    Args:
        subject_id (str): id of subject
        conn (:obj:) `database connection): connetion to the database that
            contains the data

    Returns:
        :obj: `OrderedDefaultDict`
        returns two ordered default dictionaires with data for writing to file
        one with coordinator readable data, and one with machien readable data
    """

    coordinator_readable_data = coordinator = OrderedDefaultDict()
    redcap_label = OrderedDefaultDict()
    redcap_raw = OrderedDefaultDict()
    subject_id_for_file = subject_id.replace("'","").lower()
    coordinator_readable_data['Study ID'] = subject_id_for_file
    #Gather Repeating Instrument Variables
    redcap_label['ec_id'] = subject_id_for_file
    redcap_label['redcap_repeat_instrument'] = 'form_112p_ed_subsequent_visit_chart_review'
    redcap_label['redcap_repeat_instance'] = visitnum.replace("'","")
    # redcap_label['redcap_data_access_group'] = 'jhhs'
    redcap_label['edsubshart_subvisit'] = 'Yes'

    redcap_raw['ec_id'] = subject_id_for_file
    redcap_raw['redcap_repeat_instrument'] = 'form_112p_ed_subsequent_visit_chart_review'
    redcap_raw['redcap_repeat_instance'] = visitnum.replace("'","")
    # redcap_raw['redcap_data_access_group'] = 'jhhs'
    redcap_raw['edsubshart_subvisit'] = '1'

    #Get Discharge time for time checking
    dc_info = ADT(*datapull_subsequent_sql.discharge_date_time(subject_id, csn, conn))
    #Get Dispo Status for checking
    dispo = dc_info.dispo
    dc_time = "{} {}".format(dc_info.date, dc_info.time)
    coordinator, redcap_label, redcap_raw = get_arrival_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw = get_discharge_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_dispo_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_vitals_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_oxygen_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_lab_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_flutesting_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time)
    coordinator, redcap_label, redcap_raw  = get_othervir_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time)
    coordinator, redcap_label, redcap_raw  = get_antiviral_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time)
    coordinator, redcap_label, redcap_raw  = get_dc_antiviral_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time, dispo)
    coordinator, redcap_label, redcap_raw  = get_antibiotic_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time)
    coordinator, redcap_label, redcap_raw  = get_dc_abx_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn, dc_time, dispo)
    coordinator, redcap_label, redcap_raw  = get_imaging_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)
    coordinator, redcap_label, redcap_raw  = get_diagnosis_info(coordinator, redcap_label, redcap_raw, subject_id, csn, conn)

    return coordinator, redcap_label, redcap_raw


def main():
    #Get File Path for Database and Patient data
    #Get Base File Path
    os.chdir("..")
    base_path = os.getcwd()
    sep = os.sep
    patient_data_path = base_path + sep + "Patient_Data"
    conn = sqlite3.connect(r"{}{}CEIRS.db".format(base_path,sep))
    #Create Tables that will hold data
    createtables_subsequent.create_tables(conn)
    # Get subject IDs
    cur = conn.cursor()
    sql = """SELECT STUDYID, CSN, VISITNUMBER, DataPullComplete FROM SUBSEQUENTVISITLOG"""
    cur.execute(sql)
    subjects = cur.fetchall()
    sep = os.sep
    # Get ED Enrollment Headers
    ed_subsequent_visit_header_file = open(r"{}{}ed_subsequent_visit_headers.csv".format(patient_data_path, sep), 'r')
    ed_header_reader = csv.DictReader(ed_subsequent_visit_header_file)
    ed_subsequent_visit_headers = ed_header_reader.fieldnames
    labeled_data_for_redcap = list()
    raw_data_for_redcap = list()
    
    for subject in subjects:
        subject_id = "'{}'".format(subject[0])
        csn = "'{}'".format(subject[1])
        visitnum = "'{}'".format(subject[2])
        data_pull_status = subject[3]
        if data_pull_status == "No":
            subject_id_for_file = subject_id.replace("'","").lower() + "_subsequent_visit_{}".format(visitnum.replace("'",""))
            with open(patient_data_path + sep + "{}_data.txt".format(subject_id_for_file),'w') as outfile1:
                # Write Files for Coordinators to Read
                print("Writing coordinator Data File for Subject {}".format(subject_id_for_file))
                coordinator_readable_data, redcap_label_data, redcap_raw_data = edvisit(subject_id, csn, visitnum, conn)
                for key,value in coordinator_readable_data.items():
                    outfile1.write("{}: {}\n".format(key, value))
                # Data to import into redcap
                labeled_data_for_redcap.append(redcap_label_data)
                raw_data_for_redcap.append(redcap_raw_data)
    print("Finished writing all coordinator files")
    print("Starting write to redcap data file")
    with open(patient_data_path + sep + "redcap_labeled_data.csv", 'w') as outfile2:
        redcap_file = csv.DictWriter(
            outfile2, fieldnames=ed_subsequent_visit_headers, restval='',
            lineterminator='\n')
        redcap_file.writeheader()
        for row in labeled_data_for_redcap:
            redcap_file.writerow(row)
    print("Finished writing labeled files")

    with open(patient_data_path + sep + "redcap_raw_data.csv", 'w') as outfile2:
        redcap_file = csv.DictWriter(
            outfile2, fieldnames=ed_subsequent_visit_headers, restval='',
            lineterminator='\n')
        redcap_file.writeheader()
        for row in raw_data_for_redcap:
            redcap_file.writerow(row)
    print("Finished writing raw files")
    # create comparison file to compare manual data to auto data
##    print("Writing Comparison File")
##    comparedata.compare()
##    print("Finished writing compare file")
    print("All Done!")

            

if __name__ =="__main__":
    main()
