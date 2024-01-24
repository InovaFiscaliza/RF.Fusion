#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""This module perform the following tasks:
    - import config.py as k
    - list files from the folders and subfolders from:
            - f"{k.REPO_FOLDER}/{k.TMP_FOLDER}", containing files with pending processing;
            - f"{k.REPO_FOLDER}/k.TRASH_FOLDER", containing files with processing error; and
            - f"{k.REPO_FOLDER}/20dd", where dd may be any number, for folders containing files with successfull processing.
    - Connect to the mysql using mysql.connector in localhost with k.DB_USER_NAME and k.DB_PASSWORD;
    - Access a database named RFDATA
    - Compare the information in the DIM_SPECTRUM_FILE table with file names from REPO_FOLDER subfolders with 20dd name, where dd may be any number.
            - generate two lists, one of files that are not in the database but in the repository and the other the otherway arround.
            - DIM_SPECTRUM_FILE table have fields NA_FILE, NA_PATH and NA_VOLUME that store the file name and path.
            - The REPO_FOLDER is associated with an specific NA_VOLUME named "repoSFI".
    - After user confirmation, remove entries in the DIM_SPECTRUM_FILE that are not in the REPO_FOLDER;
    - After user confirmation, move files that are in the REPO_FOLDER but not in the DIM_SPECTRUM_FILE to the TMP_FOLDER folder in the remote server;
    - Access the BPDATA database with the same credentials;
    - Compare file names from the TRASH_FOLDER and TMP_FOLDER with the files listed in PRC_TASK table in the BPDATA database.
            - PRC_TASK table have a field named NA_SERVER_FILE_PATH and NA_SERVER_FILE_NAME that store the file name and path.
            - Generate two lists of files, one for those that are not in the database but in the repository and the other, the otherway arround;
    - After user confirmation, update HOST table in the BPDATA, where:
        - NU_HOST_FILES should be the total number of files for a given host (REPO_FOLDER/20dd, TMP_FOLDER and TRASH_FOLDER);
        - NU_PENDING_PROCESSING should be the number of files in the TMP_FOLDER for a given host;
        - NU_PROCESSING_ERROR, should be the number of files in the TRASH_FOLDER for a given host.
"""
import sys
# sys.path.append('Y:\\RF.Fusion\\src\\appCataloga\\root\\etc\\appCataloga\\')
sys.path.append('/etc/appCataloga')

# Import appCataloga modules 
import config as k
import shared as sh
import db_handler as dbh

import mysql.connector
from mysql.connector import Error

from pathlib import Path
import subprocess


def list_repo_files(folder:str) -> set:
    """List files in specified folder and subfolders using find command - Linux only

    Args:
        folder (str): Folder name to list files

    Returns:
        set: Set of tuples (filename, path) of files in the specified folder and subfolders
    """
    
    command = ["find", folder, "-type", "f"]
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    files = set(result.stdout.strip().split('\n'))
    return {(Path(filename).name, Path(filename).parent) for filename in files}

def move_files_to_tmp_folder(files_to_move, tmp_folder):
    
    user_input = input("Do you wish to confirm each entry before move operation? (y/n): ")
    if user_input.lower() == 'y':
        ask_berfore = True

    for filename, path in files_to_move:

        if ask_berfore:
            user_input = input(f"Move {path}/{filename} to {tmp_folder}? (y/n): ")
            if user_input.lower() != 'y':
                continue

        src_path = Path(path) / filename
        dst_path = Path(tmp_folder) / filename
        try:
            src_path.rename(dst_path)
        except Exception as e:
            print(f"Error moving {src_path} to {dst_path}: {e}")

def clean_rfdata_files(log:sh.log) -> None:
    
    try:
        db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error("Error initializing database: {e}")
        raise

    repo_folder_files = list_repo_files(f"{k.REPO_FOLDER}/20*")

    db_files = db_rfm.list_rfdb_files()
                
    # Compare sets
    files_not_in_rfdb = repo_folder_files - db_files
    files_not_in_repo = db_files - repo_folder_files

    log.entry(f"{len(repo_folder_files)} files in the repository:")
    log.entry(f"{len(db_files)} database entries related to repository files.\n")
    
    if len(files_not_in_repo) > 0:
        log.entry(f"{len(files_not_in_repo)} files not in the repository but in the database")
        confirmation = input("Do you want to remove database entries that are missing the correspondent file? (y/n): ")
    
        if confirmation.lower() == 'y':
            db_rfm.remove_rfdb_files(files_not_in_repo)
    else:
        log.entry("No entry in the RFDATA database without correspondent file in the repository.")

    if len(files_not_in_rfdb) > 0:
        log.entry(f"{len(files_not_in_rfdb)} files not in the RFDATA database but in the repository")
        confirmation = input("Do you want to move files to TMP_FOLDER for later reprocessing? (y/n): ")

        if confirmation.lower() == 'y':
            move_files_to_tmp_folder(files_not_in_rfdb, k.TMP_FOLDER)
    else:
        log.entry("No file in the repository without correspondent entry in the RFDATA database.")


def compare_files_in_bpdata(tmp_folder, trash_folder):
    # Compare files in PRC_TASK table with files in TMP_FOLDER and TRASH_FOLDER
    cursor = mysql_conn.cursor()

    # Query to get files from PRC_TASK
    query = "SELECT NA_SERVER_FILE_NAME, NA_SERVER_FILE_PATH FROM PRC_TASK"
    cursor.execute(query)
    db_files = set((row[0], row[1]) for row in cursor.fetchall())

    # Identify files not in the database
    files_not_in_db = (tmp_files | trash_files) - db_files
    files_not_in_repo = db_files - (tmp_files | trash_files)

    # Print the results
    print("Files not in the database but in the repository:")
    print(files_not_in_db)
    print("Files not in the repository but in the database:")
    print(files_not_in_repo)

    cursor.close()
    return files_not_in_db

def update_host_table(files_info):
    # After user confirmation, update HOST table in BPDATA
    confirmation = input("Do you want to update HOST table in BPDATA? (y/n): ")
    if confirmation.lower() == 'y':
        cursor = mysql_conn.cursor()
        nu_host_files = len(files_info)
        nu_pending_processing = sum(1 for file_info in files_info if file_info[1] == 'TMP_FOLDER')
        nu_processing_error = sum(1 for file_info in files_info if file_info[1] == 'TRASH_FOLDER')
        query = f"UPDATE HOST SET NU_HOST_FILES = {nu_host_files}, NU_PENDING_PROCESSING = {nu_pending_processing}, NU_PROCESSING_ERROR = {nu_processing_error} WHERE HOST_ID = 1"  # Assuming host ID is 1
        cursor.execute(query)
        mysql_conn.commit()
        cursor.close()

def clean_bpdata_files(log:sh.log) -> None:
    
    try:
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error("Error initializing database: {e}")
        raise
    
    # Process TMP folder and database
    repo_tmp_files = list_repo_files(f"{k.REPO_FOLDER}/{k.TMP_FOLDER}")
    
    db_tmp_files = db_bp.list_bpdb_files(status=k.BP_PENDING_TASK_STATUS)
    
    log.entry(f"{len(repo_tmp_files)} files in the repository TMP_FOLDER:")
    log.entry(f"{len(db_tmp_files)} database entries related to repository TMP_FOLDER files.\n")

    files_missing_in_tmp = db_tmp_files - repo_tmp_files
        
    if len(files_missing_in_tmp) > 0:
        log.entry(f"{len(files_missing_in_tmp)} files missing in the TMP_FOLDER but in the database")
        confirmation = input("Do you want to remove database entries that are missing the correspondent file? (y/n): ")
    
        if confirmation.lower() == 'y':
            db_bp.remove_bpdb_files(files_missing_in_tmp)
    else:
        log.entry("No entry in the BPDATA database without correspondent file in the TMP_FOLDER.")

    files_to_be_processed = repo_tmp_files - db_tmp_files
    
    if len(files_to_be_processed) > 0:
        log.entry(f"{len(files_to_be_processed)} files in the TMP_FOLDER but not in the task list to be processed")
        confirmation = input("Do you want to add file to be processed? (y/n): ")

        if confirmation.lower() == 'y':
            new_tasks = db_bp.add_task_from_file(files_to_be_processed)
            for host_id, files_dict in new_tasks:
                db_bp.add_processing_task(host_id=host_id, files_set=files_dict)
    else:
        log.entry("No file in the TMP_FOLDER to be processed.")

    # ! STOPPED HERE
    # Process trash folder and database
    repo_trash_files = list_repo_files(f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}")
    
    db_trash_files = db_bp.list_bpdb_files(status=k.BP_ERROR_TASK_STATUS)
    
    # Compare sets
    files_spilled_from_trash = repo_trash_files - db_trash_files
    files_missing_in_trash = db_trash_files - repo_trash_files

    log.entry(f"{len(repo_trash_files)} files in the repository TRASH_FOLDER:")
    log.entry(f"{len(db_trash_files)} database entries related to repository TRASH_FOLDER files.\n")
    
    if len(files_missing_in_trash) > 0:
        log.entry(f"{len(files_missing_in_trash)} files missing in the TRASH_FOLDER but in the database")
        confirmation = input("Do you want to remove database entries that are missing the correspondent file? (y/n): ")
    
        if confirmation.lower() == 'y':
            db_bp.remove_bpdb_files(files_missing_in_trash)
    else:
        log.entry("No entry in the BPDATA database without correspondent file in the TRASH_FOLDER.")

        
    if len(files_spilled_from_trash) > 0:
        log.entry(f"{len(files_spilled_from_trash)} files spilled from TRASH_FOLDER")
        confirmation = input("Do you want to update database entries to be processed? (y/n): ")
    
        if confirmation.lower() == 'y':
            db_bp.update_bpdb_files(files_spilled_from_trash, status=k.BP_PENDING_TASK_STATUS)
    else:
        log.entry("No file in the TRASH_FOLDER to be processed.")

def main():
    try:                # create a warning message object
        log = sh.log(target_screen=True, target_file=False)
    except Exception as e:
        print(f"Error creating log object: {e}")
        exit(1)

    clean_rfdata_files(log)
    
    clean_bpdata_files(log)
        
if __name__ == "__main__":
    main()