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

from pathlib import Path
import subprocess


def list_repo_files(folder:str) -> set:
    """List files in specified folder and subfolders using find command - Linux only

    Args:
        folder (str): Folder name to list files

    Returns:
        set: Set of tuples (filename, path) of files in the specified folder and subfolders
    """
    
    command = f"find {folder} -type f"
    result = subprocess.run(command, capture_output=True, shell=True, text=True)
    files = set(result.stdout.strip().split('\n'))
    
    files_set = set()
    for full_filename in files:
        if len(full_filename) < 1:
            continue
        filename = str(Path(full_filename).name)
        full_path = str(Path(full_filename).parent)
        relative_path = full_path[len(k.REPO_FOLDER)+1:]
        files_set.add((filename, relative_path))
    
    return files_set

def move_file_set(files_to_move:set, target:str, log:sh.log) -> None:
    """Move files in a set to a target folder

    Args:
        files_to_move (set): Set of tuples (filename, path) of files to move
        target (str): Target folder to move files
        log (sh.log): Log object
    """
    
    if len(files_to_move) > 1:
        user_input = input("Do you wish to confirm each entry before move operation? (y/n): ")
        if user_input.lower() == 'y':
            ask_berfore = True
        else:
            ask_berfore = False
    else:
        ask_berfore = False

    # test if target folder exists and create if not
    target_folder = Path(target)
    if not target_folder.exists():
        try:
            target_folder.mkdir()
        except Exception as e:
            log.error(f"Error creating {target_folder}: {e}")
            return
    
    for filename, path in files_to_move:

        if ask_berfore:
            user_input = input(f"Move {path}/{filename} to {target}? (y/n): ")
            if user_input.lower() != 'y':
                files_to_move.pop((filename, path))
                continue
        
        src_path = Path(f"{k.REPO_FOLDER}/{path}/{filename}")
        dst_path = Path(target) / filename
        try:
            src_path.rename(dst_path)
        except Exception as e:
            log.error(f"Error moving {src_path} to {dst_path}: {e}")

def refresh_repo_files(log:sh.log) -> None:
    
    try:
        db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise

    repo_folder_files = list_repo_files(f"{k.REPO_FOLDER}/20*")

    db_files = db_rfm.list_rfdb_files()
                
    # Compare sets
    files_not_in_rfdb = repo_folder_files - db_files
    files_not_in_repo = db_files - repo_folder_files

    log.entry(f"{len(repo_folder_files)} files in the repository:")
    log.entry(f"{len(db_files)} database entries related to repository files.")
    
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
            move_file_set(files_not_in_rfdb, f"{k.REPO_FOLDER}/{k.TMP_FOLDER}", log)
    else:
        log.entry("No file in the repository without correspondent entry in the RFDATA database.")

def refresh_tmp_files(log:sh.log) -> None:
    
    try:
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise
    
    # Process TMP folder and database
    repo_tmp_files = list_repo_files(f"{k.REPO_FOLDER}/{k.TMP_FOLDER}")
    
    db_tmp_files = db_bp.list_bpdb_files(status=k.BP_PENDING_TASK_STATUS)
    
    log.entry(f"{len(repo_tmp_files)} files in the repository TMP_FOLDER:")
    log.entry(f"{len(db_tmp_files)} database entries related to repository TMP_FOLDER files.")

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
        confirmation = input(f"Do you want to add these {len(files_to_be_processed)} files into the processing queue? (y/n): ")

        if confirmation.lower() == 'y':
            db_bp.add_task_from_file(files_to_be_processed)
    else:
        log.entry("No file in the TMP_FOLDER to be processed.")

def refresh_trash_files(log:sh.log) -> None:
    """ Refresh TRASH_FOLDER files and database entries

    Args:
        log (sh.log): Log object
    """
     
    try:
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise

    # Process trash folder and database
    repo_trash_files = list_repo_files(f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}")
    
    db_trash_files = db_bp.list_bpdb_files(status=k.BP_ERROR_TASK_STATUS)
    
    # Compare sets
    files_missing_in_trash = db_trash_files - repo_trash_files
    files_spilled_from_trash = repo_trash_files - db_trash_files

    log.entry(f"{len(repo_trash_files)} file(s) in the repository TRASH_FOLDER:")
    log.entry(f"{len(db_trash_files)} database entry(ies) related to repository TRASH_FOLDER files.")
    
    if len(files_missing_in_trash) > 0:
        log.entry(f"{len(files_missing_in_trash)} file(s) missing in the TRASH_FOLDER but in the database")
        confirmation = input("Do you want to remove database entries that are missing the correspondent file? (y/n): ")
    
        if confirmation.lower() == 'y':
            db_bp.remove_bpdb_files(files_to_remove=files_missing_in_trash)
    else:
        log.entry("No entry in the BPDATA database without correspondent file in the TRASH_FOLDER.")
    
    if len(files_spilled_from_trash) > 0:
        
        class handle_trash:
            def __init__(self, files:set, log:sh.log) -> None:
                self.files = files
                self.log = log

            def move(self) -> None:
                
                full_path = f"{k.REPO_FOLDER}/{k.TMP_FOLDER}"
                
                move_file_set(self.files, full_path, self.log)
                
                # change pathname to new path
                self.files = {(filename, full_path) for filename, path in self.files}
                    
                db_bp.add_task_from_file(self.files)
            
            def delete(self) -> None:
                for filename, filepath in self.files:
                    src_path = Path(f"{k.REPO_FOLDER}/{filepath}/{filename}")
                    try:
                        src_path.unlink()
                    except Exception as e:
                        log.error(f"Error deleting {src_path}: {e}")

        log.entry(f"{len(files_spilled_from_trash)} file(s) found in TRASH_FOLDER but not in the database")
        
        finish_cleaning = False
        
        handle_trash = handle_trash(files_spilled_from_trash, log)
        
        while not finish_cleaning:
            global_option = input("Do you want to re(P)rocess all, (D)elete all or (C)onfirm each entry? (p/d/c): ")
            match global_option.lower():
                case 'p':
                    confirmation = input(f"This will reprocess {len(files_spilled_from_trash)} file(s) from TRASH_FOLDER, moving then to TMP_FOLDER. Are you sure? (y/n): ")
                    if confirmation.lower() == 'y':
                        handle_trash.move()
                        finish_cleaning = True
                    
                case 'd':
                    confirmation = input(f"This delete {len(files_spilled_from_trash)} file(s) from TRASH_FOLDER. Are you sure? (y/n): ")
                    if confirmation.lower() == 'y':
                        handle_trash.delete()
                        finish_cleaning = True
                        
                case 'c':
                    for filename, path in files_spilled_from_trash:
                        handle_trash.files = {(filename, path)}
                        ask_again = True
                        task_summary = {"move":0,
                                        "delete":0,
                                        "skip":0}
                        while ask_again:
                            single_option = input(f"re(P)rocess, (D)elete or (S)kip file: '{path}/{filename}'? (p/d/s): ")

                            match single_option.lower():
                                case 'p':
                                    handle_trash.move()
                                    task_summary["move"] += 1
                                    files_spilled_from_trash.pop((filename, path))
                                    ask_again = False
                                case 'd':
                                    handle_trash.delete()
                                    task_summary["delete"] += 1
                                    files_spilled_from_trash.pop((filename, path))
                                    ask_again = False
                                case 's':
                                    log.entry(f"Skipping {path}/{filename}.")
                                    task_summary["skip"] += 1
                                    ask_again = False
                                case _:
                                    log.entry(f"Invalid option '{single_option}'. Try again.")
                        
                    log.entry(f"{task_summary['move']} file(s) moved, {task_summary['delete']} deleted, {task_summary['skip']} skipped.")
                    
                    confirmation = input("You may review again the skipped file(s). [F]inish trash processing or go [A]gain through the skipped files? (F/A): ")
                    if confirmation.lower() != 'a':
                        finish_cleaning = True

                case _:
                    log.entry(f"Invalid option {confirmation}. Try again.")
    else:
        log.entry("No file in the TRASH_FOLDER to be processed.")

def refresh_total_files(log:sh.log) -> None:
    
    try:
        db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise
    
    try:
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        raise
    
    host_id_list = db_bp.list_all_host_ids()
    
    for (host_id,equip_id,host_uid) in host_id_list:
        rfm_file_count = db_rfm.count_rfm_host_files(   equipment_id=equip_id,
                                                        volume=k.REPO_UID)
        
        (pending_processing, error_processing) = db_bp.count_bp_host_files(host_id=host_id)
        
        new_total = rfm_file_count + pending_processing + error_processing
        
        db_bp.update_host_status(   host_id=host_id,
                                    total_files=new_total,
                                    pending_processing=pending_processing,
                                    error_processing=error_processing)
        
        log.entry(f"Host '{host_uid}' status updated. Total files: {new_total}.")

def main():
    try:                # create a warning message object
        log = sh.log(target_screen=True, target_file=False)
    except Exception as e:
        print(f"Error creating log object: {e}")
        exit(1)

    log.entry("Starting server refresh.")
    
    refresh_repo_files(log)
    
    refresh_tmp_files(log)
    
    refresh_trash_files(log)

    refresh_total_files(log)
    
    log.entry("Finish server DB and files refreshing. You may need to manually perform additional tasks. Check the log for details.")
        
if __name__ == "__main__":
    main()