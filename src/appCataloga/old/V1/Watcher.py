#! /home/lobao.lx/miniconda3/envs/rfpye/bin/python3

# # File processor
# This script perform the following tasks
# - Use whatdog to monitor folder
# -
#TODO: Change to pyinotify to properly handle linux events


# Import libraries for file processing
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
import time
import os
import shutil
import errno

# Import file with constants used in this code that are relevant to the operation
import constants as k
#import CRFSbinHandler as cbh
#import dbHandler as dbh

import logging
import sys

# Class to handle file change events from the OS
class FileHandler(RegexMatchingEventHandler):
# TODO: Improve error handling for file errors

    def __init__(self):
        super().__init__(k.FILE_TO_PROCESS_REGEX)

    def on_created(self, event):
        # print output if in verbose mode
        if k.VERBOSE:
            nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
            print(f'{nowString}: Creation event detected associated with the file {event.src_path}')

        self.settleTime(event)

    def on_modified(self, event):
        # print output if in verbose mode
        if k.VERBOSE:
            nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
            print(f'{nowString}: Modification event detected associated with the file {event.src_path}')

#TODO: Check database for modication
        self.settleTime(event)

#TODO: Process modification events. Simple use creates loop in windows
        #self.settleTime(event)

    def settleTime(self, event):
        
        #set initial vale for file size in order to hold the while loop
        file_size = -1
        
        # try testing if file changes size within a queue check time period in order to allow for slow transfers
        try:
            while file_size != os.path.getsize(event.src_path):
                file_size = os.path.getsize(event.src_path)
                time.sleep(k.QUEUE_CHECK_PERIOD)
        # if test fails, test if the file was deleted
        except:
            if os.path.exists(event.src_path):
                if k.VERBOSE: 
                    nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
                    print(f'{nowString}: Ignored file {event.src_path}')
        # if test succeeds, process the file
        else:
            self.process(event)

    def process(self, event):
        # print output if in verbose mode
        if k.VERBOSE: nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())

        # get file extention string in order to test if the file is of the correct type
        fileRoot, fileExt = os.path.splitext(os.path.basename(event.src_path))

        if (fileExt == k.FILE_EXTENSION_TO_WATCH):
            # if file has the BIN extension, try to process it
            
            # handle each possible target to save the metadata
            if (k.METADATA_TARGET_DB):
                self.storeMetadataDB(event)
            if (k.METADATA_TARGET_FILE):
                self.storeMetadataFile(event,fileRoot)

        else:
            # if file has other extension, do nothing
            if k.VERBOSE: print(f'{nowString}: Skipping {event.src_path}')
#TODO: Include special treatment to remove files in the source.

    def storeMetadataDB(self,event):
# TODO: Implement error handling for the initial database access
        # test if file exists in the database
        dbHandle = dbh.databaseHandler()
        filename = os.path.basename(event.src_path)
        fileExistInDB = dbHandle.dbFileSearch(filename)

        if not fileExistInDB:
            if k.VERBOSE:
                nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
                print(f'{nowString}: Processing {filename}')

            # get metadata from bin file
            #BinFile = cbh.BinFileHandler(event.src_path)
            #BinFile.doReveseGeocode()

            # update database with metadata
# TODO: Implement error handling to rollback partial database updates                
            #dbHandle.updateDatabase(BinFile)

            #self.fileMove(event, BinFileMetadata.newFilePath, BinFileMetadata.FileName)

            if k.VERBOSE:
                nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
                print(f"{nowString} - Finished processing {filename} to the repository")
                print(f"{nowString} - Wainting for files with '{k.FILE_EXTENSION_TO_WATCH}' extension in the '{k.FOLDER_TO_WATCH}' folder\n               Type ^C to stop")
        else:
            # If file exist, do nothing.         
            if k.VERBOSE:
                nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
                print(f'{nowString}: {filename} already exist in the database with PK {fileExistInDB}')
#TODO: Create method to delete file considerenig the effect in the location database and other tables. Proposed method is to create a trash folder and monitor it to delete files in the database. This could be handled by another process.

    def storeMetadataFile(self,event,fileRoot):

        BinFile = cbh.BinFileHandler(event.src_path)
        BinFile.doReveseGeocode()
        BinFile.exportMetadata(exportFilename=k.CSV_OUTPUT_FOLDER+fileRoot+k.CSV_EXTENSION)

#TODO: File Move operation suspended due to rsync issue. Study alternative of moving files at origin or implement different sync mechanism based on Ansible
#       self.fileMove(event, BinFile.newFilePath, BinFile.FileName)

        if k.VERBOSE:
            nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
            print(f"{nowString} - Finished processing {BinFile.metadata['File_Name']} in the repository")
            print(f"{nowString} - Wainting for files with '{k.FILE_EXTENSION_TO_WATCH}' extension in the '{k.FOLDER_TO_WATCH}' folder\n               Type ^C to stop")

    def fileMove(self, event, newpathName, newFileName):
    
#TODO: Revise error handling for file operations
        # try to move the file. Most of the time the target folder will exist
        try:
            shutil.move(event.src_path, k.TARGET_ROOT_PATH+newpathName+newFileName)
        except OSError as e:
            # if the error is due to a missig directory
            if e.errno in (errno.ENOENT, errno.ENOTDIR):
                # try creating the directory
                try:
                    os.makedirs(k.TARGET_ROOT_PATH+newpathName)
                except OSError:
                    raise ValueError(f"Can't create folder {newpathName}")
                # if folder creation is successfully, copy the file
                shutil.move(event.src_path, k.TARGET_ROOT_PATH+newpathName+newFileName)
            else:
                raise ValueError(f"Undefined file operation error when moving {newFileName} to {k.TARGET_ROOT_PATH+newpathName}")


class WatchDog:
    def __init__(self, src_path):
        self.__src_path = src_path
        self.__event_handler = FileHandler()
        self.__event_observer = Observer()

    def run(self):
        self.start()

        # counter for old file verification. Used to avoid missing files in case anything arrives while process is interrupted
        tickCounter = 0
        #check once a second for a keyboard interrupt
        try:
            while True:
                time.sleep(k.PERIOD_FOR_STOP_CHECK)

                tickCounter += 1
                if tickCounter == k.PERIOD_FOR_OLD_FILES_CHECK:
                    self.touchOldFile()
                    tickCounter = 0

        except KeyboardInterrupt:
            self.stop()

    def start(self):
        self.__schedule()
        self.__event_observer.start()

        nowString = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
        if k.VERBOSE: print(f'{nowString} - Initializing watchdog. Waiting for files with "{k.FILE_EXTENSION_TO_WATCH}" extension in the "{k.FOLDER_TO_WATCH}" folder\n               Type ^C to stop')

    def stop(self):
        self.__event_observer.stop()
        self.__event_observer.join()

    def __schedule(self):
        self.__event_observer.schedule(
            self.__event_handler,
            self.__src_path,
            recursive=True
        )

    def touchOldFile(self):
#TODO: list files older than the recap time and process then to avoid missing anything
        #get files with os.listdir() 
        #get modification time with os.path.getmtime(path)
        #check time delta from modification to now
        #if greater than k.PERIOD_FOR_OLD_FILES_CHECK change file such as it is captured by the watchdog
        # https://nitratine.net/blog/post/change-file-modification-time-in-python/
        # import time
        # import datetime
        # date = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
        # modTime = time.mktime(date.timetuple())
        # os.utime(fileLocation, (modTime, modTime))

        # no return value needed
        return True

def _main():
#TODO: Test if database is fresh and insert null value for location algorithm. If database is not fresh, check is null location exists and remove it.
    # start logging service
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # run the watchdog
    WatchDog(k.FOLDER_TO_WATCH).run()

if __name__ == '__main__':
    _main()
