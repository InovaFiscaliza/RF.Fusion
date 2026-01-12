#!/bin/sh

# Load config file to get the constants that define the files and foders used by the script
[ -e /etc/node/indexerD.cfg ] && . /etc/node/indexerD.cfg || {
    logger "indexerD Error: Could load indexerD script configuration"
    exit
}

# test if work folder does not exist and if true, create it. If folder can not be created, log error and exit
if [ ! -d "$INDEXERD_FOLDER" ]; then
    mkdir "$INDEXERD_FOLDER" || {
        logger "indexerD Error: Could not create $INDEXERD_FOLDER"
        exit
    }
fi

# test if halt cookie exist
if [ -e "$HALT_FLAG" ]; then
    # if the halt cookie exist test if it is older then the time defined in the configuration file
    if [ "$(find "$HALT_FLAG" -mmin +$HALT_TIMEOUT)" ]; then
        # if halt cookie is older than the time_out, remove it.
        rm -f "$HALT_FLAG" || {
            # If fails, log error and exit
            logger "indexerD Error: Could not remove $HALT_FLAG"
            exit
        }
    else
        # if the halt cookie exist and is newer than the time defined in the configuration file, log warning and exit
        logger "indexerD Warning: $HALT_FLAG active"
        exit 0    
    fi
else
    # if the halt cookie does not exist, create it. If fails, log error and exit
    touch "$HALT_FLAG" || {
        logger "indexerD Error: Could not create $HALT_FLAG"
        exit
    }
fi

# check temp file exist and remove it. If fails, log error, remove the halt flag and exit
if [ -e "$TEMP_CHANGED" ]; then
    rm -f "$TEMP_CHANGED" || {
        logger "indexerD Error: Could remove $TEMP_CHANGED"
        rm -f "$HALT_FLAG"
        exit
    }
fi

# check if there is no cookie for the last time the script was successful and, in that case, create one setting reference date to Unix epoch
if [ ! -e "$LAST_FILE_SEARCH_FLAG" ]; then
    touch --date "1970-01-01" "$LAST_FILE_SEARCH_FLAG" || {
        logger "indexerD Error: Could not create $LAST_FILE_SEARCH_FLAG"
        exit
    }
    logger "indexerD Warning: $LAST_FILE_SEARCH_FLAG not available. Starting backup from Unix Epoch"
fi

# check if there is a pending list of files to be backup
if [ ! -e "$DUE_BACKUP" ]; then
    touch "$DUE_BACKUP" || {
        logger "indexerD Error: Could not create $DUE_BACKUP"
        exit
    }
    logger "indexerD Warning: $DUE_BACKUP not available. Starting new backup"
fi

# store a timestamp for the start time with precision of one second, truncating information to one second.
timestamp=$(date '+%Y%m%d%H%M.%S')

# find files in the local repository that are newer than the last backup cookie saving the result. Use nice command to reduce priority and avoid system lock
nice -n 15 find "$LOCAL_REPO" -type f -newer "$LAST_FILE_SEARCH_FLAG" -printf "%h\0/%f\0\n" >"$TEMP_CHANGED"

# create a sorted list of files, without duplicates, merging $TEMP_CHANGED and $DUE_BACKUP files
nice -n 15 sort -o "$DUE_BACKUP" -u "$TEMP_CHANGED" "$DUE_BACKUP"

# print the number of files in TEMP_CHANGED and DUE_BACKUP
logger "indexerD Info: $(wc -l <"$TEMP_CHANGED") files changed since last check"
logger "indexerD Info: $(wc -l <"$DUE_BACKUP") files due for backup"

# remove temp file
rm -f "$TEMP_CHANGED" || {
    logger "indexerD Error: Could remove $TEMP_CHANGED"
    rm -f "$HALT_FLAG"
    exit
}

# update cookies timestamps with time previous to the file list. This will ensure that files that may have been created or modified during the indexing process are considered
touch -t "$timestamp" "$LAST_FILE_SEARCH_FLAG"

# open BACKUP_DONE file sort the list of files to be backup and remove duplicates. Backup process may create duplicate entries in the list
nice -n 15 sort -o "$BACKUP_DONE" -u "$BACKUP_DONE"

# remove the cookie flag that may halt a concurrent process
rm -f "$HALT_FLAG" || {
    logger "indexerD Error: Could remove $HALT_FLAG"
    exit
}

exit 0
