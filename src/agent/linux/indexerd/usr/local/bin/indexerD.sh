#!/bin/sh

# Load config file to get the constants that define the files and foders used by the script
[ -e /etc/node/indexerD.cfg ] && . /etc/node/indexerD.cfg || {
    logger "Sentinela Error: Could load indexerD script configuration"
    exit
}

# test if work folder does not exist and if true, create it. If folder can not be created, log error and exit
if [ ! -d "$SENTINELA_FOLDER" ]; then
    mkdir "$SENTINELA_FOLDER" || {
        logger "Sentinela Error: Could not create $SENTINELA_FOLDER"
        exit
    }
fi

# test if halt cookie exist and exit if so. If it does not exist, try to created it. If fail, log error and exit.
if [ -e "$HALT_FLAG" ]; then
    logger "Sentinela Warning: $HALT_FLAG active"
    exit 0
else
    touch "$HALT_FLAG" || {
        logger "Sentinela Error: Could not create $HALT_FLAG"
        exit
    }
fi

# check temp file exist and remove it. If fails, log error, remove the halt flag and exit
if [ -e "$TEMP_CHANGED" ]; then
    rm -f "$TEMP_CHANGED" || {
        logger "Sentinela Error: Could remove $WORK_FOLDER"
        rm -f "$HALT_FLAG"
        exit
    }
fi

# check if there is no cookie for the last time the script was successful and, in that case, create one setting reference date to Unix epoch
if [ ! -e "$LAST_BACKUP_FLAG" ]; then
    touch --date "1970-01-01" "$LAST_BACKUP_FLAG" || {
        logger "Sentinela Error: Could not create $LAST_BACKUP_FLAG"
        exit
    }
    logger "Sentinela Warning: $LAST_BACKUP_FLAG not available. Starting backup from Unix Epoch"
fi

# check if there is a pending list of files to be backup
if [ ! -e "$DUE_BACKUP" ]; then
    touch "$DUE_BACKUP" || {
        logger "Sentinela Error: Could not create $DUE_BACKUP"
        exit
    }
    logger "Sentinela Warning: $DUE_BACKUP not available. Starting new backup"
fi

# store a timestamp for the start time with precision of one second, truncating information to one second.
timestamp=$(date '+%Y%m%d%H%M.%S')

# find files in the local repository that are newer than the last backup cookie saving the result. Use nice command to reduce priority and avoid system lock
nice -n 15 find "$LOCAL_REPO" -type f -newer "$LAST_BACKUP_FLAG" -printf "%h\0/%f\0\n" >"$TEMP_CHANGED"

# merge listed files with existing list of files due for backup, removing duplicates
nice -n 15 sort "$DUE_BACKUP" "$TEMP_CHANGED" | uniq

# remove temp file
rm -f "$TEMP_CHANGED" || {
    logger "Sentinela Error: Could remove $WORK_FOLDER"
    rm -f "$HALT_FLAG"
    exit
}

# update cookies timestamps with time previous to the file list. This will ensure that files that may have been created or modified during the indexing process are considered
touch -t "$timestamp" "$LAST_BACKUP_FLAG"

# remove the cookie flag that may halt a concurrent process
rm -f "$HALT_FLAG" || {
    logger "Sentinela Error: Could remove $HALT_FLAG"
    exit
}

exit 0
