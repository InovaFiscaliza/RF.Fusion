#!/bin/bash

# Install an upgrade pack

TYPE=Anatel
VER=3.2.0-A1
UPGRADE_DIR=/etc/node/upgrade

log() {
    DATE=$(date +'%b %d %X')
    echo "$DATE $HOSTNAME upgrade: $1" >>$UPGRADE_DIR/upgrade."$VER".log
    echo "$DATE $HOSTNAME upgrade: $1"
}

# If we're not running this version DO NOT upgrade
REQD_UPGRADE_VER="3.2.0"

# Check /etc/upgrades.info file to see if the latest upgrade
# matches $REQD_UPGRADE_VER
if [ -e /etc/upgrades.info ]; then
    LAST_UPGRADE=$(tail -1 /etc/upgrades.info)
    CAN_DO_UPGRADE=0
    for RVER in $REQD_UPGRADE_VER; do
        if [ "$RVER" = "$LAST_UPGRADE" ]; then
            CAN_DO_UPGRADE=1
        fi
    done

    if [ $CAN_DO_UPGRADE -eq 0 ]; then
        log "An appropriate software version to upgrade from ($REQD_UPGRADE_VER) was not found"
        log "The upgrade will not proceed. Terminating"
        exit 0
    fi
else
    # no /etc/upgrades.info - exit here or we might break something
    log "Could not determine the current software version"
    log "The upgrade will not proceed. Terminating"
    exit 0
fi

# Ensures that watchd is running to avoid system boot during the upgrade
if [ -z "$(pidof watchd)" ]; then
    /usr/local/bin/watchd &
fi

# Set cwd to $UPGRADE_DIR
cd $UPGRADE_DIR || {
    log "Could not change to upgrade directory. Terminating"
    exit
}

uTYPE=$(echo $TYPE | tr 'f' 'F')
# Announce the upgrade type, version etc
log "$uTYPE Upgrade Tool $VER"
log ""
log "Unit: $HOSTNAME"
log ""
log "Time: $(date +'%X')  Date: $(date +'%x')"
log ""

# Check the packages for consistency (MD5SUM)
UPGRADE_FILES=$(ls *.deb)

log "Checking MD5SUMs of upgrade packages"
log ""

for file in $UPGRADE_FILES; do
    log "Checking $file"
    md5sum --check $file.md5
    if [ $? = 0 ]; then
        log "$file is OK"
    else
        log "$file appears to be corrupt"
        log "Aborting upgrade"
        exit 0
    fi
    log "Finished checking $file"
done
log "Finished checking upgrade packages"

# If we got this far the upgrade files look good
# Now we can proceed with the upgrade

# Upgrade packages
log ""
log "Processing the updates"
log ""

for file in $UPGRADE_FILES; do
    ufile=$(echo $file | awk -F'_' '{print $1}')
    log "Upgrading $ufile"
    dpkg -i --force-depends --force-overwrite --force-confmiss --force-confdef --force-confold $file
    [ $? = 0 ] && log "Upgrade of $ufile succeeded" || log "Upgrade of $ufile failed"
done

echo $TYPE $VER >>/etc/upgrades.info

log ""
log "Upgrade complete"

echo "upgrade.sh Terminated"

exit 0
