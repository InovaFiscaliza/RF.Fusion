#!/bin/bash

# Install an upgrade pack

TYPE=Anatel
VER=3.2.0-A1
UPGRADE_DIR=/etc/node/upgrade

log() {
    date=$(date +'%b %d %X')
    echo "$date $HOSTNAME upgrade: $1" >>$UPGRADE_DIR/upgrade."$VER".log
    echo "$date $HOSTNAME upgrade: $1"
}

# define the required upgrade version using a regex
REQ_VERSION="[3-9]\.[2-9]\.[0-9]"

# Check /etc/upgrades.info file to see if the latest upgrade
if [ -e /etc/upgrades.info ]; then
    last_upgrade=$(tail -1 /etc/upgrades.info)
    if ! [[ "$last_upgrade" =~ $REQ_VERSION ]]; then
        log "An appropriate software version to upgrade from ($REQ_VERSION) was not found"
        log "The upgrade will not proceed. Terminating"
        exit 0
    fi
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
