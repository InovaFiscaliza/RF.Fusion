#!/bin/bash
echo "All appCataloga services will be stopped"

systemctl stop appCataloga_host_check
echo "appCataloga_host_check stopped"

# Create a list with all instances of appCataloga_file_bkp@* services
# and stop them
for i in $(systemctl list-units --full --all | grep appCataloga_file_bkp@ | awk '{print $1}'); do
    systemctl stop "$i"
    echo "$i stopped"
done

systemctl stop appCataloga_file_bin_proces.service
echo "appCataloga_file_bin_proces.service stopped"

systemctl stop appCataloga_pub_metadata.service
echo "appCataloga_pub_metadata.service stopped"

systemctl stop appCataloga.service
echo "appCataloga.service stopped"

read -p "Remove log file? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Log file not removed"
else
    rm -f /var/log/appCataloga.log
    echo "Log file removed"
fi
