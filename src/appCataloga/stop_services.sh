#!/bin/bash
echo "All appCataloga services will be stopped"

systemctl stop run_host_task.service
echo "run_host_task.service stopped"

# Create a list with all instances of run_file_backup_task@* services
# and stop them
for i in $(systemctl list-units --full --all | grep run_file_backup_task@ | awk '{print $1}'); do
    systemctl stop $i
    echo "$i stopped"
done

systemctl stop run_file_bin_processing.service
echo "run_file_bin_processing.service stopped"

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

