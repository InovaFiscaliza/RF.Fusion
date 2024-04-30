#!/bin/bash
echo "All appCataloga services will be started"

systemctl start run_host_task.service
echo "run_host_task.service started"

systemctl start run_file_backup_task@0.service 
echo "run_file_backup_task@0.service started"

systemctl start run_file_bin_processing.service
echo "run_file_bin_processing.service started"

systemctl start appCataloga.service
echo "appCataloga.service started"