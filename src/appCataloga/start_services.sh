#!/bin/bash
echo "All appCataloga services will be started"

systemctl start appCataloga_host_check
echo "appCataloga_host_check started"

systemctl start appCataloga_file_bkp@0.service
echo "appCataloga_file_bkp@0.service started"

systemctl start appCataloga_file_bin_proces.service
echo "appCataloga_file_bin_proces.service started"

systemctl start appCataloga.service
echo "appCataloga.service started"
