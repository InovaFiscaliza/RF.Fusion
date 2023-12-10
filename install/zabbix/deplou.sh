#!/bin/bash

# try to download files from github if it fails, exit
# if it works, overwrite the files in the zabbix folder

#
if [ ! -f /usr/lib/zabbix/externalscripts/queryappColeta.py ]; then
    rm queryappColeta.py
fi

rm queryappColeta.py
rm queryCataloga.py
rm queryDigitizer.py
rm queryLoggerUDP.py
rm rfFusionLib.py
rm defaultConfig.py

wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryappColeta.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryCataloga.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryDigitizer.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryLoggerUDP.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/rfFusionLib.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/defaultConfig.py

dos2unix queryappColeta.py
dos2unix queryCataloga.py
dos2unix queryDigitizer.py
dos2unix queryLoggerUDP.py
dos2unix rfFusionLib.py
dos2unix defaultConfig.py

chmod 750 queryappColeta.py
chmod 750 queryCataloga.py
chmod 750 queryDigitizer.py
chmod 750 queryLoggerUDP.py
chmod 750 rfFusionLib.py
chmod 750 defaultConfig.py
