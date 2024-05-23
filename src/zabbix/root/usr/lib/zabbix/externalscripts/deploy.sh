# Quick deploy script for RF.Fusion Zabbix external scripts

rm queryappColeta.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryappColeta.py
dos2unix queryappColeta.py

rm queryCataloga.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryCataloga.py
dos2unix queryCataloga.py

rm queryDigitizer.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryDigitizer.py
dos2unix queryDigitizer.py

rm queryLoggerUDP.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/queryLoggerUDP.py
dos2unix queryLoggerUDP.py

rm rfFusionLib.py
wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/z_shared.py
dos2unix z_shared.py

# rm defaultConfig.py
# wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/src/zabbix/root/usr/lib/zabbix/externalscripts/defaultConfig.py
# dos2unix defaultConfig.py

chmod 750 *.py
chown zabbix *.py
chgrp zabbix *.py
