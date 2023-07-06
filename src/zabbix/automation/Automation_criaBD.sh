
# Scripts que criam BD (rodam com menos frequência)

source /home/lx.svc.fi.sensores.pd/scripts_automacao/one_venv_to_rule_them_all/bin/activate

cd /home/lx.svc.fi.sensores.pd/scripts_automacao/criaBD_Zabbix
python3.9 main.py

cd /home/lx.svc.fi.sensores.pd/scripts_automacao/criaBD_SP_Planilhas
python3.9 main.py

cd /home/lx.svc.fi.sensores.pd/scripts_automacao/criaBD_SP_Locais
python3.9 main.py

cd /home/lx.svc.fi.sensores.pd/scripts_automacao/acessoCGI
python3.9 main.py

cd /home/lx.svc.fi.sensores.pd/scripts_automacao/verifica_BD
python3.9 main.py

