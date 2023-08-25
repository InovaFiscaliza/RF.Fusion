#!/usr/bin/env python
"""
Access the backup list from BKPDATA database and starts the backup process threads.
    
    Usage:
        runBackup 
            
    Returns:
        (json) =  { 'Total Files': (int),
                    'Files to backup': (int),
                    'Last Backup data': (str)
                    'Days since last backup': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
        
        
"""


"""
- Loop infinito de gestão
  - Consultar BD os parâmetros de limite e tempo de espera
  - Consultar BD o quantitativo de backups pendentes
  - Consultar BD o quantitativo de processos de catalogação pendentes
  - Se processos de backup em execução < limite_bkp, disparar novo processo
  - Se processos de catalogação em execução < limite_proc, disparar novo processo
  - Aguardar tempo de espera
  
- processo de backup
  - recebe host_add, user e pass na chamada
  - realiza backup
  - atualiza BD de sumarização para o host
  - atualiza BD lista de catalogações pendentes

"""