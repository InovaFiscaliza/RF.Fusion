USE BPDATA;

UPDATE HOST SET NU_PENDING_BACKUP = NU_PENDING_BACKUP + 1 WHERE ID_HOST = '10';
        
INSERT INTO BKP_TASK (FK_HOST, DT_BKP_TASK, NO_HOST_ADDRESS,NO_HOST_PORT,NO_HOST_USER,NO_HOST_PASSWORD) VALUES ('10', NOW(), '192.168.10.33', '22', 'sshUser', 'sshuserpass')