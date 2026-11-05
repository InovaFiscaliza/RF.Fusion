/* Operational host rekey. Required session variables:
   @old_host_id, @new_host_id, @expected_host_name, @apply_host_rekey (0 or 1).
   Stop the Summary consumer first. Default execution rolls back.
   No FK checks are disabled and no RFDATA identities are changed. */
DELIMITER //
BEGIN NOT ATOMIC
    DECLARE old_name VARCHAR(100);
    DECLARE busy_flag INT;
    DECLARE worker_pid INT;
    DECLARE finished BOOLEAN DEFAULT FALSE;
    DECLARE summary_table VARCHAR(64);
    DECLARE host_column VARCHAR(64);
    DECLARE worker_locked BOOLEAN DEFAULT FALSE;
    DECLARE snapshot_locked BOOLEAN DEFAULT FALSE;
    DECLARE refs CURSOR FOR
        SELECT c.TABLE_NAME, c.COLUMN_NAME
        FROM information_schema.COLUMNS c
        JOIN information_schema.TABLES t
          ON t.TABLE_SCHEMA=c.TABLE_SCHEMA AND t.TABLE_NAME=c.TABLE_NAME
        WHERE c.TABLE_SCHEMA='RFFUSION_SUMMARY'
          AND t.TABLE_TYPE='BASE TABLE'
          AND c.COLUMN_NAME IN ('ID_HOST','FK_HOST')
        ORDER BY c.TABLE_NAME, c.COLUMN_NAME;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=TRUE;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        IF snapshot_locked THEN
            DO RELEASE_LOCK('RFFUSION_SUMMARY_HOST_CURRENT_SNAPSHOT');
        END IF;
        IF worker_locked THEN
            DO RELEASE_LOCK('RFFUSION_SUMMARY_PY_WORKER');
        END IF;
        RESIGNAL;
    END;

    IF @old_host_id IS NULL OR @new_host_id IS NULL
       OR @old_host_id <= 0 OR @new_host_id <= 0
       OR @old_host_id = @new_host_id OR @expected_host_name IS NULL
       OR COALESCE(@apply_host_rekey,0) NOT IN (0,1) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid host rekey parameters';
    END IF;
    SET worker_locked=GET_LOCK('RFFUSION_SUMMARY_PY_WORKER',0);
    IF NOT COALESCE(worker_locked,FALSE) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Stop Summary consumer before rekey';
    END IF;
    SET snapshot_locked=GET_LOCK('RFFUSION_SUMMARY_HOST_CURRENT_SNAPSHOT',5);
    IF NOT COALESCE(snapshot_locked,FALSE) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Snapshot lock unavailable';
    END IF;
    SET SESSION innodb_lock_wait_timeout=5;
    START TRANSACTION;
    SELECT NA_HOST_NAME,IS_BUSY,NU_PID INTO old_name,busy_flag,worker_pid
      FROM BPDATA.HOST WHERE ID_HOST=@old_host_id FOR UPDATE;
    IF finished OR NOT (BINARY old_name <=> BINARY @expected_host_name)
       OR COALESCE(busy_flag,0) <> 0 OR COALESCE(worker_pid,0) <> 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Source missing, name mismatch or host busy';
    END IF;
    IF EXISTS (SELECT 1 FROM BPDATA.HOST WHERE ID_HOST=@new_host_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Destination host already exists';
    END IF;
    IF EXISTS (SELECT 1 FROM BPDATA.HOST_TASK
               WHERE FK_HOST=@old_host_id AND NU_STATUS=2)
       OR EXISTS (SELECT 1 FROM BPDATA.FILE_TASK
                  WHERE FK_HOST=@old_host_id AND NU_STATUS=2) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Source has running tasks';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.COLUMNS
               WHERE TABLE_SCHEMA='RFDATA' AND COLUMN_NAME IN ('ID_HOST','FK_HOST')) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unexpected RFDATA host reference; review required';
    END IF;

    /* Clone every host field without embedding credentials in the script. */
    SELECT GROUP_CONCAT(CONCAT('`',COLUMN_NAME,'`') ORDER BY ORDINAL_POSITION),
           GROUP_CONCAT(IF(COLUMN_NAME='ID_HOST','@new_host_id',CONCAT('`',COLUMN_NAME,'`'))
                        ORDER BY ORDINAL_POSITION)
      INTO @host_columns,@host_values
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA='BPDATA' AND TABLE_NAME='HOST';
    SET @rekey_sql=CONCAT('INSERT INTO BPDATA.HOST (',@host_columns,') SELECT ',
                         @host_values,' FROM BPDATA.HOST WHERE ID_HOST=@old_host_id');
    PREPARE rekey_stmt FROM @rekey_sql;
    EXECUTE rekey_stmt;
    DEALLOCATE PREPARE rekey_stmt;

    UPDATE BPDATA.HOST_TASK SET FK_HOST=@new_host_id WHERE FK_HOST=@old_host_id;
    SELECT 'HOST_TASK' AS object_name, ROW_COUNT() AS moved_rows;
    UPDATE BPDATA.FILE_TASK SET FK_HOST=@new_host_id WHERE FK_HOST=@old_host_id;
    SELECT 'FILE_TASK' AS object_name, ROW_COUNT() AS moved_rows;
    UPDATE BPDATA.FILE_TASK_HISTORY SET FK_HOST=@new_host_id WHERE FK_HOST=@old_host_id;
    SELECT 'FILE_TASK_HISTORY' AS object_name, ROW_COUNT() AS moved_rows;

    SET finished=FALSE;
    OPEN refs;
    summary_refs: LOOP
        FETCH refs INTO summary_table,host_column;
        IF finished THEN LEAVE summary_refs; END IF;
        SET @rekey_sql=CONCAT('UPDATE RFFUSION_SUMMARY.`',summary_table,'` SET `',
            host_column,'`=@new_host_id WHERE `',host_column,'`=@old_host_id');
        PREPARE rekey_stmt FROM @rekey_sql;
        EXECUTE rekey_stmt;
        SELECT summary_table AS object_name, ROW_COUNT() AS moved_rows;
        DEALLOCATE PREPARE rekey_stmt;
    END LOOP;
    CLOSE refs;
    DELETE FROM BPDATA.HOST WHERE ID_HOST=@old_host_id;

    INSERT INTO RFFUSION_SUMMARY.SUMMARY_OUTBOX
        (NA_SCOPE_TYPE,NA_SCOPE_VALUE,NA_SOURCE_HANDLER,NA_REASON)
    VALUES ('host',CAST(@old_host_id AS CHAR),'host_identity_rekey','host_identity_rekey'),
           ('host',CAST(@new_host_id AS CHAR),'host_identity_rekey','host_identity_rekey')
    ON DUPLICATE KEY UPDATE NA_REASON=VALUES(NA_REASON),DT_CREATED_AT=UTC_TIMESTAMP();

    IF COALESCE(@apply_host_rekey,0)=1 THEN
        COMMIT;
        SELECT 'committed' AS host_rekey_result;
    ELSE
        ROLLBACK;
        SELECT 'validated_and_rolled_back' AS host_rekey_result;
    END IF;
    DO RELEASE_LOCK('RFFUSION_SUMMARY_HOST_CURRENT_SNAPSHOT');
    DO RELEASE_LOCK('RFFUSION_SUMMARY_PY_WORKER');
END//
DELIMITER ;
