\set ON_ERROR_STOP on
BEGIN;
INSERT INTO "BPDATA"."HOST" ("ID_HOST") VALUES (-2147480000);
DO $unique$
DECLARE target text; path text;
BEGIN
    SELECT string_agg(md5(i::text),'') INTO path FROM generate_series(1,90) i;
    FOREACH target IN ARRAY ARRAY['FILE_TASK','FILE_TASK_HISTORY'] LOOP
        EXECUTE format('INSERT INTO "BPDATA".%I ("FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") VALUES ($1,$2,$3)',target)
        USING -2147480000,path,repeat('x',500);
        BEGIN
            EXECUTE format('INSERT INTO "BPDATA".%I ("FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") VALUES ($1,$2,$3)',target)
            USING -2147480000,path,repeat('x',500);
            RAISE EXCEPTION 'Duplicate full identity accepted';
        EXCEPTION WHEN unique_violation THEN NULL; END;
        EXECUTE format('INSERT INTO "BPDATA".%I ("FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") VALUES ($1,$2,$3)',target)
        USING -2147480000,path || 'different',repeat('x',500);
        EXECUTE format('INSERT INTO "BPDATA".%I ("FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") VALUES ($1,NULL,$2),($1,NULL,$2)',target)
        USING -2147480000,repeat('x',500);
    END LOOP;
END;
$unique$;
ROLLBACK;
