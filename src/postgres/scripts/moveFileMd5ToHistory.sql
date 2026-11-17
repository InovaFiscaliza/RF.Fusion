-- Move file MD5 columns from the live queue to the history of an existing BPDATA.
BEGIN;
LOCK TABLE "BPDATA"."FILE_TASK" IN ACCESS EXCLUSIVE MODE;
LOCK TABLE "BPDATA"."FILE_TASK_HISTORY" IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    legacy_column text;
    has_values boolean;
BEGIN
    -- The legacy queue columns were never written by the runtime. Refuse to drop
    -- them if any value exists, so stored checksums are reviewed before removal.
    -- Dynamic SQL keeps the script repeatable after the columns are gone.
    FOREACH legacy_column IN ARRAY ARRAY['NU_HOST_FILE_MD5', 'NA_SERVER_FILE_MD5'] LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'BPDATA' AND table_name = 'FILE_TASK'
              AND column_name = legacy_column
        );
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM "BPDATA"."FILE_TASK" WHERE %I IS NOT NULL)',
            legacy_column
        ) INTO has_values;
        IF has_values THEN
            RAISE EXCEPTION 'FILE_TASK.% has values; review them before removing the column.',
                legacy_column;
        END IF;
    END LOOP;
END;
$$;

ALTER TABLE "BPDATA"."FILE_TASK_HISTORY"
    ADD COLUMN IF NOT EXISTS "NU_HOST_FILE_MD5" varchar(32) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS "NU_SERVER_FILE_MD5" varchar(32) DEFAULT NULL;

ALTER TABLE "BPDATA"."FILE_TASK"
    DROP COLUMN IF EXISTS "NU_HOST_FILE_MD5",
    DROP COLUMN IF EXISTS "NA_SERVER_FILE_MD5";
COMMIT;
