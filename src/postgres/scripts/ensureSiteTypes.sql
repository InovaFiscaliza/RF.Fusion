-- Repair canonical site types without reclassifying sites or replacing labels.
BEGIN;
LOCK TABLE "RFDATA"."DIM_SITE_TYPE" IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    type_sequence regclass;
    sequence_value bigint;
BEGIN
    IF EXISTS (
        SELECT 1 FROM "RFDATA"."DIM_SITE_TYPE" AS stored
        JOIN (VALUES (1, 'Fixed'), (2, 'Mobile')) AS expected(id, label)
          ON stored."ID_TYPE" = expected.id
        WHERE btrim(stored."NA_TYPE", chr(39)) IS DISTINCT FROM expected.label
    ) THEN
        RAISE EXCEPTION 'DIM_SITE_TYPE IDs 1 and 2 must mean Fixed and Mobile; review the catalog before repair.';
    END IF;

    INSERT INTO "RFDATA"."DIM_SITE_TYPE" ("ID_TYPE", "NA_TYPE")
    VALUES (1, 'Fixed'), (2, 'Mobile') ON CONFLICT ("ID_TYPE") DO NOTHING;

    -- Keep generated IDs above stored rows without rewinding the sequence.
    type_sequence := pg_get_serial_sequence('"RFDATA"."DIM_SITE_TYPE"', 'ID_TYPE');
    EXECUTE format('SELECT last_value FROM %s', type_sequence) INTO sequence_value;
    PERFORM setval(type_sequence, GREATEST(sequence_value,
        (SELECT max("ID_TYPE") FROM "RFDATA"."DIM_SITE_TYPE")), true);
END;
$$;
COMMIT;
