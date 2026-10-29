\set ON_ERROR_STOP on
BEGIN;
DO $rules$
DECLARE allocated bigint; other bigint; saved timestamp;
BEGIN
    CASE current_database()
    WHEN 'BPDATA' THEN
        INSERT INTO "BPDATA"."HOST" ("ID_HOST") VALUES (-2147480000);
        INSERT INTO "BPDATA"."HOST_TASK" ("ID_HOST_TASK","FK_HOST","NU_STATUS","FILTER")
        VALUES (NULL,-2147480000,-1,' { "value": 1, "value": 2 } ') RETURNING "ID_HOST_TASK" INTO allocated;
        INSERT INTO "BPDATA"."HOST_TASK" ("ID_HOST_TASK","FK_HOST") VALUES (0,-2147480000)
        RETURNING "ID_HOST_TASK" INTO other;
        IF allocated=other OR allocated<=0 OR other<=0 THEN RAISE EXCEPTION 'NULL/zero ID allocation'; END IF;
        BEGIN
            INSERT INTO "BPDATA"."HOST_TASK" ("NU_STATUS") VALUES (-129);
            RAISE EXCEPTION 'tinyint lower bound not enforced';
        EXCEPTION WHEN check_violation THEN NULL; END;
        BEGIN
            INSERT INTO "BPDATA"."HOST_TASK" ("FK_HOST") VALUES (-2147480001);
            RAISE EXCEPTION 'Missing FK accepted';
        EXCEPTION WHEN foreign_key_violation THEN NULL; END;
        BEGIN
            INSERT INTO "BPDATA"."HOST_TASK" ("FILTER") VALUES ('invalid JSON');
            RAISE EXCEPTION 'Invalid JSON accepted';
        EXCEPTION WHEN invalid_text_representation THEN NULL; END;
        BEGIN
            INSERT INTO "BPDATA"."HOST" ("ID_HOST","NA_LAST_OFFLINE_DESCRIPTION") VALUES (-2147480001,repeat('a',65536));
            RAISE EXCEPTION 'TEXT limit not enforced';
        EXCEPTION WHEN check_violation THEN NULL; END;
    WHEN 'RFDATA' THEN
        INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE" ("GEO_POINT") VALUES (public.ST_GeomFromText('POINT(1 2)',4326))
        RETURNING "ID_SITE" INTO allocated;
        IF (SELECT public.ST_SRID("GEO_POINT") FROM "RFDATA"."DIM_SPECTRUM_SITE" WHERE "ID_SITE"=allocated)<>4326
        THEN RAISE EXCEPTION 'SRID unexpectedly changed'; END IF;
        BEGIN
            INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE" ("GEO_POINT") VALUES (public.ST_GeomFromText('LINESTRING(0 0,1 1)'));
            RAISE EXCEPTION 'Non-point accepted';
        EXCEPTION WHEN invalid_parameter_value THEN NULL; END;
        INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE" ("GEO_POINT","GEOGRAPHIC_PATH")
        VALUES (public.ST_GeomFromText('POINT(3 4)'),decode('00ff','hex'));
    WHEN 'WEBFUSION' THEN
        INSERT INTO "WEBFUSION"."USERS" ("NA_USER_EMAIL","DT_UPDATED_AT") VALUES ('fixture@example.invalid','2000-01-01')
        RETURNING "ID_USER" INTO allocated;
        INSERT INTO "WEBFUSION"."USER_ROLES" ("ID_USER","NA_ROLE") VALUES (allocated,'ADMIN');
        IF (SELECT "NA_ROLE" COLLATE "C" FROM "WEBFUSION"."USER_ROLES" WHERE "ID_USER"=allocated)<>'admin'
        THEN RAISE EXCEPTION 'Role not normalized'; END IF;
        SELECT "DT_UPDATED_AT" INTO saved FROM "WEBFUSION"."USERS" WHERE "ID_USER"=allocated;
        UPDATE "WEBFUSION"."USERS" SET "NA_USER_EMAIL"="NA_USER_EMAIL" WHERE "ID_USER"=allocated;
        IF (SELECT "DT_UPDATED_AT" FROM "WEBFUSION"."USERS" WHERE "ID_USER"=allocated)<>saved
        THEN RAISE EXCEPTION 'No-op timestamp changed'; END IF;
        UPDATE "WEBFUSION"."USERS" SET "NA_USER_NAME"='changed' WHERE "ID_USER"=allocated;
        IF (SELECT "DT_UPDATED_AT" FROM "WEBFUSION"."USERS" WHERE "ID_USER"=allocated)=saved
        THEN RAISE EXCEPTION 'Timestamp not updated'; END IF;
        DELETE FROM "WEBFUSION"."USERS" WHERE "ID_USER"=allocated;
        IF EXISTS(SELECT 1 FROM "WEBFUSION"."USER_ROLES" WHERE "ID_USER"=allocated)
        THEN RAISE EXCEPTION 'Cascade missing'; END IF;
    WHEN 'RFFUSION_SUMMARY' THEN
        INSERT INTO "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK_OVERRIDE" ("FK_HOST","FK_EQUIPMENT","DT_UPDATED_AT")
        VALUES (-2147480000,-2147480000,'2000-01-01');
    ELSE RAISE EXCEPTION 'Unexpected database';
    END CASE;
END;
$rules$;
SELECT format('SET LOCAL ROLE %I','rffusion_owner_' || lower(current_database()))
\gexec
CREATE TEMP SEQUENCE fixture_sequence AS integer MAXVALUE 2;
CREATE TEMP TABLE fixture_ids (id integer NOT NULL PRIMARY KEY);
INSERT INTO rffusion_support.id_guard VALUES ('fixture_sequence',0);
CREATE TRIGGER allocate BEFORE INSERT ON fixture_ids FOR EACH ROW
EXECUTE FUNCTION rffusion_support.allocate_id('id','pg_temp','fixture_sequence');
INSERT INTO fixture_ids VALUES (1);
INSERT INTO fixture_ids VALUES (NULL);
DO $overflow$
BEGIN
    BEGIN
        INSERT INTO fixture_ids VALUES (NULL);
        RAISE EXCEPTION 'Sequence overflow not detected';
    EXCEPTION WHEN sequence_generator_limit_exceeded THEN NULL; END;
END;
$overflow$;
ROLLBACK;
