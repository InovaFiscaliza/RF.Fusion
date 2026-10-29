\set ON_ERROR_STOP on
BEGIN;
CREATE EXTENSION postgres_fdw;
CREATE SCHEMA "BPDATA";
CREATE SERVER rffusion_bpdata FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '/var/run/postgresql', port '5432', dbname 'BPDATA', updatable 'false');
GRANT USAGE ON FOREIGN SERVER rffusion_bpdata TO rffusion_view_owner;
CREATE USER MAPPING FOR rffusion_view_owner SERVER rffusion_bpdata OPTIONS (user 'rffusion_fdw_reader', password_required 'false');
CREATE FOREIGN TABLE "BPDATA"."HOST" (
  "ID_HOST" integer,
  "NA_HOST_NAME" character varying(100) COLLATE rffusion_support."general_ci",
  "NA_HOST_ADDRESS" character varying(50) COLLATE rffusion_support."general_ci",
  "NA_HOST_PORT" integer,
  "NA_HOST_USER" character varying(50) COLLATE rffusion_support."general_ci",
  "NA_HOST_PASSWORD" character varying(50) COLLATE rffusion_support."general_ci",
  "DT_LAST_BACKUP" timestamp(0) without time zone,
  "DT_LAST_PROCESSING" timestamp(0) without time zone,
  "DT_LAST_DISCOVERY" timestamp(0) without time zone,
  "IS_OFFLINE" smallint,
  "IS_BUSY" smallint,
  "NU_PID" integer,
  "DT_LAST_FAIL" timestamp(0) without time zone,
  "DT_LAST_CHECK" timestamp(0) without time zone,
  "DT_BUSY" timestamp(0) without time zone,
  "DT_LAST_OFFLINE_AT" timestamp(0) without time zone,
  "NA_LAST_OFFLINE_DESCRIPTION" text COLLATE rffusion_support."general_ci",
  "NU_HOST_CHECK_ERROR" integer
) SERVER rffusion_bpdata OPTIONS (schema_name 'BPDATA', table_name 'HOST', updatable 'false');
CREATE FOREIGN TABLE "BPDATA"."HOST_TASK" (
  "ID_HOST_TASK" integer,
  "FK_HOST" integer,
  "NU_TYPE" smallint,
  "DT_HOST_TASK" timestamp(0) without time zone,
  "NU_STATUS" smallint,
  "NU_PID" integer,
  "FILTER" json,
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci"
) SERVER rffusion_bpdata OPTIONS (schema_name 'BPDATA', table_name 'HOST_TASK', updatable 'false');
CREATE FOREIGN TABLE "BPDATA"."FILE_TASK" (
  "ID_FILE_TASK" integer,
  "FK_HOST" integer,
  "DT_FILE_TASK" timestamp(0) without time zone,
  "NU_TYPE" smallint,
  "NA_HOST_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_HOST_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "NU_HOST_FILE_MD5" character varying(32) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_MD5" character varying(32) COLLATE rffusion_support."general_ci",
  "NU_STATUS" smallint,
  "NU_PID" integer,
  "NA_EXTENSION_HOST" character varying(20) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_HOST" bigint,
  "DT_FILE_CREATED_HOST" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_HOST" timestamp(0) without time zone,
  "NA_EXTENSION_SERVER" character varying(20) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_SERVER" bigint,
  "DT_FILE_CREATED_SERVER" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_SERVER" timestamp(0) without time zone,
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci",
  "NA_ERROR_CODE" character varying(64) COLLATE rffusion_support."general_ci",
  "NA_ERROR_DETAIL" text COLLATE rffusion_support."general_ci",
  "NU_ERROR_CLASSIFIER_VERSION" smallint
) SERVER rffusion_bpdata OPTIONS (schema_name 'BPDATA', table_name 'FILE_TASK', updatable 'false');
CREATE FOREIGN TABLE "BPDATA"."FILE_TASK_HISTORY" (
  "ID_HISTORY" integer,
  "FK_HOST" integer,
  "DT_DISCOVERED" timestamp(0) without time zone,
  "DT_BACKUP" timestamp(0) without time zone,
  "DT_PROCESSED" timestamp(0) without time zone,
  "NU_STATUS_DISCOVERY" integer,
  "NU_STATUS_BACKUP" integer,
  "NU_STATUS_PROCESSING" integer,
  "NA_HOST_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_HOST_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_HOST" bigint,
  "DT_FILE_CREATED_HOST" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_HOST" timestamp(0) without time zone,
  "NA_EXTENSION_HOST" character varying(20) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_SERVER" bigint,
  "DT_FILE_CREATED_SERVER" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_SERVER" timestamp(0) without time zone,
  "NA_EXTENSION_SERVER" character varying(20) COLLATE rffusion_support."general_ci",
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci",
  "NA_ERROR_CODE" character varying(64) COLLATE rffusion_support."general_ci",
  "NA_ERROR_DETAIL" text COLLATE rffusion_support."general_ci",
  "NU_ERROR_CLASSIFIER_VERSION" smallint,
  "IS_PAYLOAD_DELETED" smallint,
  "DT_PAYLOAD_DELETED" timestamp(0) without time zone
) SERVER rffusion_bpdata OPTIONS (schema_name 'BPDATA', table_name 'FILE_TASK_HISTORY', updatable 'false');
CREATE VIEW "RFFUSION_SUMMARY"."VW_ERROR_EVENT_CANONICAL" AS
SELECT
    'FILE_TASK_HISTORY' AS "NA_SOURCE_TABLE",
    f."ID_HISTORY" AS "ID_SOURCE_ROW",
    'BACKUP' AS "NA_ERROR_SCOPE",
    f."FK_HOST" AS "FK_HOST",
    h."NA_HOST_NAME" AS "NA_HOST_NAME",
    COALESCE(f."DT_BACKUP", f."DT_DISCOVERED", f."DT_FILE_CREATED_HOST", f."DT_FILE_MODIFIED_HOST") AS "DT_EVENT_AT",
    f."NU_STATUS_BACKUP" AS "NU_STATUS_VALUE",
    NULLIF(TRIM(f."NA_ERROR_CODE"), '') AS "NA_ERROR_CODE",
    COALESCE(NULLIF(TRIM(f."NA_MESSAGE"), ''), '(Sem mensagem)') AS "NA_ERROR_SUMMARY",
    encode(sha256(convert_to(COALESCE(NULLIF(TRIM(f."NA_MESSAGE"), ''), '(Sem mensagem)'), 'UTF8')), 'hex') AS "NA_ERROR_SUMMARY_HASH",
    f."NA_ERROR_DETAIL" AS "NA_ERROR_DETAIL",
    f."NU_ERROR_CLASSIFIER_VERSION" AS "NU_ERROR_CLASSIFIER_VERSION",
    f."NA_MESSAGE" AS "NA_RAW_MESSAGE"
FROM "BPDATA"."FILE_TASK_HISTORY" f
LEFT JOIN "BPDATA"."HOST" h ON h."ID_HOST" = f."FK_HOST"
WHERE f."NU_STATUS_BACKUP" = -1

UNION ALL

SELECT
    'FILE_TASK_HISTORY' AS "NA_SOURCE_TABLE",
    f."ID_HISTORY" AS "ID_SOURCE_ROW",
    'PROCESSING' AS "NA_ERROR_SCOPE",
    f."FK_HOST" AS "FK_HOST",
    h."NA_HOST_NAME" AS "NA_HOST_NAME",
    COALESCE(f."DT_PROCESSED", f."DT_BACKUP", f."DT_DISCOVERED", f."DT_FILE_CREATED_HOST", f."DT_FILE_MODIFIED_HOST") AS "DT_EVENT_AT",
    f."NU_STATUS_PROCESSING" AS "NU_STATUS_VALUE",
    NULLIF(TRIM(f."NA_ERROR_CODE"), '') AS "NA_ERROR_CODE",
    COALESCE(NULLIF(TRIM(f."NA_MESSAGE"), ''), '(Sem mensagem)') AS "NA_ERROR_SUMMARY",
    encode(sha256(convert_to(COALESCE(NULLIF(TRIM(f."NA_MESSAGE"), ''), '(Sem mensagem)'), 'UTF8')), 'hex') AS "NA_ERROR_SUMMARY_HASH",
    f."NA_ERROR_DETAIL" AS "NA_ERROR_DETAIL",
    f."NU_ERROR_CLASSIFIER_VERSION" AS "NU_ERROR_CLASSIFIER_VERSION",
    f."NA_MESSAGE" AS "NA_RAW_MESSAGE"
FROM "BPDATA"."FILE_TASK_HISTORY" f
LEFT JOIN "BPDATA"."HOST" h ON h."ID_HOST" = f."FK_HOST"
WHERE f."NU_STATUS_PROCESSING" = -1

UNION ALL

SELECT
    'FILE_TASK' AS "NA_SOURCE_TABLE",
    t."ID_FILE_TASK" AS "ID_SOURCE_ROW",
    CASE
        WHEN t."NU_TYPE" = 1 THEN 'BACKUP_QUEUE'
        WHEN t."NU_TYPE" = 2 THEN 'PROCESSING_QUEUE'
        ELSE 'FILE_TASK'
    END AS "NA_ERROR_SCOPE",
    t."FK_HOST" AS "FK_HOST",
    h."NA_HOST_NAME" AS "NA_HOST_NAME",
    COALESCE(t."DT_FILE_TASK", t."DT_FILE_CREATED_HOST", t."DT_FILE_MODIFIED_HOST") AS "DT_EVENT_AT",
    t."NU_STATUS" AS "NU_STATUS_VALUE",
    NULLIF(TRIM(t."NA_ERROR_CODE"), '') AS "NA_ERROR_CODE",
    COALESCE(NULLIF(TRIM(t."NA_MESSAGE"), ''), '(Sem mensagem)') AS "NA_ERROR_SUMMARY",
    encode(sha256(convert_to(COALESCE(NULLIF(TRIM(t."NA_MESSAGE"), ''), '(Sem mensagem)'), 'UTF8')), 'hex') AS "NA_ERROR_SUMMARY_HASH",
    t."NA_ERROR_DETAIL" AS "NA_ERROR_DETAIL",
    t."NU_ERROR_CLASSIFIER_VERSION" AS "NU_ERROR_CLASSIFIER_VERSION",
    t."NA_MESSAGE" AS "NA_RAW_MESSAGE"
FROM "BPDATA"."FILE_TASK" t
LEFT JOIN "BPDATA"."HOST" h ON h."ID_HOST" = t."FK_HOST"
WHERE t."NU_STATUS" = -1

UNION ALL

SELECT
    'HOST_TASK' AS "NA_SOURCE_TABLE",
    ht."ID_HOST_TASK" AS "ID_SOURCE_ROW",
    'HOST_TASK' AS "NA_ERROR_SCOPE",
    ht."FK_HOST" AS "FK_HOST",
    h."NA_HOST_NAME" AS "NA_HOST_NAME",
    ht."DT_HOST_TASK" AS "DT_EVENT_AT",
    ht."NU_STATUS" AS "NU_STATUS_VALUE",
    NULL AS "NA_ERROR_CODE",
    COALESCE(NULLIF(TRIM(ht."NA_MESSAGE"), ''), '(Sem mensagem)') AS "NA_ERROR_SUMMARY",
    encode(sha256(convert_to(COALESCE(NULLIF(TRIM(ht."NA_MESSAGE"), ''), '(Sem mensagem)'), 'UTF8')), 'hex') AS "NA_ERROR_SUMMARY_HASH",
    NULL AS "NA_ERROR_DETAIL",
    NULL AS "NU_ERROR_CLASSIFIER_VERSION",
    ht."NA_MESSAGE" AS "NA_RAW_MESSAGE"
FROM "BPDATA"."HOST_TASK" ht
LEFT JOIN "BPDATA"."HOST" h ON h."ID_HOST" = ht."FK_HOST"
WHERE ht."NU_STATUS" = -1;
GRANT USAGE ON SCHEMA "BPDATA", "RFFUSION_SUMMARY" TO rffusion_view_owner;
GRANT CREATE ON SCHEMA "RFFUSION_SUMMARY" TO rffusion_view_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA "BPDATA" TO rffusion_view_owner;
ALTER VIEW "RFFUSION_SUMMARY"."VW_ERROR_EVENT_CANONICAL" OWNER TO rffusion_view_owner;
REVOKE CREATE ON SCHEMA "RFFUSION_SUMMARY" FROM rffusion_view_owner;
COMMIT;
