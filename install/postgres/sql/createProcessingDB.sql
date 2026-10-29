\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE "rffusion_owner_bpdata";
CREATE SCHEMA "BPDATA" AUTHORIZATION "rffusion_owner_bpdata";
SET LOCAL search_path = "BPDATA", public, pg_catalog;
\ir createSupport.sql
CREATE TABLE "BPDATA"."HOST" (
  "ID_HOST" integer NOT NULL,
  "NA_HOST_NAME" character varying(100) COLLATE rffusion_support."general_ci",
  "NA_HOST_ADDRESS" character varying(50) COLLATE rffusion_support."general_ci",
  "NA_HOST_PORT" integer,
  "NA_HOST_USER" character varying(50) COLLATE rffusion_support."general_ci",
  "NA_HOST_PASSWORD" character varying(50) COLLATE rffusion_support."general_ci",
  "DT_LAST_BACKUP" timestamp(0) without time zone,
  "DT_LAST_PROCESSING" timestamp(0) without time zone,
  "DT_LAST_DISCOVERY" timestamp(0) without time zone,
  "IS_OFFLINE" smallint DEFAULT 0 CHECK ("IS_OFFLINE" BETWEEN -128 AND 127),
  "IS_BUSY" smallint DEFAULT 0 CHECK ("IS_BUSY" BETWEEN -128 AND 127),
  "NU_PID" integer DEFAULT 0,
  "DT_LAST_FAIL" timestamp(0) without time zone,
  "DT_LAST_CHECK" timestamp(0) without time zone,
  "DT_BUSY" timestamp(0) without time zone,
  "DT_LAST_OFFLINE_AT" timestamp(0) without time zone,
  "NA_LAST_OFFLINE_DESCRIPTION" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_LAST_OFFLINE_DESCRIPTION") <= 65535),
  "NU_HOST_CHECK_ERROR" integer DEFAULT 0,
  PRIMARY KEY ("ID_HOST")
);
CREATE TABLE "BPDATA"."HOST_TASK" (
  "ID_HOST_TASK" integer NOT NULL,
  "FK_HOST" integer,
  "NU_TYPE" smallint DEFAULT 0 CHECK ("NU_TYPE" BETWEEN -128 AND 127),
  "DT_HOST_TASK" timestamp(0) without time zone,
  "NU_STATUS" smallint DEFAULT 0 CHECK ("NU_STATUS" BETWEEN -128 AND 127),
  "NU_PID" integer,
  "FILTER" json,
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_MESSAGE") <= 65535),
  PRIMARY KEY ("ID_HOST_TASK"),
  CONSTRAINT "FK_HOST_TASK_HOST" FOREIGN KEY ("FK_HOST") REFERENCES "BPDATA"."HOST" ("ID_HOST") ON DELETE NO ACTION ON UPDATE NO ACTION
);
CREATE SEQUENCE "BPDATA"."HOST_TASK__ID_HOST_TASK__seq" AS integer;
ALTER SEQUENCE "BPDATA"."HOST_TASK__ID_HOST_TASK__seq" OWNED BY "BPDATA"."HOST_TASK"."ID_HOST_TASK";
INSERT INTO rffusion_support.id_guard VALUES ('HOST_TASK__ID_HOST_TASK__seq', 0);
CREATE TRIGGER a00_allocate BEFORE INSERT OR UPDATE OF "ID_HOST_TASK" ON "BPDATA"."HOST_TASK" FOR EACH ROW EXECUTE FUNCTION rffusion_support.allocate_id('ID_HOST_TASK', 'BPDATA', 'HOST_TASK__ID_HOST_TASK__seq');
CREATE INDEX "HOST_TASK__FK_HOST_TASK_HOST" ON "BPDATA"."HOST_TASK" USING btree ("FK_HOST" NULLS FIRST);
CREATE TABLE "BPDATA"."FILE_TASK" (
  "ID_FILE_TASK" integer NOT NULL,
  "FK_HOST" integer,
  "DT_FILE_TASK" timestamp(0) without time zone,
  "NU_TYPE" smallint DEFAULT 0 CHECK ("NU_TYPE" BETWEEN -128 AND 127),
  "NA_HOST_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_HOST_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "NU_HOST_FILE_MD5" character varying(32) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_PATH" character varying(3000) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_NAME" character varying(512) COLLATE rffusion_support."general_ci",
  "NA_SERVER_FILE_MD5" character varying(32) COLLATE rffusion_support."general_ci",
  "NU_STATUS" smallint DEFAULT 0 CHECK ("NU_STATUS" BETWEEN -128 AND 127),
  "NU_PID" integer,
  "NA_EXTENSION_HOST" character varying(20) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_HOST" bigint,
  "DT_FILE_CREATED_HOST" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_HOST" timestamp(0) without time zone,
  "NA_EXTENSION_SERVER" character varying(20) COLLATE rffusion_support."general_ci",
  "VL_FILE_SIZE_KB_SERVER" bigint,
  "DT_FILE_CREATED_SERVER" timestamp(0) without time zone,
  "DT_FILE_MODIFIED_SERVER" timestamp(0) without time zone,
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_MESSAGE") <= 65535),
  "NA_ERROR_CODE" character varying(64) COLLATE rffusion_support."general_ci",
  "NA_ERROR_DETAIL" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_ERROR_DETAIL") <= 65535),
  "NU_ERROR_CLASSIFIER_VERSION" smallint,
  PRIMARY KEY ("ID_FILE_TASK"),
  CONSTRAINT "FK_FILE_TASK_HOST" FOREIGN KEY ("FK_HOST") REFERENCES "BPDATA"."HOST" ("ID_HOST") ON DELETE NO ACTION ON UPDATE NO ACTION
);
CREATE SEQUENCE "BPDATA"."FILE_TASK__ID_FILE_TASK__seq" AS integer;
ALTER SEQUENCE "BPDATA"."FILE_TASK__ID_FILE_TASK__seq" OWNED BY "BPDATA"."FILE_TASK"."ID_FILE_TASK";
INSERT INTO rffusion_support.id_guard VALUES ('FILE_TASK__ID_FILE_TASK__seq', 0);
CREATE TRIGGER a00_allocate BEFORE INSERT OR UPDATE OF "ID_FILE_TASK" ON "BPDATA"."FILE_TASK" FOR EACH ROW EXECUTE FUNCTION rffusion_support.allocate_id('ID_FILE_TASK', 'BPDATA', 'FILE_TASK__ID_FILE_TASK__seq');
CREATE TRIGGER b10_identity BEFORE INSERT OR UPDATE OR DELETE ON "BPDATA"."FILE_TASK" FOR EACH ROW EXECUTE FUNCTION rffusion_support.file_identity('ID_FILE_TASK');
CREATE INDEX "FILE_TASK__FK_FILE_TASK_HOST" ON "BPDATA"."FILE_TASK" USING btree ("FK_HOST" NULLS FIRST);
CREATE INDEX "FILE_TASK__idx_file_task_identity" ON "BPDATA"."FILE_TASK" USING btree ("FK_HOST" NULLS FIRST, left("NA_HOST_FILE_PATH", 191) NULLS FIRST, left("NA_HOST_FILE_NAME", 191) NULLS FIRST);
CREATE INDEX "FILE_TASK__idx_file_task_queue_host" ON "BPDATA"."FILE_TASK" USING btree ("NU_STATUS" NULLS FIRST, "NU_TYPE" NULLS FIRST, "FK_HOST" NULLS FIRST, "ID_FILE_TASK" NULLS FIRST);
CREATE TABLE "BPDATA"."FILE_TASK_HISTORY" (
  "ID_HISTORY" integer NOT NULL,
  "FK_HOST" integer NOT NULL,
  "DT_DISCOVERED" timestamp(0) without time zone,
  "DT_BACKUP" timestamp(0) without time zone,
  "DT_PROCESSED" timestamp(0) without time zone,
  "NU_STATUS_DISCOVERY" integer DEFAULT 1,
  "NU_STATUS_BACKUP" integer DEFAULT 1,
  "NU_STATUS_PROCESSING" integer DEFAULT 1,
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
  "NA_MESSAGE" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_MESSAGE") <= 65535),
  "NA_ERROR_CODE" character varying(64) COLLATE rffusion_support."general_ci",
  "NA_ERROR_DETAIL" text COLLATE rffusion_support."general_ci" CHECK (octet_length("NA_ERROR_DETAIL") <= 65535),
  "NU_ERROR_CLASSIFIER_VERSION" smallint,
  "IS_PAYLOAD_DELETED" smallint NOT NULL DEFAULT 0 CHECK ("IS_PAYLOAD_DELETED" BETWEEN -128 AND 127),
  "DT_PAYLOAD_DELETED" timestamp(0) without time zone,
  PRIMARY KEY ("ID_HISTORY"),
  CONSTRAINT "FK_HISTORY_HOST" FOREIGN KEY ("FK_HOST") REFERENCES "BPDATA"."HOST" ("ID_HOST") ON DELETE NO ACTION ON UPDATE NO ACTION
);
CREATE SEQUENCE "BPDATA"."FILE_TASK_HISTORY__ID_HISTORY__seq" AS integer;
ALTER SEQUENCE "BPDATA"."FILE_TASK_HISTORY__ID_HISTORY__seq" OWNED BY "BPDATA"."FILE_TASK_HISTORY"."ID_HISTORY";
INSERT INTO rffusion_support.id_guard VALUES ('FILE_TASK_HISTORY__ID_HISTORY__seq', 0);
CREATE TRIGGER a00_allocate BEFORE INSERT OR UPDATE OF "ID_HISTORY" ON "BPDATA"."FILE_TASK_HISTORY" FOR EACH ROW EXECUTE FUNCTION rffusion_support.allocate_id('ID_HISTORY', 'BPDATA', 'FILE_TASK_HISTORY__ID_HISTORY__seq');
CREATE TRIGGER b10_identity BEFORE INSERT OR UPDATE OR DELETE ON "BPDATA"."FILE_TASK_HISTORY" FOR EACH ROW EXECUTE FUNCTION rffusion_support.file_identity('ID_HISTORY');
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_dedup_identity" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("FK_HOST" NULLS FIRST, left("NA_HOST_FILE_PATH", 191) NULLS FIRST, left("NA_HOST_FILE_NAME", 191) NULLS FIRST, "VL_FILE_SIZE_KB_HOST" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_host_file_name" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("NA_HOST_FILE_NAME" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_server_file_name" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("NA_SERVER_FILE_NAME" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_host_date_host" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("FK_HOST" NULLS FIRST, "DT_FILE_CREATED_HOST" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_processed_recent" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("DT_PROCESSED" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_gc_retention" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("NU_STATUS_PROCESSING" NULLS FIRST, "IS_PAYLOAD_DELETED" NULLS FIRST, "DT_PROCESSED" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_backup_recreate" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("NU_STATUS_BACKUP" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
CREATE INDEX "FILE_TASK_HISTORY__idx_fth_processing_recreate" ON "BPDATA"."FILE_TASK_HISTORY" USING btree ("NU_STATUS_BACKUP" NULLS FIRST, "NU_STATUS_PROCESSING" NULLS FIRST, "ID_HISTORY" NULLS FIRST);
SELECT rffusion_support.reset_generators();
COMMIT;
