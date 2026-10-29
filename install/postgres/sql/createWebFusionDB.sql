\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE "rffusion_owner_webfusion";
CREATE SCHEMA "WEBFUSION" AUTHORIZATION "rffusion_owner_webfusion";
SET LOCAL search_path = "WEBFUSION", public, pg_catalog;
\ir createSupport.sql
CREATE TABLE "WEBFUSION"."USERS" (
  "ID_USER" bigint NOT NULL,
  "NA_USER_NAME" character varying(255) COLLATE rffusion_support."unicode_ci",
  "NA_USER_EMAIL" character varying(320) COLLATE rffusion_support."unicode_ci" NOT NULL,
  "NA_JOB_TITLE" character varying(255) COLLATE rffusion_support."unicode_ci",
  "NA_DEPARTMENT" character varying(255) COLLATE rffusion_support."unicode_ci",
  "NA_LOCATION" character varying(255) COLLATE rffusion_support."unicode_ci",
  "NA_URL_PROFILE_IMG" character varying(2048) COLLATE rffusion_support."unicode_ci",
  "DT_CREATED_AT" timestamp(0) without time zone NOT NULL DEFAULT date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'),
  "DT_UPDATED_AT" timestamp(0) without time zone NOT NULL DEFAULT date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'),
  PRIMARY KEY ("ID_USER"),
  CONSTRAINT "USERS__UQ_USERS_USER_EMAIL" UNIQUE ("NA_USER_EMAIL")
);
CREATE SEQUENCE "WEBFUSION"."USERS__ID_USER__seq" AS bigint;
ALTER SEQUENCE "WEBFUSION"."USERS__ID_USER__seq" OWNED BY "WEBFUSION"."USERS"."ID_USER";
INSERT INTO rffusion_support.id_guard VALUES ('USERS__ID_USER__seq', 0);
CREATE TRIGGER a00_allocate BEFORE INSERT OR UPDATE OF "ID_USER" ON "WEBFUSION"."USERS" FOR EACH ROW EXECUTE FUNCTION rffusion_support.allocate_id('ID_USER', 'WEBFUSION', 'USERS__ID_USER__seq');
CREATE TRIGGER z90_updated BEFORE UPDATE ON "WEBFUSION"."USERS" FOR EACH ROW EXECUTE FUNCTION rffusion_support.update_timestamp('DT_UPDATED_AT');
CREATE TABLE "WEBFUSION"."USER_ROLES" (
  "ID_USER" bigint NOT NULL,
  "NA_ROLE" character varying(9) COLLATE rffusion_support."unicode_ci" NOT NULL CHECK ("NA_ROLE" COLLATE "C" IN ('admin', 'developer')),
  "IS_ACTIVE" smallint NOT NULL DEFAULT 1 CHECK ("IS_ACTIVE" BETWEEN -128 AND 127),
  "DT_CREATED_AT" timestamp(0) without time zone NOT NULL DEFAULT date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'),
  "DT_UPDATED_AT" timestamp(0) without time zone NOT NULL DEFAULT date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'),
  PRIMARY KEY ("ID_USER", "NA_ROLE"),
  CONSTRAINT "FK_USER_ROLES_USER" FOREIGN KEY ("ID_USER") REFERENCES "WEBFUSION"."USERS" ("ID_USER") ON DELETE CASCADE ON UPDATE NO ACTION
);
CREATE TRIGGER z90_updated BEFORE UPDATE ON "WEBFUSION"."USER_ROLES" FOR EACH ROW EXECUTE FUNCTION rffusion_support.update_timestamp('DT_UPDATED_AT');
CREATE TRIGGER b00_role BEFORE INSERT OR UPDATE ON "WEBFUSION"."USER_ROLES" FOR EACH ROW EXECUTE FUNCTION rffusion_support.normalize_role();
CREATE INDEX "USER_ROLES__IX_USER_ROLES_ROLE_ACTIVE" ON "WEBFUSION"."USER_ROLES" USING btree ("NA_ROLE" NULLS FIRST, "IS_ACTIVE" NULLS FIRST);
SELECT rffusion_support.reset_generators();
COMMIT;
