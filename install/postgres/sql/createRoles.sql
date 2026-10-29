\set ON_ERROR_STOP on
\set VERBOSITY terse
\set ECHO none
SET log_statement = 'none';
SET log_min_error_statement = 'panic';
\set app_password `cat /run/secrets/rffusion-postgres-appcataloga`
\set web_password `cat /run/secrets/rffusion-postgres-webfusion`
CREATE ROLE rffusion_appcataloga LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'app_password';
CREATE ROLE rffusion_webfusion LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'web_password';
\unset app_password
\unset web_password
CREATE ROLE rffusion_fdw_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE rffusion_view_owner NOLOGIN;
CREATE ROLE rffusion_owner_bpdata NOLOGIN;
CREATE ROLE rffusion_owner_rfdata NOLOGIN;
CREATE ROLE rffusion_owner_rffusion_summary NOLOGIN;
CREATE ROLE rffusion_owner_webfusion NOLOGIN;
CREATE DATABASE "BPDATA" OWNER rffusion_owner_bpdata ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE "RFDATA" OWNER rffusion_owner_rfdata ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE "RFFUSION_SUMMARY" OWNER rffusion_owner_rffusion_summary ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE "WEBFUSION" OWNER rffusion_owner_webfusion ENCODING 'UTF8' TEMPLATE template0;
REVOKE CONNECT ON DATABASE postgres, template1 FROM PUBLIC;
