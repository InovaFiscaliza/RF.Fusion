#!/usr/bin/env bash
set +x
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/safety.sh"
postgres_test_gate "$@"
uptime
free -h
python3 - "$test_container" <<'PY'
import json
import re
import subprocess
import sys
import time
import uuid

container = sys.argv[1]
base = ["podman", "--remote=false", "exec", "-i", container, "runuser", "-u", "postgres", "--",
        "psql", "-X", "-qAt", "-h", "/var/run/postgresql", "-p", "5432", "-U", "postgres",
        "-d", "BPDATA", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]

def query(sql):
    result = subprocess.run(base, input=sql, text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()

for table, pk in (("FILE_TASK", "ID_FILE_TASK"), ("FILE_TASK_HISTORY", "ID_HISTORY")):
    for isolation in ("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"):
        for scenario in ("same_host_distinct", "different_hosts", "same_host_duplicate"):
            nonce = "rffusion_" + uuid.uuid4().hex
            owner, contender = nonce + "_one", nonce + "_two"
            second_host = -2147480001 if scenario == "different_hosts" else -2147480000
            qualified = '"BPDATA"."' + table + '"'
            query('BEGIN; INSERT INTO "BPDATA"."HOST" ("ID_HOST") VALUES (-2147480000),(-2147480001);'
                  f'INSERT INTO {qualified} ("{pk}","FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") '
                  "SELECT -2147480000,-2147480000,string_agg(md5(i::text),''),'one' FROM generate_series(1,90) i;"
                  f'INSERT INTO {qualified} ("{pk}","FK_HOST","NA_HOST_FILE_PATH","NA_HOST_FILE_NAME") '
                  f"SELECT -2147480001,{second_host},string_agg(md5(i::text),''),'two' FROM generate_series(1,90) i; COMMIT;")
            processes = []
            started = time.monotonic_ns()
            retries = 0
            observed = set()
            try:
                first_sql = f"SET application_name='{owner}'; BEGIN ISOLATION LEVEL {isolation}; "
                first_sql += f'UPDATE {qualified} SET "NA_HOST_FILE_NAME"=\'same\' WHERE "{pk}"=-2147480000; SELECT pg_sleep(5); COMMIT;\n'
                first = subprocess.Popen(base, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, text=True)
                processes.append(first)
                first.stdin.write(first_sql)
                first.stdin.close()
                deadline = time.monotonic() + 10
                owner_pid = ""
                while time.monotonic() < deadline:
                    owner_pid = query(f"SELECT pid FROM pg_stat_activity WHERE application_name='{owner}' AND wait_event='PgSleep';")
                    if owner_pid:
                        break
                    if first.poll() is not None:
                        raise RuntimeError("Writer one exited before acquiring the guard: " + first.stderr.read())
                    time.sleep(0.05)
                if not owner_pid:
                    raise RuntimeError("Guard barrier timed out")
                changed = "same" if scenario == "same_host_duplicate" else "different"
                second_sql = f"SET application_name='{contender}'; BEGIN ISOLATION LEVEL {isolation}; "
                second_sql += f'UPDATE {qualified} SET "NA_HOST_FILE_NAME"=\'{changed}\' WHERE "{pk}"=-2147480001; COMMIT;\n'
                second = subprocess.Popen(base, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE, text=True)
                processes.append(second)
                second.stdin.write(second_sql)
                second.stdin.close()
                deadline = time.monotonic() + 2
                while second.poll() is None and time.monotonic() < deadline:
                    blockers = query("SELECT unnest(pg_blocking_pids(pid)) FROM pg_stat_activity "
                                     f"WHERE application_name='{contender}' AND wait_event_type='Lock';")
                    observed.update(blockers.splitlines())
                    if owner_pid in observed:
                        break
                    time.sleep(0.05)
                first.wait(timeout=20)
                second.wait(timeout=20)
                if first.returncode:
                    raise RuntimeError("Writer one failed: " + first.stderr.read())
                error = second.stderr.read()
                status = second.returncode
                while status and re.search(r"\b(40001|40P01)\b", error) and retries < 3:
                    retries += 1
                    retry = subprocess.run(base, input=second_sql, text=True, capture_output=True, timeout=30)
                    status, error = retry.returncode, retry.stderr
                if scenario == "same_host_duplicate":
                    if status == 0 or not re.search(r"\b23505\b", error):
                        raise RuntimeError("Duplicate was not rejected by 23505: " + error)
                elif status:
                    raise RuntimeError("Distinct identities failed: " + error)
                if scenario != "different_hosts" and owner_pid not in observed:
                    raise RuntimeError("Host guard contention was not observed; timing alone is insufficient")
                count = query(f'SELECT count(*) FROM {qualified} WHERE "{pk}" IN (-2147480000,-2147480001) '
                              f'AND "NA_HOST_FILE_NAME" IN (\'same\',\'different\');')
                expected = "1" if scenario == "same_host_duplicate" else "2"
                if count != expected:
                    raise RuntimeError("Unexpected committed identity state")
                print(json.dumps({"scenario": scenario, "table": table, "isolation": isolation,
                                  "duration_ms": (time.monotonic_ns()-started)//1000000,
                                  "retries": retries, "blockers": sorted(observed), "clock": "monotonic",
                                  "result": "pass"}), flush=True)
            finally:
                primary_failed = sys.exc_info()[0] is not None
                cleanup_errors = []
                try:
                    query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                          f"WHERE application_name IN ('{owner}','{contender}') AND pid<>pg_backend_pid();")
                except (RuntimeError, subprocess.TimeoutExpired) as error:
                    cleanup_errors.append(str(error))
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                        cleanup_errors.append("Client did not terminate within timeout")
                    process.stdout.close()
                    process.stderr.close()
                try:
                    query(f'DELETE FROM {qualified} WHERE "{pk}" IN (-2147480000,-2147480001);'
                          'DELETE FROM "BPDATA"."HOST" WHERE "ID_HOST" IN (-2147480000,-2147480001);')
                except (RuntimeError, subprocess.TimeoutExpired) as error:
                    cleanup_errors.append(str(error))
                for error in cleanup_errors:
                    print("ERROR concurrency cleanup: " + error, file=sys.stderr)
                if cleanup_errors and not primary_failed:
                    raise RuntimeError("Concurrency cleanup failed")
PY
