#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
from datetime import datetime
import shared as sh
from db.dbHandlerBKP import dbHandlerBKP
import config as k
import signal

SRC_DIR = "/mnt/reposfi"
DEST_DIR = "/mnt/reposfi/tmp"

# ======================================================================
# Signal handling
# ======================================================================
def _handle_sigterm(sig, frame) -> None:
    """Handle SIGTERM/SIGINT to stop the main loop gracefully."""
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def migrate():
    log = sh.log("migrate")
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    log.entry("=== MIGRATION STARTED ===")

    for root, dirs, files in os.walk(SRC_DIR):

        # não entrar na pasta tmp
        if root.startswith(DEST_DIR):
            continue

        for fname in files:

            if not fname.lower().endswith(".bin"):
                continue

            full_path = os.path.join(root, fname)

            # ---------------------------------------------------------
            # 1) Buscar DISCOVERY completo SEM host_id
            # ---------------------------------------------------------
            discovery = db.check_file_task(
                NA_HOST_FILE_NAME=fname,
                NU_TYPE=k.FILE_TASK_DISCOVERY,
                NU_STATUS=k.TASK_DONE
            )

            # Se não existir DISCOVERY DONE → ignorar completamente
            if not discovery:
                log.entry(f"[IGNORE] Sem discovery válido → {fname}")
                continue

            task = discovery[0]
            host_id = task["FK_HOST"]

            # Carrega dados do host
            host = db.host_read_access(host_id)
            if not host:
                log.warning(f"[WARN] HOST não encontrado no banco → {host_id}")
                continue

            # ---------------------------------------------------------
            # 2) Arquivo já migrado anteriormente?
            # ---------------------------------------------------------
            if task.get("NA_SERVER_FILE_PATH"):
                log.entry(f"[SKIP] Já tratado → {fname}")
                continue

            # ---------------------------------------------------------
            # 3) Verificar HISTORY correspondente
            # ---------------------------------------------------------
            history = db.check_file_history(
                FK_HOST=host_id,
                NA_HOST_FILE_NAME=fname
            )

            if not history:
                log.warning(f"[WARN] DISCOVERY existe mas HISTORY não → {fname}")
                history = None

            # ---------------------------------------------------------
            # 4) Preparar pasta destino
            # ---------------------------------------------------------
            local_path = os.path.join(
                k.REPO_FOLDER, k.TMP_FOLDER, host["host_uid"]
            )

            try:
                os.makedirs(local_path, exist_ok=True)
            except Exception as e:
                log.error(f"Failed preparing local folder: {e}")
                continue

            # Caminho final do arquivo
            dest_file = os.path.join(local_path, fname)

            # ---------------------------------------------------------
            # 5) Mover arquivo físico
            # ---------------------------------------------------------
            try:
                shutil.move(full_path, dest_file)
            except Exception as e:
                log.error(f"[FAIL MOVING] {fname}: {e}")
                continue

            log.entry(f"[MOVE] {full_path} → {dest_file} (HOST {host_id})")

            # ---------------------------------------------------------
            # 6) Atualizar FILE_TASK (DISCOVERY)
            # ---------------------------------------------------------
            try:
                db.file_task_update(
                    task_id=task["ID_FILE_TASK"],
                    NU_TYPE=k.FILE_TASK_BACKUP_TYPE,
                    NU_STATUS=k.TASK_DONE,
                    NA_SERVER_FILE_PATH=local_path,
                    NA_SERVER_FILE_NAME=fname,
                    NA_MESSAGE=sh._compose_message(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        task_status=k.TASK_DONE,
                        path=task["NA_HOST_FILE_PATH"],
                        name=task["NA_HOST_FILE_NAME"],
                    ),
                )
            except Exception as e:
                log.error(f"Failed updating FILE_TASK {task['ID_FILE_TASK']}: {e}")

            # ---------------------------------------------------------
            # 7) Atualizar FILE_TASK_HISTORY (DISCOVERY)
            # ---------------------------------------------------------
            if history:
                try:
                    db.file_history_update(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        file_name=fname,
                        host_id=host_id,
                        NA_SERVER_FILE_PATH=local_path,
                        NA_SERVER_FILE_NAME=fname,
                        DT_BACKUP=datetime.now(),
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["NA_HOST_FILE_PATH"],
                            name=task["NA_HOST_FILE_NAME"],
                        )
                    )
                except Exception as e:
                    log.error(f"Failed updating FILE_TASK_HISTORY: {e}")

    log.entry("=== MIGRATION COMPLETE ===")


if __name__ == "__main__":
    migrate()
