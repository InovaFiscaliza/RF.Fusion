(function () {
    const pageRoot = document.getElementById("maintenance-page-root");
    if (!pageRoot) {
        return;
    }

    const queueKindSelect = document.getElementById("queue_kind");
    const taskTypeSelect = document.getElementById("task_type");
    const selectAllCheckbox = document.getElementById("maintenance-select-all");
    const rowCheckboxes = Array.from(document.querySelectorAll(".maintenance-row-checkbox"));
    const actionForm = document.getElementById("maintenance-action-form");
    const historySelectAllCheckbox = document.getElementById("maintenance-history-select-all");
    const historyRowCheckboxes = Array.from(document.querySelectorAll(".maintenance-history-row-checkbox"));
    const historyActionForm = document.getElementById("maintenance-history-action-form");
    const historyFilterForm = document.getElementById("maintenance-history-filter-form");
    const tableFilterInput = document.getElementById("maintenance-table-filter");
    const tableFilterCount = document.getElementById("maintenance-table-filter-count");
    const tableRows = Array.from(document.querySelectorAll(".maintenance-table tbody tr"));
    const hostTaskTypes = JSON.parse(pageRoot.dataset.hostTaskTypes || "{}");
    const fileTaskTypes = JSON.parse(pageRoot.dataset.fileTaskTypes || "{}");

    function getTaskTypeOptions(queueKind) {
        return queueKind === "file" ? fileTaskTypes : hostTaskTypes;
    }

    function rebuildTaskTypeOptions() {
        if (!queueKindSelect || !taskTypeSelect) {
            return;
        }

        const previousValue = taskTypeSelect.value;
        const options = getTaskTypeOptions(queueKindSelect.value);
        const optionEntries = Object.entries(options).sort(function (left, right) {
            return Number(left[0]) - Number(right[0]);
        });

        taskTypeSelect.innerHTML = "";
        taskTypeSelect.appendChild(new Option("Todos", "all"));

        optionEntries.forEach(function (entry) {
            const option = new Option(entry[1], entry[0]);
            taskTypeSelect.appendChild(option);
        });

        const hasPreviousValue = previousValue === "all" || Object.prototype.hasOwnProperty.call(options, previousValue);
        taskTypeSelect.value = hasPreviousValue ? previousValue : "all";
    }

    function syncSelectAllState() {
        if (!selectAllCheckbox) {
            return;
        }

        const visibleCheckboxes = rowCheckboxes.filter(function (checkbox) {
            const row = checkbox.closest("tr");
            return row && !row.hidden;
        });
        const checkedCount = visibleCheckboxes.filter((checkbox) => checkbox.checked).length;
        selectAllCheckbox.checked = visibleCheckboxes.length > 0 && checkedCount === visibleCheckboxes.length;
        selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < visibleCheckboxes.length;
    }

    function syncHistorySelectAllState() {
        if (!historySelectAllCheckbox) {
            return;
        }

        const visibleCheckboxes = historyRowCheckboxes.filter(function (checkbox) {
            const row = checkbox.closest("tr");
            return row && !row.hidden;
        });
        const checkedCount = visibleCheckboxes.filter((checkbox) => checkbox.checked).length;
        historySelectAllCheckbox.checked = visibleCheckboxes.length > 0 && checkedCount === visibleCheckboxes.length;
        historySelectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < visibleCheckboxes.length;
    }

    function applyTableFilter() {
        if (!tableFilterInput) {
            return;
        }

        const normalizedFilter = tableFilterInput.value.trim().toLowerCase();
        let visibleCount = 0;

        tableRows.forEach(function (row) {
            const isEmptyRow = row.querySelector(".maintenance-empty-cell");
            if (isEmptyRow) {
                row.hidden = normalizedFilter !== "";
                return;
            }

            const normalizedText = row.textContent.toLowerCase();
            const shouldShow = normalizedFilter === "" || normalizedText.indexOf(normalizedFilter) !== -1;
            row.hidden = !shouldShow;
            if (shouldShow) {
                visibleCount += 1;
            }
        });

        if (tableFilterCount) {
            tableFilterCount.textContent = visibleCount + " linha(s) visíveis";
        }

        syncSelectAllState();
    }

    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener("change", function () {
            rowCheckboxes.forEach(function (checkbox) {
                const row = checkbox.closest("tr");
                if (row && row.hidden) {
                    return;
                }
                checkbox.checked = selectAllCheckbox.checked;
            });
            syncSelectAllState();
        });
    }

    rowCheckboxes.forEach(function (checkbox) {
        checkbox.addEventListener("change", syncSelectAllState);
    });

    if (historySelectAllCheckbox) {
        historySelectAllCheckbox.addEventListener("change", function () {
            historyRowCheckboxes.forEach(function (checkbox) {
                const row = checkbox.closest("tr");
                if (row && row.hidden) {
                    return;
                }
                checkbox.checked = historySelectAllCheckbox.checked;
            });
            syncHistorySelectAllState();
        });
    }

    historyRowCheckboxes.forEach(function (checkbox) {
        checkbox.addEventListener("change", syncHistorySelectAllState);
    });

    if (queueKindSelect) {
        queueKindSelect.addEventListener("change", rebuildTaskTypeOptions);
    }

    if (tableFilterInput) {
        tableFilterInput.addEventListener("input", applyTableFilter);
    }

    if (actionForm) {
        actionForm.addEventListener("submit", function (event) {
            const submitter = event.submitter;
            const selectedCount = rowCheckboxes.filter((checkbox) => checkbox.checked).length;

            if (selectedCount === 0) {
                event.preventDefault();
                window.alert("Selecione ao menos uma task antes de aplicar a ação.");
                return;
            }

            if (!submitter) {
                return;
            }

            const actionValue = submitter.value === "suspend" ? "suspender" : "reiniciar";
            const confirmed = window.confirm(
                "Confirma " + actionValue + " " + selectedCount + " task(s) selecionada(s)?"
            );

            if (!confirmed) {
                event.preventDefault();
            }
        });
    }

    if (historyActionForm) {
        historyActionForm.addEventListener("submit", function (event) {
            const submitter = event.submitter;
            const selectedCount = historyRowCheckboxes.filter((checkbox) => checkbox.checked).length;

            if (selectedCount === 0) {
                event.preventDefault();
                window.alert("Selecione ao menos uma linha de histórico antes de recriar a FILE_TASK.");
                return;
            }

            if (!submitter) {
                return;
            }

            const actionLabel = submitter.value === "recreate_backup"
                ? "recriar backup"
                : "recriar processamento";
            const confirmed = window.confirm(
                "Confirma " + actionLabel + " para " + selectedCount + " item(ns) do histórico?"
            );

            if (!confirmed) {
                event.preventDefault();
            }
        });
    }

    if (historyFilterForm) {
        historyFilterForm.addEventListener("submit", function (event) {
            const hostNameInput = document.getElementById("history_host_name");
            const hostFileNameInput = document.getElementById("history_host_file_name");
            const serverFileNameInput = document.getElementById("history_server_file_name");
            const messageInput = document.getElementById("history_message");
            const dateFromInput = document.getElementById("history_date_from");
            const dateToInput = document.getElementById("history_date_to");
            const hasAnchoredFilter = [
                hostNameInput,
                hostFileNameInput,
                serverFileNameInput,
                messageInput,
                dateFromInput,
                dateToInput,
            ].some(function (input) {
                return input && input.value.trim() !== "";
            });

            if (!hasAnchoredFilter) {
                event.preventDefault();
                window.alert(
                    "Informe ao menos um filtro no histórico antes de consultar: host, arquivo, mensagem ou faixa de data."
                );
                return;
            }

            if (typeof window.showPageLoadingOverlay === "function") {
                window.showPageLoadingOverlay("Consultando candidatos de recriação no histórico...");
            }
        });
    }

    rebuildTaskTypeOptions();
    applyTableFilter();
    syncSelectAllState();
    syncHistorySelectAllState();
})();
