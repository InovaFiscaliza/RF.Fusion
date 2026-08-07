(function () {
    const pageRoot = document.getElementById("maintenance-page-root");
    if (!pageRoot) {
        return;
    }

    const panelStorageKey = "webfusion.maintenance.panels.v1";

    function showLoading(message) {
        if (typeof window.showPageLoadingOverlay === "function") {
            window.showPageLoadingOverlay(message);
        }
    }

    function savePanelState() {
        const panelState = {};
        document.querySelectorAll("[data-maintenance-panel]").forEach(function (panel) {
            panelState[panel.dataset.maintenancePanel] = panel.open;
        });

        try {
            window.localStorage.setItem(panelStorageKey, JSON.stringify(panelState));
        } catch (error) {
            return;
        }
    }

    function restorePanelState() {
        let panelState = {};

        try {
            panelState = JSON.parse(window.localStorage.getItem(panelStorageKey) || "{}") || {};
        } catch (error) {
            panelState = {};
        }

        document.querySelectorAll("[data-maintenance-panel]").forEach(function (panel) {
            const panelName = panel.dataset.maintenancePanel;
            if (typeof panelState[panelName] === "boolean") {
                panel.open = panelState[panelName];
            }
            panel.addEventListener("toggle", savePanelState);
        });
    }

    function bindTaskPanel(config) {
        const actionForm = document.getElementById(config.actionFormId);
        const selectAllCheckbox = document.getElementById(config.selectAllId);
        const tableFilterInput = document.getElementById(config.tableFilterId);
        const tableFilterCount = document.getElementById(config.tableFilterCountId);
        const rowCheckboxes = actionForm
            ? Array.from(actionForm.querySelectorAll(config.rowCheckboxSelector))
            : [];
        const tableRows = actionForm
            ? Array.from(actionForm.querySelectorAll(".maintenance-table tbody tr"))
            : [];
        const actionSelect = document.getElementById(config.actionSelectId);
        const applyActionButton = document.getElementById(config.applyActionButtonId);
        const actionGuidance = document.getElementById(config.actionGuidanceId);

        function selectedRows() {
            return rowCheckboxes.filter(function (checkbox) {
                return checkbox.checked;
            }).map(function (checkbox) {
                return checkbox.closest(config.rowSelector);
            }).filter(Boolean);
        }

        function setActionOptions(allowedActions, placeholder) {
            if (!actionSelect) {
                return;
            }

            Array.from(actionSelect.options).forEach(function (option, index) {
                if (index === 0) {
                    option.textContent = placeholder;
                    option.disabled = false;
                    return;
                }
                option.disabled = allowedActions.indexOf(option.value) === -1;
            });

            if (actionSelect.value && allowedActions.indexOf(actionSelect.value) === -1) {
                actionSelect.value = "";
            }
            actionSelect.disabled = allowedActions.length === 0;
        }

        function selectedActionLabel() {
            if (!actionSelect || actionSelect.selectedIndex < 0) {
                return "";
            }
            return actionSelect.options[actionSelect.selectedIndex].textContent;
        }

        function refreshActionControls() {
            if (!actionSelect) {
                return;
            }

            const rows = selectedRows();
            if (rows.length === 0) {
                setActionOptions([], "Selecione tarefas primeiro");
                if (applyActionButton) {
                    applyActionButton.disabled = true;
                }
                if (actionGuidance) {
                    actionGuidance.textContent = "Selecione ao menos uma tarefa para ver as ações compatíveis.";
                }
                return;
            }

            const allowedActions = Object.keys(config.actionLabels).filter(function (action) {
                return rows.every(function (row) {
                    return config.isActionAllowed(action, row);
                });
            });
            setActionOptions(allowedActions, "Selecione a ação");

            const canApply = allowedActions.indexOf(actionSelect.value) !== -1;
            if (applyActionButton) {
                applyActionButton.disabled = !canApply;
            }
            if (actionGuidance) {
                if (allowedActions.length === 0) {
                    actionGuidance.textContent = "A seleção combina tipos ou hosts que não possuem uma ação manual comum.";
                } else if (!canApply) {
                    actionGuidance.textContent = "Escolha a ação para aplicar às tarefas selecionadas.";
                } else {
                    actionGuidance.textContent = "A seleção receberá a ação: " + selectedActionLabel() + ".";
                }
            }
        }

        function visibleCheckboxes() {
            return rowCheckboxes.filter(function (checkbox) {
                const row = checkbox.closest("tr");
                return row && !row.hidden;
            });
        }

        function syncSelectAllState() {
            if (!selectAllCheckbox) {
                return;
            }

            const visible = visibleCheckboxes();
            const checkedCount = visible.filter(function (checkbox) {
                return checkbox.checked;
            }).length;
            selectAllCheckbox.checked = visible.length > 0 && checkedCount === visible.length;
            selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < visible.length;
        }

        function applyTableFilter() {
            if (!tableFilterInput || tableFilterInput.disabled) {
                return;
            }

            const normalizedFilter = tableFilterInput.value.trim().toLowerCase();
            let visibleCount = 0;
            tableRows.forEach(function (row) {
                if (row.querySelector(".maintenance-empty-cell")) {
                    row.hidden = normalizedFilter !== "";
                    return;
                }

                const shouldShow = normalizedFilter === ""
                    || row.textContent.toLowerCase().indexOf(normalizedFilter) !== -1;
                row.hidden = !shouldShow;
                if (shouldShow) {
                    visibleCount += 1;
                }
            });

            if (tableFilterCount) {
                tableFilterCount.textContent = visibleCount + " linha(s) visíveis";
            }
            syncSelectAllState();
            refreshActionControls();
        }

        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener("change", function () {
                visibleCheckboxes().forEach(function (checkbox) {
                    checkbox.checked = selectAllCheckbox.checked;
                });
                syncSelectAllState();
                refreshActionControls();
            });
        }

        rowCheckboxes.forEach(function (checkbox) {
            checkbox.addEventListener("change", function () {
                syncSelectAllState();
                refreshActionControls();
            });
        });

        if (tableFilterInput) {
            tableFilterInput.addEventListener("input", applyTableFilter);
        }

        if (actionSelect) {
            actionSelect.addEventListener("change", refreshActionControls);
        }

        if (actionForm) {
            actionForm.addEventListener("submit", function (event) {
                const selectedCount = selectedRows().length;
                if (selectedCount === 0) {
                    event.preventDefault();
                    window.alert("Selecione ao menos uma linha antes de aplicar a ação.");
                    return;
                }

                const submitter = event.submitter;
                const action = actionSelect ? actionSelect.value : (submitter ? submitter.value : "");
                if (!action) {
                    event.preventDefault();
                    window.alert("Selecione a ação antes de aplicar às tarefas.");
                    return;
                }

                const actionLabel = actionSelect
                    ? selectedActionLabel()
                    : (config.actionLabels[action] || "aplicar esta ação");
                if (!window.confirm("Confirma " + actionLabel + " para " + selectedCount + " item(ns)?")) {
                    event.preventDefault();
                    return;
                }

                showLoading(actionForm.dataset.loadingMessage || "Aplicando ação...");
            });
        }

        applyTableFilter();
        syncSelectAllState();
        refreshActionControls();
    }

    function bindFileTaskHostSelector() {
        const hostSelect = document.getElementById("file_task_host_id");
        const toggleButton = document.getElementById("file-task-hosts-toggle");
        if (!hostSelect || !toggleButton) {
            return;
        }

        const allOptions = Array.from(hostSelect.options);
        const defaultLabel = toggleButton.textContent;
        let showingQueueHosts = false;

        function replaceOptions(options) {
            hostSelect.innerHTML = "";
            options.forEach(function (option) {
                hostSelect.appendChild(option);
            });
        }

        function createHostOption(host) {
            const option = document.createElement("option");
            option.value = String(host.ID_HOST);
            option.textContent = host.NA_HOST_NAME + (host.IS_OFFLINE ? " (offline)" : "");
            return option;
        }

        toggleButton.addEventListener("click", async function () {
            if (showingQueueHosts) {
                const selectedHostId = hostSelect.value;
                replaceOptions(allOptions);
                if (allOptions.some(function (option) {
                    return option.value === selectedHostId;
                })) {
                    hostSelect.value = selectedHostId;
                }
                toggleButton.textContent = defaultLabel;
                toggleButton.setAttribute("aria-pressed", "false");
                showingQueueHosts = false;
                return;
            }

            if (!toggleButton.dataset.url || typeof window.fetch !== "function") {
                window.alert("Não foi possível carregar os hosts com tarefas na fila.");
                return;
            }

            const selectedHostId = hostSelect.value;
            toggleButton.disabled = true;
            toggleButton.textContent = "Carregando...";

            try {
                const response = await window.fetch(toggleButton.dataset.url, {
                    headers: { Accept: "application/json" },
                });
                if (!response.ok) {
                    throw new Error("request_failed");
                }

                const payload = await response.json();
                if (!payload || !Array.isArray(payload.hosts)) {
                    throw new Error("invalid_payload");
                }

                const allHostsOption = document.createElement("option");
                allHostsOption.value = "";
                allHostsOption.textContent = "Todos os hosts com tarefas";
                const queueHostOptions = [allHostsOption].concat(
                    payload.hosts.map(createHostOption)
                );
                replaceOptions(queueHostOptions);

                if (queueHostOptions.some(function (option) {
                    return option.value === selectedHostId;
                })) {
                    hostSelect.value = selectedHostId;
                }
                toggleButton.textContent = "Ver todos";
                toggleButton.setAttribute("aria-pressed", "true");
                showingQueueHosts = true;
            } catch (error) {
                toggleButton.textContent = defaultLabel;
                toggleButton.setAttribute("aria-pressed", "false");
                window.alert("Não foi possível carregar os hosts com tarefas na fila.");
            } finally {
                toggleButton.disabled = false;
            }
        });
    }

    function bindFileTaskPanel() {
        const actionForm = document.getElementById("file-task-action-form");
        const selectAllCheckbox = document.getElementById("file-task-select-all");
        const tableFilterInput = document.getElementById("file-task-table-filter");
        const tableFilterCount = document.getElementById("file-task-table-filter-count");
        const targetStageSelect = document.getElementById("file_task_target_stage");
        const targetStatusSelect = document.getElementById("file_task_target_status");
        const applyActionButton = document.getElementById("file-task-apply-action");
        const actionGuidance = document.getElementById("file-task-action-guidance");
        const rowCheckboxes = actionForm
            ? Array.from(actionForm.querySelectorAll(".maintenance-file-task-row-checkbox"))
            : [];
        const tableRows = actionForm
            ? Array.from(actionForm.querySelectorAll(".maintenance-table tbody tr"))
            : [];
        const taskPending = "1";
        const taskRunning = "2";
        const targetBackup = "backup";
        const targetProcess = "process";
        const availableStatuses = [taskPending, "-2", "-3"];

        function selectedRows() {
            return rowCheckboxes.filter(function (checkbox) {
                return checkbox.checked;
            }).map(function (checkbox) {
                return checkbox.closest("[data-file-task-row]");
            }).filter(Boolean);
        }

        function canPrepareBackup(row) {
            return row.dataset.historyPresent === "1" && row.dataset.taskStatus !== taskRunning;
        }

        function canPrepareProcess(row) {
            return row.dataset.historyPresent === "1"
                && row.dataset.taskStatus !== taskRunning
                && row.dataset.serverIdentity === "1";
        }

        function setSelectOptions(select, allowedValues, placeholder) {
            if (!select) {
                return;
            }

            Array.from(select.options).forEach(function (option, index) {
                if (index === 0) {
                    option.textContent = placeholder;
                    option.disabled = false;
                    return;
                }
                option.disabled = allowedValues.indexOf(option.value) === -1;
            });

            if (select.value && allowedValues.indexOf(select.value) === -1) {
                select.value = "";
            }
            select.disabled = allowedValues.length === 0;
        }

        function selectedOptionLabel(select) {
            if (!select || select.selectedIndex < 0) {
                return "";
            }
            return select.options[select.selectedIndex].textContent;
        }

        function refreshActionControls() {
            const rows = selectedRows();
            if (rows.length === 0) {
                setSelectOptions(targetStageSelect, [], "Selecione arquivos primeiro");
                setSelectOptions(targetStatusSelect, [], "Selecione a etapa");
                if (applyActionButton) {
                    applyActionButton.disabled = true;
                }
                if (actionGuidance) {
                    actionGuidance.textContent = "Selecione ao menos um arquivo para ver os destinos compatíveis.";
                }
                return;
            }

            const allowedStages = [];
            if (rows.every(canPrepareBackup)) {
                allowedStages.push(targetBackup);
            }
            if (rows.every(canPrepareProcess)) {
                allowedStages.push(targetProcess);
            }
            setSelectOptions(targetStageSelect, allowedStages, "Selecione a etapa");

            if (allowedStages.length === 0) {
                setSelectOptions(targetStatusSelect, [], "Nenhuma etapa comum");
                if (applyActionButton) {
                    applyActionButton.disabled = true;
                }
                if (actionGuidance) {
                    actionGuidance.textContent = "A seleção inclui tarefa em execução ou sem histórico suficiente. Separe os itens ou escolha outros registros.";
                }
                return;
            }

            const selectedStage = targetStageSelect ? targetStageSelect.value : "";
            let allowedStatuses = [];
            if (selectedStage === targetBackup || selectedStage === targetProcess) {
                allowedStatuses = availableStatuses.slice();
                if (selectedStage === targetBackup && rows.some(function (row) {
                    return row.dataset.hostOffline === "1";
                })) {
                    allowedStatuses = allowedStatuses.filter(function (status) {
                        return status !== taskPending;
                    });
                }
            }
            setSelectOptions(targetStatusSelect, allowedStatuses, "Selecione a situação");

            const canApply = Boolean(
                selectedStage
                && targetStatusSelect
                && allowedStatuses.indexOf(targetStatusSelect.value) !== -1
            );
            if (applyActionButton) {
                applyActionButton.disabled = !canApply;
            }
            if (actionGuidance) {
                if (!selectedStage) {
                    actionGuidance.textContent = "Escolha a etapa de destino para liberar as situações compatíveis.";
                } else if (!canApply) {
                    actionGuidance.textContent = "Escolha a situação inicial para preparar as tarefas selecionadas.";
                } else {
                    actionGuidance.textContent = "A seleção será preparada como "
                        + selectedOptionLabel(targetStageSelect)
                        + " / "
                        + selectedOptionLabel(targetStatusSelect)
                        + ".";
                }
            }
        }

        function visibleCheckboxes() {
            return rowCheckboxes.filter(function (checkbox) {
                const row = checkbox.closest("tr");
                return row && !row.hidden;
            });
        }

        function syncSelectAllState() {
            if (!selectAllCheckbox) {
                return;
            }

            const visible = visibleCheckboxes();
            const checkedCount = visible.filter(function (checkbox) {
                return checkbox.checked;
            }).length;
            selectAllCheckbox.checked = visible.length > 0 && checkedCount === visible.length;
            selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < visible.length;
        }

        function applyTableFilter() {
            if (!tableFilterInput || tableFilterInput.disabled) {
                return;
            }

            const normalizedFilter = tableFilterInput.value.trim().toLowerCase();
            let visibleCount = 0;
            tableRows.forEach(function (row) {
                if (row.querySelector(".maintenance-empty-cell")) {
                    row.hidden = normalizedFilter !== "";
                    return;
                }

                const shouldShow = normalizedFilter === ""
                    || row.textContent.toLowerCase().indexOf(normalizedFilter) !== -1;
                row.hidden = !shouldShow;
                if (shouldShow) {
                    visibleCount += 1;
                }
            });

            if (tableFilterCount) {
                tableFilterCount.textContent = visibleCount + " linha(s) visíveis";
            }
            syncSelectAllState();
            refreshActionControls();
        }

        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener("change", function () {
                visibleCheckboxes().forEach(function (checkbox) {
                    checkbox.checked = selectAllCheckbox.checked;
                });
                syncSelectAllState();
                refreshActionControls();
            });
        }

        rowCheckboxes.forEach(function (checkbox) {
            checkbox.addEventListener("change", function () {
                syncSelectAllState();
                refreshActionControls();
            });
        });

        if (tableFilterInput) {
            tableFilterInput.addEventListener("input", applyTableFilter);
        }
        if (targetStageSelect) {
            targetStageSelect.addEventListener("change", refreshActionControls);
        }
        if (targetStatusSelect) {
            targetStatusSelect.addEventListener("change", refreshActionControls);
        }
        if (actionForm) {
            actionForm.addEventListener("submit", function (event) {
                const selectedCount = selectedRows().length;
                if (selectedCount === 0) {
                    event.preventDefault();
                    window.alert("Selecione ao menos um arquivo antes de preparar tarefas.");
                    return;
                }

                if (!targetStageSelect || !targetStatusSelect || !targetStageSelect.value || !targetStatusSelect.value) {
                    event.preventDefault();
                    window.alert("Selecione a etapa de destino e a situação inicial.");
                    return;
                }

                const actionLabel = selectedOptionLabel(targetStageSelect)
                    + " / "
                    + selectedOptionLabel(targetStatusSelect);
                if (!window.confirm("Confirma preparar " + selectedCount + " item(ns) como " + actionLabel + "?")) {
                    event.preventDefault();
                    return;
                }

                showLoading(actionForm.dataset.loadingMessage || "Preparando tarefas...");
            });
        }

        applyTableFilter();
        syncSelectAllState();
        refreshActionControls();
    }

    function bindHistoryPanel() {
        const actionForm = document.getElementById("maintenance-history-action-form");
        const selectAllCheckbox = document.getElementById("maintenance-history-select-all");
        const targetStageSelect = document.getElementById("history_target_stage");
        const targetStatusSelect = document.getElementById("history_target_status");
        const applyActionButton = document.getElementById("maintenance-history-apply-action");
        const actionGuidance = document.getElementById("maintenance-history-action-guidance");
        const rowCheckboxes = actionForm
            ? Array.from(actionForm.querySelectorAll(".maintenance-history-row-checkbox"))
            : [];
        const historyFilterForm = document.getElementById("maintenance-history-filter-form");
        const taskDone = "0";
        const taskPending = "1";
        const taskRunning = "2";
        const targetBackup = "backup";
        const targetProcess = "process";
        const availableStatuses = [taskPending, "-2", "-3"];

        function selectedRows() {
            return rowCheckboxes.filter(function (checkbox) {
                return checkbox.checked;
            }).map(function (checkbox) {
                return checkbox.closest("[data-history-row]");
            }).filter(Boolean);
        }

        function canPrepareBackup(row) {
            return row.dataset.activeTask === "0"
                && row.dataset.discoveryStatus === taskDone
                && row.dataset.backupStatus !== taskRunning;
        }

        function canPrepareProcess(row) {
            return row.dataset.activeTask === "0"
                && row.dataset.backupStatus === taskDone
                && row.dataset.processingStatus !== taskRunning
                && row.dataset.serverIdentity === "1";
        }

        function setSelectOptions(select, allowedValues, placeholder) {
            if (!select) {
                return;
            }

            Array.from(select.options).forEach(function (option, index) {
                if (index === 0) {
                    option.textContent = placeholder;
                    option.disabled = false;
                    return;
                }
                option.disabled = allowedValues.indexOf(option.value) === -1;
            });

            if (select.value && allowedValues.indexOf(select.value) === -1) {
                select.value = "";
            }
            select.disabled = allowedValues.length === 0;
        }

        function selectedOptionLabel(select) {
            if (!select || select.selectedIndex < 0) {
                return "";
            }
            return select.options[select.selectedIndex].textContent;
        }

        function refreshActionControls() {
            const rows = selectedRows();
            if (rows.length === 0) {
                setSelectOptions(targetStageSelect, [], "Selecione registros primeiro");
                setSelectOptions(targetStatusSelect, [], "Selecione a etapa");
                if (applyActionButton) {
                    applyActionButton.disabled = true;
                }
                if (actionGuidance) {
                    actionGuidance.textContent = "Selecione ao menos um registro para ver as ações compatíveis.";
                }
                return;
            }

            const allowedStages = [];
            if (rows.every(canPrepareBackup)) {
                allowedStages.push(targetBackup);
            }
            if (rows.every(canPrepareProcess)) {
                allowedStages.push(targetProcess);
            }
            setSelectOptions(targetStageSelect, allowedStages, "Selecione a etapa");

            if (allowedStages.length === 0) {
                setSelectOptions(targetStatusSelect, [], "Nenhuma etapa comum");
                if (applyActionButton) {
                    applyActionButton.disabled = true;
                }
                if (actionGuidance) {
                    actionGuidance.textContent = "A seleção não possui uma etapa comum. Separe registros em pontos diferentes do ciclo ou retire itens que já estão na fila.";
                }
                return;
            }

            const selectedStage = targetStageSelect ? targetStageSelect.value : "";
            let allowedStatuses = [];
            if (selectedStage === targetBackup || selectedStage === targetProcess) {
                allowedStatuses = availableStatuses.slice();
                if (selectedStage === targetBackup && rows.some(function (row) {
                    return row.dataset.hostOffline === "1";
                })) {
                    allowedStatuses = allowedStatuses.filter(function (status) {
                        return status !== taskPending;
                    });
                }
            }
            setSelectOptions(targetStatusSelect, allowedStatuses, "Selecione a situação");

            const canApply = Boolean(
                selectedStage
                && targetStatusSelect
                && allowedStatuses.indexOf(targetStatusSelect.value) !== -1
            );
            if (applyActionButton) {
                applyActionButton.disabled = !canApply;
            }
            if (actionGuidance) {
                if (!selectedStage) {
                    actionGuidance.textContent = "Escolha a etapa de destino para liberar as situações compatíveis.";
                } else if (!canApply) {
                    actionGuidance.textContent = "Escolha a situação inicial para preparar as tarefas selecionadas.";
                } else {
                    actionGuidance.textContent = "A seleção será preparada como "
                        + selectedOptionLabel(targetStageSelect)
                        + " / "
                        + selectedOptionLabel(targetStatusSelect)
                        + ".";
                }
            }
        }

        function syncSelectAllState() {
            if (!selectAllCheckbox) {
                return;
            }

            const visible = rowCheckboxes.filter(function (checkbox) {
                const row = checkbox.closest("tr");
                return row && !row.hidden;
            });
            const checkedCount = visible.filter(function (checkbox) {
                return checkbox.checked;
            }).length;
            selectAllCheckbox.checked = visible.length > 0 && checkedCount === visible.length;
            selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < visible.length;
        }

        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener("change", function () {
                rowCheckboxes.forEach(function (checkbox) {
                    const row = checkbox.closest("tr");
                    if (row && !row.hidden) {
                        checkbox.checked = selectAllCheckbox.checked;
                    }
                });
                syncSelectAllState();
                refreshActionControls();
            });
        }

        rowCheckboxes.forEach(function (checkbox) {
            checkbox.addEventListener("change", function () {
                syncSelectAllState();
                refreshActionControls();
            });
        });

        if (targetStageSelect) {
            targetStageSelect.addEventListener("change", refreshActionControls);
        }

        if (targetStatusSelect) {
            targetStatusSelect.addEventListener("change", refreshActionControls);
        }

        if (actionForm) {
            actionForm.addEventListener("submit", function (event) {
                const selectedCount = selectedRows().length;
                if (selectedCount === 0) {
                    event.preventDefault();
                    window.alert("Selecione ao menos uma linha de histórico antes de preparar tarefas.");
                    return;
                }

                if (!targetStageSelect || !targetStatusSelect || !targetStageSelect.value || !targetStatusSelect.value) {
                    event.preventDefault();
                    window.alert("Selecione a etapa de destino e a situação inicial.");
                    return;
                }

                const actionLabel = selectedOptionLabel(targetStageSelect)
                    + " / "
                    + selectedOptionLabel(targetStatusSelect);
                if (!window.confirm("Confirma preparar " + selectedCount + " item(ns) como " + actionLabel + "?")) {
                    event.preventDefault();
                    return;
                }

                showLoading(actionForm.dataset.loadingMessage || "Preparando tarefas...");
            });
        }

        if (historyFilterForm) {
            historyFilterForm.addEventListener("submit", function (event) {
                const historyHostSelect = document.getElementById("history_host_id");
                const hostFileNameInput = document.getElementById("history_host_file_name");
                const serverFileNameInput = document.getElementById("history_server_file_name");
                const dateFieldInput = document.getElementById("history_date_field");
                const dateFromInput = document.getElementById("history_date_from");
                const dateToInput = document.getElementById("history_date_to");
                const hasIdentityFilter = [
                    historyHostSelect,
                    hostFileNameInput,
                    serverFileNameInput,
                ].some(function (input) {
                    return input && input.value.trim() !== "";
                });
                const hasDateBoundary = [dateFromInput, dateToInput].some(function (input) {
                    return input && input.value.trim() !== "";
                });

                if (hasDateBoundary && (!dateFieldInput || dateFieldInput.value === "")) {
                    event.preventDefault();
                    window.alert("Selecione o campo de data antes de informar uma faixa de datas.");
                    return;
                }

                if (
                    dateFromInput
                    && dateToInput
                    && dateFromInput.value !== ""
                    && dateToInput.value !== ""
                    && dateFromInput.value >= dateToInput.value
                ) {
                    event.preventDefault();
                    window.alert("A data final exclusiva deve ser posterior à data inicial.");
                    return;
                }

                if (!hasIdentityFilter) {
                    event.preventDefault();
                    window.alert("Selecione um host ou informe o nome completo de um arquivo. Data e mensagem apenas refinam esses filtros.");
                    return;
                }

                showLoading(historyFilterForm.dataset.loadingMessage || "Consultando histórico...");
            });
        }

        syncSelectAllState();
        refreshActionControls();
    }

    document.querySelectorAll(".maintenance-filter-form").forEach(function (form) {
        if (form.id === "maintenance-history-filter-form") {
            return;
        }
        form.addEventListener("submit", function (event) {
            const dateFieldInput = document.getElementById("file_task_date_field");
            const dateFromInput = document.getElementById("file_task_date_from");
            const dateToInput = document.getElementById("file_task_date_to");
            const hasDateBoundary = [dateFromInput, dateToInput].some(function (input) {
                return input && input.value.trim() !== "";
            });

            if (hasDateBoundary && (!dateFieldInput || dateFieldInput.value === "")) {
                event.preventDefault();
                window.alert("Selecione o campo de data antes de informar uma faixa de datas.");
                return;
            }

            if (
                dateFromInput
                && dateToInput
                && dateFromInput.value !== ""
                && dateToInput.value !== ""
                && dateFromInput.value >= dateToInput.value
            ) {
                event.preventDefault();
                window.alert("A data final exclusiva deve ser posterior à data inicial.");
                return;
            }

            showLoading(form.dataset.loadingMessage || "Consultando tarefas...");
        });
    });

    restorePanelState();
    bindFileTaskHostSelector();
    bindTaskPanel({
        actionFormId: "host-task-action-form",
        selectAllId: "host-task-select-all",
        rowCheckboxSelector: ".maintenance-host-task-row-checkbox",
        tableFilterId: "host-task-table-filter",
        tableFilterCountId: "host-task-table-filter-count",
        actionSelectId: "host_task_action",
        applyActionButtonId: "host-task-apply-action",
        actionGuidanceId: "host-task-action-guidance",
        rowSelector: "[data-host-task-row]",
        actionLabels: {
            restart: "reiniciar",
            suspend: "suspender",
        },
        isActionAllowed: function (action, row) {
            const hostDependentTypes = ["1", "2", "4"];
            const isHostDependent = hostDependentTypes.indexOf(row.dataset.taskType) !== -1;
            if (action === "restart") {
                return row.dataset.hostOffline !== "1" || !isHostDependent;
            }
            return isHostDependent;
        },
    });
    bindFileTaskPanel();
    bindHistoryPanel();
})();
