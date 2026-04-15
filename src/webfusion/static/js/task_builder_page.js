/* Task builder controller
 *
 * This file owns the interactive behavior of `/task`:
 * - keeping the "Apenas online" filter synced with the query string
 * - switching between individual and collective execution flows
 * - adapting visible filter fields based on task type and filter mode
 * - managing collective host selection/search
 * - showing the confirmation dialog before submit
 *
 * The template remains responsible for initial form values and for exposing
 * small pieces of state such as the rollback task type and selected hosts.
 */
(function () {
    const root = document.getElementById("task-builder-root");

    if (!root) {
        return;
    }

    const taskType = document.querySelector("[name='task_type']");
    const executionType = document.querySelector("[name='execution_type']");
    const hostSelect = document.querySelector("[name='host_id']");
    const hostFilterSelect = document.querySelector("[name='host_filter']");
    const modeSelect = document.querySelector("[name='mode']");
    const filePathInput = document.querySelector("[name='file_path']");
    const extensionInput = document.querySelector("[name='extension']");
    const onlineOnlyCheckbox = document.querySelector("[name='online_only']");
    const collectiveHostsSelect = document.getElementById("collective-hosts-select");
    const collectiveHostsWrapper = document.getElementById("collective-hosts-wrapper");
    const collectiveHostSearch = document.getElementById("collective-host-search");
    const submitButton = document.getElementById("task-submit-button");
    const taskBuilderForm = document.getElementById("task-builder-form");
    const confirmationDialog = document.getElementById("task-confirm-dialog");
    const confirmationCancelButton = document.getElementById("task-confirm-cancel");
    const confirmationSubmitButton = document.getElementById("task-confirm-submit");
    const confirmationType = document.getElementById("task-confirm-type");
    const confirmationExecution = document.getElementById("task-confirm-execution");
    const confirmationScope = document.getElementById("task-confirm-scope");
    const confirmationFilter = document.getElementById("task-confirm-filter");
    const taskTypeNoteTitle = document.getElementById("task-type-note-title");
    const taskTypeNote = document.getElementById("task-type-note");

    const filterSection = document.getElementById("filter-section");
    const individualConfigPanel = document.getElementById("individual-config-panel");
    const collectiveConfigPanel = document.getElementById("collective-config-panel");
    const stationProfilesPanel = document.getElementById("station-profiles-panel");
    const hostWrapper = document.getElementById("host-wrapper");
    const stationTypeWrapper = document.getElementById("station-type-wrapper");

    const filterModeTitle = document.getElementById("filter-mode-title");
    const filterModeNote = document.getElementById("filter-mode-note");
    const lastDiscoveryShell = document.getElementById("last-discovery-shell");
    const lastDiscoveryValue = document.getElementById("last-discovery-value");
    const lastDiscoveryNote = document.getElementById("last-discovery-note");
    const startWrapper = document.getElementById("start-date-wrapper");
    const endWrapper = document.getElementById("end-date-wrapper");
    const lastNWrapper = document.getElementById("last-n-wrapper");
    const fileNameWrapper = document.getElementById("file-name-wrapper");
    const maxTotalWrapper = document.getElementById("max-total-wrapper");
    const sortOrderWrapper = document.getElementById("sort-order-wrapper");

    if (!taskType || !executionType || !modeSelect || !taskBuilderForm) {
        return;
    }

    const noneOption = modeSelect.querySelector("option[value='NONE']");
    const rediscoveryOption = modeSelect.querySelector("option[value='REDISCOVERY']");
    const fileOption = modeSelect.querySelector("option[value='FILE']");

    const defaultFilePath = "/mnt/internal/data";
    const cwsmFilePath = "C:/CelPlan/CellWireless RU/Spectrum/Completed";
    const defaultExtension = ".bin";
    const cwsmExtension = ".zip";

    /* Task types 3 and 4 are utility tasks ("Atualizar estatísticas" and
     * "Verificar conexão"), so they intentionally bypass the detailed filter
     * parameter controls used by backlog-oriented tasks. */
    const FILTERLESS_TASK_TYPES = new Set(["3", "4"]);
    const stopTaskType = String(root.dataset.stopTaskType || "");
    const selectedCollectiveHostIds = new Set(
        JSON.parse(root.dataset.selectedCollectiveHostIds || "[]").map((value) => String(value))
    );
    const filterModeMeta = {
        NONE: {
            title: "Descoberta",
            note: "Usa o caminho e a extensão informados e continua a descoberta a partir da última descoberta registrada no host."
        },
        ALL: {
            title: "Cobertura completa",
            note: "A tarefa será criada sobre o conjunto completo dentro do caminho e extensão informados."
        },
        REDISCOVERY: {
            title: "Redescoberta",
            note: "Ignora a última descoberta registrada no host e varre novamente todo o caminho informado."
        },
        RANGE: {
            title: "Janela por período",
            note: "Defina a data inicial e a data final para limitar os arquivos considerados."
        },
        LAST: {
            title: "Recorte pelos últimos arquivos",
            note: "Informe quantos arquivos mais recentes devem entrar na tarefa."
        },
        FILE: {
            title: "Arquivo específico",
            note: "Informe o nome exato do arquivo dentro do caminho base selecionado."
        }
    };
    const hostCatalog = Array.from(hostSelect?.options || [])
        .filter((option) => option.value)
        .map((option) => ({
            id: String(option.value),
            name: option.dataset.hostName || option.textContent || "",
        }));
    let submitConfirmed = false;

    /* Host families are inferred from the alphabetical prefix because the
     * builder uses that lightweight classification to:
     * - suggest default path/extension pairs,
     * - decide whether a collective selection is homogeneous or mixed,
     * - reveal the family-profile editor only when it really matters.
     */
    function extractHostPrefix(hostName) {
        const match = String(hostName || "").trim().match(/^[A-Za-z]+/);
        return match ? match[0].toUpperCase() : "";
    }

    /* The online-only toggle is part of the page state, not a final task
     * submission field. We rebuild the query string so the builder refreshes
     * while preserving the current visible choices. */
    function handleOnlineOnlyFilterToggle() {
        if (!onlineOnlyCheckbox) {
            return;
        }

        const url = new URL(window.location.href);
        url.search = "";

        const formData = new FormData(taskBuilderForm);
        formData.set("online_only", onlineOnlyCheckbox.checked ? "1" : "0");

        for (const [key, value] of formData.entries()) {
            if (key === "collective_host_ids") {
                continue;
            }

            if (value === null || value === undefined) {
                continue;
            }

            const text = String(value).trim();
            if (!text) {
                continue;
            }

            url.searchParams.append(key, text);
        }

        selectedCollectiveHostIds.forEach((hostId) => {
            const text = String(hostId).trim();
            if (text) {
                url.searchParams.append("collective_host_ids", text);
            }
        });

        window.location.href = url.toString();
    }

    function syncCollectiveSelectionState() {
        if (!collectiveHostsSelect) {
            return;
        }

        /* The visible `<select multiple>` is only one projection of the true
         * collective selection state. Because the host list can be filtered by
         * family and free-text search, we keep the canonical selection in a
         * `Set` and reconcile the currently visible options back into it.
         */
        const visibleIds = Array.from(collectiveHostsSelect.options).map((option) => option.value);
        visibleIds.forEach((id) => selectedCollectiveHostIds.delete(String(id)));

        Array.from(collectiveHostsSelect.selectedOptions).forEach((option) => {
            selectedCollectiveHostIds.add(String(option.value));
        });
    }

    function renderCollectiveHosts() {
        if (!collectiveHostsSelect) {
            return;
        }

        /* Collective host rendering is rebuilt from the full catalog every
         * time because two independent filters act on the same list:
         * - family/prefix selection
         * - free-text host search
         *
         * Re-rendering from the canonical catalog is simpler and safer than
         * trying to mutate a previously filtered DOM subset in place.
         */
        syncCollectiveSelectionState();

        const selectedPrefix = hostFilterSelect ? hostFilterSelect.value : "ALL";
        const searchTerm = String(collectiveHostSearch ? collectiveHostSearch.value : "").trim().toLowerCase();

        const filteredHosts = hostCatalog.filter((host) => {
            if (selectedPrefix !== "ALL" && extractHostPrefix(host.name) !== selectedPrefix.toUpperCase()) {
                return false;
            }

            if (searchTerm && !host.name.toLowerCase().includes(searchTerm)) {
                return false;
            }

            return true;
        });

        collectiveHostsSelect.innerHTML = "";

        filteredHosts.forEach((host) => {
            const option = document.createElement("option");
            option.value = host.id;
            option.textContent = host.name;
            option.selected = selectedCollectiveHostIds.has(host.id);
            collectiveHostsSelect.appendChild(option);
        });
    }

    /* The builder derives a coarse "selection profile" so it can decide when
     * it is safe to suggest one default path/extension and when it should back
     * off because the current collective scope mixes incompatible families.
     */
    function currentSelectionProfile() {
        if (executionType.value === "collective") {
            const selectedHosts = hostCatalog.filter((host) => selectedCollectiveHostIds.has(host.id));

            if (selectedHosts.length > 0) {
                const prefixes = new Set(
                    selectedHosts
                        .map((host) => extractHostPrefix(host.name))
                        .filter(Boolean)
                );

                if (prefixes.size === 1 && prefixes.has("CWSM")) {
                    return "cwsm";
                }

                if (prefixes.size <= 1) {
                    return "default";
                }

                return "mixed";
            }

            if (hostFilterSelect && hostFilterSelect.value === "CWSM") {
                return "cwsm";
            }

            if (hostFilterSelect && hostFilterSelect.value !== "ALL") {
                return "default";
            }

            return "mixed";
        }

        const selected = hostSelect ? hostSelect.selectedOptions[0] : null;
        const hostName = selected ? (selected.dataset.hostName || selected.textContent || "") : "";

        return hostName.trim().toUpperCase().startsWith("CWSM") ? "cwsm" : "default";
    }

    /* Suggested path defaults are intentionally conservative.
     *
     * The builder only overwrites the field when the current value still looks
     * like a system default (or empty). As soon as the operator types a custom
     * path, the script stops "helping" so the form does not fight manual
     * input.
     */
    function syncSuggestedFilePath() {
        if (!filePathInput) {
            return;
        }

        const selectionProfile = currentSelectionProfile();
        const currentValue = (filePathInput.value || "").trim();
        const followsDefaultMask = currentValue === ""
            || currentValue === defaultFilePath
            || currentValue === cwsmFilePath;

        if (!followsDefaultMask) {
            return;
        }

        if (selectionProfile === "mixed") {
            filePathInput.value = "";
            return;
        }

        filePathInput.value = selectionProfile === "cwsm" ? cwsmFilePath : defaultFilePath;
    }

    /* Extension suggestions follow the same contract as the path: they are a
     * convenience for common families, not a hard rule. Mixed collective
     * scopes intentionally clear the field so the operator is forced to make
     * the ambiguity explicit instead of inheriting a misleading default.
     */
    function syncSuggestedExtension() {
        if (!extensionInput) {
            return;
        }

        const selectionProfile = currentSelectionProfile();
        const currentValue = (extensionInput.value || "").trim().toLowerCase();
        const followsDefaultMask = currentValue === ""
            || currentValue === defaultExtension
            || currentValue === cwsmExtension;

        if (!followsDefaultMask) {
            return;
        }

        if (selectionProfile === "mixed") {
            extensionInput.value = "";
            return;
        }

        extensionInput.value = selectionProfile === "cwsm" ? cwsmExtension : defaultExtension;
    }

    function updateSubmitButtonLabel() {
        if (!submitButton) {
            return;
        }

        // A tiny copy change reinforces whether the current draft fans out
        // into one task or many tasks across a collective scope.
        submitButton.textContent = executionType.value === "collective"
            ? "Criar Tarefas"
            : "Criar Tarefa";
    }

    /* Family profiles are only relevant in the "collective + ALL" scenario,
     * because that is the only moment where one draft task can expand into
     * multiple station families that each need their own discovery defaults.
     */
    function toggleStationProfilesPanel() {
        if (!stationProfilesPanel || !hostFilterSelect) {
            return;
        }

        const showProfiles = executionType.value === "collective" && hostFilterSelect.value === "ALL";
        stationProfilesPanel.hidden = !showProfiles;
    }

    function getSelectedHostLastDiscovery() {
        if (!hostSelect || !hostSelect.selectedOptions || !hostSelect.selectedOptions[0]) {
            return "-";
        }

        const selectedOption = hostSelect.selectedOptions[0];
        return String(selectedOption.dataset.lastDiscovery || "").trim() || "-";
    }

    /* The read-only `DT_LAST_DISCOVERY` context exists to teach the semantic
     * difference between Descoberta and Redescoberta. It is intentionally tied
     * only to individual execution, where one concrete host record is in play.
     */
    function syncLastDiscoveryContext() {
        if (!lastDiscoveryShell || !lastDiscoveryValue || !lastDiscoveryNote) {
            return;
        }

        const visibleForMode = ["NONE", "REDISCOVERY"].includes(String(modeSelect.value || "").toUpperCase());
        const visibleForExecution = executionType.value === "individual";
        const visible = visibleForMode && visibleForExecution;

        lastDiscoveryShell.hidden = !visible;

        if (!visible) {
            return;
        }

        lastDiscoveryValue.textContent = getSelectedHostLastDiscovery();

        if (String(modeSelect.value || "").toUpperCase() === "REDISCOVERY") {
            lastDiscoveryNote.innerHTML = "No modo <strong>Redescoberta</strong>, este marco é ignorado e a varredura recomeça do zero.";
            return;
        }

        lastDiscoveryNote.innerHTML = "No modo <strong>Descoberta</strong>, a descoberta continua a partir deste marco.";
    }

    /* Task type changes alter the meaning of the entire builder. This helper
     * keeps the explanatory card in sync so low-level task codes are always
     * translated into an operational sentence before the user submits.
     */
    function updateTaskTypeNote() {
        if (!taskTypeNote) {
            return;
        }

        const isStop = String(taskType.value) === stopTaskType;

        if (isStop) {
            if (taskTypeNoteTitle) {
                taskTypeNoteTitle.textContent = "Retirada da fila de backup";
            }

            taskTypeNote.textContent = "Retira da fila os arquivos que ainda estão em BACKUP/PENDING e os devolve para DISCOVERY/DONE dentro do filtro selecionado.";
            return;
        }

        if (taskTypeNoteTitle) {
            taskTypeNoteTitle.textContent = "Fluxo normal de backup";
        }

        taskTypeNote.textContent = "Cria a solicitação normal de backup. A estação passa por verificação do host, descoberta e, depois, o backlog elegível entra na fila de backup.";
    }

    /* Wrapper visibility and control enablement move together. Hiding a field
     * is not enough in this builder because hidden controls must also stop
     * participating in form submission and confirmation summaries.
     */
    function setFieldVisibility(wrapper, visible) {
        if (!wrapper) {
            return;
        }

        wrapper.hidden = !visible;

        const controls = wrapper.querySelectorAll("input, select, textarea");
        controls.forEach((control) => {
            control.disabled = !visible;
        });
    }

    /* Budget fields are meaningful only for backlog-promotion flows that
     * actually support a volume ceiling. Descoberta/Redescoberta and the
     * rollback action intentionally bypass them.
     */
    function toggleBudgetFields() {
        const isStop = String(taskType.value) === stopTaskType;
        const supportsBudgetMode = !["NONE", "REDISCOVERY"].includes(String(modeSelect.value || "").toUpperCase());
        const showBudgetFields = !isStop && supportsBudgetMode;
        setFieldVisibility(maxTotalWrapper, showBudgetFields);
        setFieldVisibility(sortOrderWrapper, showBudgetFields);
    }

    /* Not every mode is legal for every combination of task type and
     * execution scope. This function is the guardrail that keeps the select
     * honest and also nudges an invalid current choice back to a safe default.
     */
    function syncModeAvailability() {
        const isStop = String(taskType.value) === stopTaskType;
        const collective = executionType.value === "collective";

        if (noneOption) {
            noneOption.hidden = isStop;
            noneOption.disabled = isStop;
        }

        if (rediscoveryOption) {
            rediscoveryOption.hidden = isStop;
            rediscoveryOption.disabled = isStop;
        }

        if (fileOption) {
            fileOption.disabled = collective;
        }

        const invalidModes = new Set();

        if (collective) {
            invalidModes.add("FILE");
        }

        if (isStop) {
            invalidModes.add("NONE");
            invalidModes.add("REDISCOVERY");
        }

        if (invalidModes.has(modeSelect.value)) {
            modeSelect.value = isStop ? "ALL" : "NONE";
        }
    }

    /* The confirmation dialog needs a compact human-readable filter summary,
     * not raw form values. This formatter turns the active mode and its
     * relevant parameters into one sentence-like description.
     */
    function buildFilterSummary() {
        const parts = [];
        const modeLabel = modeSelect.selectedOptions[0]
            ? modeSelect.selectedOptions[0].textContent.trim()
            : "NONE";
        parts.push("Modo " + modeLabel);

        const extensionValue = String(extensionInput ? extensionInput.value : "").trim();
        if (extensionValue) {
            parts.push("Extensão " + extensionValue);
        }

        const filePathValue = String(filePathInput ? filePathInput.value : "").trim();
        if (filePathValue) {
            parts.push("Caminho " + filePathValue);
        }

        if (modeSelect.value === "RANGE") {
            const startDate = taskBuilderForm.elements.start_date
                ? String(taskBuilderForm.elements.start_date.value || "").trim()
                : "";
            const endDate = taskBuilderForm.elements.end_date
                ? String(taskBuilderForm.elements.end_date.value || "").trim()
                : "";

            if (startDate || endDate) {
                parts.push("Período " + (startDate || "...") + " até " + (endDate || "..."));
            }
        } else if (modeSelect.value === "LAST") {
            const lastN = taskBuilderForm.elements.last_n_files
                ? String(taskBuilderForm.elements.last_n_files.value || "").trim()
                : "";
            if (lastN) {
                parts.push("Últimos " + lastN + " arquivo(s)");
            }
        } else if (modeSelect.value === "FILE") {
            const fileName = taskBuilderForm.elements.file_name
                ? String(taskBuilderForm.elements.file_name.value || "").trim()
                : "";
            if (fileName) {
                parts.push("Arquivo " + fileName);
            }
        }

        const maxTotalField = taskBuilderForm.elements.max_total_gb;
        const sortOrderField = taskBuilderForm.elements.sort_order;
        const maxTotalGb = maxTotalField && !maxTotalField.disabled
            ? String(maxTotalField.value || "").trim()
            : "";

        if (maxTotalGb) {
            parts.push("Limite " + maxTotalGb + " GB");

            const sortOrder = sortOrderField && !sortOrderField.disabled
                ? String(sortOrderField.value || "").trim()
                : "newest_first";

            if (sortOrder === "oldest_first") {
                parts.push("Prioridade mais antigos primeiro");
            } else {
                parts.push("Prioridade mais recentes primeiro");
            }
        }

        return parts.join(" | ");
    }

    /* Execution scope also needs a narrative summary because the builder can
     * express several different target shapes: one host, one family, all
     * hosts of a family, or a hand-picked subset inside a collective scope.
     */
    function buildScopeSummary() {
        if (executionType.value === "collective") {
            const filterLabel = hostFilterSelect && hostFilterSelect.selectedOptions[0]
                ? hostFilterSelect.selectedOptions[0].textContent.trim()
                : "Todas";
            const selectedCount = selectedCollectiveHostIds.size;

            if (selectedCount > 0) {
                return "Filtro " + filterLabel + " com " + selectedCount + " host(s) selecionado(s) manualmente.";
            }

            return "Filtro " + filterLabel + " sem seleção manual de hosts.";
        }

        const selectedHost = hostSelect && hostSelect.selectedOptions[0]
            ? hostSelect.selectedOptions[0].textContent.trim()
            : "-";
        return selectedHost;
    }

    /* The confirmation dialog and the legacy `window.confirm()` fallback both
     * consume the same structured summary object so the browser-only fallback
     * remains semantically aligned with the richer dialog UI.
     */
    function buildConfirmationSummaryText() {
        const taskTypeLabel = taskType.selectedOptions[0] ? taskType.selectedOptions[0].textContent.trim() : "-";
        const executionLabel = executionType.selectedOptions[0] ? executionType.selectedOptions[0].textContent.trim() : "-";
        const scopeLabel = buildScopeSummary();
        const filterLabel = buildFilterSummary();

        return {
            taskTypeLabel,
            executionLabel,
            scopeLabel,
            filterLabel,
            confirmMessage:
                "Ação: " + taskTypeLabel + "\n" +
                "Execução: " + executionLabel + "\n" +
                "Escopo: " + scopeLabel + "\n" +
                "Filtro: " + filterLabel + "\n\n" +
                "Deseja criar esta tarefa?"
        };
    }

    /* Opening confirmation is the deliberate pause before a potentially broad
     * operational action. The builder prefers the custom dialog, but keeps a
     * browser-native fallback so older environments still get an explicit
     * confirmation step instead of silently submitting.
     */
    function openTaskConfirmation() {
        const summary = buildConfirmationSummaryText();

        if (confirmationType) {
            confirmationType.textContent = summary.taskTypeLabel;
        }

        if (confirmationExecution) {
            confirmationExecution.textContent = summary.executionLabel;
        }

        if (confirmationScope) {
            confirmationScope.textContent = summary.scopeLabel;
        }

        if (confirmationFilter) {
            confirmationFilter.textContent = summary.filterLabel;
        }

        if (confirmationDialog && typeof confirmationDialog.showModal === "function") {
            confirmationDialog.showModal();
            return;
        }

        if (window.confirm(summary.confirmMessage)) {
            submitConfirmed = true;
            if (taskBuilderForm.requestSubmit) {
                taskBuilderForm.requestSubmit();
            } else {
                taskBuilderForm.submit();
            }
        }
    }

    /* Task type changes ripple into several parts of the builder at once:
     * explanatory copy, legal filter modes, budget fields, submit label and
     * the discovery context. Grouping those updates here keeps the builder's
     * top-level semantics synchronized.
     */
    function toggleTaskType() {
        if (filterSection) {
            filterSection.style.display = "block";
        }

        updateTaskTypeNote();
        toggleBudgetFields();
        syncModeAvailability();
        toggleModeFields();
        updateSubmitButtonLabel();
        syncLastDiscoveryContext();
    }

    /* Execution mode is the largest structural switch in the UI.
     *
     * This helper flips the visible panels and then re-applies all derived
     * state that depends on that choice: legal modes, collective list render,
     * station-family profiles and suggested defaults.
     */
    function toggleExecution() {
        const collective = executionType.value === "collective";

        if (individualConfigPanel) {
            individualConfigPanel.hidden = collective;
        }

        if (collectiveConfigPanel) {
            collectiveConfigPanel.hidden = !collective;
        }

        if (hostWrapper) {
            hostWrapper.hidden = collective;
        }

        if (stationTypeWrapper) {
            stationTypeWrapper.hidden = !collective;
        }

        if (collectiveHostsWrapper) {
            collectiveHostsWrapper.hidden = !collective;
        }

        syncModeAvailability();
        toggleModeFields();
        renderCollectiveHosts();
        updateSubmitButtonLabel();
        toggleStationProfilesPanel();
        syncSuggestedFilePath();
        syncSuggestedExtension();
        syncLastDiscoveryContext();
    }

    /* Filter mode drives the parameter shell below the base filter fields.
     * Only the controls that materially affect the chosen mode stay enabled,
     * which keeps both form submission and the confirmation summary honest.
     */
    function toggleModeFields() {
        const modeMeta = filterModeMeta[modeSelect.value] || filterModeMeta.NONE;

        if (filterModeTitle) {
            filterModeTitle.textContent = modeMeta.title;
        }

        if (filterModeNote) {
            filterModeNote.textContent = modeMeta.note;
        }

        if (FILTERLESS_TASK_TYPES.has(String(taskType.value))) {
            setFieldVisibility(startWrapper, false);
            setFieldVisibility(endWrapper, false);
            setFieldVisibility(lastNWrapper, false);
            setFieldVisibility(fileNameWrapper, false);
            toggleBudgetFields();
            return;
        }

        setFieldVisibility(startWrapper, false);
        setFieldVisibility(endWrapper, false);
        setFieldVisibility(lastNWrapper, false);
        setFieldVisibility(fileNameWrapper, false);

        switch (modeSelect.value) {
            case "RANGE":
                setFieldVisibility(startWrapper, true);
                setFieldVisibility(endWrapper, true);
                break;

            case "LAST":
                setFieldVisibility(lastNWrapper, true);
                break;

            case "FILE":
                setFieldVisibility(fileNameWrapper, true);
                break;
        }

        toggleBudgetFields();
        syncLastDiscoveryContext();
    }

    /* Event wiring stays explicit because this builder mixes several state
     * axes: task type, execution scope, mode, family filter, host search and
     * confirmation flow. Keeping the listeners close to the helpers they
     * trigger makes the interaction graph easier to follow than a generic
     * event-dispatch layer would.
     */
    if (onlineOnlyCheckbox) {
        onlineOnlyCheckbox.addEventListener("change", handleOnlineOnlyFilterToggle);
    }

    taskType.addEventListener("change", toggleTaskType);
    executionType.addEventListener("change", toggleExecution);
    modeSelect.addEventListener("change", toggleModeFields);

    if (hostSelect) {
        hostSelect.addEventListener("change", function () {
            syncSuggestedFilePath();
            syncSuggestedExtension();
            syncLastDiscoveryContext();
        });
    }

    if (hostFilterSelect) {
        hostFilterSelect.addEventListener("change", function () {
            renderCollectiveHosts();
            toggleStationProfilesPanel();
            syncSuggestedFilePath();
            syncSuggestedExtension();
        });
    }

    if (collectiveHostSearch) {
        collectiveHostSearch.addEventListener("input", renderCollectiveHosts);
    }

    if (collectiveHostsSelect) {
        collectiveHostsSelect.addEventListener("change", function () {
            syncCollectiveSelectionState();
            syncSuggestedFilePath();
            syncSuggestedExtension();
        });
    }

    /* Submission is always funneled through confirmation first. The
     * `submitConfirmed` flag is the minimal state needed to distinguish:
     * - the initial user intent to submit,
     * - the second submit triggered programmatically after confirmation.
     */
    taskBuilderForm.addEventListener("submit", function (event) {
        if (submitConfirmed) {
            submitConfirmed = false;
            return;
        }

        event.preventDefault();
        openTaskConfirmation();
    });

    if (confirmationCancelButton && confirmationDialog) {
        confirmationCancelButton.addEventListener("click", function () {
            confirmationDialog.close();
        });
    }

    if (confirmationSubmitButton) {
        confirmationSubmitButton.addEventListener("click", function () {
            if (confirmationDialog) {
                confirmationDialog.close();
            }

            submitConfirmed = true;

            if (window.showPageLoadingOverlay) {
                window.showPageLoadingOverlay("Criando tarefa...");
            }

            if (taskBuilderForm.requestSubmit) {
                taskBuilderForm.requestSubmit();
            } else {
                taskBuilderForm.submit();
            }
        });
    }

    /* Startup sequence
     *
     * The server-rendered template gives us the initial raw field values, but
     * the browser still needs one reconciliation pass so all derived UI state
     * matches those values: visible panels, legal modes, collective list,
     * profile shell, suggestions and read-only context blocks.
     */
    toggleTaskType();
    toggleExecution();
    toggleModeFields();
    renderCollectiveHosts();
    updateSubmitButtonLabel();
    toggleStationProfilesPanel();
    syncSuggestedFilePath();
    syncSuggestedExtension();
    syncLastDiscoveryContext();
})();
