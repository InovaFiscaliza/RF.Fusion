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

    const actionSelect = document.getElementById("task-action");
    const taskType = document.getElementById("task-type");
    const executionType = document.querySelector("[name='execution_type']");
    const hostSelect = document.querySelector("[name='host_id']");
    const hostFilterSelect = document.querySelector("[name='host_filter']");
    const modeSelect = document.querySelector("[name='mode']");
    const filePathInput = document.querySelector("[name='file_path']");
    const extensionInput = document.querySelector("[name='extension']");
    const filePathWrapper = document.getElementById("file-path-wrapper");
    const extensionWrapper = document.getElementById("extension-wrapper");
    const filterBaseGrid = document.getElementById("filter-base-grid");
    const zabbixDefaultsNote = document.getElementById("zabbix-backup-defaults-note");
    const collectiveZabbixDefaultsInput = document.getElementById("collective-zabbix-defaults");
    const collectiveZabbixDefaultsSummary = document.getElementById("collective-zabbix-defaults-summary");
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
    const onlineWrapper = document.getElementById("online-wrapper");

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

    if (!actionSelect || !taskType || !executionType || !modeSelect || !taskBuilderForm) {
        return;
    }

    const noneOption = modeSelect.querySelector("option[value='NONE']");
    const rediscoveryOption = modeSelect.querySelector("option[value='REDISCOVERY']");
    const fileOption = modeSelect.querySelector("option[value='FILE']");

    function getSelectedActionOption() {
        return actionSelect.selectedOptions[0] || null;
    }

    function getSelectedActionFixedMode() {
        const selectedAction = getSelectedActionOption();
        return String(selectedAction?.dataset.fixedMode || "").trim().toUpperCase();
    }

    function syncActionSelection() {
        const selectedAction = getSelectedActionOption();
        if (!selectedAction) {
            return;
        }

        taskType.value = String(selectedAction.dataset.taskType || "");
        const fixedMode = getSelectedActionFixedMode();
        if (fixedMode) {
            modeSelect.value = fixedMode;
        }
    }

    const defaultFilePath = "/mnt/internal/data";
    const cwsmFilePath = "C:/CelPlan/CellWireless RU/Spectrum/Completed";
    const ums300FilePath = "C:/Users/NUC/Downloads";
    const defaultExtension = ".bin";
    const cwsmExtension = ".zip";
    const ums300Extension = ".bin";
    const zabbixDefaultsUrlTemplate = String(root.dataset.zabbixBackupDefaultsUrl || "");
    const collectiveZabbixDefaultsUrl = String(root.dataset.zabbixCollectiveBackupDefaultsUrl || "");
    const zabbixDefaultsDebounceMs = 200;
    const collectiveZabbixDefaultsDebounceMs = 350;

    /* Utility tasks bypass the detailed filter controls used by
     * backlog-oriented operations. */
    const FILTERLESS_TASK_TYPES = new Set(["3", "4", "7"]);
    const stopTaskType = String(root.dataset.stopTaskType || "");
    const backupTaskType = String(root.dataset.backupTaskType || "");
    const connectivityTestAction = "connectivity_test";

    function isConnectivityTestAction() {
        return String(actionSelect.value || "") === connectivityTestAction;
    }
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
    let lastZabbixFilePath = "";
    let lastZabbixExtension = "";
    let zabbixDefaultsRequestSequence = 0;
    let zabbixDefaultsTimer = null;
    let collectiveZabbixDefaults = {};
    let collectiveZabbixDefaultsRequestSequence = 0;
    let collectiveZabbixDefaultsTimer = null;
    let collectiveZabbixDefaultsLoading = false;
    let collectiveBaseFilterUserEdited = root.dataset.selectedFilterDefaultsCustom === "true";
    const collectiveProfilesUserEdited = new Set();

    document.querySelectorAll("[data-station-profile-custom='true']").forEach((input) => {
        collectiveProfilesUserEdited.add(
            String(input.dataset.stationProfilePrefix || "").toUpperCase()
        );
    });

    /* Host families are inferred from the alphabetical prefix because the
     * builder uses that lightweight classification to:
     * - suggest default path/extension pairs,
     * - decide whether a collective selection is homogeneous or mixed,
     * - reveal the family-profile editor only when it really matters.
     */
    function extractHostPrefix(hostName) {
        const normalizedName = String(hostName || "").trim().toUpperCase();

        if (normalizedName.startsWith("UMS")) {
            return "UMS300";
        }

        if (normalizedName.startsWith("ERMX")) {
            return "ERMX";
        }

        const match = normalizedName.match(/^[A-Z]+/);
        return match ? match[0] : "";
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

    function getCurrentCollectiveHosts() {
        const selectedPrefix = hostFilterSelect ? hostFilterSelect.value : "ALL";
        const manuallySelectedHosts = hostCatalog.filter((host) => {
            if (!selectedCollectiveHostIds.has(host.id)) {
                return false;
            }

            return selectedPrefix === "ALL"
                || extractHostPrefix(host.name) === selectedPrefix.toUpperCase();
        });

        if (manuallySelectedHosts.length > 0) {
            return manuallySelectedHosts;
        }

        return hostCatalog.filter((host) => {
            return selectedPrefix === "ALL"
                || extractHostPrefix(host.name) === selectedPrefix.toUpperCase();
        });
    }

    function setCollectiveZabbixDefaultsSummary(message) {
        if (collectiveZabbixDefaultsSummary) {
            collectiveZabbixDefaultsSummary.textContent = message;
        }
    }

    function resetCollectiveZabbixDefaults(message) {
        collectiveZabbixDefaults = {};
        collectiveZabbixDefaultsLoading = false;
        if (collectiveZabbixDefaultsInput) {
            collectiveZabbixDefaultsInput.value = "";
        }
        setCollectiveZabbixDefaultsSummary(message);
    }

    function groupCollectiveZabbixDefaults(defaults, hosts) {
        const groups = new Map();
        let missingCount = 0;

        hosts.forEach((host) => {
            const values = defaults[host.id] || {};
            const filePath = String(values.file_path || "").trim();
            const extension = String(values.extension || "").trim();

            if (!filePath || !extension) {
                missingCount += 1;
                return;
            }

            const key = filePath + "\u0000" + extension;
            const group = groups.get(key) || { filePath, extension, count: 0 };
            group.count += 1;
            groups.set(key, group);
        });

        return {
            groups: Array.from(groups.values()),
            missingCount,
        };
    }

    function isCollectiveBackup() {
        if (
            executionType.value !== "collective"
            || String(taskType.value) !== backupTaskType
        ) {
            return false;
        }

        return true;
    }

    function shouldUseCollectiveZabbixDefaults() {
        if (!isCollectiveBackup()) {
            return false;
        }

        // A fully broad collective run can contain the entire fleet. The
        // existing family profiles stay available for that case; Zabbix is
        // queried only after the operator narrows the group or hand-picks
        // hosts, which keeps the remote configuration service lightweight.
        return selectedCollectiveHostIds.size > 0
            || (hostFilterSelect && hostFilterSelect.value !== "ALL");
    }

    function syncCollectiveBackupConfigurationFields() {
        const zabbixManaged = isCollectiveBackup();

        setFieldVisibility(extensionWrapper, !zabbixManaged);
        setFieldVisibility(filePathWrapper, !zabbixManaged);

        if (filterBaseGrid) {
            filterBaseGrid.classList.toggle(
                "filter-base-grid--zabbix-managed",
                zabbixManaged
            );
        }
    }

    function applyUniformCollectiveDefaults(group, configuredHostCount) {
        if (!group || configuredHostCount !== getCurrentCollectiveHosts().length) {
            return;
        }

        if (!collectiveBaseFilterUserEdited && filePathInput) {
            const currentFilePath = String(filePathInput.value || "").trim();
            if (isSuggestedFilePath(currentFilePath)) {
                filePathInput.value = group.filePath;
                lastZabbixFilePath = group.filePath;
            }
        }

        if (!collectiveBaseFilterUserEdited && extensionInput) {
            const currentExtension = String(extensionInput.value || "").trim().toLowerCase();
            if (isSuggestedExtension(currentExtension)) {
                extensionInput.value = group.extension;
                lastZabbixExtension = group.extension.toLowerCase();
            }
        }
    }

    function updateStationProfileDefaults(defaults, hosts) {
        const hostsByPrefix = new Map();
        hosts.forEach((host) => {
            const prefix = extractHostPrefix(host.name);
            if (!prefix) {
                return;
            }
            const rows = hostsByPrefix.get(prefix) || [];
            rows.push(host);
            hostsByPrefix.set(prefix, rows);
        });

        hostsByPrefix.forEach((prefixHosts, prefix) => {
            if (collectiveProfilesUserEdited.has(prefix)) {
                return;
            }

            const grouped = groupCollectiveZabbixDefaults(defaults, prefixHosts);
            if (grouped.groups.length !== 1 || grouped.missingCount > 0) {
                return;
            }

            const values = grouped.groups[0];
            const pathInput = document.querySelector(
                "[data-station-profile-prefix='" + CSS.escape(prefix) + "'][data-station-profile-field='file_path']"
            );
            const extensionProfileInput = document.querySelector(
                "[data-station-profile-prefix='" + CSS.escape(prefix) + "'][data-station-profile-field='extension']"
            );

            if (pathInput) {
                pathInput.value = values.filePath;
            }
            if (extensionProfileInput) {
                extensionProfileInput.value = values.extension;
            }
        });
    }

    function syncCollectiveZabbixDefaultsInput() {
        if (!collectiveZabbixDefaultsInput) {
            return;
        }

        if (!shouldUseCollectiveZabbixDefaults() || collectiveBaseFilterUserEdited) {
            collectiveZabbixDefaultsInput.value = "";
            return;
        }

        const allowedDefaults = {};
        getCurrentCollectiveHosts().forEach((host) => {
            const prefix = extractHostPrefix(host.name);
            if (collectiveProfilesUserEdited.has(prefix)) {
                return;
            }

            const values = collectiveZabbixDefaults[host.id];
            if (values) {
                allowedDefaults[host.id] = values;
            }
        });
        collectiveZabbixDefaultsInput.value = Object.keys(allowedDefaults).length
            ? JSON.stringify(allowedDefaults)
            : "";
    }

    async function syncCollectiveZabbixBackupDefaults() {
        const requestSequence = ++collectiveZabbixDefaultsRequestSequence;

        if (!shouldUseCollectiveZabbixDefaults() || !collectiveZabbixDefaultsUrl) {
            resetCollectiveZabbixDefaults(
                "Caminho e extensão efetivos são consultados no Zabbix para solicitações coletivas de backup."
            );
            return;
        }

        const hosts = getCurrentCollectiveHosts();
        if (!hosts.length) {
            resetCollectiveZabbixDefaults(
                "Não há hosts elegíveis no filtro coletivo atual."
            );
            return;
        }

        collectiveZabbixDefaultsLoading = true;
        setCollectiveZabbixDefaultsSummary(
            "Consultando caminho e extensão efetivos de " + hosts.length + " host(s) no Zabbix..."
        );

        try {
            const url = new URL(collectiveZabbixDefaultsUrl, window.location.origin);
            hosts.forEach((host) => url.searchParams.append("host_id", host.id));
            const response = await fetch(url.toString(), {
                headers: { Accept: "application/json" },
                credentials: "same-origin",
            });
            if (!response.ok) {
                throw new Error("Collective Zabbix defaults request failed");
            }

            const payload = await response.json();
            if (requestSequence !== collectiveZabbixDefaultsRequestSequence) {
                return;
            }

            collectiveZabbixDefaultsLoading = false;
            collectiveZabbixDefaults = payload.defaults || {};
            const grouped = groupCollectiveZabbixDefaults(collectiveZabbixDefaults, hosts);
            const configuredCount = hosts.length - grouped.missingCount;

            updateStationProfileDefaults(collectiveZabbixDefaults, hosts);
            syncCollectiveZabbixDefaultsInput();

            if (!grouped.groups.length) {
                setCollectiveZabbixDefaultsSummary(
                    "O Zabbix não retornou caminho e extensão para este grupo; os perfis locais permanecem em uso."
                );
                return;
            }

            if (grouped.groups.length === 1 && grouped.missingCount === 0) {
                const group = grouped.groups[0];
                applyUniformCollectiveDefaults(group, configuredCount);
                setCollectiveZabbixDefaultsSummary(
                    "Zabbix: " + configuredCount + " host(s) com caminho " + group.filePath
                    + " e extensão " + group.extension + "."
                );
                return;
            }

            const descriptions = grouped.groups.slice(0, 3).map((group) => {
                return group.count + " host(s): " + group.filePath + " (" + group.extension + ")";
            });
            if (grouped.groups.length > descriptions.length) {
                descriptions.push("e mais " + (grouped.groups.length - descriptions.length) + " configuração(ões)");
            }
            if (grouped.missingCount) {
                descriptions.push(grouped.missingCount + " host(s) sem macro de backup");
            }
            setCollectiveZabbixDefaultsSummary(
                "Zabbix identificou configurações diferentes no grupo. As tarefas serão separadas por caminho e extensão: "
                + descriptions.join("; ") + "."
            );
        } catch (error) {
            if (requestSequence !== collectiveZabbixDefaultsRequestSequence) {
                return;
            }
            resetCollectiveZabbixDefaults(
                "Não foi possível consultar o Zabbix; os perfis locais permanecem em uso."
            );
        }
    }

    function scheduleCollectiveZabbixBackupDefaultsSync() {
        if (collectiveZabbixDefaultsTimer) {
            window.clearTimeout(collectiveZabbixDefaultsTimer);
        }

        // Invalidate a response for the previous collective scope immediately
        // instead of letting it briefly overwrite a newer host selection.
        collectiveZabbixDefaultsRequestSequence += 1;

        if (!shouldUseCollectiveZabbixDefaults()) {
            resetCollectiveZabbixDefaults(
                "Escolha um tipo de estação ou hosts específicos para consultar caminho e extensão efetivos no Zabbix."
            );
            return;
        }

        collectiveZabbixDefaultsTimer = window.setTimeout(function () {
            collectiveZabbixDefaultsTimer = null;
            void syncCollectiveZabbixBackupDefaults();
        }, collectiveZabbixDefaultsDebounceMs);
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

                if (prefixes.size === 1 && prefixes.has("UMS300")) {
                    return "ums300";
                }

                if (prefixes.size <= 1) {
                    return "default";
                }

                return "mixed";
            }

            if (hostFilterSelect && hostFilterSelect.value === "CWSM") {
                return "cwsm";
            }

            if (hostFilterSelect && hostFilterSelect.value === "UMS300") {
                return "ums300";
            }

            if (hostFilterSelect && hostFilterSelect.value !== "ALL") {
                return "default";
            }

            return "mixed";
        }

        const selected = hostSelect ? hostSelect.selectedOptions[0] : null;
        const hostName = selected ? (selected.dataset.hostName || selected.textContent || "") : "";

        if (extractHostPrefix(hostName) === "CWSM") {
            return "cwsm";
        }

        if (extractHostPrefix(hostName) === "UMS300") {
            return "ums300";
        }

        return "default";
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
            || currentValue === cwsmFilePath
            || currentValue === ums300FilePath;

        if (!followsDefaultMask) {
            return;
        }

        if (selectionProfile === "mixed") {
            filePathInput.value = "";
            return;
        }

        if (selectionProfile === "cwsm") {
            filePathInput.value = cwsmFilePath;
            return;
        }

        if (selectionProfile === "ums300") {
            filePathInput.value = ums300FilePath;
            return;
        }

        filePathInput.value = defaultFilePath;
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
            || currentValue === cwsmExtension
            || currentValue === ums300Extension;

        if (!followsDefaultMask) {
            return;
        }

        if (selectionProfile === "mixed") {
            extensionInput.value = "";
            return;
        }

        if (selectionProfile === "cwsm") {
            extensionInput.value = cwsmExtension;
            return;
        }

        if (selectionProfile === "ums300") {
            extensionInput.value = ums300Extension;
            return;
        }

        extensionInput.value = defaultExtension;
    }

    function setZabbixDefaultsNote(message) {
        if (zabbixDefaultsNote) {
            zabbixDefaultsNote.textContent = message;
        }
    }

    function buildZabbixDefaultsUrl(hostId) {
        return zabbixDefaultsUrlTemplate.replace("/0/", "/" + encodeURIComponent(hostId) + "/");
    }

    function isSuggestedFilePath(value) {
        return value === ""
            || value === defaultFilePath
            || value === cwsmFilePath
            || value === ums300FilePath
            || value === lastZabbixFilePath;
    }

    function isSuggestedExtension(value) {
        return value === ""
            || value === defaultExtension
            || value === cwsmExtension
            || value === ums300Extension
            || value === lastZabbixExtension;
    }

    /* Individual execution has one concrete host, so its backup defaults can
     * safely come from the Zabbix configuration. Collective execution uses a
     * separate batched request below to preserve per-host overrides without
     * making one remote call per station.
     */
    async function syncZabbixBackupDefaults() {
        const requestSequence = ++zabbixDefaultsRequestSequence;
        const hostId = hostSelect ? String(hostSelect.value || "").trim() : "";

        if (executionType.value !== "individual") {
            return;
        }

        if (!hostId || !zabbixDefaultsUrlTemplate) {
            setZabbixDefaultsNote(
                "Não há uma estação disponível para consultar no Zabbix."
            );
            return;
        }

        setZabbixDefaultsNote("Consultando caminho e extensão configurados no Zabbix...");

        try {
            const response = await fetch(buildZabbixDefaultsUrl(hostId), {
                headers: { Accept: "application/json" },
                credentials: "same-origin",
            });
            if (!response.ok) {
                throw new Error("Zabbix defaults request failed");
            }

            const defaults = await response.json();
            if (requestSequence !== zabbixDefaultsRequestSequence) {
                return;
            }

            let preservedManualValue = false;
            const zabbixFilePath = String(defaults.file_path || "").trim();
            const zabbixExtension = String(defaults.extension || "").trim();

            if (zabbixFilePath && filePathInput) {
                if (isSuggestedFilePath(String(filePathInput.value || "").trim())) {
                    filePathInput.value = zabbixFilePath;
                    lastZabbixFilePath = zabbixFilePath;
                } else {
                    preservedManualValue = true;
                }
            }

            if (zabbixExtension && extensionInput) {
                if (isSuggestedExtension(String(extensionInput.value || "").trim().toLowerCase())) {
                    extensionInput.value = zabbixExtension;
                    lastZabbixExtension = zabbixExtension.toLowerCase();
                } else {
                    preservedManualValue = true;
                }
            }

            if (defaults.source === "zabbix") {
                setZabbixDefaultsNote(
                    preservedManualValue
                        ? "Configuração consultada no Zabbix; valores preenchidos manualmente foram preservados."
                        : "Caminho e extensão carregados da configuração da estação no Zabbix."
                );
                return;
            }

            setZabbixDefaultsNote(
                "A estação não possui caminho ou extensão no Zabbix; usando sugestões do perfil local."
            );
        } catch (error) {
            if (requestSequence !== zabbixDefaultsRequestSequence) {
                return;
            }
            setZabbixDefaultsNote(
                "Não foi possível consultar o Zabbix; usando sugestões do perfil local."
            );
        }
    }

    /* A host selector can emit several changes while the operator navigates
     * with the keyboard. Debouncing keeps those intermediate selections from
     * producing unnecessary Zabbix requests or occupying WebFusion workers.
     */
    function scheduleZabbixBackupDefaultsSync() {
        if (zabbixDefaultsTimer) {
            window.clearTimeout(zabbixDefaultsTimer);
        }

        zabbixDefaultsTimer = window.setTimeout(function () {
            zabbixDefaultsTimer = null;
            void syncZabbixBackupDefaults();
        }, zabbixDefaultsDebounceMs);
    }

    function updateSubmitButtonLabel() {
        if (!submitButton) {
            return;
        }

        if (isConnectivityTestAction()) {
            submitButton.textContent = "Iniciar teste";
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

    /* Action changes alter the meaning of the entire builder. Keep the
     * explanatory card aligned with the operational intent rather than the
     * underlying queue type shared by more than one action. */
    function updateTaskTypeNote() {
        if (!taskTypeNote) {
            return;
        }

        const selectedAction = String(actionSelect.value || "");

        if (selectedAction === "backlog_rollback") {
            if (taskTypeNoteTitle) {
                taskTypeNoteTitle.textContent = "Remoção da fila de backup";
            }

            taskTypeNote.textContent = "Remove da fila os arquivos selecionados que ainda não foram copiados. Eles poderão ser identificados e enviados novamente em uma operação posterior.";
            return;
        }

        if (selectedAction === "discover") {
            if (taskTypeNoteTitle) {
                taskTypeNoteTitle.textContent = "Descoberta incremental";
            }

            taskTypeNote.textContent = "Identifica no caminho configurado os arquivos novos ou alterados desde a última descoberta registrada para a estação.";
            return;
        }

        if (selectedAction === "rediscover") {
            if (taskTypeNoteTitle) {
                taskTypeNoteTitle.textContent = "Descoberta completa";
            }

            taskTypeNote.textContent = "Examina todos os arquivos no caminho configurado, sem considerar a data da última descoberta. Use quando precisar conferir novamente todo o conteúdo da estação.";
            return;
        }

        if (selectedAction === connectivityTestAction) {
            if (taskTypeNoteTitle) {
                taskTypeNoteTitle.textContent = "Teste de conectividade";
            }

            taskTypeNote.textContent = "Executa uma checagem prioritária de ICMP e SSH para uma única estação e acompanha o resultado em tempo real.";
            return;
        }

        if (taskTypeNoteTitle) {
            taskTypeNoteTitle.textContent = "Envio para fila de backup";
        }

        taskTypeNote.textContent = "Inclui na fila de backup os arquivos que atendem ao filtro escolhido. A cópia para o repositório central é executada conforme a capacidade da fila.";
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
        const fixedMode = getSelectedActionFixedMode();
        const genericBackup = !fixedMode && !isStop;

        if (noneOption) {
            noneOption.hidden = isStop || genericBackup;
            noneOption.disabled = isStop || genericBackup;
        }

        if (rediscoveryOption) {
            rediscoveryOption.hidden = isStop || genericBackup;
            rediscoveryOption.disabled = isStop || genericBackup;
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

        if (genericBackup) {
            invalidModes.add("NONE");
            invalidModes.add("REDISCOVERY");
        }

        if (invalidModes.has(modeSelect.value)) {
            modeSelect.value = "ALL";
        }

        modeSelect.disabled = Boolean(fixedMode);
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

        if (isCollectiveBackup()) {
            parts.push("Configuração de backup definida no Zabbix");
        } else {
            const extensionValue = String(extensionInput ? extensionInput.value : "").trim();
            if (extensionValue) {
                parts.push("Extensão " + extensionValue);
            }

            const filePathValue = String(filePathInput ? filePathInput.value : "").trim();
            if (filePathValue) {
                parts.push("Caminho " + filePathValue);
            }
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
        const taskTypeLabel = actionSelect.selectedOptions[0]
            ? actionSelect.selectedOptions[0].textContent.trim()
            : "-";
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
        const connectivityTest = isConnectivityTestAction();
        if (filterSection) {
            filterSection.hidden = connectivityTest;
            filterSection.style.display = connectivityTest ? "none" : "block";
        }

        if (onlineWrapper) {
            onlineWrapper.hidden = connectivityTest;
        }

        const collectiveOption = executionType.querySelector("option[value='collective']");
        if (collectiveOption) {
            collectiveOption.disabled = connectivityTest;
        }

        if (connectivityTest) {
            executionType.value = "individual";
        }

        syncActionSelection();
        updateTaskTypeNote();

        if (connectivityTest) {
            if (individualConfigPanel) {
                individualConfigPanel.hidden = false;
            }
            if (collectiveConfigPanel) {
                collectiveConfigPanel.hidden = true;
            }
            if (hostWrapper) {
                hostWrapper.hidden = false;
            }
            if (stationTypeWrapper) {
                stationTypeWrapper.hidden = true;
            }
            if (collectiveHostsWrapper) {
                collectiveHostsWrapper.hidden = true;
            }
            if (stationProfilesPanel) {
                stationProfilesPanel.hidden = true;
            }
        }

        toggleBudgetFields();
        syncModeAvailability();
        toggleModeFields();
        updateSubmitButtonLabel();
        syncLastDiscoveryContext();
        syncCollectiveBackupConfigurationFields();
        scheduleCollectiveZabbixBackupDefaultsSync();
    }

    /* Execution mode is the largest structural switch in the UI.
     *
     * This helper flips the visible panels and then re-applies all derived
     * state that depends on that choice: legal modes, collective list render,
     * station-family profiles and suggested defaults.
     */
    function toggleExecution() {
        if (isConnectivityTestAction()) {
            executionType.value = "individual";
        }

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
        syncCollectiveBackupConfigurationFields();
        syncSuggestedFilePath();
        syncSuggestedExtension();
        scheduleZabbixBackupDefaultsSync();
        scheduleCollectiveZabbixBackupDefaultsSync();
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
            filterModeNote.textContent = isCollectiveBackup()
                ? "O caminho e a extensão são definidos pela configuração efetiva de cada estação no Zabbix."
                : modeMeta.note;
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

    actionSelect.addEventListener("change", toggleTaskType);
    executionType.addEventListener("change", toggleExecution);
    modeSelect.addEventListener("change", toggleModeFields);

    if (hostSelect) {
        hostSelect.addEventListener("change", function () {
            syncSuggestedFilePath();
            syncSuggestedExtension();
            scheduleZabbixBackupDefaultsSync();
            syncLastDiscoveryContext();
        });
    }

    if (hostFilterSelect) {
        hostFilterSelect.addEventListener("change", function () {
            renderCollectiveHosts();
            toggleStationProfilesPanel();
            syncSuggestedFilePath();
            syncSuggestedExtension();
            scheduleCollectiveZabbixBackupDefaultsSync();
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
            scheduleCollectiveZabbixBackupDefaultsSync();
        });
    }

    [filePathInput, extensionInput].forEach((input) => {
        if (!input) {
            return;
        }

        input.addEventListener("input", function () {
            collectiveBaseFilterUserEdited = true;
            syncCollectiveZabbixDefaultsInput();
        });
    });

    document.querySelectorAll("[data-station-profile-prefix]").forEach((input) => {
        input.addEventListener("input", function () {
            collectiveProfilesUserEdited.add(String(input.dataset.stationProfilePrefix || "").toUpperCase());
            syncCollectiveZabbixDefaultsInput();
        });
    });

    /* Submission is always funneled through confirmation first. The
     * `submitConfirmed` flag is the minimal state needed to distinguish:
     * - the initial user intent to submit,
     * - the second submit triggered programmatically after confirmation.
     */
    taskBuilderForm.addEventListener("submit", function (event) {
        if (isConnectivityTestAction()) {
            event.preventDefault();

            const selectedHost = hostSelect && hostSelect.selectedOptions[0];
            const hostId = Number(selectedHost?.value || 0);
            if (!hostId || typeof window.startStationConnectivityTest !== "function") {
                if (taskTypeNote) {
                    taskTypeNote.textContent = "Selecione uma estação válida para iniciar o teste de conectividade.";
                }
                return;
            }

            void window.startStationConnectivityTest(
                hostId,
                selectedHost.dataset.hostName || selectedHost.textContent.trim(),
            );
            return;
        }

        if (submitConfirmed) {
            submitConfirmed = false;
            return;
        }

        if (collectiveZabbixDefaultsLoading && shouldUseCollectiveZabbixDefaults()) {
            event.preventDefault();
            setCollectiveZabbixDefaultsSummary(
                "Aguarde a consulta de caminho e extensão no Zabbix terminar antes de criar as tarefas."
            );
            return;
        }

        event.preventDefault();
        syncCollectiveZabbixDefaultsInput();
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
    syncActionSelection();
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
