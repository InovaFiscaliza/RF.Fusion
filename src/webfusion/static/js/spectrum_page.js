/* Spectrum page controller
 *
 * This page now combines facet-style filters:
 * - equipment, state, and district constrain each other dynamically
 * - observed period hints follow the current non-date filter set
 * - result rows stay file-oriented, with lazy-loaded internal spectra
 */
(function () {
    const root = document.getElementById("spectrum-page-root");
    const queryForm = document.getElementById("spectrum-query-form");
    const equipmentField = document.getElementById("equipment_id");
    const stateField = document.getElementById("state_id");
    const districtField = document.getElementById("district_id");
    const districtHint = document.getElementById("district-filter-hint");
    const periodHint = document.getElementById("period-filter-hint");
    const clearFiltersLink = document.getElementById("clear-spectrum-filters");
    const startDateField = queryForm ? queryForm.elements.start_date : null;
    const endDateField = queryForm ? queryForm.elements.end_date : null;
    const freqStartField = queryForm ? queryForm.elements.freq_start : null;
    const freqEndField = queryForm ? queryForm.elements.freq_end : null;
    const descriptionField = queryForm ? queryForm.elements.description : null;
    const hiddenSiteField = queryForm ? queryForm.elements.site_id : null;

    if (!root || !queryForm || !equipmentField || !stateField || !districtField) {
        return;
    }

    const filtersEndpoint = root.dataset.filtersEndpoint || "/api/spectrum/filters";
    const fileSpectraEndpointBase = root.dataset.fileSpectraEndpointBase || "/api/spectrum/file";
    const usesLightweightBootstrap = root.dataset.lightweightBootstrap === "1";
    const filterCache = new Map();
    const detailCache = new Map();
    const selectConfigs = [
        {
            field: equipmentField,
            key: "equipments",
            idKey: "ID_EQUIPMENT",
            labelKey: "OPTION_LABEL",
            defaultOptionLabel: "Todas as estacoes",
        },
        {
            field: stateField,
            key: "states",
            idKey: "ID_STATE",
            labelKey: "OPTION_LABEL",
            defaultOptionLabel: "Todos os estados",
        },
        {
            field: districtField,
            key: "districts",
            idKey: "ID_DISTRICT",
            labelKey: "OPTION_LABEL",
            defaultOptionLabel: "Todos os distritos",
        },
    ];

    let filterRequestSerial = 0;
    let refreshTimer = null;
    let filterAbortController = null;
    let loadingOverlayTimer = null;
    let allowAutoDateFill = !(
        (startDateField && startDateField.value)
        || (endDateField && endDateField.value)
    );

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function appendTrimmedParam(params, key, rawValue) {
        const value = typeof rawValue === "string" ? rawValue.trim() : "";

        if (value) {
            params.set(key, value);
        }
    }

    function buildFilterQuery() {
        const params = new URLSearchParams();
        appendTrimmedParam(params, "equipment_id", equipmentField.value);
        appendTrimmedParam(params, "state_id", stateField.value);
        appendTrimmedParam(params, "district_id", districtField.value);
        appendTrimmedParam(params, "site_id", hiddenSiteField ? hiddenSiteField.value : "");
        return params;
    }

    function buildDetailQuery() {
        const source = new URLSearchParams(window.location.search);
        const params = new URLSearchParams();

        [
            "equipment_id",
            "state_id",
            "district_id",
            "site_id",
            "start_date",
            "end_date",
            "freq_start",
            "freq_end",
            "description",
        ].forEach((key) => appendTrimmedParam(params, key, source.get(key) || ""));

        return params;
    }

    function getSelectionSnapshot(field) {
        const selectedOption = field.options[field.selectedIndex];

        return {
            value: field.value ? String(field.value) : "",
            label: selectedOption ? selectedOption.textContent : "",
        };
    }

    function setFieldRefreshing(field, isRefreshing) {
        field.classList.toggle("is-refreshing", isRefreshing);
    }

    function setDistrictHint(message) {
        if (districtHint) {
            districtHint.textContent = message;
        }
    }

    function setPeriodHint(message) {
        if (periodHint) {
            periodHint.textContent = message;
        }
    }

    function scheduleLoadingOverlay(message) {
        if (loadingOverlayTimer) {
            window.clearTimeout(loadingOverlayTimer);
        }

        loadingOverlayTimer = window.setTimeout(() => {
            if (typeof window.showPageLoadingOverlay === "function") {
                window.showPageLoadingOverlay(message || "Atualizando filtros...");
            }
        }, 180);
    }

    function clearLoadingOverlay() {
        if (loadingOverlayTimer) {
            window.clearTimeout(loadingOverlayTimer);
            loadingOverlayTimer = null;
        }

        if (typeof window.hidePageLoadingOverlay === "function") {
            window.hidePageLoadingOverlay();
        }
    }

    function cancelPendingFilterRefresh() {
        filterRequestSerial += 1;

        if (refreshTimer) {
            window.clearTimeout(refreshTimer);
            refreshTimer = null;
        }

        if (filterAbortController) {
            filterAbortController.abort();
            filterAbortController = null;
        }

        selectConfigs.forEach(({ field }) => setFieldRefreshing(field, false));
        clearLoadingOverlay();
    }

    function populateSelectOptions(config, rows, selectedValue, selectedLabel) {
        const { field, idKey, labelKey, defaultOptionLabel } = config;
        field.innerHTML = "";

        const defaultOption = document.createElement("option");
        defaultOption.value = "";
        defaultOption.textContent = defaultOptionLabel;
        field.appendChild(defaultOption);

        rows.forEach((row) => {
            const option = document.createElement("option");
            option.value = String(row[idKey]);
            option.textContent = row[labelKey];
            field.appendChild(option);
        });

        const hasSelectedValue = selectedValue
            && rows.some((row) => String(row[idKey]) === String(selectedValue));

        if (selectedValue && !hasSelectedValue && selectedLabel) {
            const option = document.createElement("option");
            option.value = String(selectedValue);
            option.textContent = selectedLabel;
            field.appendChild(option);
        }

        field.value = selectedValue ? String(selectedValue) : "";
    }

    function applyAvailability(availability, options = {}) {
        const { mayAutofill = false } = options;

        if (!availability || !availability.DATE_START || !availability.DATE_END) {
            if (startDateField) {
                startDateField.removeAttribute("min");
                startDateField.removeAttribute("max");
            }

            if (endDateField) {
                endDateField.removeAttribute("min");
                endDateField.removeAttribute("max");
            }

            setPeriodHint(
                "O periodo observado e sugerido conforme o recorte geografico escolhido."
            );
            return;
        }

        if (startDateField) {
            startDateField.min = availability.DATE_START;
            startDateField.max = availability.DATE_END;
        }

        if (endDateField) {
            endDateField.min = availability.DATE_START;
            endDateField.max = availability.DATE_END;
        }

        setPeriodHint(
            `Faixa observada para o recorte geografico atual: ${availability.DATE_START_DISPLAY} ate ${availability.DATE_END_DISPLAY}.`
        );

        if (
            mayAutofill
            && allowAutoDateFill
            && startDateField
            && endDateField
        ) {
            startDateField.value = availability.DATE_START;
            endDateField.value = availability.DATE_END;
        }
    }

    function renderFilterPayload(payload, snapshots, options = {}) {
        const equipments = Array.isArray(payload.equipments) ? payload.equipments : [];
        const states = Array.isArray(payload.states) ? payload.states : [];
        const districts = Array.isArray(payload.districts) ? payload.districts : [];

        populateSelectOptions(selectConfigs[0], equipments, snapshots.equipment.value, snapshots.equipment.label);
        populateSelectOptions(selectConfigs[1], states, snapshots.state.value, snapshots.state.label);
        populateSelectOptions(selectConfigs[2], districts, snapshots.district.value, snapshots.district.label);

        setDistrictHint(
            districts.length
                ? `${districts.length} distritos disponiveis para o recorte geografico atual.`
                : "Nenhum distrito encontrado para o recorte geografico atual."
        );

        applyAvailability(payload.availability, options);
    }

    async function refreshFilterOptions(options = {}) {
        const { force = false, mayAutofill = false, loadingMessage = "" } = options;
        const params = buildFilterQuery();
        const cacheKey = params.toString();
        const currentRequest = ++filterRequestSerial;
        const snapshots = {
            equipment: getSelectionSnapshot(equipmentField),
            state: getSelectionSnapshot(stateField),
            district: getSelectionSnapshot(districtField),
        };

        if (!force && filterCache.has(cacheKey)) {
            renderFilterPayload(filterCache.get(cacheKey), snapshots, { mayAutofill });
            return;
        }

        if (filterAbortController) {
            filterAbortController.abort();
        }

        filterAbortController = new AbortController();
        selectConfigs.forEach(({ field }) => setFieldRefreshing(field, true));
        setDistrictHint("Atualizando distritos observados...");
        setPeriodHint("Atualizando periodo observado...");
        scheduleLoadingOverlay(loadingMessage || "Atualizando filtros...");

        try {
            const requestParams = new URLSearchParams(params);
            if (usesLightweightBootstrap && cacheKey === "") {
                requestParams.set("bootstrap", "1");
            }

            const response = await fetch(`${filtersEndpoint}?${requestParams.toString()}`, {
                signal: filterAbortController.signal,
            });

            if (!response.ok) {
                throw new Error(`filters request failed with status ${response.status}`);
            }

            const payload = await response.json();

            if (currentRequest !== filterRequestSerial) {
                return;
            }

            filterCache.set(cacheKey, payload);
            renderFilterPayload(payload, snapshots, { mayAutofill });
        } catch (error) {
            if (error && error.name === "AbortError") {
                return;
            }

            if (currentRequest !== filterRequestSerial) {
                return;
            }

            setDistrictHint("Nao foi possivel atualizar os distritos agora.");
            setPeriodHint("Nao foi possivel atualizar o periodo observado agora.");
        } finally {
            if (currentRequest === filterRequestSerial) {
                filterAbortController = null;
                selectConfigs.forEach(({ field }) => setFieldRefreshing(field, false));
                clearLoadingOverlay();
            }
        }
    }

    function scheduleFilterRefresh(message, options = {}) {
        if (message) {
            setDistrictHint(message);
        }

        if (refreshTimer) {
            window.clearTimeout(refreshTimer);
        }

        refreshTimer = window.setTimeout(() => {
            refreshTimer = null;
            refreshFilterOptions(options);
        }, 450);
    }

    [equipmentField, stateField, districtField].forEach((field) => {
        field.addEventListener("change", () => {
            scheduleFilterRefresh("Filtros geograficos alterados. Atualizando opcoes...", {
                mayAutofill: true,
                loadingMessage: "Atualizando filtros geograficos...",
            });
        });
    });

    [startDateField, endDateField].filter(Boolean).forEach((field) => {
        field.addEventListener("change", () => {
            allowAutoDateFill = !(
                (startDateField && startDateField.value)
                || (endDateField && endDateField.value)
            );
        });
    });

    if (!usesLightweightBootstrap) {
        refreshFilterOptions({
            force: true,
            mayAutofill: allowAutoDateFill,
        });
    } else {
        setDistrictHint("Selecione um estado ou uma estacao para carregar os distritos disponiveis.");
        setPeriodHint(
            "O periodo observado sera carregado quando houver contexto geografico suficiente."
        );
    }

    if (clearFiltersLink) {
        clearFiltersLink.addEventListener("click", () => {
            cancelPendingFilterRefresh();
        });
    }

    function renderDetailCell(value, shouldHighlight) {
        const content = escapeHtml(value ?? "—");
        return shouldHighlight ? `<strong>${content}</strong>` : content;
    }

    function buildDetailHtml(rows) {
        if (!Array.isArray(rows) || rows.length === 0) {
            return '<div class="file-detail-empty">Nenhum espectro vinculado a este arquivo.</div>';
        }

        const shouldHighlightMatches = rows.some((row) => Number(row.IS_MATCH) !== 1);
        const body = rows.map((row) => {
            const isMatch = shouldHighlightMatches && Number(row.IS_MATCH) === 1;

            return `
                <tr class="${isMatch ? "file-detail-match" : ""}">
                    <td>${renderDetailCell(row.ID_SPECTRUM, isMatch)}</td>
                    <td>${renderDetailCell(row.NA_DESCRIPTION || "—", isMatch)}</td>
                    <td>${renderDetailCell(`${row.NU_FREQ_START} – ${row.NU_FREQ_END}`, isMatch)}</td>
                    <td>${renderDetailCell(row.DT_TIME_START || "—", isMatch)}</td>
                    <td>${renderDetailCell(row.DT_TIME_END || "—", isMatch)}</td>
                    <td>${renderDetailCell(row.NU_RBW || "—", isMatch)}</td>
                    <td>${renderDetailCell(row.NU_TRACE_COUNT || "—", isMatch)}</td>
                </tr>
            `;
        }).join("");

        return `
            <div class="file-detail-header">
                Espectros internos do arquivo
                ${shouldHighlightMatches ? '<span class="file-detail-match-note">Os compativeis com a busca aparecem em negrito.</span>' : ""}
            </div>
            <div class="file-detail-table-wrap">
                <table class="file-detail-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Plano</th>
                            <th>Faixa (MHz)</th>
                            <th>Inicio</th>
                            <th>Fim</th>
                            <th>RBW</th>
                            <th>Traces</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        `;
    }

    async function loadFileDetail(fileId) {
        const params = buildDetailQuery();
        const cacheKey = `${fileId}?${params.toString()}`;

        if (detailCache.has(cacheKey)) {
            return detailCache.get(cacheKey);
        }

        const query = params.toString();
        const response = await fetch(
            `${fileSpectraEndpointBase}/${fileId}/spectra${query ? `?${query}` : ""}`
        );
        const payload = await response.json();
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        detailCache.set(cacheKey, rows);
        return rows;
    }

    document.querySelectorAll("[data-file-toggle]").forEach((button) => {
        button.addEventListener("click", async () => {
            const fileId = button.getAttribute("data-file-toggle");
            const detailRow = document.querySelector(`[data-file-detail="${fileId}"]`);

            if (!detailRow) {
                return;
            }

            const isOpen = !detailRow.hidden;
            detailRow.hidden = isOpen;
            button.textContent = isOpen ? "+" : "−";
            button.setAttribute("aria-expanded", isOpen ? "false" : "true");

            if (isOpen || detailRow.dataset.loaded === "1") {
                return;
            }

            const panel = detailRow.querySelector(".file-detail-panel");

            try {
                const rows = await loadFileDetail(fileId);
                panel.innerHTML = buildDetailHtml(rows);
                detailRow.dataset.loaded = "1";
            } catch (error) {
                panel.innerHTML = '<div class="file-detail-error">Nao foi possivel carregar os espectros deste arquivo.</div>';
            }
        });
    });
})();
