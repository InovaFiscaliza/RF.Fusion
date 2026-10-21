/* Spectrum page controller
 *
 * This page combines geographic filters and explicit search criteria:
 * - state and district narrow the equipment catalog in one direction
 * - observed periods follow geography; typed criteria apply only on search
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
    const pageJumpForm = document.getElementById("spectrum-page-jump-form");
    const pageJumpField = document.getElementById("spectrum-page-jump");
    const startDateField = queryForm ? queryForm.elements.start_date : null;
    const endDateField = queryForm ? queryForm.elements.end_date : null;
    const freq_start_field = queryForm ? queryForm.elements.freq_start : null;
    const freq_end_field = queryForm ? queryForm.elements.freq_end : null;
    const hiddenSiteField = queryForm ? queryForm.elements.site_id : null;
    const filter_status = document.getElementById("spectrum-filter-status");
    const remove_site_button = document.getElementById("remove-spectrum-site");
    const retry_filters_button = document.getElementById("retry-spectrum-filters");
    const search_button = queryForm?.querySelector('button[type="submit"]');

    if (!root || !queryForm || !equipmentField || !stateField || !districtField) {
        return;
    }

    const webfusionUrl = typeof window.webfusionUrl === "function"
        ? window.webfusionUrl
        : (pathname) => pathname;
    const filtersEndpoint = root.dataset.filtersEndpoint || webfusionUrl("/api/spectrum/filters");
    const fileSpectraEndpointBase = root.dataset.fileSpectraEndpointBase || webfusionUrl("/api/spectrum/file");
    const usesLightweightBootstrap = root.dataset.lightweightBootstrap === "1";
    const filterCache = new Map();
    const detailCache = new Map();
    const selectConfigs = [
        {
            field: equipmentField,
            key: "equipments",
            idKey: "ID_EQUIPMENT",
            labelKey: "OPTION_LABEL",
            defaultOptionLabel: "Todas as estações",
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
    let filterAbortController = null;
    let current_availability;
    // Dates supplied in a link belong to that search; new geography gets new dates.
    let automatic_period = Boolean(startDateField && endDateField
        && !startDateField.value && !endDateField.value);
    let state_inherited_from_map = Boolean(hiddenSiteField && hiddenSiteField.value && stateField.value);

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

    function validate_filter_ranges() {
        const date_error = startDateField?.value && endDateField?.value
            && startDateField.value > endDateField.value
            ? "O período inicial deve ser anterior ou igual ao período final." : "";
        const frequency_error = freq_start_field?.value && freq_end_field?.value
            && Number(freq_start_field.value) > Number(freq_end_field.value)
            ? "A frequência inicial deve ser menor ou igual à frequência final." : "";
        endDateField?.setCustomValidity(date_error);
        freq_end_field?.setCustomValidity(frequency_error);
        if (date_error || frequency_error) {
            setPeriodHint(date_error || frequency_error);
            return false;
        }
        return true;
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

    function submitPageJump(event) {
        event.preventDefault();
        if (!pageJumpField) {
            return;
        }

        const requestedPage = Number.parseInt(pageJumpField.value, 10);
        const totalPages = Number.parseInt(pageJumpField.max, 10);
        if (!Number.isInteger(requestedPage) || requestedPage < 1 || requestedPage > totalPages) {
            pageJumpField.setCustomValidity(`Informe uma página entre 1 e ${totalPages}.`);
            pageJumpField.reportValidity();
            return;
        }

        pageJumpField.setCustomValidity("");
        const targetUrl = new URL(window.location.href);
        targetUrl.searchParams.set("page", String(requestedPage));
        window.showPageLoadingOverlay?.("Carregando resultados...");
        window.location.assign(targetUrl.toString());
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
        field.setAttribute("aria-busy", String(isRefreshing));
    }

    function set_filter_pending(pending) {
        if (search_button) {
            search_button.disabled = pending;
        }
        queryForm.setAttribute("aria-busy", String(pending));
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

    function hasDateFields() {
        return Boolean(startDateField && endDateField);
    }

    function cancelPendingFilterRefresh() {
        filterRequestSerial += 1;

        if (filterAbortController) {
            filterAbortController.abort();
            filterAbortController = null;
        }

        selectConfigs.forEach(({ field }) => setFieldRefreshing(field, false));
        set_filter_pending(false);
    }

    function populateSelectOptions(config, rows, selectedValue, selectedLabel) {
        const { field, idKey, labelKey, defaultOptionLabel } = config;
        const expected_options = [
            { value: "", label: defaultOptionLabel },
            ...rows.map((row) => ({ value: String(row[idKey]), label: String(row[labelKey]) })),
        ];
        if (selectedValue && selectedLabel && !expected_options.some((option) => option.value === String(selectedValue))) {
            expected_options.push({
                value: String(selectedValue),
                label: `${selectedLabel.replace(/ — indisponível neste recorte$/, "")} — indisponível neste recorte`,
            });
        }
        // Preserve native menus when an unrelated filter finishes loading.
        if (field.options.length === expected_options.length && expected_options.every((option, index) => (
            field.options[index].value === option.value
            && field.options[index].textContent.trim() === option.label.trim()
        ))) {
            const value = selectedValue ? String(selectedValue) : "";
            if (field.value !== value) {
                field.value = value;
            }
            return;
        }
        field.innerHTML = "";

        expected_options.forEach(({ value, label }) => {
            const option = document.createElement("option");
            option.value = value;
            option.textContent = label;
            field.appendChild(option);
        });

        field.value = selectedValue ? String(selectedValue) : "";
    }

    function applyAvailability(availability) {
        current_availability = availability || null;
        if (!availability || !availability.DATE_START || !availability.DATE_END) {
            if (automatic_period && hasDateFields()) {
                startDateField.value = "";
                endDateField.value = "";
            }

            if (startDateField) {
                startDateField.removeAttribute("min");
                startDateField.removeAttribute("max");
            }

            if (endDateField) {
                endDateField.removeAttribute("min");
                endDateField.removeAttribute("max");
            }

            setPeriodHint(
                "Nenhum período observado disponível para esta combinação de filtros."
            );
            return;
        }

        if (automatic_period && hasDateFields()) {
            startDateField.value = availability.DATE_START;
            endDateField.value = availability.DATE_END;
        }

        setPeriodHint(
            `Faixa observada: ${availability.DATE_START_DISPLAY} até ${availability.DATE_END_DISPLAY}. Você pode pesquisar um período mais amplo.`
        );
        if (!automatic_period && hasDateFields() && (
            (endDateField.value && endDateField.value < availability.DATE_START)
            || (startDateField.value && startDateField.value > availability.DATE_END)
        )) {
            setPeriodHint(`O período informado está fora da faixa observada (${availability.DATE_START_DISPLAY} até ${availability.DATE_END_DISPLAY}). Revise as datas informadas.`);
        }
    }

    function renderFilterPayload(payload, snapshots) {
        const equipments = Array.isArray(payload.equipments) ? payload.equipments : [];
        const states = Array.isArray(payload.states) ? payload.states : [];
        const districts = Array.isArray(payload.districts) ? payload.districts : [];

        populateSelectOptions(selectConfigs[0], equipments, snapshots.equipment.value, snapshots.equipment.label);
        populateSelectOptions(selectConfigs[1], states, snapshots.state.value, snapshots.state.label);
        populateSelectOptions(selectConfigs[2], districts, snapshots.district.value, snapshots.district.label);

        if (!districts.length && !stateField.value) {
            setDistrictHint("Selecione um estado para carregar os distritos disponíveis.");
        } else {
            setDistrictHint(
                districts.length
                    ? `${districts.length} distritos disponiveis para o recorte geografico atual.`
                    : "Nenhum distrito encontrado para o recorte geografico atual."
            );
        }

        applyAvailability(payload.availability);
        validate_filter_ranges();
    }

    async function refreshFilterOptions(options = {}) {
        const { force = false } = options;
        if (retry_filters_button) {
            retry_filters_button.hidden = true;
        }
        const params = buildFilterQuery();
        const cacheKey = params.toString();
        const currentRequest = ++filterRequestSerial;
        const snapshots = {
            equipment: getSelectionSnapshot(equipmentField),
            state: getSelectionSnapshot(stateField),
            district: getSelectionSnapshot(districtField),
        };

        if (filterAbortController) {
            filterAbortController.abort();
            filterAbortController = null;
        }

        if (!force && filterCache.has(cacheKey)) {
            renderFilterPayload(filterCache.get(cacheKey), snapshots);
            selectConfigs.forEach(({ field }) => setFieldRefreshing(field, false));
            set_filter_pending(false);
            return;
        }

        filterAbortController = new AbortController();
        set_filter_pending(true);
        selectConfigs.forEach(({ field }) => setFieldRefreshing(field, true));
        setDistrictHint("Atualizando distritos observados...");
        setPeriodHint("Atualizando periodo observado...");

        try {
            const requestParams = new URLSearchParams(params);
            if (cacheKey === "") {
                // An explicit broadening still needs the global observed period.
                requestParams.set("bootstrap", "0");
            }
            // The server returns the complete dependent-filter snapshot in one request.
            requestParams.set("view", "selection");
            const response = await fetch(`${filtersEndpoint}?${requestParams}`, {
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
            renderFilterPayload(payload, snapshots);
        } catch (error) {
            if (error && error.name === "AbortError") {
                return;
            }

            if (currentRequest !== filterRequestSerial) {
                return;
            }

            setDistrictHint("Nao foi possivel atualizar os distritos agora.");
            setPeriodHint("Nao foi possivel atualizar o periodo observado agora.");
            if (automatic_period && hasDateFields()) {
                startDateField.value = "";
                endDateField.value = "";
            }
            if (retry_filters_button) {
                retry_filters_button.hidden = false;
            }
        } finally {
            if (currentRequest === filterRequestSerial) {
                filterAbortController = null;
                selectConfigs.forEach(({ field }) => setFieldRefreshing(field, false));
                set_filter_pending(false);
            }
        }
    }

    function refresh_geographic_filters(message) {
        cancelPendingFilterRefresh();
        // A previous context's dates must not survive a geographic change.
        automatic_period = true;
        current_availability = undefined;
        if (hasDateFields()) {
            startDateField.value = "";
            endDateField.value = "";
        }
        validate_filter_ranges();
        if (message) {
            setDistrictHint(message);
        }
        refreshFilterOptions();
    }

    function release_site_constraint() {
        if (hiddenSiteField && hiddenSiteField.value) {
            hiddenSiteField.value = "";
            const banner = document.getElementById("spectrum-site-context");
            if (banner) {
                banner.hidden = true;
            }
        }
    }

    function release_inherited_state() {
        // A map-derived state must not survive after its narrower filters are cleared.
        if (state_inherited_from_map && !districtField.value && !equipmentField.value) {
            stateField.value = "";
            state_inherited_from_map = false;
            return true;
        }
        return false;
    }

    equipmentField.addEventListener("change", () => {
        release_site_constraint();
        const released_state = release_inherited_state();
        refresh_geographic_filters("Estação alterada. Atualizando período observado...");
        mark_filters_changed(released_state ? "Estado herdado do mapa removido; a busca agora abrange todos os estados." : "");
    });

    stateField.addEventListener("change", () => {
        state_inherited_from_map = false;
        release_site_constraint();
        populateSelectOptions(selectConfigs[2], [], "", "");
        populateSelectOptions(selectConfigs[0], [], "", "");
        refresh_geographic_filters("Estado alterado. Atualizando distritos e estações...");
        mark_filters_changed("Estado alterado; distrito, estação e restrição por site foram removidos.");
    });

    districtField.addEventListener("change", () => {
        release_site_constraint();
        populateSelectOptions(selectConfigs[0], [], "", "");
        const released_state = release_inherited_state();
        refresh_geographic_filters("Distrito alterado. Atualizando estações...");
        mark_filters_changed(released_state
            ? "Estado herdado do mapa removido; a busca agora abrange todos os estados."
            : "Distrito alterado; estação e restrição por site foram removidas.");
    });

    if (!usesLightweightBootstrap) {
        refreshFilterOptions({
            force: true,
        });
    } else {
        setDistrictHint("Selecione um estado para carregar os distritos disponíveis.");
        setPeriodHint(
            "O periodo observado sera carregado quando houver contexto geografico suficiente."
        );
    }

    if (clearFiltersLink) {
        clearFiltersLink.addEventListener("click", () => {
            cancelPendingFilterRefresh();
        });
    }

    function mark_filters_changed(message = "") {
        if (filter_status) {
            filter_status.textContent = `${typeof message === "string" && message ? message : "Filtros alterados."} Clique em Pesquisar para atualizar os resultados abaixo.`;
        }
    }

    function update_local_period() {
        if (!validate_filter_ranges()) {
            return;
        }
        // An ongoing context refresh will supply its own availability without losing edits.
        if (!filterAbortController && current_availability !== undefined) {
            applyAvailability(current_availability);
        }
    }

    queryForm.addEventListener("input", (event) => {
        mark_filters_changed();
        const field = event.target;
        if (field === startDateField || field === endDateField) {
            automatic_period = false;
        }
        if ([startDateField, endDateField, freq_start_field, freq_end_field].includes(field)) {
            update_local_period();
        }
    });
    queryForm.addEventListener("submit", (event) => {
        if (!validate_filter_ranges()) {
            event.preventDefault();
            return;
        }
        if (filterAbortController) {
            event.preventDefault();
            mark_filters_changed("Aguarde a atualização dos filtros antes de pesquisar.");
            return;
        }
        cancelPendingFilterRefresh();
    });

    retry_filters_button?.addEventListener("click", () => {
        cancelPendingFilterRefresh();
        refreshFilterOptions({ force: true });
    });

    if (remove_site_button && hiddenSiteField) {
        remove_site_button.hidden = false;
        remove_site_button.addEventListener("click", () => {
            release_site_constraint();
            mark_filters_changed();
            refresh_geographic_filters("Restrição por site removida. Atualizando opções...");
        });
    }

    function renderDetailCell(value, shouldHighlight) {
        const content = escapeHtml(value ?? "—");
        return shouldHighlight ? `<strong>${content}</strong>` : content;
    }

    function formatFrequency(value) {
        const numericValue = Number(value);

        if (!Number.isFinite(numericValue)) {
            return value ?? "—";
        }

        return numericValue.toFixed(2);
    }

    function formatFrequencyRange(startValue, endValue) {
        return `${formatFrequency(startValue)} – ${formatFrequency(endValue)}`;
    }

    function buildDetailHtml(rows) {
        if (!Array.isArray(rows) || rows.length === 0) {
            return '<div class="file-detail-empty">Nenhum espectro vinculado a este arquivo.</div>';
        }

        const shouldHighlightMatches = rows.some((row) => Number(row.IS_MATCH) !== 1);
        const sorted_rows = [...rows].sort((left, right) => {
            // HTML collapses whitespace; compare plans as they appear in the table.
            const left_plan = (left.NA_DESCRIPTION || "").trim().replace(/\s+/g, " ");
            const right_plan = (right.NA_DESCRIPTION || "").trim().replace(/\s+/g, " ");
            const plan_order = left_plan.localeCompare(right_plan, "pt-BR", {
                numeric: true,
                sensitivity: "base",
            });

            return plan_order
                || Number(left.NU_FREQ_START) - Number(right.NU_FREQ_START)
                || Number(left.NU_FREQ_END) - Number(right.NU_FREQ_END);
        });
        const body = sorted_rows.map((row) => {
            const isMatch = shouldHighlightMatches && Number(row.IS_MATCH) === 1;

            return `
                <tr class="${isMatch ? "file-detail-match" : ""}">
                    <td>${renderDetailCell(row.ID_SPECTRUM, isMatch)}</td>
                    <td>${renderDetailCell(row.NA_DESCRIPTION || "—", isMatch)}</td>
                    <td>${renderDetailCell(
                        formatFrequencyRange(row.NU_FREQ_START, row.NU_FREQ_END),
                        isMatch
                    )}</td>
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

    pageJumpForm?.addEventListener("submit", submitPageJump);

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
