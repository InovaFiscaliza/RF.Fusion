/* Spectrum page controller
 *
 * This page now has one search mode:
 * - filters are spectrum-aware
 * - results are file-oriented
 * - expanded details show all spectra in the file and highlight the matches
 */
(function () {
    const root = document.getElementById("spectrum-page-root");
    const queryForm = document.getElementById("spectrum-query-form");
    const equipmentField = document.getElementById("equipment_id");
    const siteField = document.getElementById("site_id");
    const siteHint = document.getElementById("site-filter-hint");
    const startDateField = queryForm ? queryForm.elements.start_date : null;
    const endDateField = queryForm ? queryForm.elements.end_date : null;
    const freqStartField = queryForm ? queryForm.elements.freq_start : null;
    const freqEndField = queryForm ? queryForm.elements.freq_end : null;
    const descriptionField = queryForm ? queryForm.elements.description : null;

    if (!root || !queryForm || !equipmentField || !siteField) {
        return;
    }

    const localitiesEndpoint = root.dataset.localitiesEndpoint || "/api/spectrum/localities";
    const fileSpectraEndpointBase = root.dataset.fileSpectraEndpointBase || "/api/spectrum/file";
    const localityCache = new Map();
    const detailCache = new Map();
    const hasInitialLocalityOptions = siteField.options.length > 1;
    let localityRequestSerial = 0;
    let refreshTimer = null;

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function setSiteHint(message) {
        if (siteHint) {
            siteHint.textContent = message;
        }
    }

    function hasEquipmentSelection() {
        return Boolean(equipmentField.value.trim());
    }

    function appendTrimmedParam(params, key, rawValue) {
        const value = typeof rawValue === "string" ? rawValue.trim() : "";

        if (value) {
            params.set(key, value);
        }
    }

    /* The locality selector is dependent UI. It should follow the active form
     * state so operators never pick a site that is already impossible under
     * the current station/date/frequency/description filters.
     */
    function buildLocalityQuery() {
        const params = new URLSearchParams();
        appendTrimmedParam(params, "equipment_id", equipmentField.value);
        appendTrimmedParam(params, "start_date", startDateField ? startDateField.value : "");
        appendTrimmedParam(params, "end_date", endDateField ? endDateField.value : "");
        appendTrimmedParam(params, "freq_start", freqStartField ? freqStartField.value : "");
        appendTrimmedParam(params, "freq_end", freqEndField ? freqEndField.value : "");
        appendTrimmedParam(params, "description", descriptionField ? descriptionField.value : "");
        return params;
    }

    /* Detail highlighting must follow the search that produced the current
     * table, not unsaved edits sitting in the form. Using `location.search`
     * keeps the expanded panel aligned with the rendered result rows.
     */
    function buildDetailQuery() {
        const source = new URLSearchParams(window.location.search);
        const params = new URLSearchParams();

        [
            "equipment_id",
            "site_id",
            "start_date",
            "end_date",
            "freq_start",
            "freq_end",
            "description",
        ].forEach((key) => appendTrimmedParam(params, key, source.get(key) || ""));

        return params;
    }

    function resetSiteOptions(message) {
        siteField.innerHTML = '<option value="">Todas as localidades</option>';
        siteField.value = "";
        setSiteHint(message);
    }

    function markSiteOptionsStale(message) {
        setSiteHint(message);
    }

    function setSiteFieldRefreshing(isRefreshing) {
        siteField.classList.toggle("is-refreshing", isRefreshing);
    }

    function getCurrentSelectionSnapshot() {
        const selectedOption = siteField.options[siteField.selectedIndex];

        return {
            value: siteField.value ? String(siteField.value) : "",
            label: selectedOption ? selectedOption.textContent : "",
        };
    }

    function populateSiteOptions(rows, selectedValue, selectedLabel) {
        siteField.innerHTML = '<option value="">Todas as localidades</option>';

        rows.forEach((row) => {
            const option = document.createElement("option");
            option.value = String(row.ID_SITE);
            option.textContent = row.OPTION_LABEL;
            siteField.appendChild(option);
        });

        const hasSelectedValue = selectedValue
            && rows.some((row) => String(row.ID_SITE) === String(selectedValue));

        if (selectedValue && !hasSelectedValue && selectedLabel) {
            const option = document.createElement("option");
            option.value = String(selectedValue);
            option.textContent = selectedLabel;
            siteField.appendChild(option);
        }

        siteField.value = selectedValue ? String(selectedValue) : "";
        setSiteHint(
            rows.length
                ? `${rows.length} localidades disponíveis.`
                : "Nenhuma localidade encontrada para os filtros atuais."
        );
    }

    async function refreshLocalities(options = {}) {
        const { force = false } = options;

        if (!hasEquipmentSelection()) {
            resetSiteOptions("Selecione uma estação para listar as localidades conhecidas.");
            return;
        }

        const params = buildLocalityQuery();
        const cacheKey = params.toString();
        const selectionSnapshot = getCurrentSelectionSnapshot();
        const currentRequest = ++localityRequestSerial;

        if (!force && localityCache.has(cacheKey)) {
            populateSiteOptions(
                localityCache.get(cacheKey),
                selectionSnapshot.value,
                selectionSnapshot.label,
            );
            return;
        }

        setSiteFieldRefreshing(true);
        setSiteHint("Atualizando localidades observadas...");

        try {
            const response = await fetch(`${localitiesEndpoint}?${params.toString()}`);
            if (!response.ok) {
                throw new Error(`localities request failed with status ${response.status}`);
            }

            const payload = await response.json();
            const rows = Array.isArray(payload.rows) ? payload.rows : [];

            if (currentRequest !== localityRequestSerial) {
                return;
            }

            localityCache.set(cacheKey, rows);
            populateSiteOptions(
                rows,
                selectionSnapshot.value,
                selectionSnapshot.label,
            );
        } catch (error) {
            if (currentRequest !== localityRequestSerial) {
                return;
            }

            markSiteOptionsStale("Nao foi possivel atualizar as localidades agora.");
        } finally {
            if (currentRequest === localityRequestSerial) {
                setSiteFieldRefreshing(false);
            }
        }
    }

    function scheduleLocalityRefresh(message) {
        if (!hasEquipmentSelection()) {
            resetSiteOptions("Selecione uma estação para listar as localidades conhecidas.");
            return;
        }

        markSiteOptionsStale(message || "Atualizando localidades observadas...");

        if (refreshTimer) {
            window.clearTimeout(refreshTimer);
        }

        refreshTimer = window.setTimeout(() => {
            refreshLocalities();
        }, 450);
    }

    equipmentField.addEventListener("change", () => {
        resetSiteOptions("Estação alterada. Carregando localidades conhecidas...");
        scheduleLocalityRefresh("Estação alterada. Carregando localidades conhecidas...");
    });

    [startDateField, endDateField, freqStartField, freqEndField]
        .filter(Boolean)
        .forEach((field) => {
            field.addEventListener("change", () => {
                scheduleLocalityRefresh("Filtros alterados. Atualizando localidades...");
            });
        });

    if (descriptionField) {
        descriptionField.addEventListener("input", () => {
            scheduleLocalityRefresh("Filtros alterados. Atualizando localidades...");
        });
    }

    if (hasEquipmentSelection()) {
        if (!hasInitialLocalityOptions || siteField.options.length <= 2) {
            refreshLocalities();
        } else {
            setSiteHint(`${Math.max(siteField.options.length - 1, 0)} localidades disponíveis.`);
        }
    } else if (!hasInitialLocalityOptions) {
        resetSiteOptions("Selecione uma estação para listar as localidades conhecidas.");
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
                ${shouldHighlightMatches ? '<span class="file-detail-match-note">Os compatíveis com a busca aparecem em negrito.</span>' : ""}
            </div>
            <div class="file-detail-table-wrap">
                <table class="file-detail-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Plano</th>
                            <th>Faixa (MHz)</th>
                            <th>Início</th>
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
