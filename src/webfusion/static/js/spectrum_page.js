/* Spectrum page controller
 *
 * This file owns the interactive layer of `/spectrum`:
 * - dependent locality loading based on the selected equipment and mode
 * - URL normalization when switching between "Espectro" and "Arquivo"
 * - lazy loading of per-file spectrum details in file mode
 *
 * The template keeps the initial data rendered by Jinja and exposes only the
 * API endpoints needed for the browser-side refresh flows.
 */
(function () {
    const root = document.getElementById("spectrum-page-root");
    const queryForm = document.getElementById("spectrum-query-form");
    const equipmentField = document.getElementById("equipment_id");
    const siteField = document.getElementById("site_id");
    const queryModeField = document.getElementById("query_mode");
    const siteHint = document.getElementById("site-filter-hint");

    if (!root || !queryForm || !equipmentField || !siteField || !queryModeField) {
        return;
    }

    const localitiesEndpoint = root.dataset.localitiesEndpoint || "/api/spectrum/localities";
    const fileSpectraEndpointBase = root.dataset.fileSpectraEndpointBase || "/api/spectrum/file";

    let localityRequestSerial = 0;
    let refreshTimer = null;
    const localityCache = new Map();
    const detailCache = new Map();
    const hasInitialLocalityOptions = siteField.options.length > 1;

    /* This page still renders a few small HTML fragments manually, especially
     * the expandable file-detail table. Sanitizing centrally keeps those
     * fragments safe before interpolation and avoids repeating escaping logic
     * in each builder.
     */
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

    /* Build the locality-endpoint query from the minimum state that actually
     * changes the available site list.
     *
     * Localities depend only on the selected equipment and the active query
     * mode. Other filters (date range, sort, text) do not change that
     * universe, so they are intentionally left out of this request contract.
     */
    function buildLocationQuery() {
        const params = new URLSearchParams();
        const equipmentId = equipmentField.value.trim();

        if (equipmentId) {
            params.set("equipment_id", equipmentId);
        }

        params.set("query_mode", queryModeField.value || "spectrum");
        return params;
    }

    /* Switching modes keeps as much context as possible, but normalizes
     * incompatible filters so the next request stays predictable. */
    function buildModeChangeUrl(nextQueryMode) {
        /* Mode switching is more than a label change.
         *
         * `Espectro` and `Arquivo` share some filters, but others are
         * semantically incompatible. This function preserves the useful user
         * context while pruning or translating parameters that would make the
         * next screen ambiguous or invalid.
         */
        const params = new URLSearchParams();
        const formData = new FormData(queryForm);

        for (const [key, rawValue] of formData.entries()) {
            const value = typeof rawValue === "string" ? rawValue.trim() : rawValue;

            if (!value) {
                continue;
            }

            params.set(key, value);
        }

        params.set("query_mode", nextQueryMode || "spectrum");
        params.delete("page");

        if (nextQueryMode === "file") {
            params.delete("freq_start");
            params.delete("freq_end");
            params.delete("description");

            const currentSortBy = params.get("sort_by");
            const currentSortOrder = (params.get("sort_order") || "DESC").toUpperCase();

            params.delete("sort_order");

            if (!["recent", "oldest", "file_name_asc", "file_name_desc", "spectrum_count_desc", "spectrum_count_asc"].includes(currentSortBy)) {
                if (currentSortBy === "file_name") {
                    params.set("sort_by", currentSortOrder === "DESC" ? "file_name_desc" : "file_name_asc");
                } else if (currentSortBy === "spectrum_count") {
                    params.set("sort_by", currentSortOrder === "ASC" ? "spectrum_count_asc" : "spectrum_count_desc");
                } else if (currentSortBy === "oldest" || (["date_start", "date_end"].includes(currentSortBy) && currentSortOrder === "ASC")) {
                    params.set("sort_by", "oldest");
                } else {
                    params.set("sort_by", "recent");
                }
            } else if (!params.get("sort_by")) {
                params.set("sort_by", "recent");
            }
        } else {
            const currentSortBy = params.get("sort_by");
            const currentSortOrder = (params.get("sort_order") || "DESC").toUpperCase();

            params.delete("sort_order");

            if (!["recent", "oldest", "freq_start", "freq_end"].includes(currentSortBy)) {
                if (["freq_start", "freq_end"].includes(currentSortBy)) {
                    params.set("sort_by", currentSortBy);
                } else if (["date_start", "date_end"].includes(currentSortBy) && currentSortOrder === "ASC") {
                    params.set("sort_by", "oldest");
                } else {
                    params.set("sort_by", "recent");
                }
            } else if (!params.get("sort_by")) {
                params.set("sort_by", "recent");
            }
        }

        const query = params.toString();
        return `${queryForm.getAttribute("action") || window.location.pathname}${query ? `?${query}` : ""}`;
    }

    function hasEquipmentSelection() {
        return Boolean(equipmentField.value.trim());
    }

    /* Reset the locality select to its neutral state.
     *
     * This is used both before any equipment is chosen and during transitions
     * where the current locality selection is no longer trustworthy.
     */
    function resetSiteOptions(message) {
        siteField.innerHTML = '<option value="">Todas as localidades</option>';
        siteField.value = "";
        setSiteHint(message);
    }

    /* Mark the locality selector as temporarily stale without destroying the
     * current choice. This is useful while a debounce window is running or
     * after a recoverable request failure.
     */
    function markSiteOptionsStale(message) {
        setSiteHint(message);
    }

    /* The select gets a temporary visual "refreshing" treatment while the
     * locality list is being recomputed, so the user understands that the
     * control is in transition rather than simply inert.
     */
    function setSiteFieldRefreshing(isRefreshing) {
        siteField.classList.toggle("is-refreshing", isRefreshing);
    }

    /* Capture the current locality selection before refreshing the option
     * list. If the same site still exists after the refresh, we restore it so
     * the user does not lose context just because the backing list was
     * reloaded.
     */
    function getCurrentSelectionSnapshot() {
        const selectedOption = siteField.options[siteField.selectedIndex];

        return {
            value: siteField.value ? String(siteField.value) : "",
            label: selectedOption ? selectedOption.textContent : "",
        };
    }

    /* Rebuild the locality select from the latest backend response.
     *
     * The renderer keeps the "Todas as localidades" option stable and also
     * preserves a previously selected value when the backend result is still
     * compatible with it. That makes refreshes feel less destructive during
     * iterative filtering.
     */
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

    /* Locality choices depend only on station/equipment and query mode, so we
     * cache them aggressively to keep the filter responsive. */
    async function refreshLocalities(options = {}) {
        /* This loader has two important guard rails:
         * - cache by `(equipment, mode)` so repeated toggles stay snappy
         * - request serials so slow responses cannot overwrite newer choices
         *
         * Together they prevent the dependent select from feeling jumpy when
         * the operator changes equipment or mode in quick succession.
         */
        const { force = false } = options;

        if (!hasEquipmentSelection()) {
            resetSiteOptions("Selecione uma estação para listar as localidades conhecidas.");
            return;
        }

        const params = buildLocationQuery();
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
            const payload = await response.json();

            if (currentRequest !== localityRequestSerial) {
                return;
            }

            const rows = Array.isArray(payload.rows) ? payload.rows : [];
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

    /* Debounce locality refreshes after a station change.
     *
     * The select itself is dependent UI, not the primary search action, so a
     * short delay helps avoid redundant requests during quick successive
     * changes while still keeping the screen feeling immediate.
     */
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

    /* Equipment changes invalidate the known-locality universe. We reset the
     * locality control immediately so the user never keeps seeing an old site
     * list that belongs to a different station/equipment.
     */
    equipmentField.addEventListener("change", () => {
        resetSiteOptions("Estação alterada. Carregando localidades conhecidas...");
        scheduleLocalityRefresh("Estação alterada. Carregando localidades conhecidas...");
    });

    /* Query mode changes are normalized through a full URL transition instead
     * of a partial in-place update. That keeps pagination, sorting and server-
     * rendered results coherent with the chosen mode.
     */
    queryModeField.addEventListener("change", () => {
        if (window.showPageLoadingOverlay) {
            window.showPageLoadingOverlay("Atualizando modo de consulta...");
        }

        window.location.href = buildModeChangeUrl(queryModeField.value || "spectrum");
    });

    if (hasEquipmentSelection()) {
        if (!hasInitialLocalityOptions || siteField.options.length <= 2) {
            refreshLocalities();
        } else {
            setSiteHint(`${Math.max(siteField.options.length - 1, 0)} localidades disponíveis.`);
        }
    } else if (!hasInitialLocalityOptions) {
        resetSiteOptions("Selecione uma estação para listar as localidades conhecidas.");
    }

    /* The file-detail panel is rendered lazily because most rows will never be
     * expanded. The markup builder stays separate so cache hits and network
     * fetches both feed the same presentation path.
     */
    function buildDetailHtml(rows) {
        if (!Array.isArray(rows) || rows.length === 0) {
            return '<div class="file-detail-empty">Nenhum espectro vinculado a este arquivo.</div>';
        }

        const body = rows.map((row) => `
            <tr>
                <td>${escapeHtml(row.ID_SPECTRUM)}</td>
                <td>${escapeHtml(row.NA_DESCRIPTION || "—")}</td>
                <td>${escapeHtml(row.NU_FREQ_START)} – ${escapeHtml(row.NU_FREQ_END)}</td>
                <td>${escapeHtml(row.DT_TIME_START || "—")}</td>
                <td>${escapeHtml(row.DT_TIME_END || "—")}</td>
                <td>${escapeHtml(row.NU_RBW || "—")}</td>
                <td>${escapeHtml(row.NU_TRACE_COUNT || "—")}</td>
            </tr>
        `).join("");

        return `
            <div class="file-detail-header">Espectros internos do arquivo</div>
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

    /* File-mode rows stay compact by default, and expand only when the
     * operator asks to inspect the spectra aggregated under that file. */
    async function loadFileDetail(fileId) {
        /* Details are cached per file id because the expanded panel is a pure
         * inspection affordance. Once loaded, reopening the same row should be
         * instant and should not hit the endpoint again in the same page view.
         */
        if (detailCache.has(fileId)) {
            return detailCache.get(fileId);
        }

        const response = await fetch(`${fileSpectraEndpointBase}/${fileId}/spectra`);
        const payload = await response.json();
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        detailCache.set(fileId, rows);
        return rows;
    }

    /* Expansion wiring is intentionally local to each file-row toggle.
     *
     * A row can be opened/closed repeatedly, but the remote load happens only
     * on first expansion. After that the row behaves like a cached disclosure
     * panel, which keeps file mode compact without making repeat inspection
     * feel slow.
     */
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
