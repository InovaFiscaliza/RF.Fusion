/* Host page controller
 *
 * This file owns the small interactive layer of `/host`:
 * - keeping the "Apenas Online" filter in sync with the query string
 * - lazy-loading the station location history
 * - lazy-loading grouped backup/processing diagnostics when details panels open
 *
 * The template remains responsible for rendering the initial page state and
 * exposing the selected host id via `#host-page-root[data-host-id]`.
 */
(function () {
    const root = document.getElementById("host-page-root");
    const hostSelect = document.querySelector("[name='host_id']");
    const onlineOnlyCheckbox = document.getElementById("online_only");
    const locationRows = document.getElementById("host-location-history-rows");
    const hostId = root?.dataset.hostId || "";

    if (!root) {
        return;
    }

    /* All dynamic fragments on this page are rendered as HTML strings before
     * being injected into existing tables. This helper keeps those fragments
     * safe and predictable, especially because diagnostic messages and
     * locality names originate from backend payloads rather than fixed copy
     * embedded in the template.
     */
    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    /* The checkbox is meant to behave like an immediate filter toggle instead
     * of waiting for form submit, so we rewrite the current URL and keep the
     * selected host in the query string. */
    function handleOnlineOnlyToggle() {
        if (!onlineOnlyCheckbox) {
            return;
        }

        const url = new URL(window.location.href);

        if (hostSelect && hostSelect.value) {
            url.searchParams.set("host_id", hostSelect.value);
        } else {
            url.searchParams.delete("host_id");
        }

        url.searchParams.set("online_only", onlineOnlyCheckbox.checked ? "1" : "0");

        if (window.showPageLoadingOverlay) {
            window.showPageLoadingOverlay("Atualizando filtro de estações...");
        }

        window.location.href = url.toString();
    }

    /* Location history is always secondary context, so we fetch it after the
     * initial page render rather than slowing down the first HTML response.
     *
     * The `/host` view is optimized for quick operational checks: current
     * status first, historical locality trail second. This loader preserves
     * that priority by treating the history table as a progressive enhancement
     * instead of a blocking dependency for the whole screen.
     */
    function bindLocationHistoryLoader() {
        if (!hostId || !locationRows) {
            return;
        }

        let locationLoaded = false;

        /* The backend returns a compact locality-history payload. This renderer
         * translates it into the existing table shell already present in the
         * template, handling both normal rows and the explicit "no history"
         * empty state in one place.
         */
        function renderLocationHistory(payload) {
            const locationHistory = Array.isArray(payload.location_history) ? payload.location_history : [];

            if (locationHistory.length === 0) {
                locationRows.innerHTML = `
                    <tr>
                        <td colspan="6">Sem histórico de localidades no catálogo espectral para esta estação.</td>
                    </tr>
                `;
                return;
            }

            locationRows.innerHTML = locationHistory.map((row) => {
                const countyState = row.COUNTY_NAME && row.STATE_CODE
                    ? `${escapeHtml(row.COUNTY_NAME)}/${escapeHtml(row.STATE_CODE)}`
                    : row.COUNTY_NAME
                    ? escapeHtml(row.COUNTY_NAME)
                    : "—";

                return `
                    <tr>
                        <td>${escapeHtml(row.ID_SITE)}</td>
                        <td>${escapeHtml(row.LOCALITY_LABEL || "—")}</td>
                        <td>${countyState}</td>
                        <td>${escapeHtml(row.FIRST_SEEN_AT || "—")}</td>
                        <td>${escapeHtml(row.LAST_SEEN_AT || "—")}</td>
                        <td>${escapeHtml(row.SPECTRUM_COUNT || 0)}</td>
                    </tr>
                `;
            }).join("");
        }

        /* The loader is intentionally one-shot.
         *
         * Location history is informative context, not a live metric, so we
         * avoid refetching it every time the operator interacts with the page.
         * The loading/error placeholders are rendered directly into the table
         * body so the user never sees a blank panel with unclear status.
         */
        async function loadLocationHistory() {
            if (locationLoaded) {
                return;
            }

            locationRows.innerHTML = `
                <tr>
                    <td colspan="6" class="summary-card-value-loading">CARREGANDO</td>
                </tr>
            `;

            try {
                const response = await fetch(`/api/host/${hostId}/locations`);
                const payload = await response.json();
                renderLocationHistory(payload);
                locationLoaded = true;
            } catch (error) {
                locationRows.innerHTML = `
                    <tr>
                        <td colspan="6">Nao foi possivel carregar o histórico de localidades agora.</td>
                    </tr>
                `;
            }
        }

        loadLocationHistory();
    }

    /* Diagnostic tables can be large and rarely matter on the first glance, so
     * they load only when the operator explicitly opens the panel.
     *
     * This binder abstracts the shared behavior between processing and backup
     * diagnostics:
     * - wait until the `<details>` panel is opened,
     * - render a clear loading state inside the table,
     * - fetch grouped errors once,
     * - update both the summary meta line and the row list,
     * - preserve a readable failure state if the request breaks.
     *
     * The goal is to keep the first render light without making the secondary
     * diagnostics feel bolted on or inconsistent.
     */
    function bindDiagnosticPanel(panel, options) {
        if (!panel || !hostId) {
            return;
        }

        const meta = panel.querySelector(options.metaSelector);
        const rowsContainer = panel.querySelector(options.rowsSelector);
        let loaded = false;
        let loading = false;

        /* The compact meta line above each table gives the operator immediate
         * scale context ("how many grouped errors / how many occurrences")
         * without forcing them to scan the whole breakdown first.
         */
        function setMeta(message) {
            if (meta) {
                meta.textContent = message;
            }
        }

        /* Both diagnostic endpoints share the same row contract, so one row
         * builder keeps the presentation consistent across backup and
         * processing panels and guarantees the same empty-state behavior.
         */
        function buildRows(rows) {
            if (!rowsContainer) {
                return;
            }

            if (!Array.isArray(rows) || rows.length === 0) {
                rowsContainer.innerHTML = `
                    <tr>
                        <td colspan="2">${options.emptyMessage}</td>
                    </tr>
                `;
                return;
            }

            rowsContainer.innerHTML = rows.map((row) => `
                <tr>
                    <td class="diagnostic-message-cell">${escapeHtml(row.ERROR_MESSAGE || "(Sem mensagem)")}</td>
                    <td class="diagnostic-count-col">${escapeHtml(row.ERROR_COUNT || 0)}</td>
                </tr>
            `).join("");
        }

        /* The native `<details>` `toggle` event gives us a clean UX contract:
         * only spend backend work when the operator signals interest by
         * expanding the panel. The `loaded/loading` flags prevent duplicate
         * requests during fast repeated toggles or double-open scenarios.
         */
        panel.addEventListener("toggle", async () => {
            if (!panel.open || loaded || loading || !rowsContainer) {
                return;
            }

            loading = true;
            setMeta("Carregando...");
            rowsContainer.innerHTML = `
                <tr>
                    <td colspan="2">${options.loadingMessage}</td>
                </tr>
            `;

            try {
                const response = await fetch(options.url(hostId));
                const payload = await response.json();
                buildRows(payload.rows || []);
                setMeta(`${payload.error_group_count || 0} tipos / ${payload.error_total_occurrences || 0} ocorrências`);
                loaded = true;
            } catch (error) {
                rowsContainer.innerHTML = `
                    <tr>
                        <td colspan="2">${options.failureMessage}</td>
                    </tr>
                `;
                setMeta("Falha ao carregar");
            } finally {
                loading = false;
            }
        });
    }

    /* Bootstrap section
     *
     * The page has three independent interactive concerns and each one is
     * optional depending on the current server-rendered state:
     * - the online-only toggle,
     * - the locality history table,
     * - the grouped diagnostic panels.
     *
     * Wiring them here keeps startup readable and makes it obvious that the
     * page does not rely on a monolithic "init everything" routine.
     */
    if (onlineOnlyCheckbox) {
        onlineOnlyCheckbox.addEventListener("change", handleOnlineOnlyToggle);
    }

    bindLocationHistoryLoader();

    bindDiagnosticPanel(document.querySelector("[data-host-error-panel]"), {
        metaSelector: "[data-host-error-meta]",
        rowsSelector: "[data-host-error-rows]",
        url: (currentHostId) => `/api/host/${currentHostId}/processing-errors`,
        emptyMessage: "Nenhum erro de processamento agrupado para esta estação.",
        loadingMessage: "Carregando erros agrupados desta estação...",
        failureMessage: "Nao foi possivel carregar os erros agrupados agora."
    });

    bindDiagnosticPanel(document.querySelector("[data-host-backup-error-panel]"), {
        metaSelector: "[data-host-backup-error-meta]",
        rowsSelector: "[data-host-backup-error-rows]",
        url: (currentHostId) => `/api/host/${currentHostId}/backup-errors`,
        emptyMessage: "Nenhum erro de backup agrupado para esta estação.",
        loadingMessage: "Carregando erros agrupados de backup desta estação...",
        failureMessage: "Nao foi possivel carregar os erros agrupados de backup agora."
    });
})();
