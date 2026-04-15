/* Server page controller
 *
 * This file keeps `/server` interactive while the template stays focused on
 * structure and Jinja-rendered content. It owns:
 * - persistence of the host-table panel open state
 * - the immediate "Apenas Online na Tabela" toggle behavior
 * - lazy loading of grouped processing and backup diagnostics
 * - lazy loading of the station table
 * - on-demand loading of heavy server summary metrics
 */
(function () {
    const root = document.getElementById("server-page-root");

    if (!root) {
        return;
    }

    const SERVER_HOST_TABLE_PANEL_STATE_KEY = "webfusion.server.host_table_panel_open";
    const summaryBindings = {
        CURRENT_MONTH_LABEL             : document.getElementById("server-summary-month-label"),
        BACKUP_DONE_THIS_MONTH          : document.getElementById("server-summary-backup-done-month"),
        BACKUP_DONE_GB_THIS_MONTH       : document.getElementById("server-summary-backup-done-gb-month"),
        DISCOVERED_FILES_TOTAL          : document.getElementById("server-summary-discovered-total"),
        BACKUP_PENDING_FILES_TOTAL      : document.getElementById("server-summary-backup-pending-total"),
        BACKUP_ERROR_FILES_TOTAL        : document.getElementById("server-summary-backup-error-total"),
        BACKUP_QUEUE_FILES_TOTAL        : document.getElementById("server-summary-backup-queue-total"),
        BACKUP_QUEUE_GB_TOTAL           : document.getElementById("server-summary-backup-queue-gb"),
        PROCESSING_PENDING_FILES_TOTAL  : document.getElementById("server-summary-processing-pending-total"),
        PROCESSING_QUEUE_FILES_TOTAL    : document.getElementById("server-summary-processing-queue-total"),
        PROCESSING_QUEUE_GB_TOTAL       : document.getElementById("server-summary-processing-queue-gb"),
        PROCESSING_DONE_FILES_TOTAL     : document.getElementById("server-summary-processing-done-total"),
        FACT_SPECTRUM_TOTAL             : document.getElementById("server-summary-fact-spectrum-total"),
        PROCESSING_ERROR_FILES_TOTAL    : document.getElementById("server-summary-processing-error-total"),
        BACKUP_PENDING_GB_TOTAL         : document.getElementById("server-summary-backup-pending-gb"),
    };
    const processingPanel   = document.getElementById("server-processing-errors-panel");
    const processingMeta    = document.getElementById("server-processing-errors-meta");
    const processingBody    = document.getElementById("server-processing-errors-body");
    const backupPanel       = document.getElementById("server-backup-errors-panel");
    const backupMeta        = document.getElementById("server-backup-errors-meta");
    const backupBody        = document.getElementById("server-backup-errors-body");
    const hostTablePanel    = document.getElementById("server-host-table-panel");
    const hostTableMeta     = document.getElementById("server-host-table-meta");
    const hostTableBody     = document.getElementById("server-host-table-body");
    const serverFilterForm  = document.getElementById("server-filter-form");
    const onlineOnlyCheckbox = document.getElementById("online_only");

    const hostDetailBaseUrl         = root.dataset.hostDetailBaseUrl || "";
    const hostTableEndpointBase     = root.dataset.hostTableEndpoint || "";
    const processingErrorsEndpoint  = root.dataset.processingErrorsEndpoint || "";
    const backupErrorsEndpoint      = root.dataset.backupErrorsEndpoint || "";
    const summaryMetricsEndpoint    = root.dataset.summaryMetricsEndpoint || "";
    const currentOnlineOnly         = root.dataset.onlineOnly === "1";

    /* Several panels on this page still render rows through small HTML
     * fragments. This helper keeps those fragments safe before interpolation,
     * especially for host names and grouped diagnostic messages that come from
     * backend payloads rather than fixed template copy.
     */
    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    /* The host table behaves like a navigational drawer inside the dashboard.
     * Persisting its open/closed state in session storage avoids forcing the
     * operator to re-open the panel after every filter refresh or back/forward
     * navigation during the same session.
     */
    function persistServerHostTablePanelState(isOpen) {
        try {
            window.sessionStorage.setItem(
                SERVER_HOST_TABLE_PANEL_STATE_KEY,
                isOpen ? "1" : "0"
            );
        } catch (error) {
            // Storage is only a UX enhancement here; failure should not block navigation.
        }
    }

    /* The table filter behaves as a direct toggle because operators use it as
     * a navigation aid, not as a multi-step search form. */
    function handleOnlineOnlyToggle() {
        if (hostTablePanel) {
            persistServerHostTablePanelState(Boolean(hostTablePanel.open));
        }

        if (window.showPageLoadingOverlay) {
            window.showPageLoadingOverlay("Atualizando filtro de estações...");
        }

        if (serverFilterForm && serverFilterForm.requestSubmit) {
            serverFilterForm.requestSubmit();
            return;
        }

        if (serverFilterForm) {
            serverFilterForm.submit();
        }
    }

    if (hostTablePanel) {
        try {
            if (window.sessionStorage.getItem(SERVER_HOST_TABLE_PANEL_STATE_KEY) === "1") {
                hostTablePanel.open = true;
            }
        } catch (error) {
            // Ignore storage access issues and keep the default collapsed state.
        }
    }

    if (serverFilterForm && hostTablePanel) {
        serverFilterForm.addEventListener("submit", function () {
            persistServerHostTablePanelState(Boolean(hostTablePanel.open));
        });
    }

    if (onlineOnlyCheckbox) {
        onlineOnlyCheckbox.addEventListener("change", handleOnlineOnlyToggle);
    }

    /* Render a single full-width status row into one of the lazy tables.
     *
     * This keeps loading, empty and failure states visually consistent across
     * the diagnostics and host-table panels without each panel duplicating its
     * own mini-renderer for placeholder rows.
     */
    function setMessageRow(targetBody, message) {
        if (!targetBody) {
            return;
        }

        targetBody.innerHTML = "";

        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 2;
        td.textContent = message;
        tr.appendChild(td);
        targetBody.appendChild(tr);
    }

    /* Shared grouped-diagnostic row renderer used by both processing and
     * backup panels. Both endpoints expose the same payload contract, so the
     * page deliberately keeps one rendering path for those breakdown tables.
     */
    function renderRows(targetBody, rows, emptyMessage) {
        if (!targetBody) {
            return;
        }

        targetBody.innerHTML = "";

        if (!Array.isArray(rows) || rows.length === 0) {
            setMessageRow(targetBody, emptyMessage);
            return;
        }

        rows.forEach((row) => {
            const tr = document.createElement("tr");
            const messageTd = document.createElement("td");
            const countTd = document.createElement("td");

            messageTd.className = "diagnostic-message-cell";
            messageTd.textContent = row.ERROR_MESSAGE || "(Sem mensagem)";

            countTd.className = "diagnostic-count-col";
            countTd.textContent = String(row.ERROR_COUNT || 0);

            tr.appendChild(messageTd);
            tr.appendChild(countTd);
            targetBody.appendChild(tr);
        });
    }

    /* The host table is a navigational surface, not just a report.
     *
     * Each row combines status badges, queue counts and a deep link into the
     * `/host` detail page. Rendering the full row HTML in one place keeps that
     * contract obvious and preserves the currently selected online/offline
     * filter when the user drills into a station.
     */
    function renderHostRows(rows) {
        if (!hostTableBody) {
            return;
        }

        if (!Array.isArray(rows) || rows.length === 0) {
            hostTableBody.innerHTML = `
                <tr>
                    <td colspan="10">Nenhuma estação encontrada para o filtro atual.</td>
                </tr>
            `;
            return;
        }

        hostTableBody.innerHTML = rows.map((row) => {
            const connectionBadge = row.IS_OFFLINE
                ? '<span class="status-badge status-offline">Offline</span>'
                : '<span class="status-badge status-online">Online</span>';
            const busyBadge = row.IS_BUSY
                ? '<span class="status-badge status-busy">Ocupada</span>'
                : '<span class="status-badge status-idle">Disponível</span>';

            const hostUrl = new URL(hostDetailBaseUrl, window.location.origin);
            hostUrl.searchParams.set("host_id", row.ID_HOST);
            hostUrl.searchParams.set("online_only", currentOnlineOnly ? "1" : "0");

            return `
                <tr>
                    <td>${escapeHtml(row.NA_HOST_NAME || "-")}</td>
                    <td>${connectionBadge}</td>
                    <td>${busyBadge}</td>
                    <td>${escapeHtml(row.DT_LAST_CHECK || "-")}</td>
                    <td>${escapeHtml(row.NU_PENDING_FILE_BACKUP_TASKS || 0)}</td>
                    <td>${escapeHtml(row.NU_ERROR_FILE_BACKUP_TASKS || 0)}</td>
                    <td>${escapeHtml(row.NU_PENDING_FILE_PROCESS_TASKS || 0)}</td>
                    <td>${escapeHtml(row.NU_ERROR_FILE_PROCESS_TASKS || 0)}</td>
                    <td>${escapeHtml(row.PENDING_BACKUP_GB || 0)}</td>
                    <td>
                        <a
                            class="link-action"
                            href="${escapeHtml(hostUrl.toString())}"
                            data-loading-message="Carregando panorama da estação..."
                        >
                            Abrir
                        </a>
                    </td>
                </tr>
            `;
        }).join("");
    }

    /* Diagnostics are secondary detail views, so they load only when the panel
     * is expanded for the first time.
     *
     * This binder encapsulates the full lazy-panel lifecycle:
     * - show a loading state inside the table body,
     * - fetch the grouped breakdown once,
     * - update the compact meta summary above the table,
     * - preserve a readable failure state if the request breaks.
     *
     * The main dashboard remains useful without these panels, so they should
     * never delay the first paint of the server overview.
     */
    function bindDiagnosticPanel(panelElement, metaElement, bodyElement, endpoint, options) {
        if (!panelElement || !metaElement || !bodyElement || !endpoint) {
            return;
        }

        let loading = false;
        let loaded = false;

        /* The diagnostic panels are one-shot by design.
         *
         * Once a grouped breakdown has been loaded for the current page view,
         * reopening the `<details>` panel should feel instant rather than
         * issuing a new request every time.
         */
        async function loadPanel() {
            if (loading || loaded) {
                return;
            }

            loading = true;
            metaElement.textContent = "Carregando...";
            setMessageRow(bodyElement, options.loadingMessage);

            try {
                const response = await fetch(endpoint);

                if (!response.ok) {
                    throw new Error("request_failed");
                }

                const payload = await response.json();
                renderRows(bodyElement, payload.rows || [], options.emptyMessage);
                metaElement.textContent = `${payload.error_group_count || 0} tipos / ${payload.error_total_occurrences || 0} ocorrências`;
                loaded = true;
            } catch (error) {
                setMessageRow(bodyElement, options.failureMessage);
                metaElement.textContent = "Falha ao carregar";
            } finally {
                loading = false;
            }
        }

        panelElement.addEventListener("toggle", function () {
            if (panelElement.open) {
                loadPanel();
            }
        });
    }

    /* The station table is intentionally lazy because it is a navigation aid.
     * The global server totals above must remain stable and independent.
     *
     * In other words: the summary cards describe the virtual machine as a
     * whole, while the table is an optional drill-down surface. Loading the
     * table only when needed keeps the top of the page responsive and avoids
     * paying the full cost of the row list on every `/server` visit.
     */
    function bindHostTablePanel() {
        if (!hostTablePanel || !hostTableMeta || !hostTableBody || !hostTableEndpointBase) {
            return;
        }

        let loading = false;
        let loaded = false;

        /* The table request inherits the current query string so the server can
         * reuse the same filter contract already represented in the page URL.
         * This keeps the panel aligned with the visible "Apenas Online" state
         * without inventing a second client-side filtering model.
         */
        async function loadPanel() {
            if (loading || loaded) {
                return;
            }

            loading = true;
            hostTableMeta.textContent = "Carregando...";
            hostTableBody.innerHTML = `
                <tr>
                    <td colspan="10">Carregando tabela de estações...</td>
                </tr>
            `;

            try {
                const params = new URLSearchParams(window.location.search);
                const response = await fetch(`${hostTableEndpointBase}?${params.toString()}`);

                if (!response.ok) {
                    throw new Error("request_failed");
                }

                const payload = await response.json();
                renderHostRows(payload.rows || []);
                hostTableMeta.textContent = `${payload.count || 0} estação(ões) no filtro atual`;
                loaded = true;
            } catch (error) {
                hostTableBody.innerHTML = `
                    <tr>
                        <td colspan="10">Não foi possível carregar a tabela de estações agora.</td>
                    </tr>
                `;
                hostTableMeta.textContent = "Falha ao carregar";
            } finally {
                loading = false;
            }
        }

        hostTablePanel.addEventListener("toggle", function () {
            persistServerHostTablePanelState(Boolean(hostTablePanel.open));

            if (hostTablePanel.open) {
                loadPanel();
            }
        });

        if (hostTablePanel.open) {
            loadPanel();
        }
    }

    /* Summary metrics are intentionally fetched after the first HTML paint
     * because this is one of the heaviest dashboards in WebFusion.
     *
     * The cards are rendered server-side in a "loading" state first, and this
     * renderer upgrades them once the heavy consolidated metrics arrive. That
     * preserves layout stability while still keeping the first contentful
     * paint fast enough for the rest of the page to feel alive.
     */
    function renderSummaryMetrics(payload) {
        function setSummaryValue(element, text) {
            if (!element) {
                return;
            }

            element.textContent = text;
            element.classList.remove("summary-card-value-loading");
        }

        if (summaryBindings.CURRENT_MONTH_LABEL) {
            setSummaryValue(
                summaryBindings.CURRENT_MONTH_LABEL,
                payload.CURRENT_MONTH_LABEL || "-"
            );
        }

        setSummaryValue(
            summaryBindings.BACKUP_DONE_THIS_MONTH,
            String(payload.BACKUP_DONE_THIS_MONTH ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_DONE_GB_THIS_MONTH,
            `${payload.BACKUP_DONE_GB_THIS_MONTH ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.DISCOVERED_FILES_TOTAL,
            String(payload.DISCOVERED_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_PENDING_FILES_TOTAL,
            String(payload.BACKUP_PENDING_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_ERROR_FILES_TOTAL,
            String(payload.BACKUP_ERROR_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_QUEUE_FILES_TOTAL,
            String(payload.BACKUP_QUEUE_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_QUEUE_GB_TOTAL,
            `${payload.BACKUP_QUEUE_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_PENDING_FILES_TOTAL,
            String(payload.PROCESSING_PENDING_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_QUEUE_FILES_TOTAL,
            String(payload.PROCESSING_QUEUE_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_QUEUE_GB_TOTAL,
            `${payload.PROCESSING_QUEUE_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_DONE_FILES_TOTAL,
            String(payload.PROCESSING_DONE_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.FACT_SPECTRUM_TOTAL,
            String(payload.FACT_SPECTRUM_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_ERROR_FILES_TOTAL,
            String(payload.PROCESSING_ERROR_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_PENDING_GB_TOTAL,
            `${payload.BACKUP_PENDING_GB_TOTAL ?? 0} GB`
        );
    }

    /* Summary metrics degrade to zeros instead of a hard failure banner.
     *
     * This dashboard is still usable if the heavy consolidated endpoint is
     * temporarily unavailable, so the page chooses resilience over loud error
     * chrome in the summary cards themselves.
     */
    async function loadSummaryMetrics() {
        if (!summaryMetricsEndpoint) {
            return;
        }

        try {
            const response = await fetch(summaryMetricsEndpoint);

            if (!response.ok) {
                throw new Error("request_failed");
            }

            const payload = await response.json();
            renderSummaryMetrics(payload);
        } catch (error) {
            renderSummaryMetrics({
                CURRENT_MONTH_LABEL         : null,
                BACKUP_DONE_THIS_MONTH      : 0,
                BACKUP_DONE_GB_THIS_MONTH   : 0,
                DISCOVERED_FILES_TOTAL      : 0,
                BACKUP_PENDING_FILES_TOTAL  : 0,
                BACKUP_ERROR_FILES_TOTAL    : 0,
                BACKUP_QUEUE_FILES_TOTAL    : 0,
                BACKUP_QUEUE_GB_TOTAL       : 0,
                PROCESSING_PENDING_FILES_TOTAL  : 0,
                PROCESSING_QUEUE_FILES_TOTAL    : 0,
                PROCESSING_QUEUE_GB_TOTAL       : 0,
                PROCESSING_DONE_FILES_TOTAL     : 0,
                FACT_SPECTRUM_TOTAL             : 0,
                PROCESSING_ERROR_FILES_TOTAL    : 0,
                BACKUP_PENDING_GB_TOTAL         : 0,
            });
        }
    }

    /* Bootstrap sequence
     *
     * The page starts with the heavy summary metrics because those cards live
     * in the top fold and define the dashboard at a glance. The grouped error
     * panels and the host table remain lazy and are only activated when the
     * operator opens them.
     */
    loadSummaryMetrics();

    bindDiagnosticPanel(processingPanel, processingMeta, processingBody, processingErrorsEndpoint, {
        loadingMessage: "Carregando erros agrupados...",
        emptyMessage: "Nenhum erro de processamento agrupado no histórico global.",
        failureMessage: "Não foi possível carregar os erros agrupados agora."
    });

    bindDiagnosticPanel(backupPanel, backupMeta, backupBody, backupErrorsEndpoint, {
        loadingMessage: "Carregando erros agrupados de backup...",
        emptyMessage: "Nenhum erro de backup agrupado no histórico global.",
        failureMessage: "Não foi possível carregar os erros agrupados de backup agora."
    });

    bindHostTablePanel();
})();
