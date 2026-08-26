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
        DISCOVERED_GB_TOTAL             : document.getElementById("server-summary-discovered-gb-total"),
        BACKUP_DONE_FILES_TOTAL         : document.getElementById("server-summary-backup-done-total"),
        BACKUP_DONE_GB_TOTAL            : document.getElementById("server-summary-backup-done-gb-total"),
        BACKUP_PENDING_FILES_TOTAL      : document.getElementById("server-summary-backup-pending-total"),
        BACKUP_ERROR_FILES_TOTAL        : document.getElementById("server-summary-backup-error-total"),
        BACKUP_ERROR_GB_TOTAL           : document.getElementById("server-summary-backup-error-gb-total"),
        BACKUP_SUSPENDED_FILES_TOTAL    : document.getElementById("server-summary-backup-suspended-total"),
        BACKUP_SUSPENDED_GB_TOTAL       : document.getElementById("server-summary-backup-suspended-gb-total"),
        BACKUP_QUEUE_FILES_TOTAL        : document.getElementById("server-summary-backup-queue-total"),
        BACKUP_QUEUE_GB_TOTAL           : document.getElementById("server-summary-backup-queue-gb"),
        BACKUP_QUEUE_RUNNING_FILES_TOTAL: document.getElementById("server-summary-backup-queue-running-total"),
        BACKUP_QUEUE_RUNNING_GB_TOTAL   : document.getElementById("server-summary-backup-queue-running-gb"),
        BACKUP_QUEUE_SUSPENDED_FILES_TOTAL: document.getElementById("server-summary-backup-queue-suspended-total"),
        BACKUP_QUEUE_SUSPENDED_GB_TOTAL : document.getElementById("server-summary-backup-queue-suspended-gb"),
        PROCESSING_PENDING_FILES_TOTAL  : document.getElementById("server-summary-processing-pending-total"),
        PROCESSING_PENDING_GB_TOTAL     : document.getElementById("server-summary-processing-pending-gb-total"),
        PROCESSING_QUEUE_FILES_TOTAL    : document.getElementById("server-summary-processing-queue-total"),
        PROCESSING_QUEUE_GB_TOTAL       : document.getElementById("server-summary-processing-queue-gb"),
        PROCESSING_QUEUE_RUNNING_FILES_TOTAL: document.getElementById("server-summary-processing-queue-running-total"),
        PROCESSING_QUEUE_RUNNING_GB_TOTAL: document.getElementById("server-summary-processing-queue-running-gb"),
        PROCESSING_QUEUE_FROZEN_FILES_TOTAL: document.getElementById("server-summary-processing-queue-frozen-total"),
        PROCESSING_QUEUE_FROZEN_GB_TOTAL: document.getElementById("server-summary-processing-queue-frozen-gb"),
        PROCESSING_DONE_FILES_TOTAL     : document.getElementById("server-summary-processing-done-total"),
        PROCESSING_DONE_GB_TOTAL        : document.getElementById("server-summary-processing-done-gb-total"),
        FACT_SPECTRUM_TOTAL             : document.getElementById("server-summary-fact-spectrum-total"),
        PROCESSING_ERROR_FILES_TOTAL    : document.getElementById("server-summary-processing-error-total"),
        PROCESSING_ERROR_GB_TOTAL       : document.getElementById("server-summary-processing-error-gb-total"),
        PROCESSING_FROZEN_FILES_TOTAL   : document.getElementById("server-summary-processing-frozen-total"),
        PROCESSING_FROZEN_GB_TOTAL      : document.getElementById("server-summary-processing-frozen-gb-total"),
        BACKUP_PENDING_GB_TOTAL         : document.getElementById("server-summary-backup-pending-gb"),
        PAYLOAD_DELETED_FILES_TOTAL     : document.getElementById("server-summary-payload-deleted-total"),
        PAYLOAD_DELETED_GB_TOTAL        : document.getElementById("server-summary-payload-deleted-gb-total"),
    };
    const processingPanel   = document.getElementById("server-processing-errors-panel");
    const processingMeta    = document.getElementById("server-processing-errors-meta");
    const processingBody    = document.getElementById("server-processing-errors-body");
    const backupPanel       = document.getElementById("server-backup-errors-panel");
    const backupMeta        = document.getElementById("server-backup-errors-meta");
    const backupBody        = document.getElementById("server-backup-errors-body");
    const usageMetricsPanel = document.getElementById("server-usage-metrics-panel");
    const usageMetricsMeta = document.getElementById("server-usage-metrics-meta");
    const usageMetricsAnnualBody = document.getElementById("server-usage-metrics-annual-body");
    const usageMetricsMonthlyBody = document.getElementById("server-usage-metrics-monthly-body");
    const runtimeHealthPanel = document.getElementById("server-runtime-health-panel");
    const runtimeHealthMeta = document.getElementById("server-runtime-health-meta");
    const runtimeHealthList = document.getElementById("server-runtime-health-list");
    const hostTablePanel    = document.getElementById("server-host-table-panel");
    const hostTableMeta     = document.getElementById("server-host-table-meta");
    const hostTableBody     = document.getElementById("server-host-table-body");
    const onlineOnlyCheckbox = document.getElementById("online_only");

    const hostDetailBaseUrl         = root.dataset.hostDetailBaseUrl || "";
    const hostTableEndpointBase     = root.dataset.hostTableEndpoint || "";
    const processingErrorsEndpoint  = root.dataset.processingErrorsEndpoint || "";
    const backupErrorsEndpoint      = root.dataset.backupErrorsEndpoint || "";
    const usageMetricsEndpoint      = root.dataset.usageMetricsEndpoint || "";
    const summaryMetricsEndpoint    = root.dataset.summaryMetricsEndpoint || "";
    const runtimeHealthEndpoint     = root.dataset.runtimeHealthEndpoint || "";
    let currentOnlineOnly           = root.dataset.onlineOnly === "1";
    const hasInitialSummaryMetrics  = root.dataset.summaryMetricsInitialLoaded === "1";
    let refreshHostTable = null;

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

    /* The table filter is local to this dashboard panel. Reloading the full
     * page would move the operator away from the rows being compared. */
    function handleOnlineOnlyToggle() {
        currentOnlineOnly = Boolean(onlineOnlyCheckbox && onlineOnlyCheckbox.checked);
        root.dataset.onlineOnly = currentOnlineOnly ? "1" : "0";

        if (hostTablePanel) {
            persistServerHostTablePanelState(Boolean(hostTablePanel.open));
        }

        if (refreshHostTable) {
            refreshHostTable();
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
        td.colSpan = 3;
        td.textContent = message;
        tr.appendChild(td);
        targetBody.appendChild(tr);
    }

    function formatStationCountLabel(count) {
        if (count === 1) {
            return "1 estação no filtro atual";
        }

        return `${count || 0} estações no filtro atual`;
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
            const stateTd = document.createElement("td");
            const messageTd = document.createElement("td");
            const countTd = document.createElement("td");

            stateTd.textContent = row.TASK_STATE || "ERROR";
            messageTd.className = "diagnostic-message-cell";
            messageTd.textContent = row.ERROR_MESSAGE || "(Sem mensagem)";

            countTd.className = "diagnostic-count-col";
            countTd.textContent = String(row.ERROR_COUNT || 0);

            tr.appendChild(stateTd);
            tr.appendChild(messageTd);
            tr.appendChild(countTd);
            targetBody.appendChild(tr);
        });
    }

    function setUsageMetricsMessage(bodyElement, message) {
        if (!bodyElement) {
            return;
        }

        bodyElement.innerHTML = `
            <tr>
                <td colspan="4">${escapeHtml(message)}</td>
            </tr>
        `;
    }

    function renderUsageMetricsRows(bodyElement, rows, periodField, emptyMessage) {
        if (!bodyElement) {
            return;
        }

        if (!Array.isArray(rows) || rows.length === 0) {
            setUsageMetricsMessage(bodyElement, emptyMessage);
            return;
        }

        bodyElement.innerHTML = rows.map((row) => `
            <tr>
                <td>${escapeHtml(row[periodField] || "-")}</td>
                <td>${escapeHtml(row.page_view_count || 0)}</td>
                <td>${escapeHtml(row.spectrum_query_count || 0)}</td>
                <td>${escapeHtml(row.nginx_download_count || 0)}</td>
            </tr>
        `).join("");
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
                    <td>${escapeHtml(row.NU_BACKUP_PENDING_FILES_CURRENT || 0)}</td>
                    <td>${escapeHtml(row.NU_BACKUP_ERROR_FILES_CURRENT || 0)}</td>
                    <td>${escapeHtml(row.NU_PROCESSING_PENDING_FILES_CURRENT || 0)}</td>
                    <td>${escapeHtml(row.NU_PROCESSING_ERROR_FILES_CURRENT || 0)}</td>
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

        if (panelElement.open) {
            loadPanel();
        }
    }

    function bindUsageMetricsPanel() {
        if (
            !usageMetricsPanel
            || !usageMetricsMeta
            || !usageMetricsAnnualBody
            || !usageMetricsMonthlyBody
            || !usageMetricsEndpoint
        ) {
            return;
        }

        let loading = false;
        let loaded = false;

        async function loadPanel() {
            if (loading || loaded) {
                return;
            }

            loading = true;
            usageMetricsMeta.textContent = "Carregando...";
            setUsageMetricsMessage(usageMetricsAnnualBody, "Carregando consolidado anual...");
            setUsageMetricsMessage(usageMetricsMonthlyBody, "Carregando consolidado mensal...");

            try {
                const response = await fetch(usageMetricsEndpoint);

                if (!response.ok) {
                    throw new Error("request_failed");
                }

                const payload = await response.json();
                renderUsageMetricsRows(
                    usageMetricsAnnualBody,
                    payload.annual_breakdown || [],
                    "reference_year",
                    "Nenhum consolidado anual de uso foi registrado até agora."
                );
                renderUsageMetricsRows(
                    usageMetricsMonthlyBody,
                    payload.monthly_breakdown || [],
                    "reference_month",
                    "Nenhum consolidado mensal de uso foi registrado até agora."
                );
                usageMetricsMeta.textContent = `${(payload.annual_breakdown || []).length} ano(s) / ${(payload.monthly_breakdown || []).length} mês(es)`;
                loaded = true;
            } catch (error) {
                setUsageMetricsMessage(usageMetricsAnnualBody, "Não foi possível carregar o consolidado anual agora.");
                setUsageMetricsMessage(usageMetricsMonthlyBody, "Não foi possível carregar o consolidado mensal agora.");
                usageMetricsMeta.textContent = "Falha ao carregar";
            } finally {
                loading = false;
            }
        }

        usageMetricsPanel.addEventListener("toggle", function () {
            if (usageMetricsPanel.open) {
                loadPanel();
            }
        });

        if (usageMetricsPanel.open) {
            loadPanel();
        }
    }

    /* Container health is intentionally an explicit, uncached operation.
     * Infrastructure state is useful only when it represents the current
     * moment, so the page never persists or reuses an older response. */
    function runtimeHealthStatusLabel(status) {
        const labels = {
            healthy: "Disponível",
            degraded: "Atenção necessária",
            unavailable: "Indisponível",
            unconfigured: "Não configurado"
        };

        return labels[status] || "Indisponível";
    }

    function normalizeRuntimeHealthStatus(status) {
        return ["healthy", "degraded", "unavailable", "unconfigured"].includes(status)
            ? status
            : "unavailable";
    }

    function formatRuntimeHealthTimestamp(value) {
        const date = new Date(value || "");

        if (Number.isNaN(date.getTime())) {
            return "agora";
        }

        return new Intl.DateTimeFormat("pt-BR", {
            dateStyle: "short",
            timeStyle: "medium"
        }).format(date);
    }

    function renderRuntimeHealth(payload) {
        if (!runtimeHealthList) {
            return;
        }

        const components = Array.isArray(payload.components) ? payload.components : [];

        if (components.length === 0) {
            runtimeHealthList.innerHTML = `
                <div class="server-runtime-health-empty">
                    Não foi possível obter o estado atual dos containers.
                </div>
            `;
            return;
        }

        runtimeHealthList.innerHTML = components.map((component) => {
            const status = normalizeRuntimeHealthStatus(component.status);
            const checks = Array.isArray(component.checks) ? component.checks : [];
            const checkRows = checks.length > 0
                ? checks.map((check) => {
                    const checkStatus = normalizeRuntimeHealthStatus(check.status);
                    const scriptName = typeof check.script === "string" ? check.script.trim() : "";
                    const scriptRow = scriptName
                        ? `
                            <small class="server-runtime-health-check-script">
                                Script Python: <code>${escapeHtml(scriptName)}</code>
                            </small>
                        `
                        : "";

                    return `
                        <li class="server-runtime-health-check">
                            <span>${escapeHtml(check.name || "Verificação")}</span>
                            <strong class="server-runtime-health-check-status server-runtime-health-check-status--${checkStatus}">
                                ${escapeHtml(runtimeHealthStatusLabel(checkStatus))}
                            </strong>
                            <small>${escapeHtml(check.detail || "Sem detalhe adicional")}</small>
                            ${scriptRow}
                        </li>
                    `;
                }).join("")
                : `
                    <li class="server-runtime-health-check server-runtime-health-check--message">
                        ${escapeHtml(component.message || "Sem resposta atual do container.")}
                    </li>
                `;

            return `
                <section class="server-runtime-health-item server-runtime-health-item--${status}">
                    <div class="server-runtime-health-item-head">
                        <h3>${escapeHtml(component.label || component.component || "Container")}</h3>
                        <span class="server-runtime-health-status server-runtime-health-status--${status}">
                            ${escapeHtml(runtimeHealthStatusLabel(status))}
                        </span>
                    </div>
                    <ul class="server-runtime-health-checks">${checkRows}</ul>
                </section>
            `;
        }).join("");
    }

    function bindRuntimeHealthPanel() {
        if (
            !runtimeHealthPanel
            || !runtimeHealthMeta
            || !runtimeHealthList
            || !runtimeHealthEndpoint
        ) {
            return;
        }

        let loading = false;

        async function loadRuntimeHealth() {
            if (loading) {
                return;
            }

            loading = true;
            runtimeHealthMeta.textContent = "Consultando o estado atual dos containers...";

            try {
                const response = await fetch(runtimeHealthEndpoint, { cache: "no-store" });
                const payload = await response.json();

                if (!response.ok) {
                    throw new Error(payload.error || "request_failed");
                }

                renderRuntimeHealth(payload);
                runtimeHealthMeta.textContent = `Verificação concluída em ${formatRuntimeHealthTimestamp(payload.checked_at)}.`;
            } catch (error) {
                runtimeHealthList.innerHTML = `
                    <div class="server-runtime-health-empty">
                        Não foi possível consultar os containers neste momento.
                    </div>
                `;
                runtimeHealthMeta.textContent = "Falha na verificação atual. Nenhum estado anterior foi reutilizado.";
            } finally {
                loading = false;
            }
        }

        runtimeHealthPanel.addEventListener("toggle", function () {
            if (runtimeHealthPanel.open) {
                loadRuntimeHealth();
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

        let loaded = false;
        let activeRequest = null;

        /* The table request sends only the visible table filter. This keeps
         * server-wide dashboard values independent from this local navigation.
         */
        async function loadPanel(force = false) {
            if (loaded && !force) {
                return;
            }

            if (activeRequest) {
                activeRequest.abort();
            }

            const requestedOnlineOnly = currentOnlineOnly;
            const requestController = new AbortController();
            activeRequest = requestController;
            hostTableMeta.textContent = "Carregando...";
            hostTableBody.innerHTML = `
                <tr>
                    <td colspan="10">Carregando tabela de estações...</td>
                </tr>
            `;

            try {
                const params = new URLSearchParams();
                if (requestedOnlineOnly) {
                    params.set("online_only", "1");
                }
                const queryString = params.toString();
                const endpoint = queryString
                    ? `${hostTableEndpointBase}?${queryString}`
                    : hostTableEndpointBase;
                const response = await fetch(endpoint, { signal: requestController.signal });

                if (!response.ok) {
                    throw new Error("request_failed");
                }

                const payload = await response.json();
                if (activeRequest !== requestController) {
                    return;
                }
                renderHostRows(payload.rows || []);
                hostTableMeta.textContent = formatStationCountLabel(payload.count);
                loaded = true;
            } catch (error) {
                if (error.name === "AbortError") {
                    return;
                }
                hostTableBody.innerHTML = `
                    <tr>
                        <td colspan="10">Não foi possível carregar a tabela de estações agora.</td>
                    </tr>
                `;
                hostTableMeta.textContent = "Falha ao carregar";
            } finally {
                if (activeRequest === requestController) {
                    activeRequest = null;
                }
            }
        }

        refreshHostTable = function () {
            // A closed table must also forget stale rows before its next opening.
            loaded = false;
            if (hostTablePanel.open) {
                loadPanel(true);
            }
        };

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
            summaryBindings.DISCOVERED_GB_TOTAL,
            `${payload.DISCOVERED_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.BACKUP_DONE_FILES_TOTAL,
            String(payload.BACKUP_DONE_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_DONE_GB_TOTAL,
            `${payload.BACKUP_DONE_GB_TOTAL ?? 0} GB`
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
            summaryBindings.BACKUP_ERROR_GB_TOTAL,
            `${payload.BACKUP_ERROR_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.BACKUP_SUSPENDED_FILES_TOTAL,
            String(payload.BACKUP_SUSPENDED_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_SUSPENDED_GB_TOTAL,
            `${payload.BACKUP_SUSPENDED_GB_TOTAL ?? 0} GB`
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
            summaryBindings.BACKUP_QUEUE_RUNNING_FILES_TOTAL,
            String(payload.BACKUP_QUEUE_RUNNING_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_QUEUE_RUNNING_GB_TOTAL,
            `${payload.BACKUP_QUEUE_RUNNING_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.BACKUP_QUEUE_SUSPENDED_FILES_TOTAL,
            String(payload.BACKUP_QUEUE_SUSPENDED_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.BACKUP_QUEUE_SUSPENDED_GB_TOTAL,
            `${payload.BACKUP_QUEUE_SUSPENDED_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_PENDING_FILES_TOTAL,
            String(payload.PROCESSING_PENDING_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_PENDING_GB_TOTAL,
            `${payload.PROCESSING_PENDING_GB_TOTAL ?? 0} GB`
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
            summaryBindings.PROCESSING_QUEUE_RUNNING_FILES_TOTAL,
            String(payload.PROCESSING_QUEUE_RUNNING_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_QUEUE_RUNNING_GB_TOTAL,
            `${payload.PROCESSING_QUEUE_RUNNING_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_QUEUE_FROZEN_FILES_TOTAL,
            String(payload.PROCESSING_QUEUE_FROZEN_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_QUEUE_FROZEN_GB_TOTAL,
            `${payload.PROCESSING_QUEUE_FROZEN_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_DONE_FILES_TOTAL,
            String(payload.PROCESSING_DONE_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_DONE_GB_TOTAL,
            `${payload.PROCESSING_DONE_GB_TOTAL ?? 0} GB`
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
            summaryBindings.PROCESSING_ERROR_GB_TOTAL,
            `${payload.PROCESSING_ERROR_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PROCESSING_FROZEN_FILES_TOTAL,
            String(payload.PROCESSING_FROZEN_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PROCESSING_FROZEN_GB_TOTAL,
            `${payload.PROCESSING_FROZEN_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.BACKUP_PENDING_GB_TOTAL,
            `${payload.BACKUP_PENDING_GB_TOTAL ?? 0} GB`
        );
        setSummaryValue(
            summaryBindings.PAYLOAD_DELETED_FILES_TOTAL,
            String(payload.PAYLOAD_DELETED_FILES_TOTAL ?? 0)
        );
        setSummaryValue(
            summaryBindings.PAYLOAD_DELETED_GB_TOTAL,
            `${payload.PAYLOAD_DELETED_GB_TOTAL ?? 0} GB`
        );
    }

    function renderSummaryMetricsUnavailable() {
        Object.entries(summaryBindings).forEach(([key, element]) => {
            if (!element) {
                return;
            }

            if (key === "CURRENT_MONTH_LABEL") {
                element.textContent = "INDISPONIVEL";
            } else if (key.endsWith("_GB_TOTAL") || key.endsWith("_GB_THIS_MONTH")) {
                element.textContent = "- GB";
            } else {
                element.textContent = "-";
            }

            element.classList.remove("summary-card-value-loading");
        });
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
            if (!hasInitialSummaryMetrics) {
                renderSummaryMetricsUnavailable();
            }
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

    bindUsageMetricsPanel();
    bindRuntimeHealthPanel();
    bindHostTablePanel();
})();
