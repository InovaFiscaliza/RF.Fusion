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
    const webfusionUrl = typeof window.webfusionUrl === "function"
        ? window.webfusionUrl
        : (pathname) => pathname;

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
                const response = await fetch(
                    webfusionUrl(`/api/host/${hostId}/locations`)
                );
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
                        <td colspan="3">${options.emptyMessage}</td>
                    </tr>
                `;
                return;
            }

            rowsContainer.innerHTML = rows.map((row) => `
                <tr>
                    <td>${escapeHtml(row.TASK_STATE || "ERROR")}</td>
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
                        <td colspan="3">${options.loadingMessage}</td>
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
                        <td colspan="3">${options.failureMessage}</td>
                    </tr>
                `;
                setMeta("Falha ao carregar");
            } finally {
                loading = false;
            }
        });
    }

    /* Live activity intentionally has a separate lightweight endpoint from the
     * summary cards. The summary database is excellent for completed-history
     * counters, but this panel must show the task and worker message that are
     * changing right now in BPDATA.
     */
    function bindCurrentActivity() {
        const panel = document.querySelector("[data-host-current-activity]");
        const title = panel?.querySelector("[data-host-activity-title]");
        const message = panel?.querySelector("[data-host-activity-message]");
        const timestamp = panel?.querySelector("[data-host-activity-timestamp]");
        const status = panel?.querySelector("[data-host-activity-status]");
        const openButton = panel?.querySelector("[data-host-activity-open]");
        const operationalBusy = document.querySelector("[data-host-operational-busy]");

        if (!hostId || !panel || !title || !message || !timestamp || !status || !openButton) {
            return;
        }

        let currentActivity = null;
        let refreshTimer = null;
        let detailPollTimer = null;
        let trackedActivity = null;
        const CURRENT_ACTIVITY_REFRESH_MILLISECONDS = 3000;
        const DETAIL_ACTIVITY_REFRESH_MILLISECONDS = 1500;

        const dialog = document.createElement("dialog");
        dialog.className = "host-activity-dialog";
        dialog.setAttribute("aria-labelledby", "host-activity-dialog-title");
        dialog.innerHTML = `
            <div class="host-activity-dialog-card">
                <div class="host-activity-dialog-head">
                    <div>
                        <p class="host-activity-dialog-eyebrow">Fluxo da estação</p>
                        <h2 id="host-activity-dialog-title" class="host-activity-dialog-title">Acompanhando atividade</h2>
                    </div>
                    <button type="button" class="host-activity-dialog-close" aria-label="Fechar acompanhamento">
                        <i class="fa fa-times"></i>
                    </button>
                </div>
                <ol class="host-activity-progress" aria-label="Etapas da tarefa">
                    <li class="host-activity-progress-step" data-host-activity-step="waiting"><strong>Aguardando</strong><span>Na fila da estação.</span></li>
                    <li class="host-activity-progress-step" data-host-activity-step="running"><strong>Executando</strong><span>Worker em atividade.</span></li>
                    <li class="host-activity-progress-step" data-host-activity-step="finished"><strong>Concluído</strong><span>Resultado publicado.</span></li>
                </ol>
                <div class="host-activity-dialog-detail">
                    <span class="host-activity-dialog-detail-label">Estado</span>
                    <span class="host-activity-dialog-detail-value" data-host-activity-dialog-status>Aguardando</span>
                </div>
                <div class="host-activity-dialog-detail" data-host-activity-file-row hidden>
                    <span class="host-activity-dialog-detail-label">Arquivo em acompanhamento</span>
                    <span class="host-activity-dialog-detail-value" data-host-activity-dialog-file></span>
                </div>
                <div class="host-activity-dialog-detail" data-host-activity-transfer-row hidden>
                    <span class="host-activity-dialog-detail-label" data-host-activity-dialog-progress-label>Andamento da transferência</span>
                    <div class="host-activity-transfer-progress">
                        <progress max="100" value="0" data-host-activity-dialog-transfer-bar></progress>
                        <span class="host-activity-dialog-detail-value" data-host-activity-dialog-transfer></span>
                    </div>
                </div>
                <div class="host-activity-dialog-detail">
                    <span class="host-activity-dialog-detail-label">Atualização do worker</span>
                    <span class="host-activity-dialog-detail-value" data-host-activity-dialog-timestamp>Sem timestamp publicado.</span>
                </div>
                <div class="host-activity-dialog-detail">
                    <span class="host-activity-dialog-detail-label">Resultado do worker (NA_MESSAGE)</span>
                    <span class="host-activity-dialog-detail-value" data-host-activity-dialog-message>Aguardando atualização do worker.</span>
                </div>
                <p class="host-activity-dialog-footnote">Esta janela é somente de acompanhamento. Fechá-la não altera nem cancela a tarefa.</p>
            </div>
        `;
        document.body.appendChild(dialog);

        const dialogTitle = dialog.querySelector(".host-activity-dialog-title");
        const dialogStatus = dialog.querySelector("[data-host-activity-dialog-status]");
        const dialogFileRow = dialog.querySelector("[data-host-activity-file-row]");
        const dialogFile = dialog.querySelector("[data-host-activity-dialog-file]");
        const dialogTransferRow = dialog.querySelector("[data-host-activity-transfer-row]");
        const dialogProgressLabel = dialog.querySelector("[data-host-activity-dialog-progress-label]");
        const dialogTransferBar = dialog.querySelector("[data-host-activity-dialog-transfer-bar]");
        const dialogTransfer = dialog.querySelector("[data-host-activity-dialog-transfer]");
        const dialogTimestamp = dialog.querySelector("[data-host-activity-dialog-timestamp]");
        const dialogMessage = dialog.querySelector("[data-host-activity-dialog-message]");
        const closeButton = dialog.querySelector(".host-activity-dialog-close");
        let promptedSpectrumFileName = null;

        const spectrumPromptDialog = document.createElement("dialog");
        spectrumPromptDialog.className = "host-activity-dialog host-spectrum-prompt-dialog";
        spectrumPromptDialog.setAttribute("aria-labelledby", "host-spectrum-prompt-title");
        spectrumPromptDialog.innerHTML = `
            <div class="host-activity-dialog-card">
                <div class="host-activity-dialog-head">
                    <div>
                        <p class="host-activity-dialog-eyebrow">Processamento concluído</p>
                        <h2 id="host-spectrum-prompt-title" class="host-activity-dialog-title">Verificar metadados espectrais?</h2>
                    </div>
                    <button type="button" class="host-activity-dialog-close" aria-label="Fechar confirmação">
                        <i class="fa fa-times"></i>
                    </button>
                </div>
                <p class="host-activity-dialog-footnote">O arquivo processado possui dados no catálogo espectral.</p>
                <div class="host-spectrum-dialog-actions">
                    <button type="button" class="btn btn-secondary btn-secondary-action" data-host-spectrum-prompt-cancel>Agora não</button>
                    <button type="button" class="btn btn-primary btn-primary-action" data-host-spectrum-prompt-open>Ver metadados</button>
                </div>
            </div>
        `;
        document.body.appendChild(spectrumPromptDialog);

        const spectrumMetadataDialog = document.createElement("dialog");
        spectrumMetadataDialog.className = "host-activity-dialog host-spectrum-metadata-dialog";
        spectrumMetadataDialog.setAttribute("aria-labelledby", "host-spectrum-metadata-title");
        spectrumMetadataDialog.innerHTML = `
            <div class="host-activity-dialog-card">
                <div class="host-activity-dialog-head">
                    <div>
                        <p class="host-activity-dialog-eyebrow">RFDATA</p>
                        <h2 id="host-spectrum-metadata-title" class="host-activity-dialog-title">Metadados espectrais</h2>
                    </div>
                    <button type="button" class="host-activity-dialog-close" aria-label="Fechar metadados espectrais">
                        <i class="fa fa-times"></i>
                    </button>
                </div>
                <dl class="host-spectrum-metadata-list" data-host-spectrum-metadata-list></dl>
                <div class="host-spectrum-rows" data-host-spectrum-rows></div>
            </div>
        `;
        document.body.appendChild(spectrumMetadataDialog);

        const spectrumPromptClose = spectrumPromptDialog.querySelector(".host-activity-dialog-close");
        const spectrumPromptCancel = spectrumPromptDialog.querySelector("[data-host-spectrum-prompt-cancel]");
        const spectrumPromptOpen = spectrumPromptDialog.querySelector("[data-host-spectrum-prompt-open]");
        const spectrumMetadataClose = spectrumMetadataDialog.querySelector(".host-activity-dialog-close");
        const spectrumMetadataList = spectrumMetadataDialog.querySelector("[data-host-spectrum-metadata-list]");
        const spectrumRows = spectrumMetadataDialog.querySelector("[data-host-spectrum-rows]");

        function clearDetailPolling() {
            if (detailPollTimer !== null) {
                window.clearTimeout(detailPollTimer);
                detailPollTimer = null;
            }
        }

        function clearCurrentActivityRefresh() {
            if (refreshTimer !== null) {
                window.clearTimeout(refreshTimer);
                refreshTimer = null;
            }
        }

        function setStatusBadge(element, activity) {
            element.classList.remove("status-online", "status-offline", "status-busy", "status-idle", "status-warning");

            if (!activity) {
                element.classList.add("status-idle");
                element.textContent = "Disponível";
                return;
            }

            if (activity.status === 2) {
                element.classList.add("status-busy");
            } else if (activity.status === 1) {
                element.classList.add("status-idle");
            } else if (activity.status === 0) {
                element.classList.add("status-online");
            } else {
                element.classList.add("status-offline");
            }
            element.textContent = activity.status_label || "Em andamento";
        }

        function setOperationalBusy(activity) {
            if (!operationalBusy) {
                return;
            }

            const isBusy = activity
                ? true
                : operationalBusy.dataset.hostOperationalBusyInitial === "1";
            operationalBusy.classList.toggle("status-busy", isBusy);
            operationalBusy.classList.toggle("status-idle", !isBusy);
            operationalBusy.textContent = isBusy ? "Ocupada" : "Disponível";
        }

        function activityMessage(activity) {
            const details = [];
            if (activity.file_name) {
                details.push(`Arquivo: ${activity.file_name}`);
            }
            if (activity.message) {
                details.push(activity.message);
            }
            return details.join(" · ") || "Aguardando atualização do worker.";
        }

        function renderCurrentActivity(activity) {
            currentActivity = activity || null;
            setStatusBadge(status, currentActivity);
            setOperationalBusy(currentActivity);

            if (!currentActivity) {
                title.textContent = "Nenhuma tarefa em andamento";
                message.textContent = "Sem discovery, host check ou backup ativo para esta estação.";
                timestamp.textContent = "";
                openButton.disabled = true;
                return;
            }

            title.textContent = currentActivity.title || "Tarefa operacional da estação";
            message.textContent = activityMessage(currentActivity);
            timestamp.textContent = currentActivity.updated_at
                ? `Atualização do worker: ${currentActivity.updated_at}`
                : "Aguardando o primeiro timestamp do worker.";
            openButton.disabled = false;
        }

        function renderProgress(activity) {
            const steps = {
                waiting: "pending",
                running: "pending",
                finished: "pending",
            };

            if (activity.status === 1) {
                steps.waiting = "active";
            } else if (activity.status === 2) {
                steps.waiting = "complete";
                steps.running = "active";
            } else if (activity.status === 0) {
                steps.waiting = "complete";
                steps.running = "complete";
                steps.finished = "complete";
            } else {
                steps.waiting = "complete";
                steps.running = "failed";
                steps.finished = "failed";
            }

            dialog.querySelectorAll("[data-host-activity-step]").forEach((element) => {
                const state = steps[element.dataset.hostActivityStep];
                element.classList.toggle("is-active", state === "active");
                element.classList.toggle("is-complete", state === "complete");
                element.classList.toggle("is-failed", state === "failed");
            });
        }

        function formatTransferBytes(value) {
            const bytes = Math.max(0, Number(value) || 0);
            const units = ["B", "KB", "MB", "GB", "TB"];
            let unitIndex = 0;
            let displayValue = bytes;

            while (displayValue >= 1024 && unitIndex < units.length - 1) {
                displayValue /= 1024;
                unitIndex += 1;
            }

            if (unitIndex === 0) {
                return `${Math.round(displayValue)} ${units[unitIndex]}`;
            }
            return `${displayValue.toFixed(1)} ${units[unitIndex]}`;
        }

        function renderTransferProgress(activity) {
            const transfer = activity.transfer_progress;
            if (activity.is_processing && !activity.is_terminal) {
                dialogTransferRow.hidden = false;
                dialogProgressLabel.textContent = "Andamento do processamento";
                dialogTransferBar.classList.add("is-indeterminate");
                dialogTransferBar.removeAttribute("value");
                dialogTransfer.textContent = "Processando arquivo...";
                return;
            }

            const totalBytes = Number(transfer?.total_bytes || 0);
            if (!transfer || totalBytes <= 0) {
                dialogTransferRow.hidden = true;
                dialogTransferBar.value = 0;
                dialogTransferBar.classList.remove("is-indeterminate");
                dialogProgressLabel.textContent = "Andamento da transferência";
                dialogTransfer.textContent = "";
                return;
            }

            const transferredBytes = Math.min(
                Math.max(0, Number(transfer.transferred_bytes || 0)),
                totalBytes,
            );
            const percentage = Math.min(
                100,
                Math.max(0, Number(transfer.percentage || 0)),
            );
            dialogTransferRow.hidden = false;
            dialogProgressLabel.textContent = "Andamento da transferência";
            dialogTransferBar.classList.toggle(
                "is-indeterminate",
                !transfer.is_determinate,
            );

            if (!transfer.is_determinate) {
                dialogTransferBar.removeAttribute("value");
                dialogTransfer.textContent = "Preparando download...";
                return;
            }

            dialogTransferBar.value = percentage;
            dialogTransfer.textContent = (
                `Transferido ${formatTransferBytes(transferredBytes)} de `
                + `${formatTransferBytes(totalBytes)} (${percentage.toFixed(1)}%)`
            );
        }

        function appendSpectrumMetadata(label, value) {
            const term = document.createElement("dt");
            term.textContent = label;
            const detail = document.createElement("dd");
            detail.textContent = value || "Não informado";
            spectrumMetadataList.append(term, detail);
        }

        function formatSpectrumFrequency(value) {
            const frequency = Number(value);
            return Number.isFinite(frequency) ? frequency.toFixed(2) : null;
        }

        function formatSpectrumRange(spectrum) {
            const start = formatSpectrumFrequency(spectrum.frequency_start);
            const end = formatSpectrumFrequency(spectrum.frequency_end);
            return start && end ? `${start} - ${end} MHz` : "Não informada";
        }

        function appendSpectrumDetail(label, value) {
            const item = document.createElement("div");
            item.className = "host-spectrum-row-detail";
            const term = document.createElement("span");
            term.textContent = label;
            const detail = document.createElement("span");
            detail.textContent = value || "Não informado";
            item.append(term, detail);
            return item;
        }

        function renderSpectrumRows(metadata) {
            spectrumRows.replaceChildren();
            const spectra = metadata?.spectra || [];
            if (spectra.length === 0) {
                return;
            }

            const heading = document.createElement("h3");
            heading.className = "host-spectrum-rows-title";
            heading.textContent = "Espectros associados";
            spectrumRows.appendChild(heading);

            if (metadata.spectra_truncated) {
                const notice = document.createElement("p");
                notice.className = "host-spectrum-rows-note";
                notice.textContent = "Exibindo os 200 espectros mais recentes deste arquivo.";
                spectrumRows.appendChild(notice);
            }

            const list = document.createElement("div");
            list.className = "host-spectrum-rows-list";
            spectra.forEach((spectrum) => {
                const row = document.createElement("article");
                row.className = "host-spectrum-row";
                const title = document.createElement("h4");
                title.textContent = `Espectro ${spectrum.spectrum_id}`;
                row.append(
                    title,
                    appendSpectrumDetail("Faixa", formatSpectrumRange(spectrum)),
                    appendSpectrumDetail("Descrição", spectrum.description),
                    appendSpectrumDetail("Localidade", spectrum.locality),
                    appendSpectrumDetail("Equipamento", spectrum.equipment),
                    appendSpectrumDetail("Início", spectrum.time_start),
                    appendSpectrumDetail("Fim", spectrum.time_end),
                );
                list.appendChild(row);
            });
            spectrumRows.appendChild(list);
        }

        function renderSpectrumMetadata(metadata) {
            spectrumMetadataList.replaceChildren();
            spectrumRows.replaceChildren();
            if (metadata?.error) {
                appendSpectrumMetadata("Resultado", metadata.error);
                return;
            }
            if (!metadata) {
                appendSpectrumMetadata("Resultado", "Nenhum espectro foi encontrado para este arquivo.");
                return;
            }

            appendSpectrumMetadata("Arquivo", metadata.file_name);
            appendSpectrumMetadata("Espectros", String(metadata.spectrum_count));
            appendSpectrumMetadata("Início", metadata.time_start);
            appendSpectrumMetadata("Fim", metadata.time_end);
            appendSpectrumMetadata("Frequência inicial", formatSpectrumFrequency(metadata.frequency_start));
            appendSpectrumMetadata("Frequência final", formatSpectrumFrequency(metadata.frequency_end));
            appendSpectrumMetadata("Localidades", String(metadata.site_count));
            appendSpectrumMetadata("Equipamentos", String(metadata.equipment_count));
            renderSpectrumRows(metadata);
        }

        function showSpectrumPrompt(activity) {
            if (
                activity.status !== 0
                || !activity.is_processing
                || !activity.server_file_name
                || promptedSpectrumFileName === activity.server_file_name
            ) {
                return;
            }

            promptedSpectrumFileName = activity.server_file_name;
            spectrumPromptDialog.showModal();
        }

        function renderDialogActivity(activity) {
            dialogTitle.textContent = activity.title || "Atividade da estação";
            dialogStatus.textContent = activity.status_label || "Em andamento";
            dialogTimestamp.textContent = activity.updated_at || "Sem timestamp publicado.";
            dialogMessage.textContent = activity.message || "Aguardando atualização do worker.";
            dialogFileRow.hidden = !activity.file_name;
            dialogFile.textContent = activity.file_name || "";
            renderTransferProgress(activity);
            renderProgress(activity);
        }

        async function readJson(response) {
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                const error = new Error(
                    payload.error || "Não foi possível consultar a atividade da estação.",
                );
                error.status = response.status;
                throw error;
            }
            return payload;
        }

        async function fetchCurrentActivity() {
            const response = await fetch(
                webfusionUrl(`/api/host/${hostId}/activity`),
                { credentials: "same-origin", headers: { Accept: "application/json" } },
            );
            const payload = await readJson(response);
            return payload.activity || null;
        }

        async function loadCurrentActivity() {
            try {
                renderCurrentActivity(await fetchCurrentActivity());
            } catch (error) {
                title.textContent = "Atividade indisponível";
                message.textContent = error.message || "Não foi possível consultar a atividade agora.";
                timestamp.textContent = "";
                openButton.disabled = true;
                status.classList.remove("status-online", "status-offline", "status-busy", "status-idle");
                status.classList.add("status-warning");
                status.textContent = "Indisponível";
            } finally {
                clearCurrentActivityRefresh();
                refreshTimer = window.setTimeout(
                    loadCurrentActivity,
                    CURRENT_ACTIVITY_REFRESH_MILLISECONDS,
                );
            }
        }

        async function pollTrackedActivity() {
            if (!trackedActivity || !dialog.open) {
                return;
            }

            try {
                const detailUrl = new URL(
                    webfusionUrl(
                        `/api/host/${hostId}/activity/${encodeURIComponent(trackedActivity.source)}/${trackedActivity.task_id}`,
                    ),
                    window.location.origin,
                );
                if (trackedActivity.lastActivity?.file_path) {
                    detailUrl.searchParams.set("file_path", trackedActivity.lastActivity.file_path);
                }
                if (trackedActivity.lastActivity?.file_name) {
                    detailUrl.searchParams.set("file_name", trackedActivity.lastActivity.file_name);
                }
                const response = await fetch(
                    detailUrl,
                    { credentials: "same-origin", headers: { Accept: "application/json" } },
                );
                const activity = await readJson(response);
                if (!trackedActivity || !dialog.open) {
                    return;
                }
                renderDialogActivity(activity);
                trackedActivity.lastActivity = activity;

                if (activity.is_terminal) {
                    // A transfer may promote this row and release the host
                    // before the next queued file starts. Keep the dialog open
                    // and follow whichever station activity now represents work.
                    const nextActivity = await fetchCurrentActivity();
                    renderCurrentActivity(nextActivity);
                    if (nextActivity) {
                        trackedActivity = {
                            source: nextActivity.source,
                            task_id: nextActivity.task_id,
                            lastActivity: nextActivity,
                        };
                        renderDialogActivity(nextActivity);
                    } else {
                        trackedActivity = null;
                        showSpectrumPrompt(activity);
                    }
                }

                if (!activity.is_terminal) {
                    renderCurrentActivity(activity);
                }
            } catch (error) {
                if (error.status === 404 && trackedActivity?.lastActivity) {
                    // A manually pruned task has no queue row left. Its last
                    // published message is still more useful than a 404 text.
                    dialogStatus.textContent = "Resultado anterior preservado";
                    dialogMessage.textContent = trackedActivity.lastActivity.message
                        || "A tarefa saiu da fila antes de publicar nova atualização.";
                }
                else {
                    dialogStatus.textContent = "Acompanhamento indisponível";
                    dialogMessage.textContent = error.message || "Não foi possível atualizar esta tarefa.";
                }
            } finally {
                if (trackedActivity && dialog.open) {
                    detailPollTimer = window.setTimeout(
                        pollTrackedActivity,
                        DETAIL_ACTIVITY_REFRESH_MILLISECONDS,
                    );
                }
            }
        }

        function closeDialog() {
            clearDetailPolling();
            trackedActivity = null;
            if (dialog.open) {
                dialog.close();
            }
        }

        openButton.addEventListener("click", () => {
            if (!currentActivity) {
                return;
            }

            clearDetailPolling();
            trackedActivity = {
                source: currentActivity.source,
                task_id: currentActivity.task_id,
                lastActivity: currentActivity,
            };
            renderDialogActivity(currentActivity);
            dialog.showModal();
            pollTrackedActivity();
        });

        closeButton.addEventListener("click", closeDialog);
        dialog.addEventListener("cancel", (event) => {
            event.preventDefault();
            closeDialog();
        });
        dialog.addEventListener("close", clearDetailPolling);
        spectrumPromptClose.addEventListener("click", () => spectrumPromptDialog.close());
        spectrumPromptCancel.addEventListener("click", () => spectrumPromptDialog.close());
        spectrumPromptDialog.addEventListener("cancel", (event) => {
            event.preventDefault();
            spectrumPromptDialog.close();
        });
        spectrumMetadataClose.addEventListener("click", () => spectrumMetadataDialog.close());
        spectrumMetadataDialog.addEventListener("cancel", (event) => {
            event.preventDefault();
            spectrumMetadataDialog.close();
        });
        spectrumPromptOpen.addEventListener("click", async () => {
            spectrumPromptOpen.disabled = true;
            try {
                const metadataUrl = new URL(
                    webfusionUrl(`/api/host/${hostId}/processed-spectrum-metadata`),
                    window.location.origin,
                );
                metadataUrl.searchParams.set("file_name", promptedSpectrumFileName);
                const response = await fetch(metadataUrl, {
                    credentials: "same-origin",
                    headers: { Accept: "application/json" },
                });
                const payload = await readJson(response);
                renderSpectrumMetadata(payload.metadata || null);
                spectrumPromptDialog.close();
                spectrumMetadataDialog.showModal();
            } catch (error) {
                renderSpectrumMetadata({
                    error: error.message || "Não foi possível consultar os metadados espectrais.",
                });
                spectrumPromptDialog.close();
                spectrumMetadataDialog.showModal();
            } finally {
                spectrumPromptOpen.disabled = false;
            }
        });

        loadCurrentActivity();
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
    bindCurrentActivity();

    bindDiagnosticPanel(document.querySelector("[data-host-error-panel]"), {
        metaSelector: "[data-host-error-meta]",
        rowsSelector: "[data-host-error-rows]",
        url: (currentHostId) => webfusionUrl(
            `/api/host/${currentHostId}/processing-errors`
        ),
        emptyMessage: "Nenhum erro de processamento agrupado para esta estação.",
        loadingMessage: "Carregando erros agrupados desta estação...",
        failureMessage: "Nao foi possivel carregar os erros agrupados agora."
    });

    bindDiagnosticPanel(document.querySelector("[data-host-backup-error-panel]"), {
        metaSelector: "[data-host-backup-error-meta]",
        rowsSelector: "[data-host-backup-error-rows]",
        url: (currentHostId) => webfusionUrl(
            `/api/host/${currentHostId}/backup-errors`
        ),
        emptyMessage: "Nenhum erro de backup agrupado para esta estação.",
        loadingMessage: "Carregando erros agrupados de backup desta estação...",
        failureMessage: "Nao foi possivel carregar os erros agrupados de backup agora."
    });
})();
