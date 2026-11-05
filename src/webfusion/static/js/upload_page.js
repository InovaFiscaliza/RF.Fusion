/* Upload classification and sequential transfers. Comboboxes belong to the shell. */
(() => {
    "use strict";
    const root = document.getElementById("upload-page");
    if (!root) {
        return;
    }

    const input = document.getElementById("upload-input");
    const dropzone = document.getElementById("upload-dropzone");
    const list = document.getElementById("upload-list");
    const summary = document.getElementById("upload-summary");
    const feedback = document.getElementById("upload-feedback");
    const clear = document.getElementById("upload-clear");
    const category = document.getElementById("upload-category");
    const station = document.getElementById("upload-station");
    const driveType = document.getElementById("upload-drive-type");
    const stationField = document.getElementById("upload-station-field");
    const driveField = document.getElementById("upload-drive-field");
    const destinationLabel = document.getElementById("upload-destination");
    const emptyMessage = document.getElementById("upload-empty");
    const rowTemplate = document.getElementById("upload-row-template");
    const selectFileButtons = root.querySelectorAll("[data-select-files]");
    const limit = Number(root.dataset.maxFileBytes);
    const entries = [];
    let active = false;
    let dragDepth = 0;

    /** Read the current destination. Returns category, station, drive_type and folder strings. */
    function getClassification() {
        const value = { category: category.value, station: "", drive_type: "", folder: "" };
        if (value.category === "fixas" && station.value) {
            value.station = station.value;
            value.folder = `fixas/${station.value}`;
        } else if (value.category === "drive-test" && driveType.value) {
            value.drive_type = driveType.value;
            value.folder = `drive-test/${driveType.value}`;
        } else if (value.category === "rni") {
            value.folder = "rni";
        }
        return value;
    }

    /** Reflect classification in dependent fields and upload availability. Returns void. */
    function updateClassification() {
        const fixed = category.value === "fixas";
        const drive = category.value === "drive-test";
        stationField.hidden = !fixed;
        driveField.hidden = !drive;
        station.disabled = !fixed;
        station.required = fixed;
        driveType.disabled = !drive;
        driveType.required = drive;
        const selected = getClassification();
        destinationLabel.textContent = selected.folder
            ? `${root.dataset.uploadFolder}/${selected.folder}`
            : "Selecione a classificação acima.";
        selectFileButtons.forEach(button => {
            button.disabled = !selected.folder;
        });
    }

    /** @param {number} bytes Byte count. @returns {string} Localized binary size. */
    function formatSize(bytes) {
        const units = ["B", "KiB", "MiB", "GiB"];
        const index = bytes > 0 ? Math.max(0, Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)) : 0;
        return `${(bytes / (1024 ** index)).toLocaleString("pt-BR", { maximumFractionDigits: 1 })} ${units[index]}`;
    }

    /** Update queue counts, empty state and cleanup availability. Returns void. */
    function renderQueueSummary() {
        const count = state => entries.filter(entry => entry.state === state).length;
        summary.textContent = entries.length
            ? `${count("done")} concluído(s) · ${count("pending")} na fila · ${count("uploading")} enviando · ${count("cancelled")} cancelado(s) · ${count("error")} com erro`
            : "Nenhum arquivo selecionado.";
        emptyMessage.hidden = entries.length > 0;
        clear.disabled = count("done") === 0;
    }

    /**
     * Update a queued file and its row without changing its destination.
     * @param {object} entry File record with state, name and row/status/progress/cancel/rate DOM nodes.
     * @param {string} state pending, uploading, done, error or cancelled.
     * @param {string} message User-facing status.
     * @returns {void}
     */
    function setState(entry, state, message) {
        entry.state = state;
        entry.row.dataset.state = state;
        entry.status.textContent = message;
        entry.progress.hidden = state === "error" || state === "cancelled";
        entry.cancel.hidden = !["pending", "uploading", "cancelled"].includes(state);
        const action = state === "cancelled" ? "Reiniciar envio do zero" : "Cancelar envio";
        entry.cancel.title = action;
        entry.cancel.setAttribute("aria-label", `${action} de ${entry.name}`);
        entry.cancel.querySelector("i").className = state === "cancelled" ? "fa fa-play" : "fa fa-stop";
        if (state === "cancelled") {
            entry.rate.hidden = true;
        }
        renderQueueSummary();
    }

    /**
     * Send one file using its captured classification and await its outcome.
     * @param {object} entry Queue record with file, destination, DOM nodes and optional xhr.
     * @returns {Promise<void>} Resolves after success, failure or cancellation.
     */
    function uploadFile(entry) {
        return new Promise(resolve => {
            const xhr = new XMLHttpRequest();
            entry.xhr = xhr;
            xhr.open("POST", root.dataset.uploadUrl);
            xhr.setRequestHeader(root.dataset.requestHeader, root.dataset.requestHeaderValue);
            setState(entry, "uploading", "Preparando envio…");
            let startedAt;
            xhr.upload.addEventListener("progress", event => {
                if (entry.state !== "uploading" || !event.lengthComputable || event.total <= 0) {
                    return;
                }
                const percent = Math.round(event.loaded / event.total * 100);
                entry.progress.value = percent;
                entry.status.textContent = event.loaded >= event.total ? "Salvando no servidor…" : `Enviando… ${percent}%`;
                const elapsedSeconds = (performance.now() - startedAt) / 1000;
                // Exclude multipart framing; stop measuring before server-side persistence.
                const sentBytes = entry.file.size * Math.min(event.loaded / event.total, 1);
                if (elapsedSeconds > 0 && sentBytes > 0) {
                    entry.rate.textContent = `Taxa média: ${formatSize(sentBytes / elapsedSeconds)}/s`;
                    entry.rate.hidden = false;
                }
            });
            xhr.addEventListener("load", () => {
                if (entry.state !== "uploading") {
                    return;
                }
                let data = {};
                try {
                    data = JSON.parse(xhr.responseText);
                } catch (_) {
                    // Proxies may return HTML errors instead of JSON.
                }
                if (xhr.status === 201 && typeof data.name === "string") {
                    entry.progress.value = 100;
                    setState(entry, "done", `Enviado · ${data.name}`);
                    entry.file = null;
                } else {
                    const fallback = xhr.status === 413 ? "Arquivo maior que o limite do servidor." : "O servidor não confirmou o envio. Confira antes de tentar novamente.";
                    setState(entry, "error", data.error || fallback);
                }
                resolve();
            });
            xhr.addEventListener("error", () => {
                if (entry.state !== "uploading") {
                    return;
                }
                setState(entry, "error", "Conexão interrompida. Confira o destino antes de enviar novamente.");
                resolve();
            });
            xhr.addEventListener("abort", () => {
                // Aborting the client cannot roll back a file already received by the server.
                setState(entry, "cancelled", "Envio cancelado. Se o servidor já recebeu o arquivo completo, ele pode ter sido salvo.");
                resolve();
            });
            xhr.addEventListener("loadend", () => {
                entry.xhr = null;
            });
            const body = new FormData();
            body.append("category", entry.destination.category);
            if (entry.destination.station) {
                body.append("station", entry.destination.station);
            }
            if (entry.destination.drive_type) {
                body.append("drive_type", entry.destination.drive_type);
            }
            body.append("file", entry.file);
            startedAt = performance.now();
            xhr.send(body);
        });
    }

    /** Process pending files one at a time. Returns a Promise<void> after the queue settles. */
    async function drainQueue() {
        if (active) {
            return;
        }
        active = true;
        try {
            let entry;
            // One request at a time bounds server load and isolates each file's result.
            while ((entry = entries.find(item => item.state === "pending"))) {
                await uploadFile(entry);
            }
        } finally {
            active = false;
            feedback.textContent = "Fila finalizada. Confira o resultado de cada arquivo abaixo.";
            renderQueueSummary();
        }
    }

    /**
     * Cancel a pending/active transfer or restart a cancelled file from byte zero.
     * @param {object} entry Queue record with state, xhr, file and row controls.
     * @returns {void}
     */
    function toggleTransfer(entry) {
        switch (entry.state) {
            case "pending":
                setState(entry, "cancelled", "Cancelado antes de iniciar o envio.");
                break;
            case "uploading":
                entry.xhr.abort();
                break;
            case "cancelled":
                entry.progress.value = 0;
                entry.rate.hidden = true;
                setState(entry, "pending", "Aguardando novo envio do zero");
                drainQueue();
                break;
        }
    }

    /**
     * Build a row and capture the destination independently of later selector changes.
     * @param {File} file Selected browser file.
     * @param {object} destination String fields category, station, drive_type and folder.
     * @returns {object} Queue record with file, destination copy, name, state, xhr and DOM nodes.
     */
    function createEntry(file, destination) {
        const row = rowTemplate.content.firstElementChild.cloneNode(true);
        row.querySelector(".upload-file-name").textContent = file.name;
        row.querySelector(".upload-file-size").textContent = formatSize(file.size);
        row.querySelector(".upload-file-destination").textContent = `${root.dataset.uploadFolder}/${destination.folder}`;
        const entry = {
            file,
            destination: { ...destination },
            name: file.name,
            row,
            state: "pending",
            status: row.querySelector(".upload-file-status"),
            progress: row.querySelector("progress"),
            rate: row.querySelector(".upload-file-rate"),
            cancel: row.querySelector(".upload-cancel"),
            xhr: null,
        };
        entry.progress.setAttribute("aria-label", `Progresso de ${file.name}`);
        entry.cancel.addEventListener("click", () => toggleTransfer(entry));
        return entry;
    }

    /** @param {File[]} files Files to append. @returns {void} Starts the queue if classified. */
    function addFiles(files) {
        const destination = getClassification();
        if (!destination.folder) {
            feedback.textContent = "Selecione o tipo de upload e complete a classificação antes de enviar arquivos.";
            // The shell redirects focus from the native field to its combobox.
            category.focus();
            return;
        }
        for (const file of files) {
            const entry = createEntry(file, destination);
            list.append(entry.row);
            entries.push(entry);
            setState(entry, file.size > limit ? "error" : "pending",
                file.size > limit ? `O arquivo excede ${formatSize(limit)}.` : "Aguardando envio");
        }
        if (!files.length) {
            return;
        }
        feedback.textContent = "Arquivos adicionados. Mantenha esta página aberta durante o envio.";
        drainQueue();
    }

    [category, station, driveType].forEach(select => {
        select.addEventListener("change", updateClassification);
    });
    selectFileButtons.forEach(button => {
        button.addEventListener("click", () => input.click());
    });
    input.addEventListener("change", () => {
        addFiles(Array.from(input.files));
        input.value = "";
    });
    clear.addEventListener("click", () => {
        for (let index = entries.length - 1; index >= 0; index--) {
            if (entries[index].state === "done") {
                entries[index].row.remove();
                entries.splice(index, 1);
            }
        }
        renderQueueSummary();
    });
    document.addEventListener("dragenter", event => {
        if (!Array.from(event.dataTransfer.types).includes("Files")) {
            return;
        }
        event.preventDefault();
        dragDepth++;
        dropzone.classList.add("is-dragging");
    });
    document.addEventListener("dragover", event => {
        if (Array.from(event.dataTransfer.types).includes("Files")) {
            event.preventDefault();
            event.dataTransfer.dropEffect = "copy";
        }
    });
    document.addEventListener("dragleave", () => {
        dragDepth = Math.max(0, dragDepth - 1);
        if (!dragDepth) {
            dropzone.classList.remove("is-dragging");
        }
    });
    document.addEventListener("drop", event => {
        event.preventDefault();
        dragDepth = 0;
        dropzone.classList.remove("is-dragging");
        const items = Array.from(event.dataTransfer.items || []);
        if (items.some(item => item.webkitGetAsEntry && item.webkitGetAsEntry()?.isDirectory)) {
            feedback.textContent = "Selecione arquivos individuais. O envio de pastas não está disponível.";
            return;
        }
        addFiles(Array.from(event.dataTransfer.files));
    });
    window.addEventListener("beforeunload", event => {
        if (active || entries.some(entry => entry.state === "pending")) {
            event.preventDefault();
            event.returnValue = "";
        }
    });

    updateClassification();
})();
