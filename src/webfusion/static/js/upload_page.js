(() => {
    "use strict";
    const root = document.getElementById("upload-page");
    if (!root) return;
    const input = document.getElementById("upload-input");
    const dropzone = document.getElementById("upload-dropzone");
    const list = document.getElementById("upload-list");
    const summary = document.getElementById("upload-summary");
    const feedback = document.getElementById("upload-feedback");
    const clear = document.getElementById("upload-clear");
    const category = document.getElementById("upload-category");
    const station = document.getElementById("upload-station");
    const driveType = document.getElementById("upload-drive-type");
    const limit = Number(root.dataset.maxFileBytes);
    const entries = [];
    let active = false;
    let dragDepth = 0;

    function classification() {
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

    function updateClassification() {
        const fixed = category.value === "fixas";
        const drive = category.value === "drive-test";
        document.getElementById("upload-station-field").hidden = !fixed;
        document.getElementById("upload-drive-field").hidden = !drive;
        station.disabled = !fixed;
        station.required = fixed;
        driveType.disabled = !drive;
        driveType.required = drive;
        const selected = classification();
        document.getElementById("upload-destination").textContent = selected.folder
            ? `${root.dataset.uploadFolder}/${selected.folder}` : "Selecione a classificação acima.";
        root.querySelectorAll("[data-select-files]").forEach(button => { button.disabled = !selected.folder; });
    }
    [category, station, driveType].forEach(select => select.addEventListener("change", updateClassification));
    updateClassification();

    function formatSize(bytes) {
        const units = ["B", "KiB", "MiB", "GiB"];
        const index = bytes > 0 ? Math.max(0, Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)) : 0;
        return `${(bytes / (1024 ** index)).toLocaleString("pt-BR", { maximumFractionDigits: 1 })} ${units[index]}`;
    }

    function refresh() {
        const count = state => entries.filter(entry => entry.state === state).length;
        summary.textContent = entries.length
            ? `${count("done")} concluído(s) · ${count("pending")} na fila · ${count("uploading")} enviando · ${count("cancelled")} cancelado(s) · ${count("error")} com erro`
            : "Nenhum arquivo selecionado.";
        document.getElementById("upload-empty").hidden = entries.length > 0;
        clear.disabled = count("done") === 0;
    }

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
        if (state === "cancelled") entry.rate.hidden = true;
        refresh();
    }

    function send(entry) {
        return new Promise(resolve => {
            const xhr = new XMLHttpRequest();
            entry.xhr = xhr;
            xhr.open("POST", root.dataset.uploadUrl);
            xhr.setRequestHeader(root.dataset.requestHeader, root.dataset.requestHeaderValue);
            setState(entry, "uploading", "Preparando envio…");
            let startedAt;
            xhr.upload.addEventListener("progress", event => {
                if (entry.state !== "uploading" || !event.lengthComputable || event.total <= 0) return;
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
                if (entry.state !== "uploading") return;
                let data = {};
                try { data = JSON.parse(xhr.responseText); } catch (_) { /* Proxies may return HTML errors. */ }
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
                if (entry.state !== "uploading") return;
                setState(entry, "error", "Conexão interrompida. Confira o destino antes de enviar novamente.");
                resolve();
            });
            xhr.addEventListener("abort", () => {
                // Aborting the client cannot roll back a file already received by the server.
                setState(entry, "cancelled", "Envio cancelado. Se o servidor já recebeu o arquivo completo, ele pode ter sido salvo.");
                resolve();
            });
            xhr.addEventListener("loadend", () => { entry.xhr = null; });
            const body = new FormData();
            body.append("category", entry.destination.category);
            if (entry.destination.station) body.append("station", entry.destination.station);
            if (entry.destination.drive_type) body.append("drive_type", entry.destination.drive_type);
            body.append("file", entry.file);
            startedAt = performance.now();
            xhr.send(body);
        });
    }

    async function drainQueue() {
        if (active) return;
        active = true;
        try {
            let entry;
            // One request at a time bounds server load and isolates each file's result.
            while ((entry = entries.find(item => item.state === "pending"))) {
                await send(entry);
            }
        } finally {
            active = false;
            feedback.textContent = "Fila finalizada. Confira o resultado de cada arquivo abaixo.";
            refresh();
        }
    }

    function addFiles(files) {
        const destination = classification();
        if (!destination.folder) {
            feedback.textContent = "Selecione o tipo de upload e complete a classificação antes de enviar arquivos.";
            category.focus();
            return;
        }
        for (const file of files) {
            const row = document.getElementById("upload-row-template").content.firstElementChild.cloneNode(true);
            row.querySelector(".upload-file-name").textContent = file.name;
            row.querySelector(".upload-file-size").textContent = formatSize(file.size);
            row.querySelector(".upload-file-destination").textContent = `${root.dataset.uploadFolder}/${destination.folder}`;
            const entry = { file, destination: { ...destination }, name: file.name, row, state: "pending", status: row.querySelector(".upload-file-status"),
                progress: row.querySelector("progress"), rate: row.querySelector(".upload-file-rate"),
                cancel: row.querySelector(".upload-cancel"), xhr: null };
            entry.progress.setAttribute("aria-label", `Progresso de ${file.name}`);
            entry.cancel.addEventListener("click", () => {
                if (entry.state === "pending") {
                    setState(entry, "cancelled", "Cancelado antes de iniciar o envio.");
                } else if (entry.state === "uploading") {
                    entry.xhr.abort();
                } else if (entry.state === "cancelled") {
                    entry.progress.value = 0;
                    entry.rate.hidden = true;
                    setState(entry, "pending", "Aguardando novo envio do zero");
                    drainQueue();
                }
            });
            list.append(row);
            entries.push(entry);
            setState(entry, file.size > limit ? "error" : "pending",
                file.size > limit ? `O arquivo excede ${formatSize(limit)}.` : "Aguardando envio");
        }
        if (!files.length) return;
        feedback.textContent = "Arquivos adicionados. Mantenha esta página aberta durante o envio.";
        drainQueue();
    }

    root.querySelectorAll("[data-select-files]").forEach(button => button.addEventListener("click", () => input.click()));
    input.addEventListener("change", () => { addFiles(Array.from(input.files)); input.value = ""; });
    clear.addEventListener("click", () => {
        for (let index = entries.length - 1; index >= 0; index--) {
            if (entries[index].state === "done") { entries[index].row.remove(); entries.splice(index, 1); }
        }
        refresh();
    });
    document.addEventListener("dragenter", event => {
        if (!Array.from(event.dataTransfer.types).includes("Files")) return;
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
        if (!dragDepth) dropzone.classList.remove("is-dragging");
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
        if (active || entries.some(entry => entry.state === "pending")) { event.preventDefault(); event.returnValue = ""; }
    });
})();
