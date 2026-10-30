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
    const selectControls = new Map();
    let active = false;
    let dragDepth = 0;

    function enhanceSelect(select) {
        // Preserve the native select as the classification model and no-script fallback.
        const shell = select.parentElement;
        const label = document.querySelector(`label[for="${select.id}"]`);
        const trigger = document.createElement("button");
        trigger.type = "button";
        trigger.id = `${select.id}-trigger`;
        trigger.className = "form-control upload-select-trigger";
        trigger.setAttribute("role", "combobox");
        trigger.setAttribute("aria-haspopup", "listbox");
        trigger.setAttribute("aria-expanded", "false");
        const menu = document.createElement("ul");
        menu.id = `${select.id}-options`;
        menu.className = "upload-select-options";
        menu.setAttribute("role", "listbox");
        menu.hidden = true;
        trigger.setAttribute("aria-controls", menu.id);
        label.id = `${select.id}-label`;
        trigger.setAttribute("aria-labelledby", `${label.id} ${trigger.id}`);
        menu.setAttribute("aria-labelledby", label.id);
        const options = Array.from(select.options);
        let activeIndex = select.selectedIndex;
        let search = "";
        let lastTyped = 0;

        function activate(index, scroll = false) {
            activeIndex = index;
            Array.from(menu.children).forEach((item, i) => item.classList.toggle("is-active", i === index));
            const item = menu.children[index];
            if (item) {
                trigger.setAttribute("aria-activedescendant", item.id);
                if (scroll) item.scrollIntoView({ block: "nearest" });
            }
        }

        function close() {
            menu.hidden = true;
            trigger.setAttribute("aria-expanded", "false");
            trigger.removeAttribute("aria-activedescendant");
            search = "";
        }

        function open() {
            if (trigger.disabled) return;
            selectControls.forEach(control => control.close());
            menu.hidden = false;
            trigger.setAttribute("aria-expanded", "true");
            activate(select.selectedIndex, true);
        }

        function choose(index) {
            if (!options[index] || options[index].disabled || select.disabled) return;
            select.selectedIndex = index;
            close();
            select.dispatchEvent(new Event("change", { bubbles: true }));
            trigger.focus();
        }

        function sync() {
            trigger.textContent = select.selectedOptions[0]?.textContent || "";
            trigger.disabled = select.disabled;
            trigger.setAttribute("aria-required", String(select.required));
            Array.from(menu.children).forEach((item, index) => {
                item.setAttribute("aria-selected", String(index === select.selectedIndex));
            });
            close();
        }

        options.forEach((option, index) => {
            const item = document.createElement("li");
            item.id = `${select.id}-option-${index}`;
            item.setAttribute("role", "option");
            item.setAttribute("aria-disabled", String(option.disabled));
            item.textContent = option.textContent;
            item.addEventListener("pointerdown", event => event.preventDefault());
            item.addEventListener("pointermove", () => {
                if (!option.disabled) activate(index);
            });
            item.addEventListener("click", () => choose(index));
            menu.append(item);
        });
        trigger.addEventListener("click", () => { if (menu.hidden) open(); else close(); });
        trigger.addEventListener("keydown", event => {
            const key = event.key;
            if (key === "Tab" || key === "Escape") {
                close();
                if (key === "Escape") event.preventDefault();
                return;
            }
            if (["Enter", " "].includes(key)) {
                event.preventDefault();
                if (menu.hidden) open(); else choose(activeIndex);
                return;
            }
            if (["ArrowDown", "ArrowUp", "Home", "End"].includes(key)) {
                event.preventDefault();
                const wasClosed = menu.hidden;
                if (wasClosed) open();
                const enabled = options.map((option, i) => option.disabled ? -1 : i).filter(i => i >= 0);
                let position = enabled.indexOf(activeIndex);
                if (key === "Home") position = 0;
                else if (key === "End") position = enabled.length - 1;
                else if (!wasClosed) position += key === "ArrowDown" ? 1 : -1;
                activate(enabled[Math.max(0, Math.min(position, enabled.length - 1))], true);
                return;
            }
            if (key.length !== 1 || event.ctrlKey || event.metaKey || event.altKey) return;
            event.preventDefault();
            if (menu.hidden) open();
            const now = performance.now();
            search = now - lastTyped > 700 ? key : search + key;
            lastTyped = now;
            const index = options.findIndex(option => !option.disabled &&
                option.textContent.trim().toLocaleLowerCase("pt-BR").startsWith(search.toLocaleLowerCase("pt-BR")));
            if (index >= 0) activate(index, true);
        });
        shell.addEventListener("focusout", event => {
            if (!shell.contains(event.relatedTarget)) close();
        });
        document.addEventListener("pointerdown", event => {
            if (!shell.contains(event.target)) close();
        });
        shell.append(trigger, menu);
        shell.classList.add("upload-select");
        label.htmlFor = trigger.id;
        select.hidden = true;
        sync();
        selectControls.set(select, { sync, close });
    }

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
        selectControls.forEach(control => control.sync());
    }
    [category, station, driveType].forEach(enhanceSelect);
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
            document.getElementById(`${category.id}-trigger`).focus();
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
