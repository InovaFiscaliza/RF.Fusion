(() => {
    "use strict";

    const kindSelect = document.getElementById("target-kind");
    const targetSelect = document.getElementById("target-id");
    if (!kindSelect || !targetSelect) {
        return;
    }

    const options = Array.from(targetSelect.options)
        .filter((option) => option.dataset.targetKind)
        .map((option) => ({
            label: option.textContent,
            value: option.value,
            targetKind: option.dataset.targetKind,
        }));
    const selectedValue = targetSelect.value;

    const refreshTargets = (preservedValue = "") => {
        const selectedKind = kindSelect.value;
        targetSelect.replaceChildren();

        const prompt = document.createElement("option");
        prompt.value = "";
        prompt.textContent = selectedKind
            ? "Selecione uma opção"
            : "Selecione primeiro o tipo";
        targetSelect.appendChild(prompt);

        options
            .filter((option) => option.targetKind === selectedKind)
            .forEach((option) => {
                const element = document.createElement("option");
                element.value = option.value;
                element.textContent = option.label;
                element.selected = option.value === preservedValue;
                targetSelect.appendChild(element);
            });
    };

    refreshTargets(selectedValue);
    kindSelect.addEventListener("change", () => refreshTargets());

    const confirmationDialog = document.getElementById("zabbix-confirm-dialog");
    const confirmationCancelButton = document.getElementById("zabbix-confirm-cancel");
    const confirmationSubmitButton = document.getElementById("zabbix-confirm-submit");
    const confirmationTitle = document.getElementById("zabbix-confirm-title");
    const confirmationMessage = document.getElementById("zabbix-confirm-message");
    let pendingForm = null;

    const closeConfirmation = () => {
        pendingForm = null;
        if (confirmationDialog?.open) {
            confirmationDialog.close();
        }
    };

    const openConfirmation = (form) => {
        if (!confirmationDialog || !confirmationCancelButton || !confirmationTitle || !confirmationMessage) {
            return;
        }

        const macroName = form.querySelector("[name='macro_name']")?.value || "a macro selecionada";
        const targetLabel = form.dataset.targetLabel || "o item selecionado";
        const action = form.dataset.zabbixConfirm;
        const isRestore = action === "restore";

        confirmationTitle.textContent = isRestore
            ? "Restaurar configuração herdada"
            : "Confirmar alteração da macro";
        confirmationMessage.textContent = isRestore
            ? `A sobrescrita de ${macroName} será removida em ${targetLabel}. O valor herdado voltará a ser usado.`
            : `A configuração de ${macroName} será alterada em ${targetLabel}.`;
        pendingForm = form;
        confirmationDialog.showModal();
        confirmationCancelButton.focus();
    };

    document.querySelectorAll("form[data-zabbix-confirm]").forEach((form) => {
        form.addEventListener("submit", (event) => {
            if (form.dataset.zabbixConfirmed === "true") {
                delete form.dataset.zabbixConfirmed;
                return;
            }

            event.preventDefault();
            openConfirmation(form);
        });
    });

    confirmationCancelButton?.addEventListener("click", closeConfirmation);

    confirmationSubmitButton?.addEventListener("click", () => {
        if (!pendingForm) {
            closeConfirmation();
            return;
        }

        const form = pendingForm;
        pendingForm = null;
        confirmationDialog.close();
        form.dataset.zabbixConfirmed = "true";
        form.requestSubmit();
    });

    confirmationDialog?.addEventListener("keydown", (event) => {
        event.preventDefault();
        closeConfirmation();
    });

    confirmationDialog?.addEventListener("cancel", (event) => {
        event.preventDefault();
        closeConfirmation();
    });

    confirmationDialog?.addEventListener("click", (event) => {
        if (event.target === confirmationDialog) {
            closeConfirmation();
        }
    });

    const filterBuilderDialog = document.getElementById("zabbix-filter-builder-dialog");
    const filterBuilderForm = document.getElementById("zabbix-filter-builder-form");
    const filterBuilderMode = document.getElementById("zabbix-filter-mode");
    const filterBuilderApply = document.getElementById("zabbix-filter-builder-apply");
    const filterBuilderFeedback = document.getElementById("zabbix-filter-builder-feedback");
    const filterModes = new Set(["NONE", "REDISCOVERY", "ALL", "FILE", "RANGE", "LAST"]);
    let filterBuilderTarget = null;

    const emptyFilter = () => ({
        mode: "NONE",
        start_date: null,
        end_date: null,
        last_n_files: null,
        file_name: null,
        max_total_gb: null,
        sort_order: "newest_first",
    });

    const filterLabels = {
        NONE: "Descoberta Incremental",
        REDISCOVERY: "Descoberta Total",
        ALL: "Backup Completo",
        RANGE: "Backup por data",
        LAST: "Backup por últimos arquivos",
        FILE: "Backup por arquivo específico",
    };

    const encodeFilterValue = (filter) => JSON.stringify(filter).replace(/"/g, '\\"');

    const fieldValue = (name) => filterBuilderForm?.elements.namedItem(name)?.value?.trim() || "";

    const setFieldValue = (name, value) => {
        const field = filterBuilderForm?.elements.namedItem(name);
        if (field) {
            field.value = value ?? "";
        }
    };

    const dateInputValue = (value) => {
        const match = String(value || "").match(/^(\d{4}-\d{2}-\d{2})/);
        return match ? match[1] : "";
    };

    const showFilterBuilderFeedback = (message = "") => {
        if (!filterBuilderFeedback) {
            return;
        }

        filterBuilderFeedback.hidden = !message;
        filterBuilderFeedback.textContent = message;
    };

    const decodeFilterValue = (value) => {
        let normalized = String(value || "").trim();
        if ((normalized.startsWith('"') && normalized.endsWith('"'))
            || (normalized.startsWith("'") && normalized.endsWith("'"))) {
            normalized = normalized.slice(1, -1);
        }
        return JSON.parse(normalized.replace(/\\"/g, '"'));
    };

    const readFilterValue = (value) => {
        const fallback = emptyFilter();
        if (!String(value || "").trim()) {
            return fallback;
        }

        try {
            const parsed = decodeFilterValue(value);
            if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
                throw new Error("invalid-filter-object");
            }
            const parsedMode = String(parsed.mode || fallback.mode).trim().toUpperCase();
            const mode = ["LAST_N", "LAST_N_FILES"].includes(parsedMode)
                ? "LAST"
                : parsedMode;
            return {
                ...fallback,
                ...parsed,
                mode: filterModes.has(mode) ? mode : fallback.mode,
            };
        } catch (_) {
            showFilterBuilderFeedback(
                "O valor atual não contém um JSON de filtro válido. O construtor foi aberto com valores vazios e não alterará o campo até você aplicar.",
            );
            return fallback;
        }
    };

    const refreshFilterBuilderFields = () => {
        if (!filterBuilderMode || !filterBuilderForm) {
            return;
        }

        const mode = filterBuilderMode.value;
        filterBuilderForm.querySelectorAll("[data-filter-modes]").forEach((container) => {
            const isVisible = container.dataset.filterModes.split(" ").includes(mode);
            container.hidden = !isVisible;
            container.querySelectorAll("input, select").forEach((field) => {
                field.disabled = !isVisible;
            });
        });
    };

    const populateFilterBuilder = (filter) => {
        if (!filterBuilderMode) {
            return;
        }

        filterBuilderMode.value = filter.mode;
        setFieldValue("file_name", filter.file_name);
        setFieldValue("start_date", dateInputValue(filter.start_date));
        setFieldValue("end_date", dateInputValue(filter.end_date));
        setFieldValue("last_n_files", filter.last_n_files);
        setFieldValue("max_total_gb", filter.max_total_gb);
        setFieldValue("sort_order", filter.sort_order === "oldest_first" ? "oldest_first" : "newest_first");
        refreshFilterBuilderFields();
    };

    const buildFilterValue = () => {
        const mode = filterBuilderMode?.value || "NONE";
        const filter = {
            mode,
            start_date: null,
            end_date: null,
            last_n_files: null,
            file_name: null,
        };

        if (mode === "FILE") {
            filter.file_name = fieldValue("file_name");
        }
        if (mode === "RANGE") {
            filter.start_date = fieldValue("start_date") || null;
            filter.end_date = fieldValue("end_date") || null;
        }
        if (mode === "LAST") {
            const count = Number.parseInt(fieldValue("last_n_files"), 10);
            filter.last_n_files = Number.isInteger(count) && count > 0 ? count : null;
        }
        if (["ALL", "FILE", "RANGE", "LAST"].includes(mode)) {
            const limit = Number.parseFloat(fieldValue("max_total_gb"));
            if (Number.isFinite(limit) && limit > 0) {
                filter.max_total_gb = limit;
            }
            if (fieldValue("sort_order") === "oldest_first") {
                filter.sort_order = "oldest_first";
            }
        }

        return filter;
    };

    const closeFilterBuilder = () => {
        filterBuilderTarget = null;
        showFilterBuilderFeedback();
        if (filterBuilderDialog?.open) {
            filterBuilderDialog.close();
        }
    };

    document.querySelectorAll("[data-filter-builder-target]").forEach((trigger) => {
        trigger.addEventListener("click", () => {
            if (!filterBuilderDialog || !filterBuilderForm || !filterBuilderMode) {
                return;
            }

            const target = document.getElementById(trigger.dataset.filterBuilderTarget);
            if (!target) {
                return;
            }

            showFilterBuilderFeedback();
            filterBuilderTarget = target;
            populateFilterBuilder(readFilterValue(target.value));
            filterBuilderDialog.showModal();
            filterBuilderMode.focus();
        });
    });

    document.querySelectorAll("[data-filter-summary]").forEach((element) => {
        const filter = readFilterValue(element.dataset.filterSummary);
        element.textContent = filterLabels[filter.mode] || "Filtro configurado";
    });

    document.querySelectorAll(".zabbix-config-filter-value").forEach((input) => {
        try {
            input.value = JSON.stringify(decodeFilterValue(input.value));
        } catch (_) {
            // Keep the original value available for manual correction.
        }
    });

    document.querySelectorAll("form[data-zabbix-confirm]").forEach((form) => {
        form.addEventListener("submit", (event) => {
            if (event.defaultPrevented) {
                return;
            }

            const filterInput = form.querySelector(".zabbix-config-filter-value");
            if (!filterInput) {
                return;
            }

            try {
                filterInput.value = encodeFilterValue(decodeFilterValue(filterInput.value));
            } catch (_) {
                // Keep invalid JSON available for explicit user correction.
            }
        });
    });

    filterBuilderMode?.addEventListener("change", refreshFilterBuilderFields);

    filterBuilderForm?.addEventListener("submit", (event) => event.preventDefault());

    filterBuilderApply?.addEventListener("click", () => {
        if (!filterBuilderTarget || !filterBuilderForm) {
            closeFilterBuilder();
            return;
        }
        if (!filterBuilderForm.reportValidity()) {
            return;
        }

        const target = filterBuilderTarget;
        target.value = JSON.stringify(buildFilterValue());
        target.dispatchEvent(new Event("input", { bubbles: true }));
        closeFilterBuilder();
        window.setTimeout(() => target.focus(), 0);
    });

    document.querySelectorAll("[data-filter-builder-cancel]").forEach((button) => {
        button.addEventListener("click", closeFilterBuilder);
    });

    filterBuilderDialog?.addEventListener("cancel", (event) => {
        event.preventDefault();
        closeFilterBuilder();
    });

    filterBuilderDialog?.addEventListener("click", (event) => {
        if (event.target === filterBuilderDialog) {
            closeFilterBuilder();
        }
    });
})();
