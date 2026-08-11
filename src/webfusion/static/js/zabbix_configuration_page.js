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
})();
