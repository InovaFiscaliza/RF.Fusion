/* User directory dialogs keep row actions focused on one identity at a time. */
(function () {
    const createButton = document.getElementById("users-create-button");
    const createDialog = document.getElementById("users-create-dialog");
    const deleteDialog = document.getElementById("users-delete-dialog");
    const deleteName = document.getElementById("users-delete-name");
    const deleteEmail = document.getElementById("users-delete-email");
    const privilegesDialog = document.getElementById("users-privileges-dialog");
    const privilegesName = document.getElementById("users-privileges-name");
    const privilegesEmail = document.getElementById("users-privileges-email");
    const privilegesAdmin = document.getElementById("users-privileges-admin");
    const privilegesDeveloper = document.getElementById("users-privileges-developer");
    const privilegesConfirmButton = document.getElementById("users-privileges-confirm-button");
    const privilegesConfirmDialog = document.getElementById("users-privileges-confirm-dialog");
    const privilegesConfirmName = document.getElementById("users-privileges-confirm-name");
    const privilegesConfirmEmail = document.getElementById("users-privileges-confirm-email");
    const privilegesConfirmAdmin = document.getElementById("users-privileges-confirm-admin");
    const privilegesConfirmDeveloper = document.getElementById("users-privileges-confirm-developer");
    const privilegesConfirmAdminLabel = document.getElementById("users-privileges-confirm-admin-label");
    const privilegesConfirmDeveloperLabel = document.getElementById("users-privileges-confirm-developer-label");
    const filterOptionsElement = document.getElementById("users-filter-options");
    const filterOptions = filterOptionsElement ? JSON.parse(filterOptionsElement.textContent || "{}") : {};

    createButton?.addEventListener("click", function () {
        createDialog?.showModal();
    });

    document.addEventListener("click", function (event) {
        const deleteButton = event.target.closest("[data-user-delete-email]");
        const privilegesButton = event.target.closest("[data-user-privileges-email]");

        if (deleteButton && deleteDialog && deleteName && deleteEmail) {
            deleteName.textContent = String(deleteButton.dataset.userDeleteName || "usuário");
            deleteEmail.value = String(deleteButton.dataset.userDeleteEmail || "");
            deleteDialog.showModal();
            return;
        }

        if (
            !privilegesButton
            || !privilegesDialog
            || !privilegesName
            || !privilegesEmail
            || !privilegesAdmin
            || !privilegesDeveloper
        ) {
            return;
        }

        privilegesName.textContent = String(privilegesButton.dataset.userPrivilegesName || "usuário");
        privilegesEmail.value = String(privilegesButton.dataset.userPrivilegesEmail || "");
        privilegesAdmin.checked = privilegesButton.dataset.userIsAdmin === "true";
        privilegesDeveloper.checked = privilegesButton.dataset.userIsDeveloper === "true";
        privilegesDialog.showModal();
    });

    privilegesConfirmButton?.addEventListener("click", function () {
        if (
            !privilegesConfirmDialog
            || !privilegesConfirmName
            || !privilegesConfirmEmail
            || !privilegesConfirmAdmin
            || !privilegesConfirmDeveloper
            || !privilegesConfirmAdminLabel
            || !privilegesConfirmDeveloperLabel
        ) {
            return;
        }

        const isAdmin = privilegesAdmin?.checked === true;
        const isDeveloper = privilegesDeveloper?.checked === true;
        privilegesConfirmName.textContent = privilegesName?.textContent || "usuário";
        privilegesConfirmEmail.value = privilegesEmail?.value || "";
        privilegesConfirmAdmin.value = isAdmin ? "1" : "";
        privilegesConfirmDeveloper.value = isDeveloper ? "1" : "";
        privilegesConfirmAdminLabel.textContent = isAdmin ? "Ativo" : "Inativo";
        privilegesConfirmDeveloperLabel.textContent = isDeveloper ? "Ativo" : "Inativo";
        privilegesConfirmDialog.showModal();
    });

    document.querySelectorAll("[data-users-dialog-close]").forEach(function (button) {
        button.addEventListener("click", function () {
            const dialogId = button.dataset.usersDialogClose;
            document.getElementById(dialogId)?.close();
        });
    });

    document.querySelectorAll("[data-users-combobox]").forEach(function (combobox) {
        const input = combobox.querySelector("[data-users-combobox-input]");
        const valueInput = combobox.querySelector("[data-users-combobox-value]");
        const menu = combobox.querySelector("[data-users-combobox-menu]");
        const optionKey = input?.id === "users-email-filter"
            ? "users"
            : input?.id === "users-job-title-filter"
                ? "job_titles"
                : "departments";
        const options = Array.isArray(filterOptions[optionKey]) ? filterOptions[optionKey] : [];

        if (!input || !valueInput || !menu) {
            return;
        }

        function renderOptions() {
            const searchTerm = input.value.trim().toLocaleLowerCase("pt-BR");
            const visibleOptions = options.filter(function (option) {
                return String(option.label || "").toLocaleLowerCase("pt-BR").includes(searchTerm);
            }).slice(0, 100);

            if (visibleOptions.length === 0) {
                menu.innerHTML = '<div class="users-combobox-empty">Nenhuma opção encontrada.</div>';
                return;
            }

            menu.replaceChildren(...visibleOptions.map(function (option) {
                const button = document.createElement("button");
                button.type = "button";
                button.className = "users-combobox-option";
                button.textContent = String(option.label || "");
                button.dataset.value = String(option.value || "");
                button.dataset.label = String(option.label || "");
                return button;
            }));
        }

        input.addEventListener("focus", function () {
            renderOptions();
            menu.hidden = false;
        });

        input.addEventListener("input", function () {
            // Typed text must not retain a previous exact filter selection.
            valueInput.value = "";
            renderOptions();
            menu.hidden = false;
        });

        menu.addEventListener("click", function (event) {
            const option = event.target.closest("[data-value]");
            if (!option) {
                return;
            }

            input.value = option.dataset.label || "";
            valueInput.value = option.dataset.value || "";
            menu.hidden = true;
        });
    });

    document.addEventListener("click", function (event) {
        if (!event.target.closest("[data-users-combobox]")) {
            document.querySelectorAll("[data-users-combobox-menu]").forEach(function (menu) {
                menu.hidden = true;
            });
        }
    });
})();
