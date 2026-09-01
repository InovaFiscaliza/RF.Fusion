/* User directory interactions that require a named confirmation target. */
(function () {
    const deleteDialog = document.getElementById("users-delete-dialog");
    const deleteName = document.getElementById("users-delete-name");
    const deleteEmail = document.getElementById("users-delete-email");
    const deleteCancel = document.getElementById("users-delete-cancel");

    if (!deleteDialog || !deleteName || !deleteEmail) {
        return;
    }

    document.addEventListener("click", function (event) {
        const deleteButton = event.target.closest("[data-user-delete-email]");

        if (!deleteButton) {
            return;
        }

        deleteName.textContent = String(deleteButton.dataset.userDeleteName || "usuário");
        deleteEmail.value = String(deleteButton.dataset.userDeleteEmail || "");
        deleteDialog.showModal();
    });

    deleteCancel?.addEventListener("click", function () {
        deleteDialog.close();
    });
})();
