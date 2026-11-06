/* Base page controller
 *
 * This file centralizes the "page is busy" behavior shared by the whole
 * WebFusion shell. The goal is not just to show a spinner: it keeps links,
 * form submits and download actions speaking the same visual language, while
 * preventing each module page from re-implementing the same loading logic.
 *
 * In practice, `base.html` renders the overlay markup once and this script:
 * 1. exposes tiny global helpers for page-specific modules,
 * 2. listens to common navigation/submit patterns at the document level,
 * 3. decides when the overlay should stay visible or auto-hide,
 * 4. clears stale overlay state when the browser restores a cached page,
 * 5. provides the shared combobox interaction for native form fields.
 */
(function () {
    const overlay = document.getElementById("page-loading-overlay");
    const messageNode = document.getElementById("page-loading-message");
    const navigationLayer = document.getElementById("wf-navigation-layer");
    const navigationToggle = document.getElementById("wf-navigation-toggle");
    const navigationDrawer = document.getElementById("wf-navigation-drawer");
    const navigationClose = document.getElementById("wf-navigation-close");
    const navigationBackdrop = document.getElementById("wf-navigation-backdrop");
    const webfusionRoot = String(document.body?.dataset.webfusionRoot || "/");
    const NAVIGATION_CLOSE_DELAY_MS = 180;
    let navigationReturnFocus = null;
    let navigationCloseTimer = null;

    /* Browser-generated routes must preserve the reverse-proxy prefix. */
    function buildWebFusionUrl(pathname) {
        const normalizedRoot = webfusionRoot === "/"
            ? ""
            : webfusionRoot.replace(/\/+$/, "");
        const normalizedPath = String(pathname || "").replace(/^\/+/, "");
        return `${normalizedRoot}/${normalizedPath}`;
    }

    const DOWNLOAD_ACTION_ENDPOINT = buildWebFusionUrl(
        "api/server/usage-metrics/download-action"
    );

    /* Show the shared overlay with an optional contextual message.
     *
     * The helper is intentionally small because many screens call it directly
     * right before navigation or background-heavy transitions. If the shell is
     * not present, the function fails quietly so page modules do not need
     * defensive checks around every call.
     */
    function showPageLoadingOverlay(message) {
        if (!overlay) {
            return;
        }

        if (messageNode) {
            messageNode.textContent = message || "Aguarde enquanto a solicitação é processada.";
        }

        overlay.classList.add("is-visible");
        overlay.setAttribute("aria-hidden", "false");
        document.body.classList.add("page-loading-open");
    }

    /* Hide the shared overlay and restore body interaction.
     *
     * This is the symmetric counterpart to `showPageLoadingOverlay()`. Keeping
     * the state reset here avoids each caller needing to remember which CSS
     * classes and accessibility attributes must be reverted together.
     */
    function hidePageLoadingOverlay() {
        if (!overlay) {
            return;
        }

        overlay.classList.remove("is-visible");
        overlay.setAttribute("aria-hidden", "true");
        document.body.classList.remove("page-loading-open");
    }

    /* Download actions navigate away quickly, so the metric request must be
     * fire-and-forget. `sendBeacon` is preferred because the browser keeps it
     * alive during navigation; `fetch(..., keepalive)` is the fallback.
     */
    function recordDownloadAction() {
        try {
            if (navigator.sendBeacon) {
                navigator.sendBeacon(DOWNLOAD_ACTION_ENDPOINT, new Blob([], { type: "application/octet-stream" }));
                return;
            }

            fetch(DOWNLOAD_ACTION_ENDPOINT, {
                method: "POST",
                keepalive: true,
                credentials: "same-origin",
            }).catch(function () {
                // Metrics must never block the actual download navigation.
            });
        } catch (error) {
            // Ignore telemetry failures and let the download continue.
        }
    }

    /* Expose the overlay helpers globally because page-specific scripts still
     * need a tiny imperative API for cases that cannot be captured by generic
     * listeners alone, such as lazy transitions and custom navigation flows. */
    window.showPageLoadingOverlay = showPageLoadingOverlay;
    window.hidePageLoadingOverlay = hidePageLoadingOverlay;
    window.webfusionUrl = buildWebFusionUrl;

    /* Keep the drawer isolated from page modules. It only owns shell focus and
     * visibility, while navigation itself remains regular server-side links. */
    function openNavigationDrawer() {
        if (!navigationLayer || !navigationToggle || !navigationDrawer) {
            return;
        }

        window.clearTimeout(navigationCloseTimer);
        navigationReturnFocus = document.activeElement instanceof HTMLElement
            ? document.activeElement
            : null;
        navigationLayer.hidden = false;
        navigationToggle.setAttribute("aria-expanded", "true");
        document.body.classList.add("wf-navigation-open");

        window.requestAnimationFrame(function () {
            navigationLayer.classList.add("is-open");
            navigationClose?.focus();
        });
    }

    function closeNavigationDrawer(options = {}) {
        if (!navigationLayer || !navigationToggle) {
            return;
        }

        const restoreFocus = options.restoreFocus !== false;
        const returnFocus = navigationReturnFocus;
        navigationLayer.classList.remove("is-open");
        navigationToggle.setAttribute("aria-expanded", "false");
        document.body.classList.remove("wf-navigation-open");
        window.clearTimeout(navigationCloseTimer);
        navigationCloseTimer = window.setTimeout(function () {
            navigationLayer.hidden = true;
            if (restoreFocus && returnFocus?.isConnected) {
                returnFocus.focus();
            }
        }, NAVIGATION_CLOSE_DELAY_MS);
    }

    function keepDrawerFocus(event) {
        if (event.key === "Escape") {
            event.preventDefault();
            closeNavigationDrawer();
            return;
        }

        if (event.key !== "Tab" || !navigationDrawer) {
            return;
        }

        const focusableNodes = navigationDrawer.querySelectorAll(
            "a[href], button:not([disabled]), [tabindex]:not([tabindex='-1'])"
        );
        const focusable = Array.from(focusableNodes);
        if (focusable.length === 0) {
            event.preventDefault();
            navigationDrawer.focus();
            return;
        }

        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    }

    navigationToggle?.addEventListener("click", openNavigationDrawer);
    navigationClose?.addEventListener("click", closeNavigationDrawer);
    navigationBackdrop?.addEventListener("click", closeNavigationDrawer);
    navigationDrawer?.addEventListener("keydown", keepDrawerFocus);

    /* Global link interception for regular page navigation.
     *
     * We listen in the capture phase so the overlay appears as early as
     * possible, even if page modules attach their own click handlers later.
     * The guard clauses deliberately ignore:
     * - prevented events,
     * - non-left clicks,
     * - modifier-assisted clicks meant for new tabs/windows,
     * - links that already declare a different lifecycle (`_blank`, download).
     *
     * That keeps the overlay tied to true in-app navigation instead of every
     * clickable anchor on the screen.
     */
    document.addEventListener("click", function (event) {
        const link = event.target.closest("a[data-loading-message]");
        if (!link) {
            return;
        }

        if (event.defaultPrevented || event.button !== 0) {
            return;
        }

        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
            return;
        }

        if (link.target === "_blank" || link.hasAttribute("download")) {
            return;
        }

        showPageLoadingOverlay(link.getAttribute("data-loading-message"));
    }, true);

    /* Download interception uses the same overlay vocabulary, but with a
     * different lifecycle.
     *
     * Unlike standard navigation, a download often keeps the current page in
     * place while the browser negotiates the transfer in the background. The
     * short timeout avoids leaving the whole UI visually blocked after the
     * request has already been handed off to the browser.
     */
    document.addEventListener("click", function (event) {
        const link = event.target.closest("a[data-download-loading-message]");
        if (!link) {
            return;
        }

        if (event.defaultPrevented || event.button !== 0) {
            return;
        }

        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
            return;
        }

        recordDownloadAction();
        showPageLoadingOverlay(link.getAttribute("data-download-loading-message"));
        window.setTimeout(hidePageLoadingOverlay, 4500);
    }, true);

    /* Form submission interception covers the most common "wait" state in the
     * application: searches, filters and task actions that trigger a server
     * round-trip. Pages opt in with `data-loading-message`, which keeps the
     * behavior declarative from the template side and avoids inline JS. */
    document.addEventListener("submit", function (event) {
        if (event.defaultPrevented) {
            return;
        }

        const form = event.target.closest("form[data-loading-message]");
        if (!form) {
            return;
        }

        showPageLoadingOverlay(form.getAttribute("data-loading-message"));
    });

    /* Browser back/forward cache may revive a previous DOM snapshot with the
     * overlay still visible. `pageshow` is the safest global cleanup point to
     * guarantee that returning to a page never traps the user behind stale UI
     * state from an earlier navigation. */
    window.addEventListener("pageshow", function () {
        closeNavigationDrawer({ restoreFocus: false });

        if (!window.webfusionInitialPageLoadPending) {
            hidePageLoadingOverlay();
        }
    });

    // Shared form controls initialize after the page controllers.
    const controls = new Map();

    // Keep the original fields as the form model, including page-specific metadata.
    function enhanceSelect(select) {
        if (select.multiple || select.size > 1 || controls.has(select)) return;
        const labels = Array.from(select.labels);
        let shell = select.parentElement;
        if (!shell.matches(".select-shell, .station-map-select-wrap, .users-select-wrap")) {
            shell = document.createElement("div");
            select.before(shell);
            shell.append(select);
        }
        shell.classList.add("select-shell");
        if (!select.id) {
            let number = controls.size;
            while (document.getElementById(`wf-select-${number}`)) number++;
            select.id = `wf-select-${number}`;
        }
        const id = select.id;
        const label = labels[0] || shell.parentElement.querySelector("label");
        const trigger = document.createElement("button");
        trigger.type = "button";
        trigger.id = `${id}-trigger`;
        trigger.className = "form-control wf-select-trigger";
        trigger.setAttribute("role", "combobox");
        trigger.setAttribute("aria-haspopup", "listbox");
        trigger.setAttribute("aria-expanded", "false");
        const menu = document.createElement("ul");
        menu.id = `${id}-options`;
        menu.className = "wf-select-menu";
        menu.setAttribute("role", "listbox");
        const usePopover = typeof menu.showPopover === "function";
        if (usePopover) menu.popover = "manual";
        menu.hidden = true;
        trigger.setAttribute("aria-controls", menu.id);
        const error = document.createElement("span");
        error.id = `${id}-error`;
        error.className = "wf-select-error";
        error.setAttribute("role", "alert");
        error.hidden = true;
        trigger.setAttribute("aria-describedby", [select.getAttribute("aria-describedby"), error.id].filter(Boolean).join(" "));
        if (label) {
            label.id ||= `${id}-label`;
            label.htmlFor = trigger.id;
            trigger.setAttribute("aria-labelledby", `${label.id} ${trigger.id}`);
            menu.setAttribute("aria-labelledby", label.id);
        } else {
            trigger.setAttribute("aria-label", select.getAttribute("aria-label") || select.name);
        }

        let options = [];
        let activeIndex = -1;
        let search = "";
        let lastTyped = 0;

        function close() {
            if (usePopover && menu.matches(":popover-open")) menu.hidePopover();
            menu.hidden = true;
            shell.classList.remove("is-open");
            trigger.setAttribute("aria-expanded", "false");
            trigger.removeAttribute("aria-activedescendant");
            search = "";
        }

        function activate(index) {
            activeIndex = index;
            Array.from(menu.children).forEach((item, i) => item.classList.toggle("is-active", i === index));
            const item = menu.children[index];
            if (!item) return;
            trigger.setAttribute("aria-activedescendant", item.id);
            // Scroll only the list; scrolling the page would move its anchor.
            if (item.offsetTop < menu.scrollTop) menu.scrollTop = item.offsetTop;
            else if (item.offsetTop + item.offsetHeight > menu.scrollTop + menu.clientHeight) {
                menu.scrollTop = item.offsetTop + item.offsetHeight - menu.clientHeight;
            }
        }

        function sync() {
            trigger.textContent = select.selectedOptions[0]?.label.trim() || "Selecione uma opção";
            trigger.disabled = select.matches(":disabled");
            trigger.hidden = select.hidden;
            trigger.setAttribute("aria-busy", select.getAttribute("aria-busy") || "false");
            trigger.classList.toggle("is-refreshing", select.classList.contains("is-refreshing"));
            trigger.setAttribute("aria-required", String(select.required));
            if (select.validity.valid) {
                error.hidden = true;
                trigger.removeAttribute("aria-invalid");
            }
            if (trigger.disabled || select.hidden) close();
            Array.from(menu.children).forEach((item, index) => {
                item.setAttribute("aria-selected", String(options[index]?.selected || false));
            });
        }

        function choose(index) {
            const option = options[index];
            if (!option || option.matches(":disabled") || select.matches(":disabled")) return;
            const changed = select.selectedIndex !== option.index;
            select.selectedIndex = option.index;
            close();
            if (changed) {
                select.dispatchEvent(new Event("input", { bubbles: true }));
                select.dispatchEvent(new Event("change", { bubbles: true }));
            }
            sync();
            if (trigger.getClientRects().length) trigger.focus();
        }

        function rebuild() {
            close();
            options = Array.from(select.options).filter(option => !option.hidden && !option.closest("optgroup[hidden]"));
            menu.replaceChildren(...options.map((option, index) => {
                const item = document.createElement("li");
                item.id = `${id}-option-${index}`;
                item.setAttribute("role", "option");
                item.setAttribute("aria-disabled", String(option.matches(":disabled")));
                item.textContent = option.label.trim();
                item.addEventListener("pointerdown", event => event.preventDefault());
                item.addEventListener("pointermove", () => { if (!option.matches(":disabled")) activate(index); });
                item.addEventListener("click", () => choose(index));
                return item;
            }));
            sync();
        }

        function open() {
            if (select.matches(":disabled")) return;
            controls.forEach(control => control.close());
            // Read live values too: assigning select.value does not emit a DOM mutation.
            sync();
            const rect = shell.getBoundingClientRect();
            const below = window.innerHeight - rect.bottom - 12;
            const above = rect.top - 12;
            const scale = rect.width / shell.offsetWidth || 1;
            const upwards = below < 180 && above > below;
            menu.classList.toggle("opens-up", upwards);
            menu.style.maxHeight = `${Math.max(0, Math.min(280, (upwards ? above : below) / scale))}px`;
            menu.hidden = false;
            // The top layer keeps lists visible inside clipped panels and dialogs.
            if (usePopover) {
                menu.style.width = `${rect.width / scale}px`;
                menu.style.left = `${rect.left / scale}px`;
                menu.style.top = `${(rect.bottom + 4) / scale}px`;
                menu.showPopover();
                if (upwards) menu.style.top = `${(rect.top - 4) / scale - menu.offsetHeight}px`;
            }
            shell.classList.add("is-open");
            trigger.setAttribute("aria-expanded", "true");
            const selected = options.findIndex(option => option.selected && !option.matches(":disabled"));
            activate(selected >= 0 ? selected : options.findIndex(option => !option.matches(":disabled")));
        }

        trigger.addEventListener("click", () => { if (menu.hidden) open(); else close(); });
        trigger.addEventListener("keydown", event => {
            const key = event.key;
            if (key === "Tab" || key === "Escape") {
                close();
                if (key === "Escape") {
                    event.preventDefault();
                    event.stopPropagation();
                }
                return;
            }
            if (key === "Enter" || key === " ") {
                event.preventDefault();
                if (menu.hidden) open(); else choose(activeIndex);
                return;
            }
            if (["ArrowDown", "ArrowUp", "Home", "End"].includes(key)) {
                event.preventDefault();
                const wasClosed = menu.hidden;
                if (wasClosed) open();
                const enabled = options.map((option, i) => option.matches(":disabled") ? -1 : i).filter(i => i >= 0);
                let position = enabled.indexOf(activeIndex);
                if (key === "Home") position = 0;
                else if (key === "End") position = enabled.length - 1;
                else if (!wasClosed) position += key === "ArrowDown" ? 1 : -1;
                activate(enabled[Math.max(0, Math.min(position, enabled.length - 1))]);
                return;
            }
            if (key.length !== 1 || event.ctrlKey || event.metaKey || event.altKey) return;
            event.preventDefault();
            if (menu.hidden) open();
            const now = performance.now();
            search = now - lastTyped > 700 ? key : search + key;
            lastTyped = now;
            const index = options.findIndex(option => !option.matches(":disabled") &&
                option.label.trim().toLocaleLowerCase("pt-BR").startsWith(search.toLocaleLowerCase("pt-BR")));
            if (index >= 0) activate(index);
        });
        shell.addEventListener("focusout", event => { if (!shell.contains(event.relatedTarget)) close(); });
        select.addEventListener("invalid", event => {
            event.preventDefault();
            error.textContent = select.validationMessage;
            error.hidden = false;
            trigger.setAttribute("aria-invalid", "true");
            trigger.focus();
        });
        select.addEventListener("change", sync);
        select.addEventListener("input", sync);
        select.form?.addEventListener("reset", () => queueMicrotask(() => { close(); sync(); }));
        shell.append(trigger, menu, error);
        shell.classList.add("wf-select");
        select.classList.add("wf-select-model");
        select.tabIndex = -1;
        select.setAttribute("aria-hidden", "true");
        rebuild();
        new MutationObserver(rebuild).observe(select, {
            subtree: true, childList: true, characterData: true, attributes: true,
            attributeFilter: ["disabled", "required", "hidden", "selected", "label", "value", "class", "aria-busy"],
        });
        select.addEventListener("focus", () => trigger.focus({ preventScroll: true }));
        controls.set(select, { shell, close, sync });
    }

    function initializeSelects(root) {
        if (root.nodeType !== Node.ELEMENT_NODE && root !== document) return;
        if (root.matches?.("select")) enhanceSelect(root);
        root.querySelectorAll("select").forEach(enhanceSelect);
    }

    function initializeFormControls() {
        initializeSelects(document);
        new MutationObserver(records => {
            records.forEach(record => record.addedNodes.forEach(initializeSelects));
            controls.forEach((control, select) => {
                if (!select.isConnected) {
                    control.close();
                    controls.delete(select);
                }
            });
        }).observe(document.body, { childList: true, subtree: true });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initializeFormControls, { once: true });
    } else {
        initializeFormControls();
    }

    document.addEventListener("pointerdown", event => {
        controls.forEach(control => { if (!control.shell.contains(event.target)) control.close(); });
    });
    // Page controllers may change other fields synchronously in response to a change.
    document.addEventListener("change", () => queueMicrotask(() => controls.forEach(control => control.sync())));
    document.addEventListener("click", () => queueMicrotask(() => controls.forEach(control => control.sync())));
    window.addEventListener("resize", () => controls.forEach(control => control.close()));
    document.addEventListener("scroll", event => {
        controls.forEach(control => {
            if (!control.shell.contains(event.target)) control.close();
        });
    }, true);
})();
