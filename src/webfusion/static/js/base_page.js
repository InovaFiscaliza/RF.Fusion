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
 * 4. clears stale overlay state when the browser restores a cached page.
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
})();
