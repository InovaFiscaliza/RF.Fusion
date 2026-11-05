(() => {
    "use strict";

    const controls = [];
    const selector = ".wf-selector-page .select-shell > select, #task-builder-root .select-shell > select";

    // Keep the original fields as the form model, including page-specific metadata.
    document.querySelectorAll(selector).forEach((select, number) => {
        if (select.multiple || select.size > 1) return;
        const shell = select.parentElement;
        const id = select.id || `wf-select-${number}`;
        const label = select.labels[0] || shell.parentElement.querySelector("label");
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
            trigger.textContent = select.selectedOptions[0]?.textContent.trim() || "Selecione uma opção";
            trigger.disabled = select.disabled;
            trigger.setAttribute("aria-required", String(select.required));
            if (select.validity.valid) {
                error.hidden = true;
                trigger.removeAttribute("aria-invalid");
            }
            if (select.disabled) close();
            Array.from(menu.children).forEach((item, index) => {
                item.setAttribute("aria-selected", String(options[index]?.selected || false));
            });
        }

        function choose(index) {
            const option = options[index];
            if (!option || option.disabled || select.disabled) return;
            const changed = select.value !== option.value;
            select.value = option.value;
            close();
            if (changed) {
                select.dispatchEvent(new Event("input", { bubbles: true }));
                select.dispatchEvent(new Event("change", { bubbles: true }));
            }
            sync();
            trigger.focus();
        }

        function rebuild() {
            close();
            options = Array.from(select.options).filter(option => !option.hidden);
            menu.replaceChildren(...options.map((option, index) => {
                const item = document.createElement("li");
                item.id = `${id}-option-${index}`;
                item.setAttribute("role", "option");
                item.setAttribute("aria-disabled", String(option.disabled));
                item.textContent = option.textContent.trim();
                item.addEventListener("pointerdown", event => event.preventDefault());
                item.addEventListener("pointermove", () => { if (!option.disabled) activate(index); });
                item.addEventListener("click", () => choose(index));
                return item;
            }));
            sync();
        }

        function open() {
            if (select.disabled) return;
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
            shell.classList.add("is-open");
            trigger.setAttribute("aria-expanded", "true");
            const selected = options.findIndex(option => option.selected && !option.disabled);
            activate(selected >= 0 ? selected : options.findIndex(option => !option.disabled));
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
                const enabled = options.map((option, i) => option.disabled ? -1 : i).filter(i => i >= 0);
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
            const index = options.findIndex(option => !option.disabled &&
                option.textContent.trim().toLocaleLowerCase("pt-BR").startsWith(search.toLocaleLowerCase("pt-BR")));
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
            attributeFilter: ["disabled", "required", "hidden", "selected", "label", "value"],
        });
        controls.push({ shell, close, sync });
    });

    document.addEventListener("pointerdown", event => {
        controls.forEach(control => { if (!control.shell.contains(event.target)) control.close(); });
    });
    // Page controllers may change other fields synchronously in response to a change.
    document.addEventListener("change", () => queueMicrotask(() => controls.forEach(control => control.sync())));
    document.addEventListener("click", () => queueMicrotask(() => controls.forEach(control => control.sync())));
    window.addEventListener("resize", () => controls.forEach(control => control.close()));
})();
