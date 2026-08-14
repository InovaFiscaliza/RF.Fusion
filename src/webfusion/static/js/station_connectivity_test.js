(() => {
    "use strict";

    const triggers = Array.from(document.querySelectorAll("[data-station-connectivity-test]"));

    const webfusionUrl = typeof window.webfusionUrl === "function"
        ? window.webfusionUrl
        : (pathname) => pathname;
    let activeTask = null;
    let pollTimer = null;

    const dialog = document.createElement("dialog");
    dialog.className = "station-connectivity-test-dialog";
    dialog.setAttribute("aria-labelledby", "station-connectivity-test-title");
    dialog.innerHTML = `
        <div class="station-connectivity-test-card">
            <div class="station-connectivity-test-head">
                <div>
                    <p class="station-connectivity-test-eyebrow">Teste de estação</p>
                    <h2 id="station-connectivity-test-title" class="station-connectivity-test-title">Verificando conectividade</h2>
                </div>
                <button type="button" class="station-connectivity-test-close" aria-label="Fechar teste de estação">
                    <i class="fa fa-times"></i>
                </button>
            </div>
            <div class="station-connectivity-test-status" aria-live="polite">
                <span class="station-connectivity-test-status-label">Aguardando</span>
                <span class="station-connectivity-test-status-message">Preparando o teste.</span>
            </div>
            <ol class="station-connectivity-test-steps">
                <li class="station-connectivity-test-step" data-test-step="queue"><i class="fa fa-clock-o station-connectivity-test-step-icon"></i><div><strong>Fila prioritária</strong><span>Aguardando o worker de conectividade.</span></div></li>
                <li class="station-connectivity-test-step" data-test-step="icmp"><i class="fa fa-exchange station-connectivity-test-step-icon"></i><div><strong>ICMP</strong><span>Confirmando resposta da estação.</span></div></li>
                <li class="station-connectivity-test-step" data-test-step="ssh"><i class="fa fa-terminal station-connectivity-test-step-icon"></i><div><strong>SSH</strong><span>Validando acesso configurado.</span></div></li>
                <li class="station-connectivity-test-step" data-test-step="persist"><i class="fa fa-refresh station-connectivity-test-step-icon"></i><div><strong>Atualização</strong><span>Registrando o estado operacional.</span></div></li>
            </ol>
            <p class="station-connectivity-test-footnote">Fechar esta janela não cancela a tarefa. O resultado permanece registrado no histórico operacional.</p>
        </div>
    `;
    document.body.appendChild(dialog);

    const title = dialog.querySelector(".station-connectivity-test-title");
    const statusLabel = dialog.querySelector(".station-connectivity-test-status-label");
    const statusMessage = dialog.querySelector(".station-connectivity-test-status-message");
    const closeButton = dialog.querySelector(".station-connectivity-test-close");
    const stepOrder = ["queue", "icmp", "ssh", "persist"];

    const clearPolling = () => {
        if (pollTimer !== null) {
            window.clearTimeout(pollTimer);
            pollTimer = null;
        }
    };

    const closeDialog = () => {
        clearPolling();
        if (dialog.open) {
            dialog.close();
        }
    };

    const setStepStates = (states) => {
        dialog.querySelectorAll("[data-test-step]").forEach((element) => {
            const state = states[element.dataset.testStep] || "pending";
            element.classList.toggle("is-complete", state === "complete");
            element.classList.toggle("is-active", state === "active");
            element.classList.toggle("is-failed", state === "failed");
        });
    };

    const stageFromPayload = (payload) => {
        if (stepOrder.includes(payload.stage)) {
            return payload.stage;
        }
        const message = String(payload.message || "").toLowerCase();
        if (message.startsWith("icmp:")) {
            return "icmp";
        }
        if (message.startsWith("ssh:")) {
            return "ssh";
        }
        if (message.startsWith("atualizando")) {
            return "persist";
        }
        if (payload.is_terminal) {
            return "persist";
        }
        return "queue";
    };

    const failureStageFromPayload = (payload) => {
        const message = String(payload.message || "").toLowerCase();
        if (message.includes("ssh") || message.includes("autenticação")) {
            return "ssh";
        }
        if (message.includes("icmp") || message.includes("unreachable")) {
            return "icmp";
        }
        return stageFromPayload(payload);
    };

    const buildStepStates = (payload) => {
        const states = Object.fromEntries(stepOrder.map((step) => [step, "pending"]));
        const terminal = Boolean(payload.is_terminal);
        const failed = Number(payload.status) < 0;
        const currentStep = terminal && failed
            ? failureStageFromPayload(payload)
            : stageFromPayload(payload);
        const currentIndex = stepOrder.indexOf(currentStep);

        stepOrder.slice(0, currentIndex).forEach((step) => {
            states[step] = "complete";
        });

        if (terminal && !failed) {
            stepOrder.forEach((step) => {
                states[step] = "complete";
            });
            return states;
        }

        states[currentStep] = failed ? "failed" : "active";

        if (terminal && currentStep !== "persist") {
            states.persist = "complete";
        }

        return states;
    };

    const renderPayload = (payload) => {
        const terminal = Boolean(payload.is_terminal);
        title.textContent = terminal
            ? `${payload.host_name || "Estação"}: ${payload.status_label || "Concluído"}`
            : `Testando ${payload.host_name || "estação"}`;
        statusLabel.textContent = payload.status_label || "Em execução";
        statusMessage.textContent = payload.message || "Aguardando atualização do teste.";
        setStepStates(buildStepStates(payload));
    };

    const readJson = async (response) => {
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
            throw new Error(payload.error || "Não foi possível acompanhar o teste da estação.");
        }
        return payload;
    };

    const pollTask = async () => {
        if (!activeTask) {
            return;
        }

        try {
            const response = await fetch(
                webfusionUrl(`/api/host/${activeTask.hostId}/connectivity-test/${activeTask.taskId}`),
                { credentials: "same-origin", headers: { Accept: "application/json" } },
            );
            const payload = await readJson(response);
            renderPayload(payload);
            if (!payload.is_terminal && dialog.open) {
                pollTimer = window.setTimeout(pollTask, 1000);
            }
        } catch (error) {
            statusLabel.textContent = "Acompanhamento indisponível";
            statusMessage.textContent = error.message || "Não foi possível acompanhar o teste da estação.";
            setStepStates({ queue: "failed" });
        }
    };

    const startTest = async ({ hostId, hostName, trigger = null }) => {
        hostId = Number(hostId || 0);
        if (!hostId) {
            return;
        }

        clearPolling();
        activeTask = { hostId, taskId: null };
        title.textContent = `Testando ${hostName || "estação"}`;
        statusLabel.textContent = "Solicitando teste";
        statusMessage.textContent = "A tarefa será enviada para a fila prioritária de conectividade.";
        setStepStates({ queue: "active" });
        if (trigger) {
            trigger.disabled = true;
        }
        dialog.showModal();

        try {
            const response = await fetch(
                webfusionUrl(`/api/host/${hostId}/connectivity-test`),
                {
                    method: "POST",
                    credentials: "same-origin",
                    headers: { Accept: "application/json" },
                },
            );
            const payload = await readJson(response);
            activeTask.taskId = payload.task_id;
            renderPayload(payload);
            if (!payload.is_terminal) {
                pollTimer = window.setTimeout(pollTask, 500);
            }
        } catch (error) {
            statusLabel.textContent = "Teste não iniciado";
            statusMessage.textContent = error.message || "Não foi possível iniciar o teste da estação.";
            setStepStates({ queue: "failed" });
        } finally {
            if (trigger) {
                trigger.disabled = false;
            }
        }
    };

    window.startStationConnectivityTest = (hostId, hostName) => startTest({ hostId, hostName });

    closeButton.addEventListener("click", closeDialog);
    dialog.addEventListener("cancel", (event) => {
        event.preventDefault();
        closeDialog();
    });
    dialog.addEventListener("close", clearPolling);

    triggers.forEach((trigger) => {
        trigger.addEventListener("click", () => startTest({
            hostId: trigger.dataset.hostId,
            hostName: trigger.dataset.hostName,
            trigger,
        }));
    });
})();
