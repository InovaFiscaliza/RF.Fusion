/* Task list page controller
 *
 * The recent-task page only needs one small interaction today: dismissing the
 * post-creation summary overlay so the operator can inspect the list without
 * reloading the page. Keeping it here avoids inline handlers in the template
 * and keeps the page structure aligned with the other WebFusion modules.
 *
 * Even though this file is intentionally tiny, it still follows the same
 * pattern as the larger page controllers:
 * - guard against missing DOM,
 * - keep behavior out of the template,
 * - isolate the page-specific interaction in one obvious place.
 */
(function () {
    const root          = document.getElementById("task-list-page-root");
    const backdrop      = document.getElementById("task-summary-backdrop");
    const closeButton   = document.getElementById("task-summary-close-button");

    /* The creation-summary overlay only exists when the user has just come
     * from the task builder. On a plain visit to `/task/list`, this script
     * should quietly do nothing rather than treating the missing nodes as an
     * error state.
     */
    if (!root || !backdrop || !closeButton) {
        return;
    }

    /* Dismissing the summary is modeled as DOM removal instead of a CSS hide
     * because the overlay is a transient, one-shot affordance. Once closed, it
     * should disappear from the interaction tree entirely and stop competing
     * with the task list underneath.
     */
    function closeTaskSummary() {
        backdrop.remove();
    }

    /* The page currently has only one active control, so explicit direct
     * wiring stays clearer than introducing a generic event-dispatch layer.
     */
    closeButton.addEventListener("click", closeTaskSummary);
})();
