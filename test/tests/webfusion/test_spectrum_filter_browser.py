"""Regressões da navegação dos filtros em um navegador Chromium real."""

from pathlib import Path
import json
import unittest
from urllib.parse import parse_qs, urlparse

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


ROOT = Path(__file__).resolve().parents[3]
PAGE = """<!doctype html><html lang="pt-BR"><body>
<div id="spectrum-page-root" data-lightweight-bootstrap="0"
 data-filters-endpoint="/api/spectrum/filters"></div>
<form id="spectrum-query-form">
<div id="spectrum-site-context">Palmas/TO
<button type="button" id="remove-spectrum-site" hidden>Remover site</button></div>
<select id="state_id" name="state_id"><option value="">Todos</option>
<option value="1" selected>TO</option></select>
<select id="district_id" name="district_id"><option value="">Todos</option>
<option value="10" selected>Palmas</option></select>
<select id="equipment_id" name="equipment_id"><option value="">Todas</option>
<option value="A" selected>Estação A</option></select>
<input name="site_id" type="hidden" value="42">
<input name="start_date" type="date" value="2025-12-12">
<input name="end_date" type="date" value="2026-10-20">
<input name="freq_start" type="number"><input name="freq_end" type="number">
<input name="description" type="text">
<div id="district-filter-hint"></div><div id="period-filter-hint"></div>
<p id="spectrum-filter-status" role="status"></p>
<button type="button" id="retry-spectrum-filters" hidden>Tentar novamente</button>
<button type="submit">Pesquisar</button></form></body></html>"""
STATES = [
    {"ID_STATE": 1, "OPTION_LABEL": "TO"},
    {"ID_STATE": 2, "OPTION_LABEL": "SP"},
]
DISTRICTS = {
    "1": [{"ID_DISTRICT": 10, "OPTION_LABEL": "Palmas"},
          {"ID_DISTRICT": 11, "OPTION_LABEL": "Araguaína"}],
    "2": [{"ID_DISTRICT": 20, "OPTION_LABEL": "São Paulo"}],
}
EQUIPMENTS = {
    "1": [{"ID_EQUIPMENT": "A", "OPTION_LABEL": "Estação A"},
          {"ID_EQUIPMENT": "B", "OPTION_LABEL": "Estação B"}],
    "2": [{"ID_EQUIPMENT": "C", "OPTION_LABEL": "Estação C"}],
}


@unittest.skipIf(sync_playwright is None, "Playwright necessário para os testes de navegador")
class SpectrumFilterBrowserTests(unittest.TestCase):
    """Exercita o controlador real com respostas determinísticas da API."""

    def setUp(self) -> None:
        self.runtime = sync_playwright().start()
        self.browser = self.runtime.chromium.launch(headless=True, args=["--no-sandbox"])
        self.page = self.browser.new_page()
        self.requests = []
        self.errors = []
        self.page.on("pageerror", lambda error: self.errors.append(str(error)))
        self.page.route("**/api/spectrum/filters?*", self.respond)
        self.page.route("**/spectrum", lambda route: route.fulfill(content_type="text/html", body=PAGE))
        self.page.goto("http://spectrum.test/spectrum")
        self.page.add_script_tag(path=str(ROOT / "src/webfusion/static/js/spectrum_page.js"))
        self.wait_ready()

    def tearDown(self) -> None:
        self.browser.close()
        self.runtime.stop()
        self.assertEqual(self.errors, [])

    def respond(self, route) -> None:
        params = {key: values[0] for key, values in parse_qs(urlparse(route.request.url).query).items()}
        self.requests.append(params)
        state = params.get("state_id")
        site = params.get("site_id")
        districts = DISTRICTS.get(state, [])
        equipment = EQUIPMENTS.get(state, EQUIPMENTS["1"] + EQUIPMENTS["2"])
        if params.get("district_id") == "11":
            equipment = EQUIPMENTS["1"][1:]
        if site and params.get("view") != "selection":
            districts = DISTRICTS["1"][:1]
            equipment = EQUIPMENTS["1"][:1]
        availability = None if params.get("bootstrap") == "1" else {
            "DATE_START": "2025-01-01", "DATE_END": "2025-12-12",
            "DATE_START_DISPLAY": "01/01/2025", "DATE_END_DISPLAY": "12/12/2025",
        }
        if availability and state == "2":
            availability.update(DATE_START="2024-02-01", DATE_END="2024-10-10")
        if availability and params.get("equipment_id") == "B":
            availability.update(DATE_START="2017-09-05", DATE_END="2017-09-05",
                                DATE_START_DISPLAY="05/09/2017 01:00:26", DATE_END_DISPLAY="05/09/2017 01:32:23")
        if availability and params.get("freq_start") == "100":
            availability.update(DATE_START="2023-02-01", DATE_END="2023-10-10")
        if params.get("description") == "Plano B":
            equipment = EQUIPMENTS["1"][1:]
        route.fulfill(content_type="application/json", body=json.dumps({
            "states": STATES if params.get("view") == "selection" else STATES[:1] if site or state == "1" else STATES,
            "districts": districts, "equipments": equipment,
            "availability": availability,
        }))

    def wait_ready(self) -> None:
        self.page.wait_for_function("""() => !document.querySelector('.is-refreshing')
            && !document.getElementById('period-filter-hint').textContent.includes('Atualizando')
            && document.querySelector('#state_id option[value="2"]')""")

    def assert_observed_dates(self) -> None:
        expected = ("2024-02-01", "2024-10-10") if self.page.locator("#state_id").input_value() == "2" else ("2025-01-01", "2025-12-12")
        if self.page.locator("#equipment_id").input_value() == "B":
            expected = ("2017-09-05", "2017-09-05")
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), expected[0])
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), expected[1])
        self.assertIsNone(self.page.locator('[name="end_date"]').get_attribute("max"))

    def test_map_site_does_not_trap_state_or_district_catalogs(self) -> None:
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "42")
        self.assertEqual(self.page.locator('#district_id option[value="11"]').count(), 1)
        self.page.select_option("#state_id", "2")
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "")
        self.assertEqual(self.page.locator("#district_id").input_value(), "")
        self.assertEqual(self.page.locator("#equipment_id").input_value(), "")
        self.assertFalse(self.page.locator("#spectrum-site-context").is_visible())
        self.wait_ready()
        self.assertEqual(self.page.locator('#district_id option[value="20"]').count(), 1)
        self.assertEqual(self.page.locator('#equipment_id option[value="C"]').count(), 1)
        self.assert_observed_dates()

    def test_only_geography_changes_fetch_one_complete_snapshot(self) -> None:
        count = len(self.requests)
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertEqual(len(self.requests) - count, 1)
        self.assertEqual(self.requests[-1]["view"], "selection")
        count = len(self.requests)
        self.page.locator('[name="freq_start"]').fill("100")
        self.page.locator('[name="freq_end"]').fill("200")
        self.page.locator('[name="description"]').fill("Plano B")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests) - count, 0)
        count = len(self.requests)
        self.page.select_option("#state_id", "1")
        self.wait_ready()
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertEqual(len(self.requests) - count, 1)

    def test_search_waits_for_the_complete_current_snapshot(self) -> None:
        self.open_with_automatic_period()
        self.page.evaluate("""() => {
            const original = window.fetch.bind(window);
            window.fetch = async (...args) => {
                const response = await original(...args);
                await new Promise(resolve => setTimeout(resolve, 250));
                return response;
            };
        }""")
        self.page.select_option("#state_id", "2")
        self.assertTrue(self.page.locator('button[type="submit"]').is_disabled())
        self.wait_ready()
        self.assertFalse(self.page.locator('button[type="submit"]').is_disabled())
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2024-02-01")
        self.assertEqual(self.page.locator('#district_id option[value="20"]').count(), 1)

    def test_all_states_releases_site_and_dependents(self) -> None:
        count = len(self.requests)
        self.page.select_option("#state_id", "")
        self.wait_ready()
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "")
        self.assertEqual(self.page.locator("#district_id").input_value(), "")
        self.assertEqual(self.page.locator('#equipment_id option[value="C"]').count(), 1)
        self.assertTrue(all("site_id" not in params for params in self.requests[count:]))
        self.assert_observed_dates()

    def test_district_change_releases_site_and_equipment(self) -> None:
        self.page.select_option("#district_id", "11")
        self.wait_ready()
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "")
        self.assertEqual(self.page.locator("#state_id").input_value(), "1")
        self.assertEqual(self.page.locator("#equipment_id").input_value(), "")
        self.assertEqual(self.page.locator('#equipment_id option[value="B"]').count(), 1)
        self.assertEqual(self.page.locator('#equipment_id option[value="A"]').count(), 0)
        self.assert_observed_dates()

    def test_all_districts_and_equipment_release_state_inherited_from_map(self) -> None:
        count = len(self.requests)
        self.page.select_option("#district_id", "")
        self.page.select_option("#equipment_id", "")
        self.wait_ready()
        self.assertEqual(self.page.locator("#state_id").input_value(), "")
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "")
        self.assertEqual(self.page.locator('#equipment_id option[value="C"]').count(), 1)
        self.assertNotIn("state_id", self.requests[-1])
        self.assert_observed_dates()

    def test_all_districts_preserves_state_chosen_manually(self) -> None:
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.page.select_option("#district_id", "20")
        self.wait_ready()
        self.page.select_option("#district_id", "")
        self.wait_ready()
        self.assertEqual(self.page.locator("#state_id").input_value(), "2")
        self.assert_observed_dates()

    def test_equipment_change_releases_site_without_erasing_geography(self) -> None:
        self.page.evaluate("window.stateOption = document.querySelector('#state_id option[value=\"2\"]')")
        self.page.select_option("#equipment_id", "B")
        self.wait_ready()
        self.assertTrue(self.page.evaluate("window.stateOption === document.querySelector('#state_id option[value=\"2\"]')"))
        self.assertEqual(self.page.locator('[name="site_id"]').input_value(), "")
        self.assertEqual(self.page.locator("#state_id").input_value(), "1")
        self.assertEqual(self.page.locator("#district_id").input_value(), "10")
        self.assertEqual(self.page.locator("#equipment_id").input_value(), "B")
        self.assert_observed_dates()

    def test_rapid_state_changes_and_cached_return(self) -> None:
        self.page.select_option("#state_id", "2")
        self.page.wait_for_timeout(480)
        self.page.select_option("#state_id", "1")
        self.wait_ready()
        self.assertEqual(self.page.locator("#state_id").input_value(), "1")
        self.assertEqual(self.page.locator('#district_id option[value="11"]').count(), 1)
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.page.select_option("#state_id", "1")
        self.wait_ready()
        self.assertEqual(self.page.locator("#state_id").input_value(), "1")
        self.assert_observed_dates()

    def test_delayed_response_cannot_restore_previous_state(self) -> None:
        self.page.evaluate("""() => {
            const original = window.fetch.bind(window);
            window.fetch = async (url, options) => {
                const params = new URL(url, location.href).searchParams;
                if (params.get('state_id') === '2') {
                    const response = await original(url);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    return response;
                }
                return original(url, options);
            };
        }""")
        self.page.select_option("#state_id", "2")
        self.page.wait_for_timeout(550)
        self.page.select_option("#state_id", "1")
        self.wait_ready()
        self.page.wait_for_timeout(1100)
        self.assertEqual(self.page.locator("#state_id").input_value(), "1")
        self.assertEqual(self.page.locator('#district_id option[value="20"]').count(), 0)
        self.assert_observed_dates()

    def test_failed_geography_refresh_does_not_leave_previous_dates(self) -> None:
        def unavailable(route):
            route.fulfill(status=503, content_type="application/json", body="{}")

        self.page.route("**/api/spectrum/filters?*", unavailable)
        self.page.select_option("#state_id", "2")
        self.page.locator("#retry-spectrum-filters").wait_for(state="visible")
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "")
        self.page.unroute("**/api/spectrum/filters?*", unavailable)
        self.page.click("#retry-spectrum-filters")
        self.wait_ready()
        self.assertFalse(self.page.locator("#retry-spectrum-filters").is_visible())
        self.assertEqual(self.page.locator('#equipment_id option[value="C"]').count(), 1)
        self.assert_observed_dates()

    def test_equipment_change_replaces_old_period_without_an_extra_button(self) -> None:
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2025-12-12")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2026-10-20")
        self.page.select_option("#equipment_id", "B")
        self.wait_ready()
        self.assert_observed_dates()
        self.assertNotIn("fora da faixa", self.page.locator("#period-filter-hint").inner_text())
        self.assertEqual(self.page.locator("#use-observed-period").count(), 0)

    def open_with_automatic_period(self) -> None:
        self.page.goto("http://spectrum.test/spectrum")
        self.page.locator('[name="start_date"]').fill("")
        self.page.locator('[name="end_date"]').fill("")
        self.page.add_script_tag(path=str(ROOT / "src/webfusion/static/js/spectrum_page.js"))
        self.wait_ready()

    def test_automatic_dates_follow_geography_without_filtering_themselves(self) -> None:
        self.open_with_automatic_period()
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2025-01-01")
        count = len(self.requests)
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertFalse(self.page.locator('button[type="submit"]').is_disabled())
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2024-02-01")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2024-10-10")
        self.assertTrue(all("start_date" not in params for params in self.requests[count:]))

    def test_manual_date_edits_stay_local_until_a_new_geography_is_selected(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        district_options = self.page.locator("#district_id").inner_html()
        equipment_options = self.page.locator("#equipment_id").inner_html()
        self.page.locator('[name="start_date"]').fill("2024-03-01")
        self.page.locator('[name="end_date"]').fill("2024-04-01")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assertEqual(self.page.locator("#district_id").inner_html(), district_options)
        self.assertEqual(self.page.locator("#equipment_id").inner_html(), equipment_options)
        self.assertFalse(self.page.locator('button[type="submit"]').is_disabled())
        self.assertNotIn("Atualizando", self.page.locator("#period-filter-hint").inner_text())
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertEqual(len(self.requests), count + 1)
        self.assertNotIn("start_date", self.requests[-1])
        self.assertNotIn("end_date", self.requests[-1])
        self.assert_observed_dates()

    def test_date_validation_is_local(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        self.page.locator('[name="start_date"]').fill("2025-12-13")
        self.assertFalse(self.page.locator('[name="end_date"]').evaluate("field => field.checkValidity()"))
        self.page.locator('[name="end_date"]').fill("2025-12-14")
        self.assertTrue(self.page.locator('[name="end_date"]').evaluate("field => field.checkValidity()"))
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2025-12-13")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2025-12-14")

    def test_clearing_dates_does_not_refill_them_or_fetch_filters(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        self.page.locator('[name="start_date"]').fill("")
        self.page.locator('[name="end_date"]').fill("")
        self.page.locator('[name="freq_start"]').fill("70")
        self.page.locator('[name="freq_end"]').fill("110")
        self.page.locator('[name="description"]').focus()
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "")
        self.assertFalse(self.page.locator('button[type="submit"]').is_disabled())

    def test_empty_availability_clears_old_period_and_its_validation_error(self) -> None:
        def no_observations(route):
            route.fulfill(content_type="application/json", body=json.dumps({
                "states": STATES, "districts": DISTRICTS["2"],
                "equipments": EQUIPMENTS["2"], "availability": None,
            }))

        self.page.locator('[name="start_date"]').fill("2027-01-01")
        self.assertFalse(self.page.locator('[name="end_date"]').evaluate("field => field.checkValidity()"))
        self.page.route("**/api/spectrum/filters?*", no_observations)
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "")
        self.assertTrue(self.page.locator('[name="end_date"]').evaluate("field => field.checkValidity()"))
        self.assertIn("Nenhum período observado", self.page.locator("#period-filter-hint").inner_text())

    def test_one_day_period_is_submitted_without_refreshing_filters(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        self.page.locator('[name="start_date"]').fill("2025-12-11")
        self.page.locator('[name="end_date"]').fill("2025-12-12")
        self.page.locator('[name="freq_start"]').fill("70")
        self.page.locator('[name="freq_end"]').fill("110")
        self.page.locator('[name="description"]').fill("Plano B")
        self.page.evaluate("""() => {
            document.querySelector('#spectrum-query-form').addEventListener('submit', event => {
                window.submitted = {
                    blocked: event.defaultPrevented,
                    fields: Object.fromEntries(new FormData(event.target)),
                };
                event.preventDefault();
            });
        }""")
        self.page.click('button[type="submit"]')
        submitted = self.page.evaluate("window.submitted")
        self.assertFalse(submitted["blocked"])
        self.assertEqual(submitted["fields"]["start_date"], "2025-12-11")
        self.assertEqual(submitted["fields"]["end_date"], "2025-12-12")
        self.assertEqual(submitted["fields"]["freq_start"], "70")
        self.assertEqual(submitted["fields"]["freq_end"], "110")
        self.assertEqual(submitted["fields"]["description"], "Plano B")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)

    def test_date_edit_during_geography_refresh_preserves_the_pending_context(self) -> None:
        self.open_with_automatic_period()
        self.page.evaluate("""() => {
            const original = window.fetch.bind(window);
            window.fetch = async (...args) => {
                const response = await original(...args);
                await new Promise(resolve => setTimeout(resolve, 700));
                return response;
            };
        }""")
        count = len(self.requests)
        self.page.select_option("#state_id", "2")
        self.page.locator('[name="start_date"]').fill("2024-10-09")
        self.page.locator('[name="end_date"]').fill("2024-10-10")
        self.wait_ready()
        self.assertEqual(len(self.requests), count + 1)
        self.assertEqual(self.page.locator('#district_id option[value="20"]').count(), 1)
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2024-10-09")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2024-10-10")

    def test_frequency_and_description_wait_for_search(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        equipment_options = self.page.locator("#equipment_id").inner_html()
        self.page.locator('[name="freq_start"]').fill("100")
        self.page.locator('[name="freq_end"]').fill("200")
        self.page.locator('[name="description"]').fill("Plano B")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assert_observed_dates()
        self.assertEqual(self.page.locator("#equipment_id").inner_html(), equipment_options)
        self.page.select_option("#state_id", "2")
        self.wait_ready()
        self.assertEqual(len(self.requests), count + 1)
        for key in ("freq_start", "freq_end", "description"):
            self.assertNotIn(key, self.requests[-1])
        self.assertEqual(self.page.locator('[name="freq_start"]').input_value(), "100")
        self.assertEqual(self.page.locator('[name="freq_end"]').input_value(), "200")
        self.assertEqual(self.page.locator('[name="description"]').input_value(), "Plano B")

    def test_clearing_map_geography_updates_global_automatic_period(self) -> None:
        self.open_with_automatic_period()
        self.page.select_option("#district_id", "")
        self.wait_ready()
        self.assertEqual(self.page.locator("#state_id").input_value(), "")
        self.assertEqual(self.page.locator('[name="start_date"]').input_value(), "2025-01-01")
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2025-12-12")
        self.assertTrue(any(params == {"bootstrap": "0", "view": "selection"} for params in self.requests))

    def test_inverted_frequency_does_not_erase_automatic_dates(self) -> None:
        self.open_with_automatic_period()
        count = len(self.requests)
        self.page.locator('[name="freq_start"]').fill("200")
        self.page.locator('[name="freq_end"]').fill("100")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assertEqual(self.page.locator('[name="end_date"]').input_value(), "2025-12-12")
        self.assertFalse(self.page.locator('[name="freq_end"]').evaluate("field => field.checkValidity()"))
        self.page.locator('[name="freq_end"]').fill("300")
        self.page.wait_for_timeout(600)
        self.assertEqual(len(self.requests), count)
        self.assertTrue(self.page.locator('[name="freq_end"]').evaluate("field => field.checkValidity()"))


if __name__ == "__main__":
    unittest.main()
