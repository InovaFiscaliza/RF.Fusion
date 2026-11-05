"""Regressões dos menus compartilhados com os controladores reais das páginas."""

from pathlib import Path
import unittest

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


STATIC = Path(__file__).resolve().parents[3] / "src/webfusion/static"
PAGE = """<!doctype html><html lang="pt-BR"><meta charset="utf-8"><body>
<div id="host-page-root" class="wf-selector-page">
<form id="query"><div class="wf-selector-fields">
<div class="wf-selector-field wf-selector-field--type">
<label for="host-query-type">Tipo</label><div class="select-shell">
<select id="host-query-type" name="host_type" class="form-control">
<option value="">Todos</option><option value="celplan" data-host-prefix="CWSM">Celplan</option>
<option value="rfeye" data-host-prefix="RFEYE">RFEye</option></select></div></div>
<div class="wf-selector-field"><label for="station">Estação</label><div class="select-shell">
<select id="station" name="host_id" required class="form-control">
<option value="">Selecione uma estação</option><option value="1">CWSM211001</option>
<option value="2">RFEYE002</option><option value="3" disabled>Indisponível</option>
</select></div></div></div><button type="submit">Consultar</button></form></div>
<form id="configuration" class="wf-selector-page"><label for="target-kind">Tipo</label>
<div class="select-shell"><select id="target-kind" name="target_kind" class="form-control">
<option value="host">Estação</option><option value="template">Template</option></select></div>
<label for="target-id">Estação ou template</label><div class="select-shell">
<select id="target-id" name="target_id" required class="form-control">
<option value="">Selecione</option><option value="10" data-target-kind="host">CWSM10</option>
<option value="20" data-target-kind="template">Template com descrição longa</option>
</select></div></form>
<div id="task-builder-root"><form id="task"><label>Modo</label><div class="select-shell">
<select name="mode" class="form-control"><option value="ALL">Todos</option>
<option value="RANGE">Faixa por período</option><option value="FILE" hidden>Arquivo</option>
</select></div><select multiple name="collective_hosts"><option value="1">CWSM1</option></select>
</form></div><div class="station-map-select-wrap"><select id="map"><option>Todos</option></select></div>
</body></html>"""


@unittest.skipIf(sync_playwright is None, "Playwright necessário para testes de navegador")
class SharedSelectBrowserTests(unittest.TestCase):
    """Valida seleção, filtros e geometria sem rede, banco ou tarefas reais."""

    def setUp(self) -> None:
        """Cria uma página isolada e carrega os assets reais. Returns: None."""
        self.runtime = sync_playwright().start()
        self.addCleanup(self.runtime.stop)
        self.browser = self.runtime.chromium.launch(headless=True, args=["--no-sandbox"])
        self.addCleanup(self.browser.close)
        self.page = self.browser.new_page(viewport={"width": 1920, "height": 1080})
        self.page.set_default_timeout(5000)
        self.errors = []
        self.page.on("pageerror", lambda error: self.errors.append(str(error)))
        self.page.route("**/*", lambda route: route.fulfill(content_type="text/html", body=PAGE))
        self.page.goto("http://select.test/host")
        for name in ["AnatelReactiveTheme.AnatelReactiveTheme.css", "webfusion_base.css", "wf_select_20261105.css"]:
            self.page.add_style_tag(path=str(STATIC / "css" / name))
        for name in ["host_page.js", "configuration_page.js", "wf_select_20261105.js"]:
            self.page.add_script_tag(path=str(STATIC / "js" / name))

    def tearDown(self) -> None:
        """Encerra o navegador e exige ausência de erros JavaScript. Returns: None."""
        self.assertEqual(self.errors, [])

    def test_station_filter_rebuilds_and_preserves_form_events(self) -> None:
        """Filtrar tipos mantém valores e dispara os eventos do campo. Returns: None."""
        self.page.evaluate("""() => {
            window.changes = [];
            document.querySelector('#station').addEventListener('change', event => changes.push(event.target.value));
        }""")
        self.page.locator("#host-query-type-trigger").click()
        self.page.locator("#host-query-type-options").get_by_role("option", name="Celplan", exact=True).click()
        self.page.locator("#station-trigger").click()
        self.assertEqual(self.page.locator("#station-options [role=option]").count(), 2)
        self.page.locator("#station-options").get_by_role("option", name="CWSM211001").click()
        self.assertEqual(self.page.locator("#station").input_value(), "1")
        self.assertEqual(self.page.evaluate("changes"), ["1"])
        self.assertEqual(self.page.evaluate("new FormData(document.querySelector('#query')).get('host_id')"), "1")

    def test_configuration_catalog_follows_kind(self) -> None:
        """Trocar estação por template reconstrói a lista visível. Returns: None."""
        self.page.locator("#target-kind-trigger").click()
        self.page.locator("#target-kind-options").get_by_role("option", name="Template", exact=True).click()
        self.page.locator("#target-id-trigger").click()
        self.assertEqual(self.page.locator("#target-id-options [role=option]").all_text_contents(),
                         ["Selecione uma opção", "Template com descrição longa"])
        self.page.keyboard.press("End")
        self.page.keyboard.press("Enter")
        self.assertEqual(self.page.locator("#target-id").input_value(), "20")

    def test_required_field_focus_and_reset(self) -> None:
        """Obrigatoriedade e reset continuam funcionais no formulário. Returns: None."""
        self.assertFalse(self.page.evaluate("document.querySelector('#query').reportValidity()"))
        self.assertEqual(self.page.evaluate("document.activeElement.id"), "station-trigger")
        self.assertTrue(self.page.locator("#station-error").is_visible())
        self.page.keyboard.type("cwsm")
        self.page.keyboard.press("Enter")
        self.assertEqual(self.page.locator("#station").input_value(), "1")
        self.assertFalse(self.page.locator("#station-error").is_visible())
        self.page.evaluate("document.querySelector('#query').reset()")
        self.page.wait_for_function("document.querySelector('#station-trigger').textContent === 'Selecione uma estação'")

    def test_task_disabled_and_hidden_options_follow_model(self) -> None:
        """Estados dinâmicos respeitam campos e opções desabilitados. Returns: None."""
        self.page.evaluate("""() => {
            const select = document.querySelector('[name=mode]');
            select.value = 'RANGE'; select.disabled = true;
        }""")
        trigger = self.page.locator("#task .wf-select-trigger")
        self.page.wait_for_function("document.querySelector('#task .wf-select-trigger').disabled")
        self.assertEqual(trigger.inner_text(), "Faixa por período")
        self.page.evaluate("document.querySelector('[name=mode]').disabled = false")
        trigger.click()
        self.assertEqual(self.page.locator("#task [role=option]").count(), 2)
        self.page.keyboard.press("Escape")
        self.assertEqual(trigger.get_attribute("aria-expanded"), "false")
        self.assertEqual(self.page.locator("select[multiple]").get_attribute("aria-hidden"), None)
        self.assertEqual(self.page.locator("#map").get_attribute("aria-hidden"), None)

    def test_menu_alignment_at_desktop_and_mobile_widths(self) -> None:
        """Menus seguem a largura do campo com texto longo e zoom. Returns: None."""
        for width, zoom in [(1920, 1), (1366, 1), (800, 1), (390, 1), (1920, 1.25)]:
            with self.subTest(width=width, zoom=zoom):
                self.page.set_viewport_size({"width": width, "height": 1080})
                self.page.evaluate("value => document.body.style.zoom = value", zoom)
                self.page.locator("#station-trigger").click()
                trigger = self.page.locator("#station-trigger").bounding_box()
                menu = self.page.locator("#station-options").bounding_box()
                self.assertAlmostEqual(trigger["x"], menu["x"], delta=2)
                self.assertAlmostEqual(trigger["width"], menu["width"], delta=2)
                self.assertGreaterEqual(menu["y"], 0)
                self.assertLessEqual(menu["y"] + menu["height"], 1080)
                self.page.keyboard.press("Escape")

    def test_keyboard_skips_disabled_and_outside_click_closes(self) -> None:
        """Teclado ignora opções desabilitadas e clique externo fecha. Returns: None."""
        self.page.locator("#station-trigger").click()
        self.page.keyboard.press("End")
        self.page.keyboard.press("Enter")
        self.assertEqual(self.page.locator("#station").input_value(), "2")
        self.page.locator("#station-trigger").click()
        self.page.mouse.click(1900, 1000)
        self.assertFalse(self.page.locator("#station-options").is_visible())
        self.page.locator("#target-kind-trigger").click()
        self.page.keyboard.press("Tab")
        self.assertFalse(self.page.locator("#target-kind-options").is_visible())

    def test_long_catalog_opens_upwards_with_scroll(self) -> None:
        """Catálogos grandes cabem na janela e nomes longos quebram linha. Returns: None."""
        self.page.evaluate("""() => {
            const select = document.querySelector('#station');
            for (let i = 4; i < 304; i++) select.add(new Option('Estação ' + i + ' nome '.repeat(35), String(i)));
            document.querySelector('#host-page-root').style.marginTop = '850px';
        }""")
        self.page.locator("#station-trigger").click()
        self.assertIn("opens-up", self.page.locator("#station-options").get_attribute("class"))
        self.page.keyboard.press("End")
        self.page.keyboard.press("Enter")
        self.assertEqual(self.page.locator("#station").input_value(), "303")
        self.page.set_viewport_size({"width": 390, "height": 1080})
        self.page.locator("#station-trigger").click()
        self.assertLessEqual(self.page.locator("#station-trigger").bounding_box()["width"], 390)
        self.assertFalse(self.page.evaluate("document.querySelector('#station-options').scrollWidth > document.querySelector('#station-options').clientWidth"))
