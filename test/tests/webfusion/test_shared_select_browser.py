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
        for name in ["AnatelReactiveTheme.AnatelReactiveTheme.css", "webfusion_base.css"]:
            self.page.add_style_tag(path=str(STATIC / "css" / name))
        for name in ["host_page.js", "configuration_page.js", "base_page.js"]:
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
        self.assertEqual(self.page.locator("#map").get_attribute("aria-hidden"), "true")

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

    def test_new_fields_without_module_wrapper_are_enhanced_once(self) -> None:
        """Campos dinâmicos recebem um menu único sem opt-in da página. Returns: None."""
        self.page.evaluate("""() => {
            const form = document.createElement('form');
            form.id = 'dynamic';
            form.innerHTML = '<label>Ordenação<select name="sort"><option value="name">Nome</option><option value="date">Data</option></select></label>';
            document.body.append(form);
        }""")
        trigger = self.page.locator("#dynamic [role=combobox]")
        trigger.click()
        self.page.locator("#dynamic [role=option]").get_by_text("Data", exact=True).click()
        self.assertEqual(self.page.evaluate("new FormData(document.querySelector('#dynamic')).get('sort')"), "date")
        self.assertEqual(self.page.locator("#dynamic [role=combobox]").count(), 1)
        self.page.locator("#map-trigger").click()
        self.assertTrue(self.page.locator("#map-options").is_visible())

    def test_shell_waits_for_page_controller_initialization(self) -> None:
        """O shell lê o valor definido pelo controlador carregado depois. Returns: None."""
        source = (STATIC / "js/base_page.js").read_text()
        html = '''<!doctype html><meta charset="utf-8">
        <label for="initial">Ordenação</label><select id="initial">
        <option value="name">Nome</option><option value="date">Data</option></select>
        <script src="/static/js/base_page.js"></script>
        <script>document.querySelector('#initial').value = 'date';</script>'''
        self.page.route("**/init-fixture", lambda route: route.fulfill(content_type="text/html", body=html))
        self.page.route("**/static/js/base_page.js", lambda route: route.fulfill(content_type="application/javascript", body=source))
        self.page.goto("http://select.test/init-fixture")
        self.assertEqual(self.page.locator("#initial-trigger").inner_text(), "Data")
        self.assertEqual(self.page.locator("#initial-trigger").count(), 1)

    def test_dialog_menu_escapes_panel_clipping_and_keeps_modal_open(self) -> None:
        """Menus em diálogos escapam do recorte e preservam o foco. Returns: None."""
        self.page.evaluate("""() => {
            const dialog = document.createElement('dialog');
            dialog.id = 'modal';
            dialog.innerHTML = '<div style="height:70px;overflow:hidden"><label for="modal-select">Etapa</label><select id="modal-select"><option>Um</option><option>Dois</option><option>Três</option></select></div>';
            document.body.append(dialog);
            dialog.showModal();
        }""")
        self.page.locator("#modal-select-trigger").click()
        self.page.locator("#modal-select-options").get_by_role("option", name="Três").click()
        self.assertEqual(self.page.locator("#modal-select").input_value(), "Três")
        self.page.locator("#modal-select-trigger").click()
        self.page.keyboard.press("Escape")
        self.assertTrue(self.page.evaluate("document.querySelector('#modal').open"))
        self.assertEqual(self.page.evaluate("document.activeElement.id"), "modal-select-trigger")

    def test_optgroup_disabled_and_duplicate_values(self) -> None:
        """Grupos desabilitados e opções com valores iguais mantêm a semântica. Returns: None."""
        self.page.evaluate("""() => {
            const select = document.querySelector('#map');
            select.innerHTML = '<optgroup disabled label="Bloqueados"><option value="x">Bloqueado</option></optgroup><optgroup hidden label="Ocultos"><option>Oculto</option></optgroup><option value="same">Primeiro</option><option value="same" label="Segundo">Outro texto</option>';
            select.selectedIndex = 2;
        }""")
        self.page.locator("#map-trigger").click()
        self.assertEqual(self.page.locator("#map-options [role=option]").all_text_contents(), ["Bloqueado", "Primeiro", "Segundo"])
        self.page.keyboard.press("End")
        self.page.keyboard.press("Enter")
        self.assertEqual(self.page.locator("#map-trigger").inner_text(), "Segundo")
        self.assertEqual(self.page.evaluate("document.querySelector('#map').selectedIndex"), 3)

    def _open_upload_page(self) -> None:
        """Carrega o template e os assets reais do upload sem serviços externos. Returns: None."""
        from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader

        environment = Environment(loader=ChoiceLoader([
            DictLoader({"base.html": '<!doctype html><meta charset="utf-8">{% block content %}{% endblock %}'}),
            FileSystemLoader(STATIC.parent / "templates"),
        ]))
        environment.globals["url_for"] = lambda endpoint, **kwargs: f"/{endpoint}"
        html = environment.get_template("upload/upload.html").render(
            max_file_bytes=512 * 1024 * 1024, upload_folder="/upload", request_header="X-Upload",
            request_header_value="1", stations=[{"ID_HOST": 1, "NA_HOST_NAME": "CWSM1"}],
            drive_test_types={"smp-romes": "SMP ROMES", "espectro": "Espectro"},
        )
        self.page.route("**/upload-fixture", lambda route: route.fulfill(content_type="text/html", body=html))
        self.page.goto("http://select.test/upload-fixture")
        for name in ["AnatelReactiveTheme.AnatelReactiveTheme.css", "webfusion_base.css", "upload_page.css"]:
            self.page.add_style_tag(path=str(STATIC / "css" / name))
        for name in ["upload_page.js", "base_page.js"]:
            self.page.add_script_tag(path=str(STATIC / "js" / name))

    def test_upload_template_uses_shell_for_classification(self) -> None:
        """O upload real usa os menus comuns sem alterar a classificação. Returns: None."""
        self._open_upload_page()
        self.page.locator("#upload-category-trigger").click()
        self.page.locator("#upload-category-options").get_by_role("option", name="Estações fixas").click()
        self.page.locator("#upload-station-trigger").click()
        self.page.locator("#upload-station-options").get_by_role("option", name="CWSM1").click()
        self.assertEqual(self.page.locator("#upload-destination").inner_text(), "/upload/fixas/1")
        self.assertFalse(self.page.locator("[data-select-files]").first.is_disabled())
        self.assertEqual(self.page.locator(".upload-select-trigger").count(), 0)

    def test_upload_queue_preserves_destination_and_runs_sequentially(self) -> None:
        """Cada arquivo retém o destino original até concluir a fila. Returns: None."""
        self._open_upload_page()
        requests = []
        self.page.route("**/upload.upload_file", lambda route: requests.append(route))
        self.page.select_option("#upload-category", "fixas")
        self.page.select_option("#upload-station", "1")
        self.page.locator("#upload-input").set_input_files([
            {"name": "one.bin", "mimeType": "application/octet-stream", "buffer": b"one"},
            {"name": "two.bin", "mimeType": "application/octet-stream", "buffer": b"two"},
        ])
        self.page.wait_for_function("document.querySelectorAll('[data-state=uploading]').length === 1")
        self.assertEqual(self.page.locator("[data-state=pending]").count(), 1)
        self.assertEqual(len(requests), 1)
        self.page.select_option("#upload-category", "rni")
        requests[0].fulfill(status=201, json={"name": "one.bin"})
        self.page.wait_for_function("document.querySelectorAll('[data-state=done]').length === 1")
        self.page.wait_for_function("document.querySelectorAll('[data-state=uploading]').length === 1")
        self.assertEqual(len(requests), 2)
        body = requests[1].request.post_data_buffer.decode()
        self.assertIn('name="category"\r\n\r\nfixas', body)
        self.assertIn('name="station"\r\n\r\n1', body)
        self.assertEqual(self.page.locator(".upload-file-destination").all_text_contents(),
                         ["/upload/fixas/1", "/upload/fixas/1"])
        requests[1].fulfill(status=201, json={"name": "two.bin"})
        self.page.wait_for_function("document.querySelectorAll('[data-state=done]').length === 2")
        self.page.locator("#upload-clear").click()
        self.assertEqual(self.page.locator(".upload-row").count(), 0)
        self.assertTrue(self.page.locator("#upload-empty").is_visible())

    def test_upload_pending_cancel_restart_and_failure(self) -> None:
        """Cancelar e retomar mantém a fila e apresenta falhas do servidor. Returns: None."""
        self._open_upload_page()
        requests = []
        self.page.route("**/upload.upload_file", lambda route: requests.append(route))
        self.page.select_option("#upload-category", "rni")
        self.page.locator("#upload-input").set_input_files([
            {"name": "one.bin", "mimeType": "application/octet-stream", "buffer": b"one"},
            {"name": "two.bin", "mimeType": "application/octet-stream", "buffer": b"two"},
        ])
        self.page.locator(".upload-row").nth(1).locator(".upload-cancel").click()
        self.assertEqual(self.page.locator("[data-state=cancelled]").count(), 1)
        requests[0].fulfill(status=201, json={"name": "one.bin"})
        self.page.wait_for_function("document.querySelector('#upload-feedback').textContent.startsWith('Fila finalizada')")
        self.assertEqual(len(requests), 1)
        self.page.locator(".upload-row").nth(1).locator(".upload-cancel").click()
        self.page.wait_for_function("document.querySelectorAll('[data-state=uploading]').length === 1")
        self.assertEqual(len(requests), 2)
        requests[1].fulfill(status=409, json={"error": "Arquivo já existe"})
        self.page.wait_for_function("document.querySelectorAll('[data-state=error]').length === 1")
        row = self.page.locator("[data-state=error]")
        self.assertEqual(row.locator(".upload-file-status").inner_text(), "Arquivo já existe")
        self.assertFalse(row.locator("progress").is_visible())
        self.assertFalse(row.locator(".upload-cancel").is_visible())

    def test_upload_missing_classification_focuses_common_combobox(self) -> None:
        """Arquivos sem destino focam o seletor pelo contrato comum. Returns: None."""
        self._open_upload_page()
        self.page.locator("#upload-input").set_input_files([
            {"name": "one.bin", "mimeType": "application/octet-stream", "buffer": b"one"},
        ])
        self.assertEqual(self.page.locator(".upload-row").count(), 0)
        self.assertEqual(self.page.evaluate("document.activeElement.id"), "upload-category-trigger")

    def test_upload_layout_stays_inside_mobile_viewport(self) -> None:
        """A classificação e a fila cabem em janela estreita. Returns: None."""
        self._open_upload_page()
        self.page.set_viewport_size({"width": 390, "height": 844})
        self.page.select_option("#upload-category", "fixas")
        self.page.select_option("#upload-station", "1")
        self.assertFalse(self.page.evaluate("document.documentElement.scrollWidth > innerWidth"))
        self.page.locator("#upload-station-trigger").click()
        self.assertTrue(self.page.locator("#upload-station-options").is_visible())

    def test_searchable_user_menu_shares_visual_style(self) -> None:
        """Busca de usuários preserva seus valores usando o visual comum. Returns: None."""
        self.page.evaluate("""() => {
            document.body.insertAdjacentHTML('beforeend', '<script type="application/json" id="users-filter-options">{"users":[{"value":"ana@example.test","label":"Ana Silva"}]}</script><div class="select-shell wf-combobox" data-users-combobox><input id="users-email-filter" type="search" class="form-control wf-select-input" data-users-combobox-input><input type="hidden" data-users-combobox-value><div class="wf-select-menu" hidden data-users-combobox-menu></div></div>');
        }""")
        self.page.add_style_tag(path=str(STATIC / "css/users_page.css"))
        self.page.add_script_tag(path=str(STATIC / "js/users_page.js"))
        self.page.locator("#users-email-filter").fill("Ana")
        menu = self.page.locator("[data-users-combobox-menu]")
        self.assertTrue(menu.is_visible())
        menu.get_by_role("button", name="Ana Silva").click()
        self.assertEqual(self.page.locator("[data-users-combobox-value]").input_value(), "ana@example.test")
        self.assertFalse(menu.is_visible())
        for attribute in ["borderRadius", "fontSize", "padding", "backgroundColor"]:
            self.assertEqual(
                self.page.locator("#station-options").evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
                menu.evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
            )
