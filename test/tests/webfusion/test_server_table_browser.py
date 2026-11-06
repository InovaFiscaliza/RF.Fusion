"""Regressões de navegação e rolagem da tabela de estações do servidor."""

from pathlib import Path
import unittest
from jinja2 import Environment

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


WEBFUSION = Path(__file__).resolve().parents[3] / "src/webfusion"


@unittest.skipIf(sync_playwright is None, "Playwright necessário para testes de navegador")
class ServerTableBrowserTests(unittest.TestCase):
    """Exercita os assets e a tabela reais com respostas HTTP simuladas."""

    def setUp(self) -> None:
        """Prepara um catálogo isolado com métricas distintas. Returns: None."""
        self.runtime = sync_playwright().start()
        self.addCleanup(self.runtime.stop)
        self.browser = self.runtime.chromium.launch(headless=True, args=["--no-sandbox"])
        self.addCleanup(self.browser.close)
        self.page = self.browser.new_page(viewport={"width": 1280, "height": 800})
        self.page.set_default_timeout(5000)
        self.errors = []
        self.page.on("pageerror", lambda error: self.errors.append(str(error)))
        source = (WEBFUSION / "templates/server/server.html").read_text()
        table = source.index('<div class="annual-breakdown server-host-table-scroll"')
        start = source.rfind("<section ", 0, table)
        end = source.index("</section>", table) + len("</section>")
        panel = Environment().from_string(source[start:end]).render(online_only=True)
        panel = panel.replace('id="server-host-table-panel"', 'id="server-host-table-panel" open')
        html = '''<!doctype html><meta charset="utf-8"><body class="desktop">
        <div class="layout"><main class="main-content conteudo-centralizado">
        <div id="server-page-root" class="server-page-stack" data-online-only="1"
         data-host-detail-base-url="/host" data-host-table-endpoint="/api/hosts">'''
        html += panel + "</div></main></div>"
        self.payload = {"count": 40, "rows": [
            {
                "ID_HOST": index, "NA_HOST_NAME": f"CWSM21{index:04}",
                "IS_OFFLINE": False, "IS_BUSY": False, "DT_LAST_CHECK": "2026-11-05 12:00:00",
                "NU_BACKUP_PENDING_FILES_CURRENT": 1234, "PENDING_BACKUP_GB": 12.75,
                "NU_BACKUP_DONE_THIS_MONTH": 42, "VL_BACKUP_DONE_GB_THIS_MONTH": 3.5,
                "NU_BACKUP_ERROR_FILES_CURRENT": 2,
                "NU_PROCESSING_PENDING_FILES_CURRENT": 789,
                "VL_PROCESSING_PENDING_GB_CURRENT": 9.25,
                "NU_PROCESSING_ERROR_FILES_CURRENT": 0,
            } for index in range(1, 41)
        ]}
        self.page.route("**/server", lambda route: route.fulfill(content_type="text/html", body=html))
        self.page.route("**/api/hosts*", lambda route: route.fulfill(json=self.payload))
        self.page.goto("http://server.test/server")
        for name in [
            "_Basic.css", "OutSystemsReactWidgets.css",
            "AnatelReactiveTheme.AnatelReactiveTheme.css",
            "AnatelReactiveTheme.AnatelReactiveTheme.extra.css",
            "Desempenho_CW.Desempenho.css", "webfusion_base.css", "server_page.css",
        ]:
            self.page.add_style_tag(path=str(WEBFUSION / "static/css" / name))
        self.page.add_script_tag(path=str(WEBFUSION / "static/js/server_page.js"))
        self.page.wait_for_function("document.querySelectorAll('#server-host-table-body tr').length === 40")

    def tearDown(self) -> None:
        """Exige ausência de erros JavaScript. Returns: None."""
        self.assertEqual(self.errors, [])

    def test_metrics_and_station_navigation(self) -> None:
        """Rótulos e valores distinguem backup e processamento. Returns: None."""
        row = self.page.locator("#server-host-table-body tr").first
        self.assertEqual(row.locator(".server-host-number").all_text_contents(), ["1.234", "12,75", "42", "3,50", "789", "9,25", "2", "0"])
        self.assertEqual(self.page.locator(".server-host-table thead tr").count(), 1)
        self.assertEqual(self.page.locator(".server-host-table thead th").count(), 10)
        self.assertNotIn("Online", row.locator(".server-host-identity").inner_text())
        self.assertIn("Online", row.locator(".server-host-context").inner_text())
        self.assertIn("host_id=1", row.locator("a").get_attribute("href"))
        self.assertIn("online_only=1", row.locator("a").get_attribute("href"))
        self.assertIn("2026-11-05 12:00:00", row.locator(".server-host-check").inner_text())

    def test_header_and_station_column_remain_fixed(self) -> None:
        """No desktop a rolagem mantém o cabeçalho e o nome da estação. Returns: None."""
        scroll = self.page.locator(".server-host-table-scroll")
        header = self.page.locator(".server-host-table thead")
        head_before = header.bounding_box()
        identity = self.page.locator("#server-host-table-body .server-host-identity").nth(10)
        left_before = identity.bounding_box()["x"]
        scroll.evaluate("el => { el.style.maxWidth = '800px'; el.scrollTop = 600; el.scrollLeft = 400; }")
        self.assertGreater(scroll.evaluate("el => el.scrollTop"), 0)
        self.assertGreater(scroll.evaluate("el => el.scrollLeft"), 0)
        self.assertAlmostEqual(header.bounding_box()["y"], head_before["y"], delta=2)
        self.assertAlmostEqual(identity.bounding_box()["x"], left_before, delta=2)
        self.assertAlmostEqual(self.page.locator("thead .server-host-identity").bounding_box()["x"], left_before, delta=2)
        self.assertNotEqual(identity.evaluate("el => getComputedStyle(el).backgroundColor"), "rgba(0, 0, 0, 0)")
        self.assertNotEqual(self.page.locator("thead th").first.evaluate("el => getComputedStyle(el).backgroundColor"), "rgba(0, 0, 0, 0)")

    def test_mobile_scroll_stays_inside_table(self) -> None:
        """Em tela estreita a página não ganha rolagem horizontal. Returns: None."""
        for width in [320, 390, 640]:
            with self.subTest(width=width):
                self.page.set_viewport_size({"width": width, "height": 844})
                scroll = self.page.locator(".server-host-table-scroll")
                self.assertFalse(self.page.evaluate("document.documentElement.scrollWidth > innerWidth"))
                self.assertTrue(scroll.evaluate("el => el.scrollWidth > el.clientWidth"))
                scroll.evaluate("el => { el.scrollLeft = el.scrollWidth; el.scrollTop = 500; }")
                self.assertEqual(scroll.evaluate("el => el.scrollTop"), 0)
                identity = self.page.locator("#server-host-table-body .server-host-identity").first
                self.assertLessEqual(identity.bounding_box()["width"], 140)
                self.assertEqual(identity.locator("a").evaluate("el => el.getClientRects().length"), 1)
                self.assertTrue(identity.locator("a").evaluate("el => el.scrollWidth <= el.clientWidth"))
                self.assertAlmostEqual(identity.bounding_box()["x"], scroll.bounding_box()["x"] + 1, delta=2)

    def test_table_uses_standard_header_and_row_style(self) -> None:
        """A tabela herda tipografia e espaçamento do padrão existente. Returns: None."""
        self.page.evaluate('''() => {
            const table = document.createElement('table');
            table.className = 'stats-table'; table.id = 'reference-table';
            table.innerHTML = '<thead><tr><th>Referência</th></tr></thead><tbody><tr><td>123</td></tr></tbody>';
            document.body.append(table);
        }''')
        for attribute in ["fontSize", "fontWeight", "color", "padding"]:
            self.assertEqual(
                self.page.locator(".server-host-table thead th").nth(1).evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
                self.page.locator("#reference-table th").evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
            )
        for attribute in ["fontSize", "padding", "color"]:
            self.assertEqual(
                self.page.locator("#server-host-table-body td").first.evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
                self.page.locator("#reference-table td").evaluate("(el, key) => getComputedStyle(el)[key]", attribute),
            )

    def test_empty_state_spans_all_columns(self) -> None:
        """O estado vazio respeita as dez colunas da tabela. Returns: None."""
        self.payload.update(count=0, rows=[])
        self.page.evaluate("document.querySelector('#server-page-root').dispatchEvent(new Event('server-metrics-refresh'))")
        self.page.wait_for_function("document.querySelector('#server-host-table-body').textContent.includes('Nenhuma estação')")
        self.assertEqual(self.page.locator("#server-host-table-body td").get_attribute("colspan"), "10")
