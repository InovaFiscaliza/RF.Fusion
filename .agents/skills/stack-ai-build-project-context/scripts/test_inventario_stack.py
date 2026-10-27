#!/usr/bin/env python3
"""Testes de inventario_stack.py."""

import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import inventario_stack as inv  # noqa: E402


def escrever(caminho: Path, conteudo: str = "conteudo\n") -> Path:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(conteudo, encoding="utf-8")
    return caminho


class Base(unittest.TestCase):
    def setUp(self):
        self.raiz = Path(tempfile.mkdtemp(prefix="stack-teste-"))
        self.addCleanup(shutil.rmtree, self.raiz, ignore_errors=True)

    def stack_minima(self):
        escrever(self.raiz / "AGENTS.md", "# Regras\n")
        (self.raiz / ".agents" / "references").mkdir(parents=True)
        (self.raiz / ".agents" / "skills").mkdir(parents=True)

    def skill(self, nome, name=None, description="faz algo", com_frontmatter=True):
        corpo = "# titulo\n"
        if com_frontmatter:
            corpo = f"---\nname: {name or nome}\ndescription: {description}\n---\n\n# {nome}\n"
        escrever(self.raiz / ".agents" / "skills" / nome / "SKILL.md", corpo)


class TesteEstadoDaStack(Base):
    def test_stack_ausente(self):
        dados = inv.coletar_stack(self.raiz)
        self.assertEqual(dados["estado"], "ausente")
        self.assertEqual(sorted(dados["faltando"]), ["agents_md", "references", "skills"])

    def test_stack_parcial_lista_o_que_falta(self):
        escrever(self.raiz / "AGENTS.md", "# Regras\n")
        (self.raiz / ".agents" / "skills").mkdir(parents=True)
        dados = inv.coletar_stack(self.raiz)
        self.assertEqual(dados["estado"], "parcial")
        self.assertEqual(dados["faltando"], ["references"])

    def test_stack_minima_vazia(self):
        self.stack_minima()
        self.assertEqual(inv.coletar_stack(self.raiz)["estado"], "minima_presente")

    def test_stack_povoada_por_reference(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "arquitetura.md", "# Arquitetura\n")
        self.assertEqual(inv.coletar_stack(self.raiz)["estado"], "povoada")

    def test_stack_povoada_por_agents_extenso(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "linha\n" * 30)
        self.assertEqual(inv.coletar_stack(self.raiz)["estado"], "povoada")

    def test_agent_md_singular_tambem_conta(self):
        escrever(self.raiz / "AGENT.md", "# Regras\n")
        (self.raiz / ".agents" / "references").mkdir(parents=True)
        (self.raiz / ".agents" / "skills").mkdir(parents=True)
        self.assertEqual(inv.coletar_stack(self.raiz)["pecas"]["agents_md"], "AGENT.md")

    def test_detecta_ponteiros_e_indices(self):
        self.stack_minima()
        escrever(self.raiz / "CLAUDE.md", "@AGENTS.md\n")
        escrever(self.raiz / ".github" / "copilot-instructions.md", "ler AGENTS.md\n")
        escrever(self.raiz / ".agents" / "skills" / "README.md", "# indice\n")
        dados = inv.coletar_stack(self.raiz)
        self.assertIn("CLAUDE.md", dados["ponteiros"])
        self.assertIn(".github/copilot-instructions.md", dados["ponteiros"])
        self.assertIn(".agents/skills/README.md", dados["indices"])

    def test_reference_com_e_sem_evidencias(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "com.md", "# Com\n\n## Evidencias\n\ntabela\n")
        escrever(self.raiz / ".agents" / "references" / "sem.md", "# Sem\n\ntexto\n")
        refs = {r["arquivo"]: r for r in inv.coletar_stack(self.raiz)["references"]}
        self.assertTrue(refs[".agents/references/com.md"]["tem_evidencias"])
        self.assertFalse(refs[".agents/references/sem.md"]["tem_evidencias"])

    def test_evidencias_acentuado_reconhecido(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "a.md", "# A\n\n## Evidências\n\nx\n")
        self.assertTrue(inv.coletar_stack(self.raiz)["references"][0]["tem_evidencias"])


class TesteFrontmatter(unittest.TestCase):
    def test_campos_simples(self):
        campos = inv.frontmatter("---\nname: exemplo\ndescription: faz algo\n---\n\n# corpo\n")
        self.assertEqual(campos["name"], "exemplo")
        self.assertEqual(campos["description"], "faz algo")

    def test_descricao_em_bloco(self):
        campos = inv.frontmatter("---\nname: exemplo\ndescription: >\n  linha um\n  linha dois\n---\n")
        self.assertEqual(campos["name"], "exemplo")
        self.assertTrue(campos["description"])

    def test_sem_frontmatter(self):
        self.assertEqual(inv.frontmatter("# titulo\n"), {})

    def test_aspas_removidas(self):
        self.assertEqual(inv.frontmatter("---\nname: \"x\"\n---\n")["name"], "x")


class TesteCenso(Base):
    def test_ignora_diretorios_de_dependencia(self):
        escrever(self.raiz / "src" / "app.py", "print(1)\n")
        escrever(self.raiz / "node_modules" / "pacote" / "index.js", "x\n")
        escrever(self.raiz / ".git" / "config", "x\n")
        dados = inv.coletar_censo(self.raiz, 10)
        nomes = [d["nome"] for d in dados["diretorios_topo"]]
        self.assertIn("src", nomes)
        self.assertNotIn("node_modules", nomes)
        self.assertNotIn(".git", nomes)
        self.assertEqual(dados["arquivos_examinados"], 1)

    def test_detecta_manifestos_e_qualidade_e_ci(self):
        escrever(self.raiz / "pyproject.toml", "[tool]\n")
        escrever(self.raiz / "servico" / "go.mod", "module x\n")
        escrever(self.raiz / ".editorconfig", "root = true\n")
        escrever(self.raiz / ".github" / "workflows" / "ci.yml", "on: push\n")
        dados = inv.coletar_censo(self.raiz, 10)
        self.assertIn("pyproject.toml", dados["manifestos"])
        self.assertIn("servico/go.mod", dados["manifestos"])
        self.assertIn(".editorconfig", dados["configuracao_qualidade"])
        self.assertIn(".github/workflows", dados["integracao_continua"])

    def test_detecta_testes_e_migracoes(self):
        escrever(self.raiz / "tests" / "test_x.py", "x\n")
        escrever(self.raiz / "db" / "migrations" / "0001.sql", "create\n")
        dados = inv.coletar_censo(self.raiz, 10)
        self.assertIn("tests", dados["diretorios_de_teste"])
        self.assertIn("db/migrations", dados["diretorios_de_migracao"])

    def test_histograma_de_extensoes(self):
        escrever(self.raiz / "a.py")
        escrever(self.raiz / "b.py")
        escrever(self.raiz / "c.md")
        extensoes = dict(inv.coletar_censo(self.raiz, 10)["extensoes"])
        self.assertEqual(extensoes[".py"], 2)
        self.assertEqual(extensoes[".md"], 1)


class TesteOrcamentoDeContexto(Base):
    def skill_com_descricao(self, nome, descricao, bloco=False, user_invoked=False):
        marca = "disable-model-invocation: true\n" if user_invoked else ""
        if bloco:
            corpo = f"---\nname: {nome}\ndescription: >\n  {descricao}\n{marca}---\n\n# {nome}\n"
        else:
            corpo = f"---\nname: {nome}\ndescription: {descricao}\n{marca}---\n\n# {nome}\n"
        escrever(self.raiz / ".agents" / "skills" / nome / "SKILL.md", corpo)

    def test_extrai_descricao_inline_e_em_bloco(self):
        inline = "---\nname: x\ndescription: faz algo util\n---\n"
        self.assertEqual(inv.extrair_descricao(inline), "faz algo util")
        bloco = "---\nname: x\ndescription: >\n  primeira linha\n  segunda linha\nallowed-tools: Read\n---\n"
        self.assertEqual(inv.extrair_descricao(bloco), "primeira linha segunda linha")
        self.assertEqual(inv.extrair_descricao("# sem frontmatter\n"), "")

    def test_descricao_em_bloco_nao_vira_marcador(self):
        # o parser de frontmatter devolve "<bloco>"; a medicao precisa do texto inteiro
        self.stack_minima()
        self.skill_com_descricao("grande", "d" * 400, bloco=True)
        skill = inv.coletar_stack(self.raiz)["skills"][0]
        self.assertEqual(skill["descricao_chars"], 400)
        self.assertEqual(skill["descricao_tokens"], 100)

    def test_orcamento_soma_descricoes_e_compara_com_agents(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "r" * 200)
        for nome in "abcde":
            self.skill_com_descricao(nome, "d" * 400)
        orc = inv.coletar_stack(self.raiz)["orcamento"]
        self.assertEqual(orc["descricoes_skills"], 5)
        self.assertEqual(orc["descricoes_tokens"], 500)
        self.assertEqual(orc["permanente_tokens"], orc["agents_md_tokens"] + 500)
        self.assertTrue(orc["descricoes_acima_do_agents"])

    def test_catalogo_pequeno_nao_alerta(self):
        # stack recem instalada: duas skills sempre superam um AGENTS.md vazio
        self.stack_minima()
        self.skill_com_descricao("a", "d" * 800)
        self.skill_com_descricao("b", "d" * 800)
        self.assertFalse(inv.coletar_stack(self.raiz)["orcamento"]["descricoes_acima_do_agents"])

    def test_agents_maior_que_descricoes_nao_alerta(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "r" * 4000)
        self.skill_com_descricao("a", "curta")
        self.assertFalse(inv.coletar_stack(self.raiz)["orcamento"]["descricoes_acima_do_agents"])

    def test_stack_sem_skill_nao_alerta(self):
        self.stack_minima()
        self.assertFalse(inv.coletar_stack(self.raiz)["orcamento"]["descricoes_acima_do_agents"])

    def test_verificar_avisa_sobre_orcamento(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "r" * 100)
        for nome in "abcde":
            self.skill_com_descricao(nome, "d" * 800)
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertTrue(any("orcamento" in a for a in avisos), avisos)

    def test_user_invoked_sai_do_piso_permanente(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "r" * 200)
        self.skill_com_descricao("modelo", "d" * 400)
        self.skill_com_descricao("humana", "d" * 400, user_invoked=True)
        orc = inv.coletar_stack(self.raiz)["orcamento"]
        self.assertEqual(orc["descricoes_tokens"], 200)
        self.assertEqual(orc["descricoes_tokens_permanentes"], 100)
        self.assertEqual(orc["descricoes_tokens_sob_demanda"], 100)
        self.assertEqual(orc["descricoes_model_invocadas"], 1)
        self.assertEqual(orc["descricoes_user_invocadas"], 1)
        self.assertEqual(orc["permanente_tokens"], orc["agents_md_tokens"] + 100)

    def test_catalogo_user_invoked_nao_alerta_orcamento(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "# Regras\n" + "r" * 100)
        for nome in "abcde":
            self.skill_com_descricao(nome, "d" * 800, user_invoked=True)
        self.assertFalse(inv.coletar_stack(self.raiz)["orcamento"]["descricoes_acima_do_agents"])

    def test_verificar_avisa_ativacao_pelo_nome_de_user_invoked(self):
        self.stack_minima()
        self.skill_com_descricao("etapa", "resumo humano da skill", user_invoked=True)
        escrever(self.raiz / "AGENTS.md", "# Regras\n\n- Ao criar pagina, acione a skill `etapa`.\n")
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertTrue(any("pelo nome" in a for a in avisos), avisos)

    def test_verificar_aceita_ponteiro_para_o_skill_md(self):
        self.stack_minima()
        self.skill_com_descricao("etapa", "resumo humano da skill", user_invoked=True)
        escrever(
            self.raiz / "AGENTS.md",
            "# Regras\n\n- Ao criar pagina, leia `.agents/skills/etapa/SKILL.md`.\n",
        )
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertFalse(any("pelo nome" in a for a in avisos), avisos)

    def test_verificar_nao_confunde_prosa_com_ordem_de_ativar(self):
        # "nenhuma delas e acionada sozinha. A `etapa` fica ativa" diz o contrario de ativar
        self.stack_minima()
        self.skill_com_descricao("etapa", "resumo humano da skill", user_invoked=True)
        escrever(
            self.raiz / "AGENTS.md",
            "# Regras\n\nAs de apoio sao opt-in: nenhuma delas e acionada sozinha. A `etapa` fica "
            "ativa na sessao ate voce mandar parar.\n",
        )
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertFalse(any("pelo nome" in a for a in avisos), avisos)

    def test_verificar_aceita_comando_barra_e_nome_de_model_invoked(self):
        self.stack_minima()
        self.skill_com_descricao("etapa", "resumo humano da skill", user_invoked=True)
        self.skill_com_descricao("livre", "Use quando o pedido envolver X")
        escrever(
            self.raiz / "AGENTS.md",
            "# Regras\n\n- Digite `/etapa` para rodar.\n- Acione a skill `livre` quando precisar.\n",
        )
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertFalse(any("pelo nome" in a for a in avisos), avisos)

    def test_verificar_avisa_gatilho_em_descricao_user_invoked(self):
        self.stack_minima()
        self.skill_com_descricao("gatilho", 'Use com "/gatilho" quando quiser rodar', user_invoked=True)
        escrever(
            self.raiz / "AGENTS.md",
            "# Regras\n\n- Consulte `.agents/skills/gatilho/SKILL.md`.\n",
        )
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertTrue(any("frase de gatilho" in a for a in avisos), avisos)

    def test_verificar_nao_reclama_de_gatilho_em_model_invoked(self):
        self.stack_minima()
        self.skill_com_descricao("modelo", "Use quando o pedido envolver X")
        avisos = inv.verificar(self.raiz)["avisos"]
        self.assertFalse(any("frase de gatilho" in a for a in avisos), avisos)

    def test_references_contam_como_sob_demanda(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "x.md", "# X\n" + "c" * 800)
        self.assertGreaterEqual(inv.coletar_stack(self.raiz)["orcamento"]["references_tokens"], 200)


class TesteCodificacao(Base):
    def bytes_em(self, nome, dados: bytes) -> Path:
        caminho = self.raiz / nome
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_bytes(dados)
        return caminho

    def test_rotula_cada_codificacao(self):
        casos = {
            b"": "vazio",
            b"select 1\n": "ascii",
            "aten\u00e7\u00e3o\n".encode("utf-8"): "utf-8",
            b"\xef\xbb\xbf" + "aten\u00e7\u00e3o\n".encode("utf-8"): "utf-8-bom",
            "aten\u00e7\u00e3o\n".encode("latin-1"): "8-bits",
            "select 1\n".encode("utf-16-le") * 40: "utf-16le-sem-bom",
            "select 1\n".encode("utf-16"): "utf-16le",
            "select 1\n".encode("utf-16-be"): "utf-16be-sem-bom",
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x01": "binario",
        }
        for dados, esperado in casos.items():
            self.assertEqual(inv.detectar_codificacao(dados), esperado, dados[:12])

    def test_cauda_cortada_pela_amostra_nao_vira_8_bits(self):
        # ultimo caractere multibyte truncado no limite de leitura
        dados = "aten\u00e7\u00e3o".encode("utf-8")[:-1]
        self.assertEqual(inv.detectar_codificacao(dados), "utf-8")

    def test_terminacao_de_linha(self):
        self.assertEqual(inv.detectar_terminacao(b"a\r\nb\r\n", "ascii"), "crlf")
        self.assertEqual(inv.detectar_terminacao(b"a\nb\n", "ascii"), "lf")
        self.assertEqual(inv.detectar_terminacao(b"a\r\nb\n", "ascii"), "misto")
        self.assertEqual(inv.detectar_terminacao("a\r\n".encode("utf-16"), "utf-16le"), "crlf")

    def test_padrao_por_extensao_com_excecoes(self):
        for i in range(6):
            self.bytes_em(f"src/p{i}.sql", "select '\u00e1'\r\n".encode("utf-16"))
        self.bytes_em("src/legado.sql", "select '\u00e1'\r\n".encode("latin-1"))
        self.bytes_em("doc.md", "t\u00edtulo\n".encode("utf-8"))
        grupos = {g["extensao"]: g for g in inv.coletar_censo(self.raiz, 10)["codificacao"]["por_extensao"]}
        self.assertEqual(grupos[".sql"]["padrao"], "utf-16le")
        self.assertEqual(grupos[".sql"]["terminacao"], "crlf")
        self.assertEqual(grupos[".sql"]["excecoes"][0]["codificacao"], "8-bits")
        self.assertEqual(grupos[".sql"]["excecoes"][0]["exemplos"], ["src/legado.sql"])
        self.assertEqual(grupos[".md"]["padrao"], "utf-8")

    def test_ascii_e_vazio_nao_contam_como_excecao(self):
        for i in range(3):
            self.bytes_em(f"a{i}.asp", "resposta '\u00e9'\r\n".encode("latin-1"))
        self.bytes_em("puro.asp", b"resposta\r\n")
        self.bytes_em("vazio.asp", b"")
        grupo = next(g for g in inv.coletar_censo(self.raiz, 10)["codificacao"]["por_extensao"]
                     if g["extensao"] == ".asp")
        self.assertEqual(grupo["padrao"], "8-bits")
        self.assertEqual(grupo["arquivos"], 5)
        self.assertEqual(grupo["decisivos"], 3)
        self.assertEqual(grupo["neutros"], 2)
        self.assertEqual(grupo["cobertura"], 1.0)
        self.assertEqual(grupo["excecoes"], [])

    def test_comando_de_busca_por_familia(self):
        self.bytes_em("a.sql", "select '\u00e1'\r\n".encode("utf-16"))
        self.bytes_em("b.asp", "resposta '\u00e9'\r\n".encode("latin-1"))
        cod = inv.coletar_censo(self.raiz, 10)["codificacao"]
        familias = {f["familia"] for f in cod["familias_de_busca"]}
        self.assertEqual(familias, {"utf-16-bom", "8-bits"})
        self.assertTrue(cod["repositorio_misto"])
        self.assertIn("-E latin1", inv.busca_de("8-bits"))
        self.assertIn("-E utf-16le", inv.busca_de("utf-16le-sem-bom"))

    def test_repositorio_de_codificacao_unica_nao_marca_misto(self):
        self.bytes_em("a.py", "x = '\u00e1'\n".encode("utf-8"))
        self.bytes_em("b.md", "t\u00edtulo\n".encode("utf-8"))
        self.assertFalse(inv.coletar_censo(self.raiz, 10)["codificacao"]["repositorio_misto"])


class TesteVerificar(Base):
    def test_stack_limpa_sem_problemas(self):
        self.stack_minima()
        self.skill("exemplo")
        escrever(
            self.raiz / ".agents" / "references" / "arq.md",
            "# Arq\n\n## Evidencias\n\ntabela\n",
        )
        escrever(self.raiz / "AGENTS.md", "# Regras\n\nDetalhe em `.agents/references/arq.md`.\n")
        dados = inv.verificar(self.raiz)
        self.assertEqual(dados["erros"], [])
        self.assertEqual(dados["avisos"], [])

    def test_diretorio_sem_skill_md_e_aviso_e_nao_erro(self):
        self.stack_minima()
        (self.raiz / ".agents" / "skills" / "auxiliar").mkdir(parents=True)
        dados = inv.verificar(self.raiz)
        self.assertEqual(dados["erros"], [])
        self.assertTrue(any("sem SKILL.md em nenhum nivel" in a for a in dados["avisos"]))

    def test_suite_de_sub_skills_e_reconhecida(self):
        self.stack_minima()
        escrever(
            self.raiz / ".agents" / "skills" / "suite" / "fase-um" / "SKILL.md",
            "---\nname: fase-um\ndescription: faz algo\n---\n\n# fase-um\n",
        )
        dados = inv.verificar(self.raiz)
        self.assertEqual(dados["erros"], [])
        self.assertEqual(dados["avisos"], [])
        nomes = [s["diretorio"] for s in inv.coletar_stack(self.raiz)["skills"]]
        self.assertIn("suite/fase-um", nomes)

    def test_sub_skill_com_name_divergente(self):
        self.stack_minima()
        escrever(
            self.raiz / ".agents" / "skills" / "suite" / "fase-um" / "SKILL.md",
            "---\nname: outro\ndescription: faz algo\n---\n\n# x\n",
        )
        self.assertTrue(any("diverge do diretorio" in e for e in inv.verificar(self.raiz)["erros"]))

    def test_skill_sem_frontmatter(self):
        self.stack_minima()
        self.skill("solta", com_frontmatter=False)
        self.assertTrue(any("frontmatter" in e for e in inv.verificar(self.raiz)["erros"]))

    def test_name_divergente_do_diretorio(self):
        self.stack_minima()
        self.skill("pasta-a", name="outro-nome")
        self.assertTrue(any("diverge do diretorio" in e for e in inv.verificar(self.raiz)["erros"]))

    def test_name_ausente_e_aviso(self):
        self.stack_minima()
        escrever(
            self.raiz / ".agents" / "skills" / "s" / "SKILL.md",
            "---\ndescription: faz algo\n---\n\n# s\n",
        )
        dados = inv.verificar(self.raiz)
        self.assertEqual(dados["erros"], [])
        self.assertTrue(any("campo name ausente" in a for a in dados["avisos"]))

    def test_description_ausente(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "skills" / "s" / "SKILL.md", "---\nname: s\n---\n\n# s\n")
        self.assertTrue(any("description ausente" in e for e in inv.verificar(self.raiz)["erros"]))

    def test_ponteiro_quebrado_em_agents(self):
        self.stack_minima()
        escrever(self.raiz / "AGENTS.md", "Ver `.agents/references/inexistente.md`.\n")
        self.assertTrue(any("nao existe" in e for e in inv.verificar(self.raiz)["erros"]))

    def test_reference_orfa_gera_aviso(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "orfa.md", "# Orfa\n\n## Evidencias\n\nx\n")
        self.assertTrue(any("nenhum artefato" in a for a in inv.verificar(self.raiz)["avisos"]))

    def test_reference_sem_evidencias_gera_aviso(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "r.md", "# R\n\ntexto\n")
        escrever(self.raiz / "AGENTS.md", "Ver `.agents/references/r.md`.\n")
        self.assertTrue(any("sem secao de evidencias" in a for a in inv.verificar(self.raiz)["avisos"]))

    def test_reference_longa_sem_sumario(self):
        self.stack_minima()
        corpo = "# R\n" + "linha\n" * 320 + "\n## Evidencias\n\nx\n"
        escrever(self.raiz / ".agents" / "references" / "longa.md", corpo)
        escrever(self.raiz / "AGENTS.md", "Ver `.agents/references/longa.md`.\n")
        self.assertTrue(any("sem sumario" in a for a in inv.verificar(self.raiz)["avisos"]))

    def test_readme_de_indice_nao_gera_aviso_de_orfao(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "README.md", "# indice\n")
        self.assertEqual(inv.verificar(self.raiz)["avisos"], [])


DOSSIE_MINIMO = """Frente: {frente}
Escopo investigado: src/
Eixos cobertos: persistencia e dados
Exemplares lidos por inteiro: src/a.py, src/b.py
Universo examinado: 40 arquivos comparaveis, rg -E latin1

Achados:
- A1: {afirmacao}
  eixo: persistencia e dados
  evidencia: src/a.py:1, src/b.py:2 ({ocorrencias} ocorrencias em {arquivos} arquivos, de {universo} comparaveis)
  comando: {comando}
  comando-contraexemplo: {contra_comando}
  contraexemplos: {contraexemplos}
  fonte-declarativa: {declarativa}
  alcance: src/
  classificacao: {classificacao}
  por que importa: nada obvio

Processos observados:
- P1: {unidade}
  arquivos que participam: src/a.py (entrada), src/b.py (escrita)
  sequencia: 1. abre 2. escreve 3. fecha
  obrigatorio: abrir antes de escrever
  variavel: o nome do destino
  exemplares: {exemplares} lidos por inteiro | src/a.py, src/b.py
  recorrencia: 40 unidades, contadas com rg --files-with-matches
  onde se erra: esquecer o fechamento

Candidatos a guardrail:
- G1: padrao identificado: {guardrail}
  desvio reconhecivel: abre conexao propria
  como se reconhece no codigo: chamada direta ao driver
  comando: rg -n conexao src/
"""


def dossie(**kwargs) -> str:
    campos = {
        "frente": "persistencia", "afirmacao": "toda escrita passa por X",
        "ocorrencias": 8, "arquivos": 4, "universo": 10, "comando": "rg -n X src/",
        "contra_comando": "rg -n -v X src/", "contraexemplos": "0 | procurei Y em src/",
        "declarativa": "nenhuma", "classificacao": "convencao",
        "unidade": "rotina de escrita", "exemplares": 3,
        "guardrail": "escrita por X (8 de 10 em src/)",
    }
    campos.update(kwargs)
    return DOSSIE_MINIMO.format(**campos)


class TesteClassificacaoCalculada(unittest.TestCase):
    def test_faixas(self):
        casos = [
            ((8, 4, 0, False), "convencao"),
            ((5, 3, 0, False), "convencao"),
            ((5, 2, 0, False), "dominante-com-excecoes"),
            ((2, 2, 0, True), "convencao"),
            ((2, 2, 0, False), "isolado"),
            ((10, 5, 1, False), "dominante-com-excecoes"),
            ((10, 5, 3, False), "dominante-com-excecoes"),
            ((10, 5, 8, False), "concorrentes"),
            ((4, 3, 2, False), "concorrentes"),
        ]
        for (oc, arq, contra, decl), esperado in casos:
            self.assertEqual(inv.calcular_classificacao(oc, arq, contra, decl), esperado,
                             f"{oc}/{arq}/{contra}/{decl}")


class TesteValidarAchado(Base):
    def validar(self, texto: str) -> dict:
        caminho = self.raiz / "dossie.md"
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(texto, encoding="utf-8")
        return inv.validar_achado(caminho)

    def test_dossie_completo_passa(self):
        dados = self.validar(dossie())
        self.assertEqual(dados["erros"], [])
        self.assertEqual(dados["achados"][0]["calculada"], "convencao")

    def test_campo_ausente_ou_molde_e_erro(self):
        dados = self.validar(dossie().replace("  alcance: src/\n", "  alcance: <alcance>\n"))
        self.assertIn("alcance", dados["achados"][0]["faltando"])
        self.assertTrue(any("alcance" in e for e in dados["erros"]))

    def test_eixo_e_obrigatorio(self):
        dados = self.validar(dossie().replace("  eixo: persistencia e dados\n", "", 1))
        self.assertIn("eixo", dados["achados"][0]["faltando"])

    def test_comando_de_contraexemplo_igual_ao_de_confirmacao(self):
        dados = self.validar(dossie(contra_comando="rg  -n   X  src/"))
        self.assertTrue(any("comando-contraexemplo igual" in e for e in dados["erros"]))

    def test_contraexemplos_sem_quantidade(self):
        dados = self.validar(dossie(contraexemplos="nao procurei"))
        self.assertTrue(any("comecar pela quantidade" in e for e in dados["erros"]))

    def test_classificacao_declarada_acima_da_calculada_e_erro(self):
        dados = self.validar(dossie(ocorrencias=5, arquivos=2, universo=6))
        self.assertEqual(dados["achados"][0]["calculada"], "dominante-com-excecoes")
        self.assertTrue(any("acima da calculada" in e for e in dados["erros"]))

    def test_classificacao_declarada_abaixo_e_apenas_aviso(self):
        dados = self.validar(dossie(classificacao="concorrentes"))
        self.assertEqual(dados["erros"], [])
        self.assertTrue(any("abaixo da calculada" in a for a in dados["avisos"]))

    def test_classificacao_invalida_e_erro(self):
        dados = self.validar(dossie(classificacao="alta"))
        self.assertTrue(any("nao esta entre" in e for e in dados["erros"]))

    def test_fonte_declarativa_habilita_convencao_com_duas_ocorrencias(self):
        dados = self.validar(dossie(ocorrencias=2, arquivos=2, universo=3, declarativa="docs/padroes.md"))
        self.assertEqual(dados["achados"][0]["calculada"], "convencao")

    def test_arquivos_maior_que_ocorrencias_nao_fecha(self):
        dados = self.validar(dossie(ocorrencias=2, arquivos=5, universo=9))
        self.assertTrue(any("nao fecha" in e for e in dados["erros"]))

    def test_evidencia_sem_contagem(self):
        texto = dossie().replace("(8 ocorrencias em 4 arquivos, de 10 comparaveis)", "varios lugares")
        self.assertTrue(any("N ocorrencias em M arquivos" in e for e in self.validar(texto)["erros"]))

    def test_universo_ausente_e_aviso(self):
        texto = dossie().replace("(8 ocorrencias em 4 arquivos, de 10 comparaveis)",
                                 "(8 ocorrencias em 4 arquivos)")
        dados = self.validar(texto)
        self.assertEqual(dados["erros"], [])
        self.assertTrue(any("universo comparavel" in a for a in dados["avisos"]))

    def test_universo_menor_que_arquivos_e_erro(self):
        dados = self.validar(dossie(ocorrencias=8, arquivos=4, universo=3))
        self.assertTrue(any("menor que os 4 arquivos" in e for e in dados["erros"]))

    def test_convencao_que_cobre_menos_da_metade_do_universo_avisa(self):
        dados = self.validar(dossie(ocorrencias=8, arquivos=4, universo=40))
        self.assertTrue(any("confirme se o universo" in a for a in dados["avisos"]))

    def test_cabecalho_incompleto_e_dossie_sem_achado(self):
        dados = self.validar("Frente: x\n\nAchados:\n")
        self.assertTrue(any("Escopo investigado" in e for e in dados["erros"]))
        self.assertTrue(any("Eixos cobertos" in e for e in dados["erros"]))
        self.assertTrue(any("nenhum achado" in e for e in dados["erros"]))


class TesteProcessoEGuardrail(Base):
    def validar(self, texto: str) -> dict:
        caminho = self.raiz / "dossie.md"
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(texto, encoding="utf-8")
        return inv.validar_achado(caminho)

    def test_processo_completo_passa(self):
        dados = self.validar(dossie())
        self.assertEqual(len(dados["processos"]), 1)
        self.assertEqual(dados["processos"][0]["faltando"], [])
        self.assertEqual(dados["processos"][0]["exemplares"], 3)

    def test_campo_de_processo_ausente_e_erro(self):
        texto = dossie().replace("  sequencia: 1. abre 2. escreve 3. fecha\n", "", 1)
        dados = self.validar(texto)
        self.assertIn("sequencia", dados["processos"][0]["faltando"])
        self.assertTrue(any("sequencia" in e for e in dados["erros"]))

    def test_processo_com_um_exemplar_avisa(self):
        dados = self.validar(dossie(exemplares=1))
        self.assertTrue(any("nao ha sequencia comprovada" in a for a in dados["avisos"]))

    def test_dossie_sem_processo_avisa(self):
        texto = dossie().split("Processos observados:")[0]
        dados = self.validar(texto)
        self.assertTrue(any("sem 'Processos observados'" in a for a in dados["avisos"]))

    def test_guardrail_completo_passa(self):
        dados = self.validar(dossie())
        self.assertEqual(len(dados["guardrails"]), 1)
        self.assertEqual(dados["guardrails"][0]["faltando"], [])

    def test_guardrail_sem_contagem_e_erro(self):
        dados = self.validar(dossie(guardrail="escrita por X"))
        self.assertTrue(any("sem contagem visivel" in e for e in dados["erros"]))

    def test_guardrail_sem_desvio_e_erro(self):
        texto = dossie().replace("  desvio reconhecivel: abre conexao propria\n", "", 1)
        dados = self.validar(texto)
        self.assertTrue(any("desvio reconhecivel" in e for e in dados["erros"]))


class TesteCodigosDeSaida(Base):
    def test_stack_ausente_retorna_um(self):
        self.assertEqual(inv.main(["stack", "--raiz", str(self.raiz), "--json"]), 1)

    def test_stack_minima_retorna_zero(self):
        self.stack_minima()
        self.assertEqual(inv.main(["stack", "--raiz", str(self.raiz), "--json"]), 0)

    def test_verificar_com_erro_retorna_dois(self):
        self.stack_minima()
        self.skill("x", com_frontmatter=False)
        self.assertEqual(inv.main(["verificar", "--raiz", str(self.raiz), "--json"]), 2)

    def test_verificar_com_aviso_retorna_um(self):
        self.stack_minima()
        escrever(self.raiz / ".agents" / "references" / "orfa.md", "# Orfa\n\n## Evidencias\n\nx\n")
        self.assertEqual(inv.main(["verificar", "--raiz", str(self.raiz), "--json"]), 1)

    def test_verificar_limpo_retorna_zero(self):
        self.stack_minima()
        self.assertEqual(inv.main(["verificar", "--raiz", str(self.raiz), "--json"]), 0)

    def test_raiz_inexistente_retorna_dois(self):
        self.assertEqual(inv.main(["censo", "--raiz", str(self.raiz / "nao-existe")]), 2)

    def test_censo_retorna_zero(self):
        escrever(self.raiz / "a.py")
        self.assertEqual(inv.main(["censo", "--raiz", str(self.raiz), "--json"]), 0)


class TesteLeituraTolerante(Base):
    def test_arquivo_latin1_nao_quebra(self):
        caminho = self.raiz / "legado.md"
        caminho.write_bytes("# Descrição\n".encode("latin-1"))
        self.assertIn("Descri", inv.ler_texto(caminho))


class TestAuditarComando(Base):
    """Logica de auditoria independente do shell e das ferramentas da maquina."""

    def test_ancora_expoe_sufixo_de_outra_palavra(self):
        # Arrange: base, ancora, caixa e comentario; a variante de binario e ambiental.
        cmd = "grep -rn -E 'id' a.txt | wc -l"
        retornos = [(0, "3", ""), (0, "1", ""), (0, "3", ""), (1, "0", "")]
        with mock.patch.object(Path, "exists", return_value=False), \
             mock.patch.object(inv, "_rodar", side_effect=retornos) as rodar:
            # Act
            dados = inv.auditar_comando(cmd, self.raiz)

        # Assert
        self.assertEqual(dados["numero"], 3)
        chaves = {a["armadilha"] for a in dados["achados"]}
        self.assertIn("ancora", chaves)
        self.assertIn("'(^|[^A-Za-z0-9_])id'", rodar.call_args_list[1].args[0])
        self.assertFalse(dados["inconclusiva"])

    def test_sem_divergencia_nao_gera_alerta(self):
        # Arrange
        cmd = "grep -rn -F 'nao-existe-nada-assim' b.txt | wc -l"
        saida = io.StringIO()
        retornos = [(0, "0", ""), (0, "0", ""), (0, "0", ""), (1, "0", "")]
        with mock.patch.object(Path, "exists", return_value=False), \
             mock.patch.object(inv, "_rodar", side_effect=retornos), contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        self.assertEqual(codigo, 0)
        self.assertEqual(dados["numero"], 0)
        self.assertFalse(dados["inconclusiva"])

    def test_comentario_e_detectado(self):
        # Arrange
        cmd = "grep -rn -E 'checkAccess' c.txt | wc -l"
        saida = io.StringIO()
        retornos = [(0, "3", ""), (0, "3", ""), (0, "3", ""), (0, "2", "")]
        with mock.patch.object(Path, "exists", return_value=False), \
             mock.patch.object(inv, "_rodar", side_effect=retornos), contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        chaves = {a["armadilha"] for a in dados["achados"]}
        self.assertIn("comentario", chaves)
        self.assertEqual(codigo, 1)

    def test_comando_nao_numerico_nao_quebra(self):
        # Arrange
        with mock.patch.object(inv, "_rodar", return_value=(0, "linha", "")):
            # Act
            dados = inv.auditar_comando("cat d.txt", self.raiz)

        # Assert
        self.assertIsNone(dados["numero"])
        self.assertEqual(dados["achados"], [])
        self.assertTrue(dados["inconclusiva"])

    def test_execucao_invalida_ou_sem_controles_nao_retorna_sucesso(self):
        casos = [
            ("comando-inexistente", (127, "", "comando ausente")),
            ("cat d.txt", (0, "linha", "")),
            ("comando-com-erro", (0, "0", "erro intermediario")),
            ("comando-qualquer", (1, "0", "")),
            ("grep -cE 'id' a.txt | python -c 'import sys; print(0); sys.exit(1)'", (1, "0", "")),
            ("grep -cE 'id' a.txt; exit 1", (1, "0", "")),
            ("grep -cE 'id' a.txt\nexit 1", (1, "0", "")),
            ('python -c "import sys; print(0); sys.exit(1)" "|" grep -cE "id"', (1, "0", "")),
            (r'python -c "import sys; print(0); sys.exit(1)" \| grep -cE "id"', (1, "0", "")),
            ('python -c "import sys; print(0); sys.exit(1)" ^| grep -cE "id"', (1, "0", "")),
            ("grep -cE 'id' /dev/null x# 2>/dev/null | python -c 'import sys; print(0); sys.exit(1)'", (1, "0", "")),
            ("python -c 'import sys; print(0); sys.exit(1)' # | grep -cE 'id'", (1, "0", "")),
            ("python -c 'import sys; print(0); sys.exit(1)' $(true | grep -cE 'id' /dev/null)", (1, "0", "")),
            ("python -c 'import sys; print(0); sys.exit(1)' `true | grep -cE 'id' /dev/null`", (1, "0", "")),
            ("echo 0", (0, "0", "")),
        ]
        for cmd, retorno in casos:
            with self.subTest(comando=cmd):
                # Arrange
                saida = io.StringIO()
                with mock.patch.object(inv, "_rodar", return_value=retorno), \
                     contextlib.redirect_stdout(saida):
                    # Act
                    codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd])

                # Assert
                self.assertEqual(codigo, 2)
                self.assertIn("Auditoria inconclusiva", saida.getvalue())
                self.assertNotIn("Sem divergencia", saida.getvalue())

    def test_pipe_literal_real_nao_separa_comandos(self):
        # Arrange: grep e um argumento do Python, sem executar uma pipeline.
        cmd = f'"{sys.executable}" -c "import sys; print(0); sys.exit(1)" "|" grep -cE "id"'
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        self.assertEqual(dados["codigo"], 1)
        self.assertEqual(dados["numero"], 0)
        self.assertEqual(dados["erro"], "")
        self.assertEqual(codigo, 2)
        self.assertTrue(dados["inconclusiva"])

    def test_variante_que_falha_torna_a_auditoria_inconclusiva(self):
        # Arrange
        cmd = "grep -rn -E 'id' a.txt | wc -l"
        retornos = [(0, "3", ""), (127, "", "controle ausente"), (0, "3", ""), (1, "0", "")]
        saida = io.StringIO()
        with mock.patch.object(Path, "exists", return_value=False), \
             mock.patch.object(inv, "_rodar", side_effect=retornos), contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        self.assertEqual(codigo, 2)
        self.assertEqual(dados["numero"], 3)
        self.assertEqual(dados["testes"][0]["erro"], "controle ausente")
        self.assertFalse(dados["testes"][0]["valido"])

    def test_grep_contador_final_sem_correspondencias_e_valido(self):
        for cmd in ("grep -n -cE 'ausente' a.txt", "/usr/bin/grep -n -cE 'ausente' a.txt"):
            with self.subTest(comando=cmd):
                # Arrange
                saida = io.StringIO()
                with mock.patch.object(Path, "exists", return_value=False), \
                     mock.patch.object(inv, "_rodar", return_value=(1, "0", "")), \
                     contextlib.redirect_stdout(saida):
                    # Act
                    codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

                # Assert
                self.assertEqual(codigo, 0)
                self.assertFalse(json.loads(saida.getvalue())["inconclusiva"])

    def test_rodar_preserva_diagnostico_com_codigo_final_zero(self):
        # Arrange
        processo = mock.Mock(returncode=0, stdout="0\n", stderr="erro intermediario\n")
        with mock.patch.object(inv.subprocess, "run", return_value=processo):
            # Act
            resultado = inv._rodar("comando", self.raiz)

        # Assert
        self.assertEqual(resultado, (0, "0", "erro intermediario"))

    def test_timeout_e_diagnostico_de_falha(self):
        # Arrange
        erro = inv.subprocess.TimeoutExpired("comando", 300)
        with mock.patch.object(inv.subprocess, "run", side_effect=erro):
            # Act
            resultado = inv._rodar("comando", self.raiz)

        # Assert
        self.assertEqual(resultado, (-1, "", "<timeout>"))


@unittest.skipUnless(os.name == "posix" and shutil.which("grep") and shutil.which("wc"),
                     "integracoes reais exigem shell POSIX, grep e wc")
class TestAuditarComandoPosix(Base):
    def test_hash_em_argumento_ou_comentario_nao_oculta_erro_final(self):
        casos = [
            f'grep -cE "id" /dev/null x# 2>/dev/null | "{sys.executable}" -c "import sys; print(0); sys.exit(1)"',
            f'"{sys.executable}" -c "import sys; print(0); sys.exit(1)" # | grep -cE "id"',
        ]
        for cmd in casos:
            with self.subTest(comando=cmd):
                # Arrange
                saida = io.StringIO()
                with contextlib.redirect_stdout(saida):
                    # Act
                    codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

                # Assert
                dados = json.loads(saida.getvalue())
                self.assertEqual(dados["codigo"], 1)
                self.assertEqual(dados["numero"], 0)
                self.assertEqual(dados["erro"], "")
                self.assertEqual(codigo, 2)
                self.assertTrue(dados["inconclusiva"])

    def test_ancora_real_separa_id_de_uuid_e_valid(self):
        # Arrange
        escrever(self.raiz / "a.txt", "id = 1\nuuid = 2\nvalid = 3\n")

        # Act
        dados = inv.auditar_comando("grep -rn -E 'id' a.txt | wc -l", self.raiz)

        # Assert
        self.assertEqual(dados["numero"], 3)
        self.assertIn("ancora", {a["armadilha"] for a in dados["achados"]})
        self.assertFalse(dados["inconclusiva"])

    def test_zero_real_de_correspondencias_e_comparacao_valida(self):
        # Arrange
        escrever(self.raiz / "b.txt", "linha\n")

        # Act
        dados = inv.auditar_comando("grep -rn -E 'ausente' b.txt | wc -l", self.raiz)

        # Assert
        self.assertEqual(dados["numero"], 0)
        self.assertEqual(dados["achados"], [])
        self.assertFalse(dados["inconclusiva"])

    def test_comentarios_reais_sao_contados(self):
        # Arrange
        escrever(self.raiz / "c.txt", "checkAccess(a)\n# checkAccess(b)\n# checkAccess(c)\n")

        # Act
        dados = inv.auditar_comando("grep -rn -E 'checkAccess' c.txt | wc -l", self.raiz)

        # Assert
        self.assertIn("comentario", {a["armadilha"] for a in dados["achados"]})
        self.assertFalse(dados["inconclusiva"])

    def test_saida_real_nao_numerica_e_inconclusiva(self):
        # Arrange
        escrever(self.raiz / "d.txt", "linha\n")

        # Act
        dados = inv.auditar_comando("cat d.txt", self.raiz)

        # Assert
        self.assertIsNone(dados["numero"])
        self.assertTrue(dados["inconclusiva"])

    def test_erro_intermediario_real_impede_sucesso_da_pipeline(self):
        # Arrange
        cmd = "(printf 'falha intermediaria\\n' >&2; exit 7) | wc -l"
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        self.assertEqual(dados["codigo"], 0)
        self.assertEqual(dados["numero"], 0)
        self.assertIn("falha intermediaria", dados["erro"])
        self.assertEqual(codigo, 2)

    def test_comando_real_inexistente_e_falha(self):
        # Arrange
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz),
                               "--cmd", "comando-auditoria-inexistente", "--json"])

        # Assert
        self.assertEqual(codigo, 2)
        self.assertTrue(json.loads(saida.getvalue())["erro"])

    def test_erro_final_real_nao_e_confundido_com_grep_sem_correspondencias(self):
        # Arrange
        escrever(self.raiz / "a.txt", "id\n")
        cmd = "grep -cE 'id' a.txt | (printf '0\\n'; exit 1)"
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            # Act
            codigo = inv.main(["auditar-comando", "--raiz", str(self.raiz), "--cmd", cmd, "--json"])

        # Assert
        dados = json.loads(saida.getvalue())
        self.assertEqual(dados["codigo"], 1)
        self.assertEqual(dados["erro"], "")
        self.assertEqual(codigo, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
