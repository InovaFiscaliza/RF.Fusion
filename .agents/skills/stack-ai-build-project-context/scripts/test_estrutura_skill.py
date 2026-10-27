#!/usr/bin/env python3
"""Testes de estrutura da propria skill: frontmatter, ponteiros e orfaos.

Nao testa comportamento de agente. Pega o defeito barato que aparece ao editar
a skill: arquivo renomeado sem atualizar quem o cita, ponteiro que nao resolve
e arquivo que entrou em references/ ou agents/ e nunca e acionado.
"""

import re
import unittest
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1]
NOME = "stack-ai-build-project-context"

PONTEIRO = re.compile(r"`((?:references|agents|assets|scripts)/[\w.-]+\.(?:md|py))`")


def texto(rel: str) -> str:
    return (SKILL / rel).read_text(encoding="utf-8")


class TesteFrontmatter(unittest.TestCase):
    def test_abre_com_frontmatter(self):
        self.assertTrue(texto("SKILL.md").startswith("---\n"))

    def test_name_bate_com_o_diretorio(self):
        self.assertEqual(SKILL.name, NOME)
        self.assertRegex(texto("SKILL.md"), rf"^---\nname: {re.escape(NOME)}\n")

    def test_tem_description(self):
        self.assertRegex(texto("SKILL.md"), r"\ndescription: ")


class TestePonteiros(unittest.TestCase):
    """Todo caminho citado entre crases no SKILL.md precisa existir."""

    def test_ponteiros_do_skill_resolvem(self):
        for alvo in sorted(set(PONTEIRO.findall(texto("SKILL.md")))):
            with self.subTest(alvo=alvo):
                self.assertTrue((SKILL / alvo).is_file(), f"ponteiro morto: {alvo}")

    def test_ponteiros_das_references_resolvem(self):
        for arquivo in sorted(SKILL.glob("references/*.md")):
            conteudo = arquivo.read_text(encoding="utf-8")
            for alvo in sorted(set(PONTEIRO.findall(conteudo))):
                with self.subTest(arquivo=arquivo.name, alvo=alvo):
                    self.assertTrue((SKILL / alvo).is_file(),
                                    f"{arquivo.name} cita {alvo}, que nao existe")


class TesteOrfaos(unittest.TestCase):
    """Arquivo que ninguem aciona custa manutencao e nunca e lido."""

    def test_toda_reference_e_citada(self):
        s = texto("SKILL.md")
        for arquivo in sorted(SKILL.glob("references/*.md")):
            with self.subTest(arquivo=arquivo.name):
                self.assertIn(arquivo.name, s, f"{arquivo.name} nunca e citada no SKILL.md")

    def test_todo_agente_e_citado(self):
        s = texto("SKILL.md") + "".join(p.read_text(encoding="utf-8")
                                        for p in SKILL.glob("references/*.md"))
        for arquivo in sorted(SKILL.glob("agents/*.md")):
            with self.subTest(arquivo=arquivo.name):
                self.assertIn(arquivo.name, s,
                              f"{arquivo.name} nunca e citado no SKILL.md nem nas references")

    def test_todo_template_e_citado(self):
        s = texto("SKILL.md") + "".join(p.read_text(encoding="utf-8")
                                        for p in SKILL.glob("references/*.md"))
        for arquivo in sorted(SKILL.glob("assets/*.md")):
            with self.subTest(arquivo=arquivo.name):
                self.assertIn(arquivo.name, s,
                              f"{arquivo.name} nunca e citado no SKILL.md nem nas references")


if __name__ == "__main__":
    unittest.main()
