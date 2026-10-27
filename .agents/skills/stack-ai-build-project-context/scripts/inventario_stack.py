#!/usr/bin/env python3
"""Inventario da stack de IA e censo da codebase de destino.

Subcomandos:
  stack      estado das pecas da stack no repositorio de destino
  censo      levantamento barato e deterministico da superficie da codebase
  verificar  checagens mecanicas dos artefatos escritos pela skill
  validar-achado  campos, contagens e faixa de confianca de um dossie de investigacao

O script nao avalia conteudo. Ele responde onde as coisas estao e o que esta
mecanicamente quebrado; julgamento de conteudo pertence ao Gauntlet Loop.
"""

from __future__ import annotations

import argparse
import subprocess
import json
import os
import re
import shlex
import sys
from collections import Counter
from pathlib import Path

DIRS_IGNORADOS = {
    ".git", ".hg", ".svn", "node_modules", "vendor", "venv", ".venv",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".tox",
    "dist", "build", "target", "out", "bin", "obj", ".gradle", ".idea",
    ".vscode-test", "coverage", ".next", ".nuxt", ".terraform", "bower_components",
}

MANIFESTOS = {
    "package.json", "pyproject.toml", "setup.py", "requirements.txt", "Pipfile",
    "go.mod", "Cargo.toml", "composer.json", "Gemfile", "pom.xml",
    "build.gradle", "build.gradle.kts", "build.sbt", "mix.exs", "pubspec.yaml",
    "Makefile", "CMakeLists.txt", "Dockerfile", "docker-compose.yml",
}

CONFIGS_QUALIDADE = {
    ".editorconfig", ".eslintrc", ".eslintrc.json", ".eslintrc.js", "eslint.config.js",
    ".prettierrc", ".prettierrc.json", "ruff.toml", ".flake8", "setup.cfg",
    ".rubocop.yml", "phpcs.xml", "phpcs.xml.dist", "phpstan.neon", "psalm.xml",
    ".golangci.yml", ".golangci.yaml", "tox.ini", ".pre-commit-config.yaml",
    ".gitattributes", "checkstyle.xml", ".stylelintrc",
}

CI_CAMINHOS = [
    ".github/workflows", ".gitlab-ci.yml", "Jenkinsfile", "azure-pipelines.yml",
    ".circleci/config.yml", ".drone.yml", "bitbucket-pipelines.yml", ".travis.yml",
]

LIMITE_LEITURA = 1_048_576  # arquivo maior que isto e amostrado, nao lido inteiro

BOMS = (
    (b"\x00\x00\xfe\xff", "utf-32be"),
    (b"\xff\xfe\x00\x00", "utf-32le"),
    (b"\xef\xbb\xbf", "utf-8-bom"),
    (b"\xfe\xff", "utf-16be"),
    (b"\xff\xfe", "utf-16le"),
)

CODEC_DE_LEITURA = {
    "ascii": "ascii", "utf-8": "utf-8", "utf-8-bom": "utf-8-sig",
    "utf-16le": "utf-16", "utf-16be": "utf-16",
    "utf-16le-sem-bom": "utf-16-le", "utf-16be-sem-bom": "utf-16-be",
    "utf-32le": "utf-32", "utf-32be": "utf-32",
    "8-bits": "latin-1", "vazio": "ascii",
}

# Familia de busca: codificacoes diferentes que exigem o mesmo comando.
FAMILIA_DE_BUSCA = {
    "ascii": "texto", "utf-8": "texto", "utf-8-bom": "texto", "vazio": "texto",
    "utf-16le": "utf-16-bom", "utf-16be": "utf-16-bom",
    "utf-16le-sem-bom": "utf-16-sem-bom", "utf-16be-sem-bom": "utf-16-sem-bom",
    "utf-32le": "utf-32", "utf-32be": "utf-32",
    "8-bits": "8-bits", "binario": "binario",
}

COMANDO_POR_FAMILIA = {
    "texto": "rg -n <padrao> <escopo>  |  sem rg: grep -rn --binary-files=text <padrao> <escopo>",
    "utf-16-bom": "rg -n <padrao> <escopo>  |  sem rg: iconv -f UTF-16 -t UTF-8 <arquivo> | grep -n <padrao>",
    "utf-16-sem-bom": "rg -n -E utf-16le <padrao> <escopo>  |  sem rg: iconv -f UTF-16LE -t UTF-8 <arquivo> | grep -n <padrao>",
    "utf-32": "iconv -f UTF-32 -t UTF-8 <arquivo> | rg -n <padrao>",
    "8-bits": "rg -n -E latin1 <padrao> <escopo>  |  sem rg: LC_ALL=C grep -rn --binary-files=text <padrao> <escopo>",
    "binario": "nao pesquisavel como texto; converta com iconv antes de contar",
}

NOTA_ORDEM_GREP = (
    "atencao: no grep vale a ultima opcao passada, e -I anula -a. "
    "'grep -aRIl' pula arquivo tratado como binario e devolve contagem menor sem avisar. "
    "Use --binary-files=text, que nao depende de ordem."
)

# Sinais de dependencia: manifesto nao e a unica forma de declarar dependencia, e em
# sistema legado quase nunca e a usada.
RE_COMPONENTE = re.compile(r"""(?:create_?object|activexobject)\s*\(\s*["']([^"']{2,80})["']""", re.IGNORECASE)
RE_INCLUDE_MARCACAO = re.compile(r"""#\s*include\s+(?:file|virtual)\s*=\s*["']([^"']{1,120})["']""", re.IGNORECASE)
RE_INCLUDE_CHAMADA = re.compile(
    r"""\b(?:include|require)(?:_once)?\s*\(?\s*["']([^"']{1,120}\.(?:php|inc|asp|lib|jsp|cfm))["']""",
    re.IGNORECASE,
)
RE_IMPORT_MODULO = re.compile(r"""(?:from|require)\s*\(?\s*["']([a-z0-9@][^"']{1,60})["']""", re.IGNORECASE)
RE_DRIVER = re.compile(r"""\b(?:provider|driver)\s*=\s*\{?\s*([A-Za-z0-9 ._+-]{2,40})""", re.IGNORECASE)
RE_URI_BANCO = re.compile(
    r"""\b(jdbc:[a-z0-9]+|mongodb(?:\+srv)?://|postgres(?:ql)?://|mysql://|sqlserver://|oracle:thin)""",
    re.IGNORECASE,
)
RE_LIB_VERSIONADA = re.compile(r"^(.{1,60}?)[-._]v?(\d+(?:\.\d+){1,3})(?:\.min|\.pack)?\.(js|css)$", re.IGNORECASE)

# Extensoes que nao sinalizam formato de ferramenta por si so. O que sobrar delas, com
# volume, e candidato a dependencia proprietaria e entra no relatorio para inspecao.
EXTENSOES_COMUNS = {
    ".md", ".txt", ".rst", ".adoc", ".json", ".yml", ".yaml", ".toml", ".ini", ".cfg", ".xml",
    ".html", ".htm", ".css", ".js", ".ts", ".jsx", ".tsx", ".py", ".rb", ".php", ".java",
    ".cs", ".go", ".rs", ".c", ".h", ".cpp", ".hpp", ".sh", ".bat", ".ps1", ".sql", ".pl",
    ".gif", ".png", ".jpg", ".jpeg", ".svg", ".ico", ".bmp", ".webp", ".pdf", ".zip", ".gz",
    ".lock", ".log", ".csv", ".tsv", ".env", ".gitignore", ".gitattributes", "<sem extensao>",
    ".asp", ".aspx", ".asa", ".jsp", ".cfm", ".vbs", ".vb", ".xsl", ".xslt",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".rtf", ".tif", ".tiff",
}
MINIMO_FORMATO_PROPRIO = 3  # abaixo disso a extensao nao diz nada sobre dependencia

PISTAS_TESTE = {"test", "tests", "spec", "specs", "__tests__", "testing"}
PISTAS_MIGRACAO = {"migration", "migrations", "migrate", "ddl", "schema"}
MINIMO_PARA_ALERTA = 5  # abaixo disso o catalogo e pequeno demais para a comparacao dizer algo
NOMES_AGENTS = ("AGENTS.md", "AGENT.md")
PONTEIROS = ("CLAUDE.md", ".github/copilot-instructions.md", ".cursorrules", "GEMINI.md")

RE_FRONTMATTER = re.compile(r"^---\s*\n(.*?)\n---\s*(\n|$)", re.DOTALL)
RE_DESCRICAO = re.compile(r"^description:\s*(.*?)(?=\n[A-Za-z_-]+:\s|\Z)", re.DOTALL | re.MULTILINE)
RE_CAMPO = re.compile(r"^(name|description)\s*:\s*(.*)$", re.MULTILINE)
RE_LINK_AGENTS = re.compile(r"`?(\.agents/[A-Za-z0-9_./-]+\.md)`?")
RE_TITULO_EVIDENCIAS = re.compile(r"^#{1,6}\s+Evid[eê]ncias\s*$", re.MULTILINE | re.IGNORECASE)
# Skill marcada assim nao entra no contexto do modelo: so o humano digitando /nome, ou
# outro artefato mandando ler o SKILL.md dela, a alcanca. A descricao deixa de ser custo
# permanente e passa a ser custo sob demanda.
RE_USER_INVOKED = re.compile(r"^disable-model-invocation:\s*true\s*$", re.MULTILINE | re.IGNORECASE)
# Frase de gatilho so serve a quem e alcancado pelo modelo. Numa skill user-invoked ela e
# ruido: o humano le lista de comando, e o modelo nem ve a descricao.
GATILHOS_DE_MODELO = (
    'use quando', 'use com "', 'ative com', 'ativacao: /', 'digite "', 'digitar "',
    'pedir "', 'quando o pedido', 'dispare',
)


def ler_texto(caminho: Path) -> str:
    """Le texto tolerando encoding desconhecido, comum em repositorio legado."""
    dados = caminho.read_bytes()
    for codec in ("utf-8", "latin-1"):
        try:
            return dados.decode(codec)
        except UnicodeDecodeError:
            continue
    return dados.decode("utf-8", errors="replace")


def caminhar(raiz: Path):
    for pasta, subdirs, arquivos in os.walk(raiz):
        subdirs[:] = [d for d in subdirs if d not in DIRS_IGNORADOS and d != ".git"]
        for arquivo in arquivos:
            yield Path(pasta) / arquivo


def estimar_tokens(texto: str) -> int:
    """Estimativa por caracteres. Serve para comparar destinos, nao para faturar."""
    return round(len(texto) / 4)


def extrair_descricao(texto: str) -> str:
    """Descricao inteira do frontmatter, inclusive escalar de bloco.

    A descricao e custo permanente: ela entra no contexto em toda tarefa,
    esteja a skill sendo usada ou nao. Por isso e medida por inteiro.
    """
    achado = RE_FRONTMATTER.match(texto)
    if not achado:
        return ""
    bruto = RE_DESCRICAO.search(achado.group(1))
    if not bruto:
        return ""
    valor = bruto.group(1).strip()
    if valor[:2] in (">-", "|-") or valor[:1] in (">", "|"):
        valor = valor.lstrip(">|-").strip()
    return " ".join(valor.split())


def frontmatter(texto: str) -> dict:
    achado = RE_FRONTMATTER.match(texto)
    if not achado:
        return {}
    bloco = achado.group(1)
    campos = {}
    for chave, valor in RE_CAMPO.findall(bloco):
        valor = valor.strip()
        if valor in (">", "|", ">-", "|-"):
            valor = "<bloco>"
        campos[chave] = valor.strip("'\"")
    return campos


# --------------------------------------------------------------------------- stack

def coletar_stack(raiz: Path) -> dict:
    agents = next((raiz / n for n in NOMES_AGENTS if (raiz / n).is_file()), None)
    dir_refs = raiz / ".agents" / "references"
    dir_skills = raiz / ".agents" / "skills"

    refs = []
    if dir_refs.is_dir():
        for arquivo in sorted(dir_refs.rglob("*.md")):
            texto = ler_texto(arquivo)
            titulo = next((l.lstrip("# ").strip() for l in texto.splitlines() if l.startswith("#")), "")
            refs.append({
                "arquivo": arquivo.relative_to(raiz).as_posix(),
                "titulo": titulo,
                "linhas": len(texto.splitlines()),
                "tem_evidencias": bool(RE_TITULO_EVIDENCIAS.search(texto)),
            })

    skills = []
    if dir_skills.is_dir():
        # rglob cobre suites, em que um diretorio agrupa varias sub-skills.
        for skill_md in sorted(dir_skills.rglob("SKILL.md")):
            texto_skill = ler_texto(skill_md)
            campos = frontmatter(texto_skill)
            descricao = extrair_descricao(texto_skill)
            bloco = RE_FRONTMATTER.match(texto_skill)
            user_invoked = bool(bloco and RE_USER_INVOKED.search(bloco.group(1)))
            skills.append({
                "diretorio": skill_md.parent.relative_to(dir_skills).as_posix(),
                "arquivo": skill_md.relative_to(raiz).as_posix(),
                "name": campos.get("name", ""),
                "tem_descricao": bool(campos.get("description")),
                "user_invoked": user_invoked,
                "descricao": descricao,
                "descricao_chars": len(descricao),
                "descricao_tokens": estimar_tokens(descricao),
            })

    ponteiros = [p for p in PONTEIROS if (raiz / p).is_file()]
    indices = [
        c.relative_to(raiz).as_posix()
        for c in (dir_skills / "README.md", raiz / ".agents" / "README.md", dir_refs / "README.md")
        if c.is_file()
    ]

    pecas = {
        "agents_md": agents.relative_to(raiz).as_posix() if agents else None,
        "references": dir_refs.relative_to(raiz).as_posix() if dir_refs.is_dir() else None,
        "skills": dir_skills.relative_to(raiz).as_posix() if dir_skills.is_dir() else None,
    }
    presentes = sum(1 for v in pecas.values() if v)
    estado = "minima_presente" if presentes == 3 else ("ausente" if presentes == 0 else "parcial")
    texto_agents = ler_texto(agents) if agents else ""
    linhas_agents = len(texto_agents.splitlines()) if agents else 0
    if estado == "minima_presente" and (refs or skills or linhas_agents > 20):
        estado = "povoada"

    tokens_agents = estimar_tokens(texto_agents)
    tokens_descricoes = sum(s["descricao_tokens"] for s in skills)
    # Eixo de invocacao: so a descricao de skill alcancavel pelo modelo entra no contexto de
    # toda tarefa. Somar as duas categorias superestima o piso permanente.
    tokens_desc_permanentes = sum(s["descricao_tokens"] for s in skills if not s["user_invoked"])
    tokens_desc_sob_demanda = tokens_descricoes - tokens_desc_permanentes
    model_invocadas = [s for s in skills if not s["user_invoked"]]
    tokens_refs = sum(estimar_tokens(ler_texto(raiz / r["arquivo"])) for r in refs)

    return {
        "raiz": str(raiz),
        "estado": estado,
        "pecas": pecas,
        "faltando": [k for k, v in pecas.items() if not v],
        "agents_md_linhas": linhas_agents,
        "orcamento": {
            "agents_md_tokens": tokens_agents,
            "descricoes_tokens": tokens_descricoes,
            "descricoes_skills": len(skills),
            "descricoes_tokens_permanentes": tokens_desc_permanentes,
            "descricoes_tokens_sob_demanda": tokens_desc_sob_demanda,
            "descricoes_model_invocadas": len(model_invocadas),
            "descricoes_user_invocadas": len(skills) - len(model_invocadas),
            "permanente_tokens": tokens_agents + tokens_desc_permanentes,
            "references_tokens": tokens_refs,
            # Catalogo pequeno sempre "vence" um AGENTS.md recem instalado, e avisar ali
        # e ruido. O sinal que interessa e o catalogo que cresceu sem ninguem medir.
        "descricoes_acima_do_agents": tokens_desc_permanentes > tokens_agents and len(model_invocadas) >= MINIMO_PARA_ALERTA,
        },
        "ponteiros": ponteiros,
        "indices": indices,
        "references": refs,
        "skills": skills,
    }


# --------------------------------------------------------------- codificacao

def detectar_codificacao(dados: bytes) -> str:
    """Rotula a codificacao de um arquivo a partir de seus bytes.

    Sufixo nao decide codificacao: o mesmo `.sql` aparece em UTF-16LE com BOM e
    em ISO-8859 no mesmo repositorio. Quem decide sao os bytes.
    """
    if not dados:
        return "vazio"
    for bom, rotulo in BOMS:
        if dados.startswith(bom):
            return rotulo
    if b"\x00" in dados:
        # UTF-16 sem BOM alterna byte nulo em uma das paridades; qualquer outra
        # distribuicao de nulos e binario de verdade.
        pares = dados[: len(dados) - len(dados) % 2]
        metade = len(pares) // 2
        impares = sum(1 for i in range(1, len(pares), 2) if pares[i] == 0)
        parespos = sum(1 for i in range(0, len(pares), 2) if pares[i] == 0)
        if metade:
            if impares > metade * 0.8 and parespos < metade * 0.2:
                return "utf-16le-sem-bom"
            if parespos > metade * 0.8 and impares < metade * 0.2:
                return "utf-16be-sem-bom"
        return "binario"
    try:
        dados.decode("ascii")
        return "ascii"
    except UnicodeDecodeError:
        pass
    try:
        dados.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as erro:
        # sequencia cortada no fim da amostra nao decide a codificacao do arquivo
        if erro.start >= len(dados) - 3:
            try:
                dados[: erro.start].decode("utf-8")
                return "utf-8"
            except UnicodeDecodeError:
                pass
    return "8-bits"


def detectar_terminacao(dados: bytes, codificacao: str) -> str:
    codec = CODEC_DE_LEITURA.get(codificacao)
    if codec is None:
        return "n/a"
    texto = dados.decode(codec, errors="replace")
    crlf = texto.count("\r\n")
    lf = texto.count("\n") - crlf
    cr = texto.count("\r") - crlf
    presentes = [nome for nome, qtd in (("crlf", crlf), ("lf", lf), ("cr", cr)) if qtd]
    if not presentes:
        return "sem quebra"
    return presentes[0] if len(presentes) == 1 else "misto"


def busca_de(codificacao: str) -> str:
    return COMANDO_POR_FAMILIA[FAMILIA_DE_BUSCA.get(codificacao, "texto")]


# ASCII e arquivo vazio nao contradizem nenhum padrao: cabem em UTF-8 e em
# qualquer codificacao de 8 bits. Contar como excecao inventaria divergencia.
NEUTRAS = {"ascii", "vazio"}


def perfilar_codificacao(perfis: dict, top: int) -> dict:
    """Consolida o padrao de codificacao por extensao, suas excecoes reais e como buscar em cada uma.

    O padrao sai dos arquivos que revelam codificacao. Cobertura e medida sobre
    esses, nao sobre o total: 96% de 1.401 arquivos decisivos informa mais que
    86% de 1.567, em que 166 nada dizem.
    """
    grupos = []
    for ext, perfil in perfis.items():
        codificacoes = perfil["codificacoes"]
        total = sum(codificacoes.values())
        decisivas = Counter({c: n for c, n in codificacoes.items() if c not in NEUTRAS})
        neutras = total - sum(decisivas.values())
        if decisivas:
            padrao, qtd = decisivas.most_common(1)[0]
            cobertura = round(qtd / sum(decisivas.values()), 3)
        else:
            padrao, qtd, cobertura = "ascii", total, 1.0
        grupos.append({
            "extensao": ext,
            "arquivos": total,
            "decisivos": sum(decisivas.values()) or total,
            "neutros": neutras if decisivas else 0,
            "padrao": padrao,
            "cobertura": cobertura,
            "terminacao": perfil["terminacoes"].most_common(1)[0][0],
            "busca": busca_de(padrao),
            "excecoes": [
                {
                    "codificacao": cod,
                    "arquivos": n,
                    "exemplos": perfil["exemplos"][cod][:3],
                    "busca": busca_de(cod),
                }
                for cod, n in decisivas.most_common()[1:]
            ],
        })
    grupos.sort(key=lambda g: (-g["arquivos"], g["extensao"]))

    familias = sorted({FAMILIA_DE_BUSCA.get(c, "texto")
                       for p in perfis.values() for c in p["codificacoes"]
                       if c not in NEUTRAS})
    return {
        "por_extensao": grupos[:top],
        "extensoes_omitidas": max(0, len(grupos) - top),
        "familias_de_busca": [{"familia": f, "comando": COMANDO_POR_FAMILIA[f]} for f in familias],
        "repositorio_misto": len([f for f in familias if f != "binario"]) > 1,
    }


# --------------------------------------------------------------------------- censo

LIMITE_VARREDURA = 300_000  # caracteres por arquivo na busca por sinais de dependencia


def texto_da_amostra(amostra: bytes, codificacao: str) -> str:
    """Decodifica a amostra ja lida para varredura de sinais. Nunca levanta."""
    if codificacao in ("binario", "vazio"):
        return ""
    codec = CODEC_DE_LEITURA.get(codificacao, "latin-1")
    try:
        return amostra.decode(codec, errors="replace")[:LIMITE_VARREDURA]
    except (LookupError, UnicodeError):
        return ""


def novos_sinais() -> dict:
    return {
        "componentes": Counter(), "includes": Counter(), "modulos": Counter(),
        "drivers": Counter(), "bibliotecas": {}, "exemplos": {},
    }


def coletar_sinais(sinais: dict, rel: str, nome: str, texto: str) -> None:
    """Sinais de dependencia declarados no proprio codigo, nao em manifesto."""
    lib = RE_LIB_VERSIONADA.match(nome)
    if lib:
        chave = f"{lib.group(1).lower()} {lib.group(2)}"
        sinais["bibliotecas"].setdefault(chave, rel)

    if not texto:
        return

    def registrar(balde: str, valor: str) -> None:
        valor = valor.strip()
        if not valor:
            return
        sinais[balde][valor] += 1
        sinais["exemplos"].setdefault(f"{balde}:{valor}", rel)

    for achado in RE_COMPONENTE.findall(texto):
        registrar("componentes", achado)
    for achado in RE_INCLUDE_MARCACAO.findall(texto):
        registrar("includes", achado)
    for achado in RE_INCLUDE_CHAMADA.findall(texto):
        registrar("includes", achado)
    for achado in RE_DRIVER.findall(texto):
        registrar("drivers", achado)
    for achado in RE_URI_BANCO.findall(texto):
        registrar("drivers", achado)
    if nome.lower().endswith((".js", ".ts", ".jsx", ".tsx", ".mjs")):
        for achado in RE_IMPORT_MODULO.findall(texto):
            if not achado.startswith((".", "/")):
                registrar("modulos", achado.split("/")[0] if not achado.startswith("@") else achado)


def resumir_sinais(sinais: dict, extensoes: Counter, top: int) -> dict:
    formatos = [
        {"extensao": ext, "arquivos": qtd}
        for ext, qtd in extensoes.most_common()
        if ext not in EXTENSOES_COMUNS and qtd >= MINIMO_FORMATO_PROPRIO
    ]
    def listar(balde: str) -> list:
        return [
            {"nome": nome, "ocorrencias": qtd, "exemplo": sinais["exemplos"].get(f"{balde}:{nome}", "")}
            for nome, qtd in sinais[balde].most_common(top)
        ]
    return {
        "componentes": listar("componentes"),
        "includes": listar("includes"),
        "modulos": listar("modulos"),
        "drivers": listar("drivers"),
        "bibliotecas": [{"nome": n, "exemplo": c} for n, c in sorted(sinais["bibliotecas"].items())][:top],
        "formatos_proprios": formatos[:top],
    }


def coletar_censo(raiz: Path, top: int) -> dict:
    extensoes: Counter = Counter()
    por_topo: Counter = Counter()
    bytes_topo: Counter = Counter()
    manifestos, qualidade, testes, migracoes, documentos = set(), set(), set(), set(), []
    perfis: dict = {}
    sinais = novos_sinais()
    total = 0

    for arquivo in caminhar(raiz):
        try:
            rel = arquivo.relative_to(raiz)
        except ValueError:
            continue
        total += 1
        ext = arquivo.suffix.lower() or "<sem extensao>"
        extensoes[ext] += 1
        topo = rel.parts[0] if len(rel.parts) > 1 else "<raiz>"
        por_topo[topo] += 1
        try:
            bytes_topo[topo] += arquivo.stat().st_size
        except OSError:
            pass

        try:
            with arquivo.open("rb") as fh:
                amostra = fh.read(LIMITE_LEITURA)
        except OSError:
            amostra = None
        if amostra is not None:
            codificacao = detectar_codificacao(amostra)
            perfil = perfis.setdefault(ext, {
                "codificacoes": Counter(), "terminacoes": Counter(), "exemplos": {},
            })
            perfil["codificacoes"][codificacao] += 1
            perfil["terminacoes"][detectar_terminacao(amostra, codificacao)] += 1
            exemplos = perfil["exemplos"].setdefault(codificacao, [])
            if len(exemplos) < 3:
                exemplos.append(rel.as_posix())
            coletar_sinais(sinais, rel.as_posix(), arquivo.name, texto_da_amostra(amostra, codificacao))

        if arquivo.name in MANIFESTOS:
            manifestos.add(rel.as_posix())
        if arquivo.name in CONFIGS_QUALIDADE or arquivo.name.startswith(".eslintrc"):
            qualidade.add(rel.as_posix())
        partes = {p.lower() for p in rel.parts[:-1]}
        pasta_rel = "/".join(rel.parts[:-1]) or "<raiz>"
        if partes & PISTAS_TESTE:
            testes.add(pasta_rel)
        # Migracao precisa de nome de pasta explicito ou de DDL de fato no diretorio;
        # so o nome "scripts" produz falso positivo em qualquer repositorio.
        if partes & PISTAS_MIGRACAO or ext == ".sql":
            migracoes.add(pasta_rel)
        if ext in (".md", ".rst", ".adoc") and len(rel.parts) <= 3:
            documentos.append(rel.as_posix())

    ci = [c for c in CI_CAMINHOS if (raiz / c).exists()]

    return {
        "raiz": str(raiz),
        "arquivos_examinados": total,
        "extensoes": extensoes.most_common(top),
        "diretorios_topo": [
            {"nome": nome, "arquivos": qtd, "bytes": bytes_topo.get(nome, 0)}
            for nome, qtd in por_topo.most_common(top)
        ],
        "manifestos": sorted(manifestos),
        "configuracao_qualidade": sorted(qualidade),
        "integracao_continua": ci,
        "diretorios_de_teste": sorted(testes),
        "diretorios_de_migracao": sorted(migracoes)[:top],
        "documentacao": sorted(documentos)[:top],
        "codificacao": perfilar_codificacao(perfis, top),
        "dependencias": resumir_sinais(sinais, extensoes, top),
    }


# ----------------------------------------------------------------------- verificar

def verificar(raiz: Path) -> dict:
    erros, avisos = [], []
    estado = coletar_stack(raiz)

    dir_skills = raiz / ".agents" / "skills"
    if dir_skills.is_dir():
        for topo in sorted(p for p in dir_skills.iterdir() if p.is_dir()):
            # Diretorio de topo pode ser uma skill, uma suite de sub-skills ou uma
            # pasta auxiliar. Sem SKILL.md em nenhum nivel, avisa em vez de errar.
            if not any(topo.rglob("SKILL.md")):
                avisos.append(
                    f"{topo.relative_to(raiz).as_posix()}: diretorio sem SKILL.md em nenhum nivel"
                )
        for skill_md in sorted(dir_skills.rglob("SKILL.md")):
            skill_dir = skill_md.parent
            campos = frontmatter(ler_texto(skill_md))
            if not campos:
                erros.append(f"{skill_md.relative_to(raiz).as_posix()}: frontmatter ausente ou invalido")
                continue
            if not campos.get("name"):
                # Parte dos harnesses deriva o nome do diretorio; ausencia e risco, nao quebra.
                avisos.append(f"{skill_md.relative_to(raiz).as_posix()}: campo name ausente")
            elif campos["name"] != skill_dir.name:
                erros.append(
                    f"{skill_md.relative_to(raiz).as_posix()}: name '{campos['name']}' diverge do diretorio '{skill_dir.name}'"
                )
            if not campos.get("description"):
                erros.append(f"{skill_md.relative_to(raiz).as_posix()}: campo description ausente")

    fontes = [raiz / p for p in NOMES_AGENTS]
    if (raiz / ".agents").is_dir():
        fontes += sorted((raiz / ".agents").rglob("*.md"))
    referenciados = set()
    for arquivo in fontes:
        if not arquivo.is_file():
            continue
        for alvo in RE_LINK_AGENTS.findall(ler_texto(arquivo)):
            referenciados.add(alvo)
            if not (raiz / alvo).exists():
                erros.append(f"{arquivo.relative_to(raiz).as_posix()}: aponta para '{alvo}', que nao existe")

    orc = estado["orcamento"]
    if orc["descricoes_acima_do_agents"]:
        avisos.append(
            f"orcamento: {orc['descricoes_model_invocadas']} descricoes model-invocadas custam "
            f"~{orc['descricoes_tokens_permanentes']} tokens permanentes contra ~{orc['agents_md_tokens']} do "
            f"AGENTS.md; conhecimento em skill nao economiza contexto"
        )

    # Eixo de invocacao. Skill user-invoked e alcancada pelo humano digitando /nome ou por um
    # artefato que mande ler o SKILL.md dela pelo caminho. O que nao funciona e mandar o agente
    # ativa la pelo nome: sem description carregada, o modelo nao sabe que ela existe.
    nomes_user = {s["name"]: s["arquivo"] for s in estado["skills"] if s["user_invoked"] and s["name"]}
    if nomes_user:
        alvo = "|".join(re.escape(n) for n in sorted(nomes_user, key=len, reverse=True))
        re_ativa = re.compile(
            # Sem cruzar fronteira de frase: "nao e acionada sozinha. A `x`" nao e ordem de ativar.
            r"\b(ativ\w+|acion\w+|invoq\w+|invoc\w+|dispar\w+|cham\w+)\b[^\n.;:!?]{0,24}?"
            r"[`'\"]?(" + alvo + r")[`'\"]?(?![\w/-])",
            re.IGNORECASE)
        for arquivo in fontes:
            if not arquivo.is_file():
                continue
            for numero, linha in enumerate(ler_texto(arquivo).splitlines(), 1):
                if "SKILL.md" in linha:
                    continue
                achado = re_ativa.search(linha)
                if not achado:
                    continue
                nome = achado.group(2)
                if f"/{nome}" in linha or f"skills/{nome}" in linha:
                    continue
                avisos.append(
                    f"{arquivo.relative_to(raiz).as_posix()}:{numero}: manda ativar '{nome}' pelo nome, e ela e "
                    f"user-invoked; sem description no contexto o modelo nao a ve. Aponte o caminho: "
                    f"`{nomes_user[nome]}`"
                )

    for skill in estado["skills"]:
        if not skill["user_invoked"]:
            continue
        desc = skill["descricao"].lower()
        gatilho = next((g for g in GATILHOS_DE_MODELO if g in desc), None)
        if gatilho:
            avisos.append(
                f"{skill['arquivo']}: description de skill user-invoked com frase de gatilho "
                f"({gatilho!r}); ela nao dispara o modelo e vira ruido para quem le"
            )

    for ref in estado["references"]:
        if ref["arquivo"].endswith("README.md"):
            continue
        if not ref["tem_evidencias"]:
            avisos.append(f"{ref['arquivo']}: sem secao de evidencias")
        if ref["arquivo"] not in referenciados:
            avisos.append(f"{ref['arquivo']}: nenhum artefato da stack aponta para esta reference")
        if ref["linhas"] > 300 and "## Sumario" not in ler_texto(raiz / ref["arquivo"]):
            avisos.append(f"{ref['arquivo']}: {ref['linhas']} linhas sem sumario no topo")

    return {"raiz": str(raiz), "erros": erros, "avisos": avisos, "estado": estado["estado"]}


# ------------------------------------------------------------- validar achado

CAMPOS_DO_ACHADO = (
    "eixo", "evidencia", "comando", "comando-contraexemplo", "contraexemplos",
    "fonte-declarativa", "alcance", "classificacao",
)
CAMPOS_DO_PROCESSO = (
    "arquivos que participam", "sequencia", "obrigatorio", "exemplares", "recorrencia",
)
CAMPOS_DO_GUARDRAIL = ("desvio reconhecivel", "comando")
CABECALHO_DO_DOSSIE = (
    "Frente", "Escopo investigado", "Eixos cobertos", "Exemplares lidos por inteiro",
    "Universo examinado",
)
# Da mais forte para a mais fraca: declarar acima da calculada e erro, abaixo e aviso.
CLASSIFICACOES = ("convencao", "dominante-com-excecoes", "concorrentes", "isolado")
SECOES_DE_ITEM = {
    "achados": "achados",
    "processos observados": "processos",
    "candidatos a guardrail": "guardrails",
}
RE_CONTAGEM = re.compile(
    r"\((\d+)\s+ocorrencias?\s+em\s+(\d+)\s+arquivos?(?:,\s*de\s+(\d+)\s+compar\w+)?\)",
    re.IGNORECASE,
)
RE_CONTAGEM_GUARDRAIL = re.compile(r"\((\d+)\s+de\s+(\d+)", re.IGNORECASE)
RE_ACHADO = re.compile(r"^-\s*([A-Za-z0-9_]+)\s*:\s*(.*)$")
RE_CAMPO_ACHADO = re.compile(r"^\s+([a-z][a-z -]*[a-z])\s*:\s*(.*)$")
RE_SECAO = re.compile(r"^([A-Z][^:]*):\s*(.*)$")


def nao_preenchido(valor: str) -> bool:
    """Campo que voltou como o proprio molde, em vez de resposta."""
    valor = valor.strip()
    return not valor or (valor.startswith("<") and valor.endswith(">"))


def normalizar_comando(valor: str) -> str:
    return " ".join(valor.split())


def calcular_classificacao(ocorrencias: int, arquivos: int, contraexemplos: int,
                           declarativa: bool = False) -> str:
    """Aplica a regra de protocolo-investigacao.md. A faixa e calculada, nunca declarada."""
    if declarativa and contraexemplos == 0 and ocorrencias >= 2:
        return "convencao"
    if ocorrencias < 3:
        return "isolado"
    casos = ocorrencias + contraexemplos
    divergencia = contraexemplos / casos if casos else 1.0
    if divergencia <= 0.05 and ocorrencias >= 5 and arquivos >= 3:
        return "convencao"
    if divergencia < 0.30:
        return "dominante-com-excecoes"
    return "concorrentes"


def separar_dossie(texto: str) -> tuple[dict, dict]:
    cabecalho: dict = {}
    itens: dict = {"achados": [], "processos": [], "guardrails": []}
    atual, balde, secao = None, None, ""
    for linha in texto.splitlines():
        if not linha.strip():
            continue
        campo = RE_CAMPO_ACHADO.match(linha)
        if campo and atual is not None:
            atual["campos"][campo.group(1)] = campo.group(2).strip()
            continue
        if balde:
            item = RE_ACHADO.match(linha)
            if item:
                atual = {"id": item.group(1), "titulo": item.group(2).strip(), "campos": {}}
                itens[balde].append(atual)
                continue
        secao_nova = RE_SECAO.match(linha)
        if secao_nova:
            secao = secao_nova.group(1)
            atual = None
            balde = SECOES_DE_ITEM.get(secao.strip().lower())
            if secao in CABECALHO_DO_DOSSIE:
                cabecalho[secao] = secao_nova.group(2).strip()
    return cabecalho, itens


def _faltando(campos: dict, obrigatorios: tuple, titulo: str) -> list:
    faltando = [c for c in obrigatorios if c not in campos or nao_preenchido(campos[c])]
    if nao_preenchido(titulo):
        faltando.insert(0, "titulo")
    return faltando


def validar_achado(caminho: Path) -> dict:
    texto = ler_texto(caminho)
    cabecalho, itens = separar_dossie(texto)
    erros, avisos, resultado, processos, guardrails = [], [], [], [], []

    for campo in CABECALHO_DO_DOSSIE:
        if campo not in cabecalho or nao_preenchido(cabecalho[campo]):
            erros.append(f"dossie: cabecalho '{campo}' ausente ou nao preenchido")
    if not itens["achados"]:
        erros.append("dossie: nenhum achado encontrado sob a secao 'Achados:'")
    if not itens["processos"]:
        avisos.append(
            "dossie sem 'Processos observados': a fase de skill fica sem entrada. "
            "Se a frente tocou uma unidade do sistema, ela tem processo a descrever"
        )

    for achado in itens["achados"]:
        ident, campos = achado["id"], achado["campos"]
        item = {"id": ident, "afirmacao": achado["titulo"]}
        faltando = _faltando(campos, CAMPOS_DO_ACHADO, achado["titulo"])
        if "titulo" in faltando:
            faltando[faltando.index("titulo")] = "afirmacao"
        item["faltando"] = faltando
        for campo in faltando:
            erros.append(f"{ident}: campo '{campo}' ausente ou nao preenchido")

        contagem = RE_CONTAGEM.search(campos.get("evidencia", ""))
        if not contagem:
            if "evidencia" not in faltando:
                erros.append(f"{ident}: evidencia sem '(N ocorrencias em M arquivos)'")
            resultado.append(item)
            continue
        ocorrencias, arquivos = int(contagem.group(1)), int(contagem.group(2))
        universo = int(contagem.group(3)) if contagem.group(3) else None
        item["ocorrencias"], item["arquivos"], item["universo"] = ocorrencias, arquivos, universo
        if arquivos > ocorrencias:
            erros.append(f"{ident}: {arquivos} arquivos com {ocorrencias} ocorrencias; a contagem nao fecha")
        if universo is None:
            avisos.append(f"{ident}: evidencia sem universo comparavel; contagem sem proporcao nao classifica padrao")
        elif universo < arquivos:
            erros.append(f"{ident}: universo de {universo} menor que os {arquivos} arquivos citados")

        bruto = campos.get("contraexemplos", "").strip()
        primeiro = bruto.split("|")[0].strip().split()
        if not primeiro or not primeiro[0].isdigit():
            if "contraexemplos" not in faltando:
                erros.append(f"{ident}: contraexemplos precisa comecar pela quantidade, zero inclusive")
            resultado.append(item)
            continue
        contraexemplos = int(primeiro[0])
        item["contraexemplos"] = contraexemplos

        confirma = normalizar_comando(campos.get("comando", ""))
        derruba = normalizar_comando(campos.get("comando-contraexemplo", ""))
        if confirma and derruba and confirma == derruba:
            erros.append(
                f"{ident}: comando-contraexemplo igual ao comando de confirmacao; "
                "buscar a mesma coisa duas vezes nao e procurar contraexemplo"
            )

        declarativa = bool(campos.get("fonte-declarativa")) and \
            campos["fonte-declarativa"].strip().lower() not in ("nenhuma", "nao", "n/a")
        calculada = calcular_classificacao(ocorrencias, arquivos, contraexemplos, declarativa)
        declarada = campos.get("classificacao", "").strip().lower()
        item["calculada"], item["declarada"] = calculada, declarada
        if declarada in CLASSIFICACOES and declarada != calculada:
            if CLASSIFICACOES.index(declarada) < CLASSIFICACOES.index(calculada):
                erros.append(
                    f"{ident}: classificacao declarada '{declarada}' acima da calculada '{calculada}' "
                    f"({ocorrencias} ocorrencias, {arquivos} arquivos, {contraexemplos} contraexemplos)"
                )
            else:
                avisos.append(f"{ident}: classificacao declarada '{declarada}' abaixo da calculada '{calculada}'")
        elif declarada and declarada not in CLASSIFICACOES:
            erros.append(f"{ident}: classificacao '{declarada}' nao esta entre {', '.join(CLASSIFICACOES)}")

        if calculada == "isolado":
            avisos.append(f"{ident}: padrao isolado; nao vira regra, guardrail nem skill")
        if calculada == "convencao" and universo and ocorrencias / universo < 0.5:
            avisos.append(
                f"{ident}: convencao com {ocorrencias} de {universo} comparaveis; "
                "confirme se o universo declarado e o certo"
            )
        resultado.append(item)

    for processo in itens["processos"]:
        ident, campos = processo["id"], processo["campos"]
        item = {"id": ident, "unidade": processo["titulo"],
                "faltando": _faltando(campos, CAMPOS_DO_PROCESSO, processo["titulo"])}
        for campo in item["faltando"]:
            erros.append(f"{ident}: campo '{campo}' ausente ou nao preenchido")
        numero = re.search(r"\d+", campos.get("exemplares", ""))
        if numero:
            item["exemplares"] = int(numero.group())
            if item["exemplares"] < 2:
                avisos.append(
                    f"{ident}: processo sustentado por {item['exemplares']} exemplar; "
                    "com menos de dois nao ha sequencia comprovada, ha um arquivo"
                )
        processos.append(item)

    for guardrail in itens["guardrails"]:
        ident, campos = guardrail["id"], guardrail["campos"]
        item = {"id": ident, "padrao": guardrail["titulo"],
                "faltando": _faltando(campos, CAMPOS_DO_GUARDRAIL, guardrail["titulo"])}
        for campo in item["faltando"]:
            erros.append(f"{ident}: campo '{campo}' ausente ou nao preenchido")
        if not RE_CONTAGEM_GUARDRAIL.search(guardrail["titulo"]):
            erros.append(f"{ident}: guardrail sem contagem visivel no formato '(N de M)'")
        guardrails.append(item)

    return {"arquivo": str(caminho), "cabecalho": cabecalho, "achados": resultado,
            "processos": processos, "guardrails": guardrails,
            "erros": erros, "avisos": avisos}


# ---------------------------------------------------------------------- relatorios

# --------------------------------------------------- auditar comando publicado

# Armadilhas medidas em rodadas reais. Cada uma nasceu de um artefato que reproduzia
# o proprio numero e mesmo assim media outra coisa. Reproduzir nao e o mesmo que medir certo.
ARMADILHAS = (
    ("ancora",
     "termo alfanumerico sem ancora: pode casar sufixo de outra palavra",
     "Ancore com (^|[^A-Za-z0-9_]) ou \\b. Termo curto casa dentro de palavra maior: "
     "'id' casa 'uuid' e 'valid'."),
    ("caixa",
     "busca sensivel a caixa num repositorio de grafia mista",
     "Acrescente -i. Caso real: '#include' sem -i devolveu zero porque o arquivo usa '#INCLUDE'."),
    ("escopo",
     "--include restringe a extensao e pode esconder onde o padrao vive",
     "Rode sem --include e compare. Restringir a extensao principal esconde o que vive em "
     "arquivos incluidos, gerados, de teste ou de configuracao."),
    ("comentario",
     "parte das ocorrencias esta em linha comentada",
     "Separe ativo de comentado. Caso real: metade das diretivas de acesso estava comentada."),
    ("ferramenta",
     "outra ferramenta de busca devolve numero diferente para o mesmo comando",
     "Fixe o binario no comando publicado. Em ambiente agentico 'grep' pode ser funcao ou "
     "alias que troca a semantica, e opcoes como -I ou --exclude-dir divergem entre ferramentas."),
)

RE_PADRAO = re.compile(r"""-(?:E|F|P)?\s*(['"])(.+?)\1""")
RE_INCLUDE_EXT = re.compile(r"--include=(['\"]?)\*\.([A-Za-z0-9]+)\1")


def _rodar(comando: str, raiz: Path) -> tuple[int, str, str]:
    """Executa o comando como esta escrito e preserva stdout e stderr."""
    try:
        r = subprocess.run(comando, shell=True, cwd=str(raiz), capture_output=True,
                           text=True, timeout=300)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return -1, "", "<timeout>"
    except Exception as exc:  # pragma: no cover
        return -1, "", f"<erro: {exc}>"


def _numero(saida: str):
    linhas = [l.strip() for l in saida.splitlines() if l.strip()]
    if len(linhas) == 1 and linhas[0].isdigit():
        return int(linhas[0])
    return None


def _contagem_valida(comando: str, codigo: int, numero, erro: str) -> bool:
    """grep -c devolve 1 com contagem zero quando nao encontra correspondencias."""
    if numero is None or erro:
        return False
    if codigo == 0:
        return True
    if codigo != 1 or numero != 0 or "\n" in comando or "\r" in comando:
        return False
    # Preserva aspas para distinguir um argumento "|" de um operador do shell.
    # Escapes de operadores ou aspas ficam inconclusivos: dependem do shell usado.
    if re.search(r"""[\\^][|;&'\"]""", comando):
        return False
    try:
        lexer = shlex.shlex(comando, posix=False, punctuation_chars="|;&")
        lexer.commenters = ""
        tokens = list(lexer)
    except ValueError:
        return False
    # Comentarios, agrupamentos e expansoes fora de aspas exigem interpretar o shell.
    if any(t in (";", "&&", "||", "&") or
           (not t.startswith(("'", '"')) and re.search(r"[#()$`%!]", t)) for t in tokens):
        return False
    ultimo = tokens[max((i + 1 for i, t in enumerate(tokens) if t == "|"), default=0):]
    return bool(ultimo and Path(ultimo[0]).name == "grep" and
                any(re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", t) for t in ultimo[1:]))


def _variantes(comando: str) -> list:
    """Gera as variantes de teste a partir do comando publicado."""
    saidas = []
    m = RE_PADRAO.search(comando)
    padrao = m.group(2) if m else ""

    # ancora: so faz sentido quando o padrao tem termo alfanumerico solto
    if padrao and re.search(r"[A-Za-z_][A-Za-z0-9_]+", padrao):
        if not re.search(r"\(\^\||\\b|\[\^A-Za-z0-9_\]", padrao):
            termos = re.findall(r"[A-Za-z_][A-Za-z0-9_]+", padrao)
            # o ultimo termo e onde a regex gulosa costuma casar sufixo de outra palavra
            for alvo in dict.fromkeys(reversed(termos)):
                if alvo.lower() in ("cntrl", "space", "alnum", "alpha", "digit", "punct", "upper", "lower"):
                    continue
                novo = re.sub(r"(?<![A-Za-z0-9_])" + re.escape(alvo),
                              "(^|[^A-Za-z0-9_])" + alvo, padrao, count=1)
                if novo != padrao:
                    saidas.append(("ancora", comando.replace(padrao, novo, 1)))
                    break

    # caixa
    if re.search(r"grep\s+-[A-Za-z]*", comando) and "i" not in (
            re.search(r"grep\s+-([A-Za-z]*)", comando).group(1) or ""):
        saidas.append(("caixa", re.sub(r"(grep\s+-)([A-Za-z]*)", r"\1i\2", comando, count=1)))

    # escopo por extensao
    if RE_INCLUDE_EXT.search(comando):
        saidas.append(("escopo", RE_INCLUDE_EXT.sub("", comando)))

    # ferramenta: o mesmo comando com o binario real do sistema.
    # Em ambiente agentico o 'grep' do shell pode ser funcao ou alias que troca a semantica.
    for binario in ("/usr/bin/grep", "/bin/grep"):
        if Path(binario).exists() and re.search(r"(^|[|;&\s])grep\s", comando):
            saidas.append(("ferramenta",
                           re.sub(r"(^|[|;&\s])grep(\s)", r"\1" + binario + r"\2", comando)))
            break

    # comentario: conta quantas das linhas casadas parecem comentadas
    if " -c" not in comando and "wc -l" in comando:
        base = comando.replace("-l ", "-n ").replace("-ril", "-rin").replace("-rl", "-rn")
        base = base.replace("| wc -l", "")
        # aceita 'arquivo:linha:' e 'linha:'; grep -n num arquivo unico nao prefixa o nome
        # marcadores de comentario de linha das familias mais comuns:
        # ' Basic/VB  // C/Java/JS/Go/Rust  # sh/Python/Ruby/Perl/YAML  -- SQL/Lua/Haskell
        # % LaTeX/Erlang/Matlab  ; Lisp/asm/ini  <!-- HTML/XML  * COBOL/pascal-ish
        marcadores = "'|//|#|--|%|;|<!--|\\*|rem[[:space:]]|REM[[:space:]]"
        saidas.append(("comentario",
                       base + f' | grep -cE "^([^:]*:)?[0-9]+:[[:space:]]*({marcadores})"'))
    return saidas


def auditar_comando(comando: str, raiz: Path) -> dict:
    """Roda o comando publicado e as variantes que expoem armadilhas de medicao."""
    raiz = raiz.resolve()
    rc, saida, erro = _rodar(comando, raiz)
    base = _numero(saida)
    achados, testes = [], []
    inconclusiva = not _contagem_valida(comando, rc, base, erro)
    legenda = {k: (d, c) for k, d, c in
               [(a[0], a[1], a[2]) for a in ARMADILHAS]}

    for chave, variante in ([] if inconclusiva else _variantes(comando)):
        vrc, vsaida, verro = _rodar(variante, raiz)
        valor = _numero(vsaida)
        valido = _contagem_valida(variante, vrc, valor, verro)
        testes.append({"armadilha": chave, "comando": variante,
                       "devolveu": valor if valor is not None else vsaida[:60],
                       "codigo": vrc, "erro": verro[:200], "valido": valido})
        if not valido:
            inconclusiva = True
            continue
        if chave == "comentario":
            if valor > 0:
                achados.append({"armadilha": chave, "base": base, "variante": valor,
                                "descricao": legenda[chave][0], "correcao": legenda[chave][1]})
        elif valor != base:
            achados.append({"armadilha": chave, "base": base, "variante": valor,
                            "descricao": legenda[chave][0], "correcao": legenda[chave][1]})

    return {"comando": comando, "codigo": rc, "numero": base,
            "saida": saida[:200], "erro": erro[:200], "testes": testes, "achados": achados,
            "inconclusiva": inconclusiva or not testes}


def imprimir_auditoria_de_comando(dados: dict) -> None:
    print(f"Comando: {dados['comando']}")
    if dados["numero"] is None:
        print(f"  devolveu (nao numerico): {dados['saida'][:120]}")
        print("  AVISO  a auditoria automatica compara numeros; use um comando que termine em 'wc -l'")
    else:
        print(f"  devolveu: {dados['numero']}")
    if dados["erro"]:
        print(f"  AVISO  {dados['erro']}")
    print()
    for t in dados["testes"]:
        print(f"  teste {t['armadilha']:<11} -> {t['devolveu']}")
        if not t["valido"]:
            print(f"  AVISO  controle sem contagem valida (codigo {t['codigo']}): {t['erro']}")
    print()
    if dados["inconclusiva"]:
        print("Auditoria inconclusiva: o comando ou um controle nao produziu comparacao valida.")
        _imprimir_nao_cobertas()
        return
    if not dados["achados"]:
        print("Sem divergencia entre o comando e as variantes de controle.")
        _imprimir_nao_cobertas()
        return
    for a in dados["achados"]:
        print(f"ALERTA  {a['armadilha']}: {a['descricao']}")
        print(f"        publicado devolve {a['base']}, controle devolve {a['variante']}")
        print(f"        {a['correcao']}")
    print()
    print(f"{len(dados['achados'])} alerta(s). Divergencia nao e erro automatico: e pergunta.")
    print("Decida qual dos dois numeros responde a afirmacao do artefato e publique esse comando.")
    _imprimir_nao_cobertas()


NAO_COBERTAS = (
    "amostra: de quantos diretorios diferentes vieram os exemplares lidos por inteiro?",
    "definicao contra uso: este numero conta chamadas, ou tambem as proprias definicoes?",
    "consequencia: a afirmacao diz o que acontece em execucao? voce nao executou nada",
    "negativa: se a frase diz 'nao ha' ou 'e o unico', quantas buscas diferentes a sustentam?",
    "homonimos: o mesmo nome e definido em mais de um lugar, com corpos diferentes?",
    "fronteira: este simbolo e atribuido neste repositorio, ou so lido?",
)


def _imprimir_nao_cobertas() -> None:
    print()
    print("Esta ferramenta compara numeros. Ela NAO decide as perguntas abaixo,")
    print("que derrubaram artefato em rodada real e exigem busca sua:")
    for q in NAO_COBERTAS:
        print(f"  - {q}")
    print("  Detalhe e teste de cada uma em references/armadilhas-de-medicao.md")
    print("  O shell atual nao comprova falhas silenciosas em etapas intermediarias de pipelines.")


def imprimir_stack(dados: dict) -> None:
    print(f"Estado da stack: {dados['estado']}")
    for peca, valor in dados["pecas"].items():
        print(f"  {peca:12} {valor or 'AUSENTE'}")
    if dados["faltando"]:
        print(f"  faltando:    {', '.join(dados['faltando'])}")
    print(f"  AGENTS.md:   {dados['agents_md_linhas']} linhas")
    print(f"  ponteiros:   {', '.join(dados['ponteiros']) or 'nenhum'}")
    print(f"  indices:     {', '.join(dados['indices']) or 'nenhum'}")
    print(f"\nReferences ({len(dados['references'])}):")
    for ref in dados["references"]:
        marca = "" if ref["tem_evidencias"] else "  [sem evidencias]"
        print(f"  {ref['arquivo']} ({ref['linhas']} linhas){marca}")
    print(f"\nSkills ({len(dados['skills'])}):")
    for skill in dados["skills"]:
        marca = "" if skill["tem_descricao"] else "  [sem description]"
        eixo = "user " if skill["user_invoked"] else "model"
        print(f"  {skill['diretorio']:44} {eixo}  descricao ~{skill['descricao_tokens']:4} tokens{marca}")

    orc = dados["orcamento"]
    print("\nOrcamento de contexto (estimativa por caracteres):")
    print(f"  AGENTS.md                     ~{orc['agents_md_tokens']:6} tokens  permanente")
    print(f"  {orc['descricoes_model_invocadas']:2} descricoes model-invocadas  ~{orc['descricoes_tokens_permanentes']:6} tokens  permanente")
    print(f"  {orc['descricoes_user_invocadas']:2} descricoes user-invocadas   ~{orc['descricoes_tokens_sob_demanda']:6} tokens  sob demanda")
    print(f"  piso permanente por tarefa    ~{orc['permanente_tokens']:6} tokens")
    print(f"  references (corpo)            ~{orc['references_tokens']:6} tokens  sob demanda")
    if orc["descricoes_acima_do_agents"]:
        print("  ATENCAO: as descricoes de skill custam mais que o AGENTS.md inteiro.")
        print("  Quebrar conhecimento em skill nao economiza contexto: a descricao e permanente.")


def imprimir_censo(dados: dict) -> None:
    print(f"Arquivos examinados: {dados['arquivos_examinados']}")
    print("\nExtensoes:")
    for ext, qtd in dados["extensoes"]:
        print(f"  {ext:16} {qtd}")
    print("\nDiretorios de topo:")
    for item in dados["diretorios_topo"]:
        print(f"  {item['nome']:24} {item['arquivos']:6} arquivos  {item['bytes'] // 1024:8} KB")
    cod = dados["codificacao"]
    print("\nCodificacao por extensao:")
    print(f"  {'extensao':14} {'arqs':>6} {'decis':>6}  {'padrao':16} {'cobert':>7}  {'linha':8} excecoes")
    for grupo in cod["por_extensao"]:
        excecoes = ", ".join(f"{e['codificacao']}x{e['arquivos']}" for e in grupo["excecoes"]) or "-"
        print(
            f"  {grupo['extensao']:14} {grupo['arquivos']:6} {grupo['decisivos']:6}  {grupo['padrao']:16}"
            f" {grupo['cobertura'] * 100:6.1f}%  {grupo['terminacao']:8} {excecoes}"
        )
    if cod["extensoes_omitidas"]:
        print(f"  (+{cod['extensoes_omitidas']} extensoes omitidas; aumente --top)")
    for grupo in cod["por_extensao"]:
        for excecao in grupo["excecoes"]:
            print(f"    excecao {grupo['extensao']} {excecao['codificacao']}: {', '.join(excecao['exemplos'])}")
    print("\nComandos de busca por familia presente:")
    for familia in cod["familias_de_busca"]:
        print(f"  {familia['familia']:16} {familia['comando']}")
    if cod["repositorio_misto"]:
        print("  ATENCAO: repositorio de codificacao mista. Fixe o comando por escopo antes de contar.")
    print(f"  {NOTA_ORDEM_GREP}")

    dep = dados.get("dependencias", {})
    print("\nSinais de dependencia declarados no codigo:")
    if not any(dep.get(chave) for chave in dep):
        print("  nenhum")
    for rotulo, chave in (
        ("Componentes instanciados", "componentes"),
        ("Includes mais frequentes", "includes"),
        ("Modulos importados", "modulos"),
        ("Driver ou provedor de dados", "drivers"),
    ):
        itens = dep.get(chave) or []
        if not itens:
            continue
        print(f"  {rotulo}:")
        for item in itens:
            print(f"    {item['ocorrencias']:6}x  {item['nome']}   ex.: {item['exemplo']}")
    if dep.get("bibliotecas"):
        print("  Bibliotecas com versao no nome do arquivo:")
        for item in dep["bibliotecas"]:
            print(f"    {item['nome']}   ex.: {item['exemplo']}")
    if dep.get("formatos_proprios"):
        print("  Formatos de ferramenta, candidatos a dependencia:")
        for item in dep["formatos_proprios"]:
            print(f"    {item['arquivos']:6}   {item['extensao']}")
    print("  Estes sinais alimentam as secoes de contexto e dependencias do AGENTS.md, em topicos.")

    for rotulo, chave in (
        ("Manifestos", "manifestos"),
        ("Configuracao de qualidade", "configuracao_qualidade"),
        ("Integracao continua", "integracao_continua"),
        ("Diretorios de teste", "diretorios_de_teste"),
        ("Diretorios de migracao", "diretorios_de_migracao"),
        ("Documentacao", "documentacao"),
    ):
        print(f"\n{rotulo}:")
        itens = dados[chave] or ["nenhum"]
        for item in itens:
            print(f"  {item}")


def imprimir_verificacao(dados: dict) -> None:
    for erro in dados["erros"]:
        print(f"ERRO   {erro}")
    for aviso in dados["avisos"]:
        print(f"AVISO  {aviso}")
    if not dados["erros"] and not dados["avisos"]:
        print("Sem problemas mecanicos.")
    print(f"\n{len(dados['erros'])} erro(s), {len(dados['avisos'])} aviso(s)")


def imprimir_validacao_de_achado(dados: dict) -> None:
    for chave, valor in dados["cabecalho"].items():
        print(f"{chave}: {valor}")
    print(f"\nAchados: {len(dados['achados'])}")
    for item in dados["achados"]:
        numeros = ""
        if "ocorrencias" in item:
            numeros = f"  {item['ocorrencias']} ocorrencias / {item['arquivos']} arquivos"
            if "contraexemplos" in item:
                numeros += f" / {item['contraexemplos']} contraexemplos"
        faixa = ""
        if item.get("calculada"):
            faixa = f"  classificacao calculada: {item['calculada']}"
            if item.get("declarada") and item["declarada"] != item["calculada"]:
                faixa += f" (declarada: {item['declarada']})"
        print(f"  {item['id']}: {item['afirmacao'][:70]}")
        if numeros:
            print(f"   {numeros}")
        if faixa:
            print(f"   {faixa}")
        if item["faltando"]:
            print(f"    faltando: {', '.join(item['faltando'])}")
    print(f"\nProcessos observados: {len(dados['processos'])}")
    for item in dados["processos"]:
        print(f"  {item['id']}: {item['unidade'][:70]}")
        if item.get("exemplares") is not None:
            print(f"    exemplares: {item['exemplares']}")
        if item["faltando"]:
            print(f"    faltando: {', '.join(item['faltando'])}")
    print(f"\nCandidatos a guardrail: {len(dados['guardrails'])}")
    for item in dados["guardrails"]:
        print(f"  {item['id']}: {item['padrao'][:70]}")
        if item["faltando"]:
            print(f"    faltando: {', '.join(item['faltando'])}")
    print()
    for erro in dados["erros"]:
        print(f"ERRO   {erro}")
    for aviso in dados["avisos"]:
        print(f"AVISO  {aviso}")
    if not dados["erros"] and not dados["avisos"]:
        print("Sem problemas mecanicos.")
    print(f"\n{len(dados['erros'])} erro(s), {len(dados['avisos'])} aviso(s)")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("comando", choices=("stack", "censo", "verificar", "validar-achado", "auditar-comando"))
    parser.add_argument("--raiz", default=".", help="raiz do repositorio de destino")
    parser.add_argument("--arquivo", help="dossie de achados, para validar-achado")
    parser.add_argument("--cmd", help="comando publicado a auditar, para auditar-comando")
    parser.add_argument("--json", action="store_true", help="saida em JSON")
    parser.add_argument("--top", type=int, default=15, help="itens por lista no censo")
    args = parser.parse_args(argv)

    if args.comando == "validar-achado":
        if not args.arquivo:
            print("validar-achado exige --arquivo", file=sys.stderr)
            return 2
        caminho = Path(args.arquivo).resolve()
        if not caminho.is_file():
            print(f"dossie inexistente: {caminho}", file=sys.stderr)
            return 2
        dados = validar_achado(caminho)
        if args.json:
            print(json.dumps(dados, indent=2, ensure_ascii=False))
        else:
            imprimir_validacao_de_achado(dados)
        return 2 if dados["erros"] else (1 if dados["avisos"] else 0)

    raiz = Path(args.raiz).resolve()
    if not raiz.is_dir():
        print(f"raiz inexistente: {raiz}", file=sys.stderr)
        return 2

    if args.comando == "auditar-comando":
        if not args.cmd:
            print("auditar-comando exige --cmd '<comando publicado>'", file=sys.stderr)
            return 2
        dados = auditar_comando(args.cmd, raiz)
        if args.json:
            print(json.dumps(dados, indent=2, ensure_ascii=False))
        else:
            imprimir_auditoria_de_comando(dados)
        return 2 if dados["inconclusiva"] else (1 if dados["achados"] else 0)

    if args.comando == "stack":
        dados = coletar_stack(raiz)
        if args.json:
            print(json.dumps(dados, indent=2, ensure_ascii=False))
        else:
            imprimir_stack(dados)
        return 0 if dados["estado"] in ("minima_presente", "povoada") else 1

    if args.comando == "censo":
        dados = coletar_censo(raiz, args.top)
        if args.json:
            print(json.dumps(dados, indent=2, ensure_ascii=False))
        else:
            imprimir_censo(dados)
        return 0

    dados = verificar(raiz)
    if args.json:
        print(json.dumps(dados, indent=2, ensure_ascii=False))
    else:
        imprimir_verificacao(dados)
    return 2 if dados["erros"] else (1 if dados["avisos"] else 0)


if __name__ == "__main__":
    sys.exit(main())
