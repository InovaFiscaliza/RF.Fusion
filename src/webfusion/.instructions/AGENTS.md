## Comments and maintainability

Write comments for:
- intent;
- architectural decisions;
- operational context;
- non-obvious behavior;
- business rules;
- edge cases;
- limitations and assumptions.

Do NOT write comments that merely describe obvious syntax.

Bad:
    # Increment counter
    counter += 1

Good:
    # Counter is intentionally monotonic because workers may retry
    # the same task concurrently after process crashes.
    counter += 1

Prefer comments explaining:
- WHY something exists;
- WHY a specific approach was chosen;
- WHAT can break if modified incorrectly;
- external system constraints;
- performance considerations;
- concurrency assumptions;
- integration quirks.

Dentro de funções, inclua comentários nos pontos de decisão e nos trechos que
não forem evidentes pela leitura do código. Explique a regra, a condição ou a
restrição que motivou a decisão e o risco de manutenção caso ela seja alterada.
Não comente instruções óbvias nem repita o nome de variáveis ou chamadas.

## Docstrings obrigatórias

Toda função e método deve possuir uma docstring que descreva claramente:

- a finalidade da função e o que ela faz;
- cada argumento de entrada, seu significado e tipo (`int`, `float`, `str`,
    `list`, etc.);
- o formato esperado de argumentos estruturados. Para `dict`, documente as
    chaves, os tipos dos valores e quais são obrigatórias; para `list`, informe
    o tipo e o formato de cada item;
- cada valor de saída, seu significado e tipo;
- o formato retornado quando a saída for uma estrutura ou um dicionário, com
    chaves, tipos e obrigatoriedade.

Use as seções `Args:` e `Returns:` para manter a leitura previsível. Quando a
função não retornar valor útil, declare explicitamente `None` em `Returns:`.
Inclua também `Raises:` quando uma exceção fizer parte do contrato esperado.

Toda classe deve possuir uma docstring que descreva sua responsabilidade,
quando deve ser usada e quais dados ou recursos administra. Classes com estado
devem incluir a seção `Attributes:`, documentando cada atributo relevante com:

- nome do atributo;
- significado e finalidade;
- tipo;
- formato esperado, quando for `dict`, `list` ou outra estrutura;
- obrigatoriedade e valor inicial, quando isso afetar o comportamento.

Não é necessário documentar atributos locais, temporários ou evidentes que não
façam parte do estado ou do contrato da classe.

Modelo:

```python
def build_summary(site_id: int, measurements: list[dict[str, float]]) -> dict[str, float]:
        """Calcula o resumo das medições de um site.

        Args:
                site_id: Identificador do site. Tipo: int.
                measurements: Medições utilizadas no cálculo. Tipo: list[dict[str, float]].
                        Cada item deve conter as chaves `frequency` e `power`, ambas float.

        Returns:
                Resumo calculado. Tipo: dict[str, float]. Contém as chaves obrigatórias
                `minimum_power`, `maximum_power` e `average_power`.
        """
```

Modelo de classe:

```python
class SummaryCache:
    """Mantém resumos calculados para reduzir consultas repetidas.

    Attributes:
        summaries: Resumos indexados pelo identificador do site. Tipo:
            dict[int, dict[str, float]]. Cada valor contém as chaves
            obrigatórias `minimum_power`, `maximum_power` e `average_power`.
        max_entries: Limite de resumos mantidos em memória. Tipo: int.
    """
```

For long or critical flows:
- add section comments separating logical blocks;
- explain the lifecycle/state transitions;
- explain interactions with database/external systems.

When modifying existing code:
- preserve useful comments;
- improve outdated comments;
- do not remove operational context comments.

Avoid:
- redundant comments;
- decorative comments;
- excessive banner comments;
- commenting every line.

The code should be understandable by an engineer debugging production issues at 2 AM.