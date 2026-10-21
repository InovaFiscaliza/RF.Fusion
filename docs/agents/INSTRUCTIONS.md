# Regras gerais de manutenção

## Escopo e preservação de comportamento

Estas regras valem para todo o repositório. Consulte também o `AGENTS.md` do
subprojeto e seus contratos arquiteturais antes de alterar código.

- Não alterar lógica de negócio sem pedido explícito. Refatorações preservam
  comportamento observável, banco, contratos externos e ciclo de vida das tasks.
- Não inventar arquitetura nem mover funções para novos módulos sem previsão
  no contrato arquitetural aplicável. Se faltar previsão, atualizar o contrato
  antes da implementação.
- Não adicionar bibliotecas externas durante refatorações.
- Fazer mudanças focadas; não reformatar ou anotar código fora do escopo.

## Clareza e contratos de código

- Manter um nível coerente de abstração por função. Separar responsabilidades
  que tenham modos diferentes de falha e recuperação; evitar wrappers sem valor.
- Reduzir aninhamento com guard clauses. Preferir `match/case` para três ou mais
  alternativas discretas de um mesmo valor; usar `if` para condições e intervalos.
- Usar `snake_case` em novos identificadores Python, preservando APIs existentes,
  nomes de classes estabelecidos e nomes SQL `ALL_CAPS` usados como argumentos.
- Usar nomes claros e curtos. Funções internas ao módulo têm prefixo `_`;
  funções importadas por outros módulos integram a interface pública.
- Tipar entradas e retornos das funções alteradas. Usar tipos concretos para
  handlers de banco; não substituir por `Any`.
- Constantes de configuração, estados, limites e identificadores semânticos
  pertencem ao `config.py` do escopo, em vez de literais mágicos.
- Helpers sinalizam falha por exceção. Ausência legítima de resultado ou uma
  disputa esperada podem ter retorno próprio quando o contrato o definir;
  não converter uma falha em sucesso vazio.
- Passar contexto explicitamente, sem usar estado mutável global como parâmetro
  oculto. Preservar os estados globais de runtime exigidos pela arquitetura.
- Antes de remover código aparentemente morto ou unir duplicações, conferir
  chamadas, imports, scripts, descoberta dinâmica e contratos externos. Não
  manter código comentado como arquivo histórico.

## Idioma, comentários e docstrings

Documentação Markdown deve ficar em português (PT-BR). Comentários de código
devem ficar em inglês simples, com frases curtas.

Preservar comentários úteis de operação. Nos pontos de decisão ou trechos não
evidentes, explicar a regra, restrição, motivo e risco de manutenção. Evitar
comentários que apenas repitam a instrução, decoração ou narração linha a linha.
Fluxos críticos devem explicar transições de estado e interação com sistemas
externos.

Funções e métodos devem possuir docstrings com o contrato completo:

- finalidade;
- `Args:` com significado e tipo de cada entrada;
- `Returns:` com significado e tipo da saída, declarando `None` se não houver
  valor útil;
- chaves, tipos e obrigatoriedade de dicionários, e formato dos itens de listas;
- `Raises:` quando exceções fizerem parte do contrato esperado.

Classes devem documentar responsabilidade, uso e recursos administrados.
Classes com estado incluem `Attributes:` com nome, significado, finalidade,
tipo, formato e valores iniciais ou obrigatoriedade relevantes. Não é preciso
documentar temporários que não pertençam ao estado ou ao contrato da classe.

## Ambiente e segurança operacional

Antes de executar testes, instalar dependências ou iniciar processos, identificar
a aplicação e ler seu `ENVIRONMENT.md`. Suítes mistas seguem o ambiente autorizado
para os componentes importados. Não instalar Flask no container do appCataloga
para contornar a separação do WebFusion.

Antes, durante e após operações potencialmente pesadas, verificar CPU, RAM e
swap com `uptime`, `free -h`, `ps` ou `top`. Isso inclui suítes extensas,
reconciliações, migrações, agregações em produção e varreduras volumosas.
Executar uma operação pesada por vez, salvo capacidade previamente confirmada;
acompanhar comandos longos com esperas limitadas. Se houver degradação,
interromper a operação de maior custo, preservar consistência e informar o risco.

Credenciais devem permanecer fora do Git e não podem aparecer em comandos,
logs ou respostas. Usar o mecanismo operacional documentado para cada ambiente.

## Comunicação de mudanças de código

Antes de editar código, informar:

1. A regra arquitetural ou de manutenção aplicável.
2. Os arquivos que serão alterados.
3. Se a mudança é arquitetural, de refatoração ou tem impacto comportamental.

Depois de editar, informar:

1. O que mudou.
2. O que não mudou.
3. Comandos de validação executados ou a executar, distinguindo os dois casos.
4. Riscos e pontos de revisão manual.
