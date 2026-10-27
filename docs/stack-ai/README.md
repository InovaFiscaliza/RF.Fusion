# Documentação da stack de IA

Esta pasta explica a stack de inteligência artificial instalada neste repositório: o que ela é, como usar o SpecKit e como mantê-la atualizada.

O conteúdo é genérico e vale para qualquer projeto. Ele é distribuído pela skill `stack-ai-init`, a mesma que instala a stack. Nada aqui descreve o sistema deste repositório em particular.

## Qual arquivo ler

| Arquivo | O que contém | Quando ler |
|---|---|---|
| [`stack-de-ia.md`](stack-de-ia.md) | O que é a stack, o que são agentes e skills, quais ferramentas de IA funcionam aqui, o que foi instalado e como começar | Uma vez, ao entrar no projeto |
| [`speckit.md`](speckit.md) | O que é o SpecKit, quando usar, as 10 fases e como invocar cada uma | Antes de começar uma demanda grande ou ambígua |
| [`prompts-exemplo.md`](prompts-exemplo.md) | Prompts prontos para adaptar e enviar ao assistente | Toda vez que for pedir algo ao agente |
| [`manutencao-da-stack.md`](manutencao-da-stack.md) | Como atualizar o SpecKit e os demais arquivos da stack | Apenas quem mantém a stack, e raramente |

## O que não está nesta pasta

| Assunto | Onde está |
|---|---|
| Regras de código, limites de escrita e padrões do projeto | `AGENTS.md`, na raiz do repositório |
| Regra que o agente segue ao editar uma fase do SpecKit | `.agents/references/speckit.md` |
| Formato do relatório de revisão técnica, para a skill de revisão do projeto | `.agents/references/template-revisao-tecnica.md` |
| O que este sistema faz e como rodar o ambiente | `README.md`, na raiz do repositório |

## Manutenção desta pasta

Os arquivos daqui são distribuídos pela skill `stack-ai-init` e chegam iguais em todo repositório que recebe a stack. Ao alterar qualquer um deles, altere também a cópia da skill, na mesma entrega. Sem isso, as duas versões passam a divergir e a conferência da instalação deixa de ser confiável.

Pelo mesmo motivo, não escreva aqui nada específico deste projeto. Conteúdo específico vai para o `README.md` da raiz ou para o `AGENTS.md`.
