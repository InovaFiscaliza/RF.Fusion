# Payload de resposta do appAnalise

Data: 14/10/2026.

Este documento registra o formato observado nas respostas de exemplo e o
contrato consumido por `payload_parser.py`. A identificação estruturada do
hardware descrita abaixo documenta o significado de `Receiver`; não modifica
o protocolo, o parser ou a persistência existentes.

## Estrutura da resposta

O parser do serviço recebe o resultado dentro de `Answer`. As respostas em
[exemplos.json](../../../../../../../../docs/planejamento/drive-test/exemplos.json) apresentam
diretamente o conteúdo desse objeto, com `General` e `Spectra`.
`Spectra` pode ser um objeto para um espectro ou uma lista de objetos.
O conjunto atual contém seis respostas: quatro com espectro único, uma CelPlan
com 11 espectros e uma RFEye com 19. Tratar identidade, GPS e arquivos
relacionados no nível de cada espectro, sem supor homogeneidade em outras respostas.

| Campo | Formato observado | Significado |
|---|---|---|
| `General` | Objeto | Identificação do arquivo de saída do processamento |
| `General.FilePath` | String | Diretório informado pelo appAnalise; não comprova o caminho remoto original da estação |
| `General.FileName` | String | Nome do arquivo de saída |
| `General.Extension` | String | Extensão do arquivo de saída |
| `Spectra.Receiver` | String | Identificação do receptor/instrumento daquele espectro |
| `Spectra.MetaData` | Objeto | Configuração espectral e metadados auxiliares |
| `Spectra.GPS` | Objeto | Coordenada resumida, origem e estatísticas das leituras |
| `Spectra.RelatedFiles` | Lista de objetos | Arquivos relacionados, tarefa, período e quantidade de varreduras |
| `Spectra.Data`, `Spectra.FileMap` | Listas vazias nos exemplos | Os exemplos não fornecem a série de medições nem uma trajetória por esses campos |
| `Spectra.Enable` | String `on` nos exemplos | Estado informado pelo produtor; sua semântica não determina fixo/móvel |

No protocolo do serviço, `Error` pode informar falha; `Answer` também pode ser
uma string de erro. Essas respostas não são objetos de espectro válidos.

## Identificação do hardware em Receiver

Nos exemplos EB500 e SA2500, `Receiver` possui quatro componentes separados
por vírgula, nesta ordem:

```text
fabricante,modelo,numero_de_serie,versao_firmware
```

Exemplo recebido:

```json
{
  "Receiver": "ROHDE&SCHWARZ,EB500,100.633/012,V04.90-4072.8710.00"
}
```

Representação estruturada correspondente:

```json
{
  "fabricante": "ROHDE&SCHWARZ",
  "modelo": "EB500",
  "numero_de_serie": "100.633/012",
  "versao_firmware": "V04.90-4072.8710.00"
}
```

| Campo estruturado | Tipo | Conteúdo |
|---|---|---|
| `fabricante` | String | Primeiro componente: fabricante do instrumento |
| `modelo` | String | Segundo componente: modelo do instrumento |
| `numero_de_serie` | String | Terceiro componente: serial da unidade física |
| `versao_firmware` | String | Quarto componente: versão informada pelo instrumento |

Outro exemplo observado:

```json
{
  "fabricante": "TEKTRONIX",
  "modelo": "SA2500",
  "numero_de_serie": "B040237",
  "versao_firmware": "7.050"
}
```

### Regras de interpretação

- Serial e firmware permanecem strings. Preservar pontos, barras, letras e
  zeros; `100.633/012` não é um número decimal e `7.050` não deve virar `7.05`.
- Fabricante, modelo e serial identificam o hardware. Firmware é informação
  versionada do instrumento e fica fora da identidade física. Uma atualização
  de firmware não cria outra unidade.
- Conservar `Receiver` bruto junto da interpretação. Normalização para consulta
  não deve apagar a informação recebida nem modificar silenciosamente o serial.
- A extração dos quatro componentes se aplica aos formatos observados EB500 e
  SA2500. Não presumir que toda família use essa mesma estrutura: RFEye e
  CelPlan possuem regras próprias de identificação no parser atual.
- Componente ausente ou formato não reconhecido não autoriza inventar serial
  pelo nome do arquivo, pelo hostname ou pelo modelo.
- A identificação física não informa, sozinha, se o instrumento estava em
  ERMx/UMS300 ou em drive-test. Esse contexto é resolvido separadamente.

O parser atual normaliza `Receiver` como uma identificação textual; ainda não
produz o objeto de quatro campos acima. O domínio também possui a exceção que
persiste o hostname para ERMx/UMS. Documentar o receptor não altera essa regra
nem aprova uma migração de `DIM_SPECTRUM_EQUIPMENT`.

### CelPlan e RFEye: identificação integrada

Essas famílias apresentam um identificador integrado, sem os quatro componentes
separados por vírgula:

| Família | `Receiver` observado | Identificação canônica no parser atual |
|---|---|---|
| CelPlan | `CWSM22010045` | `cwsm22010045` |
| RFEye | `RFeye002095` | `rfeye002095` |

```json
{
  "Receiver": "CWSM22010045"
}
```

```json
{
  "Receiver": "RFeye002095"
}
```

Definição do responsável: para CelPlan e RFEye, o próprio nome oficial da unidade
é seu serial. Portanto, `numero_de_serie` corresponde a `cwsm22010045` ou
`rfeye002095` nesses exemplos, preservando o identificador completo, sem remover
o prefixo nem extrair somente seus dígitos.

Esses valores não fornecem fabricante, modelo e firmware separados. Não aplicar
o desmembramento de EB500/SA2500 nem inventar os demais componentes.

### Nome operacional e serial resolvidos na dimensão

Para toda a série CelPlan, o nome operacional abreviado em `BPDATA.HOST` e o
nome oficial retornado pelo processamento podem ser diferentes. O par confirmado
é `CWSM211006` e `cwsm21100006`; ambos representam a mesma unidade.

A direção definida para o planejamento é resolver essa correspondência na
própria `RFDATA.DIM_SPECTRUM_EQUIPMENT`, acrescentando `NA_HWSERIAL`:

| Família | `NA_EQUIPMENT`: nome operacional | `NA_HWSERIAL`: serial/identificador oficial |
|---|---|---|
| CelPlan | `CWSM211006` | `cwsm21100006` |
| RFEye | `RFeye002095` | `rfeye002095` |

`NA_EQUIPMENT` mantém a referência operacional para conciliar com o host do
Zabbix; `NA_HWSERIAL` guarda a identificação recebida do hardware, interpretada
por família. Para CelPlan, os dois nomes coexistem na mesma linha, sem criar
dois equipamentos apenas porque a grafia é diferente.

O fluxo convencional fornece host e identificação do payload para registrar
essa associação. O upload consulta a identificação na dimensão e resolve o
nome operacional a partir do registro encontrado, sem depender de heurística
de nomes no summary para constituir essa correspondência.

`RFFUSION_SUMMARY.HOST_EQUIPMENT_LINK` já faz a conciliação atualmente. No modelo
pretendido, seu vínculo passa a derivar da correspondência registrada na dimensão;
ele continua sendo modelo de leitura, sem ser a origem do cadastro. Isso exige
revisão de migração e consumidores; nenhum código é alterado nesta documentação.

Uma mesma identificação de hardware pode participar de contextos diferentes,
como ERMx e móvel. `NA_HWSERIAL` não deve ser tratado como chave globalmente
única da dimensão nem resolver sozinho o contexto da medição.

O parser reconhece `rfeye` seguido de seis dígitos, inclusive dentro de textos
mais longos. Para CelPlan, reconhece identificadores CWSM de oito dígitos e
formas curtas de seis/sete dígitos, com mapeamentos por família e exceção legada
definidos em `payload_parser.py`. Os summaries também conciliam famílias por
assinaturas e exceções históricas. A normalização não é simples remoção de zeros;
esses casos precisam ser reconciliados ao preencher os pares na dimensão para
toda a série CWSM, sem criar uma exceção apenas para a estação do exemplo.

## Metadados espectrais e arquivos relacionados

`MetaData` contém, nos exemplos, `DataType`, `FreqStart`, `FreqStop`,
`DataPoints`, `Resolution`, `VBW`, `Threshold`, `TraceMode`, `TraceIntegration`,
`Detector`, `LevelUnit`, `Antenna` e `Others`.

`FreqStart` e `FreqStop` chegam em Hz; o parser atual os converte para MHz.
`Others` chega como string contendo JSON nos exemplos EB500/SA2500, incluindo
informações como `gpsType`, configuração de aquisição e tarefa. Nos exemplos
CelPlan/RFEye, é uma string vazia. `Antenna` é um objeto nos exemplos de
analisadores/RFEye e uma lista vazia nos espectros CelPlan. Não exigir objeto
de antena ou JSON não vazio em `Others` para todos os formatos.

Os valores observados de `DataType` são `1` e `2` para EB500/SA2500, `1000`
para CelPlan e `67` para RFEye. A semântica depende de confirmação do produtor;
não interpretar `1/2` como enumeração universal de móvel/fixo.

Cada item de `RelatedFiles` contém `File`, `Task`, `Id`, `Description`,
`BeginTime`, `EndTime`, `NumSweeps` e `RevisitTime`. Os horários usam textos
como `13-Sep-2026 05:49:58`. O fuso horário não está explícito nesse texto e
não deve ser inferido pelo local de execução do agente.

As tarefas dos analisadores contêm `Fixed Station` ou `Drive-test`; CelPlan
retorna `Task="Undefined"` e `Description="Undefined"`; RFEye retorna
`Task="Script2026v1LoggerFixed.cfg"`. Ausência do texto `Fixed Station` não
comprova mobilidade, e `Undefined` não invalida os espectros por si só.
`Detector` é uma string vazia no exemplo RFEye; o valor de `TraceMode` é `Peak`.
Preservar essa distinção, sem inventar um detector a partir do tipo de traço.

## GPS resumido e estatísticas

| Campo | Tipo observado | Significado conhecido |
|---|---|---|
| `Status` | Inteiro | `-1` nos exemplos de coordenada manual; `1` nos exemplos com GPS ligado |
| `Count` | Inteiro | Quantidade reportada: `1` para coordenada manual e várias leituras nos exemplos com GPS |
| `Latitude`, `Longitude` | Números | Coordenada resumida em graus decimais |
| `Latitude_std`, `Longitude_std` | Números | Desvio-padrão reportado por eixo, em graus |
| `stdRange` | Lista de três números | Estatística adicional; confirmar definição e tratamento de outliers antes de utilizá-la |
| `Location` | String | Localidade informada pelo processamento |
| `LocationSource` | String | Fonte da localidade; `IBGE` ou `API` nos exemplos |
| `Edited` | Booleano | Indicador informado de edição; `false` nos exemplos atuais |

`Status=-1` com coordenadas válidas não equivale a localização ausente. Nos
exemplos manuais, `Count=1` e desvios zero não representam uma série GPS que
comprovou estabilidade; a posição foi informada manualmente.

Com GPS ligado, os desvios permitem avaliar a dispersão das posições sem
reconstruir toda a trajetória. Comparações em metros devem corrigir a escala
da longitude pela latitude. Esse resumo não fornece ordem temporal dos pontos
nem distância percorrida. Limiares de classificação e filtragem de outliers
permanecem decisões de domínio a calibrar.

### Comparação dos exemplos disponíveis

| Exemplo | Quantidade de espectros | GPS | Dispersão horizontal aproximada |
|---|---|---|---|
| EB500, Anápolis | 1 | Manual, `Status=-1`, `Count=1` | Não estimada por série GPS |
| EB500, Aracaju, drive-test | 1 | Ligado, `Count=5176` | 3.409 m |
| SA2500, Linhares | 1 | Manual, `Status=-1`, `Count=1` | Não estimada por série GPS |
| EB500, Brasília, fixo | 1 | Ligado, `Count=168` | 63,38 m |
| CelPlan, `CWSM22010045` | 11 | Ligado, `Count` entre 522 e 4314 | 1,83–3,66 m por espectro |
| RFEye, `RFeye002095` | 19 | Ligado, `Count=1028` | 4,02 m em cada espectro |

Para essa comparação, foi utilizada a aproximação esférica local:

```text
escala = pi * 6371000 / 180 metros por grau
sigma_lat_m = escala * Latitude_std
sigma_lon_m = escala * cos(Latitude em radianos) * Longitude_std
dispersao_horizontal_m = sqrt(sigma_lat_m² + sigma_lon_m²)
```

Ela combina a dispersão dos dois eixos em torno da média e não mede percurso.
Os valores não definem um limiar de produção; o conjunto contém um único exemplo
móvel e várias faixas/janelas de dois arquivos fixos adicionais, não 34
aquisições independentes para calibração.

No RFEye, os 19 espectros repetem o mesmo resumo GPS, incluindo `Count=1028`.
Não somar essas contagens como se fossem 19 conjuntos distintos de leituras.
No CelPlan, `Location` alterna entre Belo Horizonte/MG e Contagem/MG, enquanto
as coordenadas médias variam somente poucos metros e a fonte pode ser `API`
ou `IBGE`. Mudança do rótulo de localidade não comprova deslocamento da estação.
Não recalcular a dispersão original usando somente a lista de coordenadas
médias dos espectros nem usar `stdRange` como classificador sem definir sua semântica.

O parser atual conserva a coordenada, mas não encaminha `Status`, `Count`,
desvios e `stdRange` na representação normalizada por espectro. Preservar esses
campos será uma mudança futura de contrato, não implementada por esta documentação.

## Referências

- [Parser do payload](payload_parser.py).
- [Exemplos de respostas](../../../../../../../../docs/planejamento/drive-test/exemplos.json).
- [Planejamento da ingestão manual e de drive-test](../../../../../../../../docs/planejamento/PLANEJAMENTO_DRIVE_TEST.md).
