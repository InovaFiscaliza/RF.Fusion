# Taxonomia de banco de dados

## 6. Classes de tabelas

### 6.1. Classes transacionais

| Nome | Abreviatura | Descrição |
|---|---|---|
| Tabela | `TB` | Tabelas genéricas, destinadas a armazenar informações do sistema. |
| Histórico | `TH` | Tabelas destinadas a armazenar informações de histórico, espelhando uma outra tabela. |
| Log | `TL` | Tabelas para registro de ações realizadas no sistema. Deve identificar o momento e o responsável pelas alterações. |
| Temporária | `TT` | Tabelas destinadas a armazenar dados de forma temporária |

### 6.2. Classes analíticas

| Nome | Abreviatura | Descrição |
|---|---|---|
| Dimensão | `DIM` | Tabelas destinadas a armazenar as dimensões do modelo de dados analítico. |
| Fato | `FATO` | Tabelas a armazenar os fatos negociais. |
| Stage | `STG` | Tabelas que armazenam dados brutos de outras fontes de dados sem tratamento ou validação negocial. |

## 7. Classes de colunas

Os campos serão classificados de acordo com a natureza dos dados armazenados.
Segue relação das classes de valores.

### 7.1. Classes temporais

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Ano | `AA` | Atribuída aos dados de natureza numérica que expressam o ano no calendário civil.<br>Exemplo: `AA_CONTRATO` | `smallint` |
| Dia | `DD` | Atribuída aos dados de natureza numérica que expressam o dia no calendário civil.<br>Exemplo: `DD_VENCIMENTO_PADO` | `tinynt` |
| Data/Hora | `DH` | Atribuída aos dados de que expressam o dia, mês, ano, hora, minuto e segundo.<br>Exemplo: `DH_CRIACAO_REGISTRO` | `datetime` |
| Data | `DT` | Atribuída aos dados que expressam o dia, mês e ano no calendário civil.<br>Exemplo: `DT_CADASTRO_ENTIDADE` | `date` |
| Hora | `HH` | Atribuída aos dados que expressam uma hora em qualquer formato com ou sem precisão de minutos e/ou quaisquer outras frações.<br>Exemplo: `HH_CHAMADA`<br>Importante: A documentação da coluna deve deixar claro qual é o formato e granularidade do conteúdo. | `tinyint, float, numeric, time` |
| Minuto | `MI` | Atribuída a dados de natureza numérica que expressam o minuto.<br>Exemplo: `MI_COLETA` | `tinynt` |
| Mês | `MM` | Atribuída aos dados de natureza numérica que expressam o mês em um calendário civil.<br>Exemplo: `MM_VENCIMENTO` | `tinynt` |

### 7.2. Classes de identificação

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Código | `CO` | Identificador unívoco de um objeto, alfanumérico, formado por um ou mais atributos. Podem ser utilizados como chave primária, como no caso do número de CPF ou CNPJ.<br>Exemplos:<br>`CO_DEPENDENTE_EMPREGADO`<br>`CO_CPF`, `CO_CNPJ` | `char, varchar, nchar, nvarchar` |
| Indicador | `IC` | Atribuída aos dados de natureza binária (sim ou não, ligado ou desligado, etc.)<br>Exemplo:<br>`IC_EMPREGADO_ATIVO` | `bit` |
| Identificador | `ID` | Atribuída aos dados identificadores de registros. Comumente é parte da chave primária ou chave estrangeira. Pode ser auto incremental.<br>Exemplo:<br>`ID_ENTIDADE` | `int ou bigint` |
| Sigla | `SG` | Atribuída aos dados de natureza alfabética ou alfanumérica que expressam a forma sintética de um conteúdo extenso através da redução de palavras ou expressões a letras ou sílabas iniciais.<br>Exemplo: `SG_UF` | `char ou varchar` |

### 7.3. Classes de Texto

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Descrição | `DE` | Atribuída aos dados cujo conteúdo discursivo e resumido, com até 1.000 caracteres, descreve algo associado ao objeto.<br>Exemplo: `DE_SITUACAO_CONTRATO` | `varchar, nvarchar, text` |
| Endereço | `ED` | Atribuída aos dados de natureza textual utilizado para designar qualquer tipo de endereço. Na documentação, explicar a natureza do endereço.<br>Exemplos: `ED_ENTIDADE`, `ED_EMAIL`, `ED_SITE` | `varchar, nvarchar` |
| Nome | `NO` | Atribuída aos dados de natureza alfabética ou alfanumérica cujo conteúdo expressa uma denominação por extenso, e composta de palavras, abreviaturas ou ambas.<br>Exemplo: `NO_EMPREGADO` | `varchar, nvarchar` |
| Texto | `TX` | Atribuída aos dados cujo conteúdo, livre e em forma discursiva, se utiliza, em geral, para registrar ou documentar algo associado ao objeto de dado. Atribuído a dados que possam ultrapassar os 1.000 caracteres<br>Exemplo: `TX_ADITIVO_CONTRATO` | `varchar, nvarchar, text` |

### 7.4. Classes Numéricas

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Número | `NU` | Atribuída aos dados de natureza numérica cuja identificação se faz por valores absolutos, e que não se enquadram nas demais categorias de identificação.<br>Exemplo: `NU_VERSAO` | `int, smallint, bigint` |
| Percentual | `PC` | Atribuída aos dados de natureza numérica que expressam uma parte de uma quantidade de 100 (cem) unidades.<br>Exemplo: `PC_INADIMPLENCIA` | `decimal` |
| Quantidade | `QT` | Atribuída aos dados de natureza numérica que determinam um conjunto de coisas e pessoas consideradas como equivalentes e suscetíveis de aumento e diminuição. Ao nomeá-los devem ser relacionados a uma unidade de medida para expressar a real grandeza.<br>Exemplo: `QT_EQUIPAMENTO_UNIDADE` | `int, bigint` |
| Valor | `VR` | Atribuída aos dados de natureza numérica que expressam uma importância monetária. ou grandezas de caráter decimal.<br>Exemplos: `VR_SALARIO` e `VR_NIVEL_SINAL` | `decimal` |

### 7.5. Classes Binárias

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Binário | `BI` | Atribuída a dados binários armazenados no banco de dados.<br>Exemplos: `BI_IMAGEM`, `BI_HASH_USUARIO` | `binary, varbinary, varbinary(max)` |

### 7.6. Classes Especializadas

| Nome | Abreviatura | Descrição e exemplos | Tipo (SQL Server) |
|---|---|---|---|
| Geometria | `GE` | Atribuída aos dados de natureza espacial geométrico que representam objetos geométricos.<br>Exemplo: `GE_POLIGONO` | `geometry` |
| Geografia | `GF` | Atribuída aos dados de natureza espacial geográfico que representam informações sobre o local físico e a forma de objetos geométricos.<br>Exemplo: `GF_MUNICIPIO` | `geography` |
| XML | `XL` | Atribuída aos dados de natureza textual, estruturada no padrão XML.<br>Exemplo: `XL_RETORNO_API` | `xml` |

#### 7.6.1. Considerações sobre os campos espaciais (Geometria e Geografia)

**7.6.2.** Ao nomear campos destinados à representação de dados espaciais, é necessário que este reflita claramente o tipo de informação espacial armazenada. Utilize "PONTO" para dados do tipo ponto, "LINHA" para representar linhas e multilinhas, e "POLIGONO" para polígonos e multipolígonos.

**7.6.3.** Evite o uso de coleções de geometrias diversas.

**7.6.4.** Na eventualidade de existirem múltiplos campos geométricos na mesma tabela, é recomendado adicionar um sufixo explicativo para elucidar o propósito de cada um. Exemplos incluem:

- `GE_PONTO`
- `GF_LINHA`
- `GE_POLIGONO_EXTERNO`
- `GE_POLIGONO_INTERNO`

**7.6.5.** Caso haja dados separados de latitude e longitude, sempre deverá ser criado um campo de geometria unindo os dois.

**7.6.6.** Se for necessário, pode ser armazenada a geometria em texto WKT, mas ela não prescinde da existência do campo de geometria binário.

**7.6.7.** Todo campo de geometria deve ter um índice espacial associado.

### 7.7. Combinações de classes não aceitas

As seguintes combinações de classes **não serão aceitas**:

| Classe | Código |
|---|---|
| `AA` | `AA_ANO` |
| `CO` | `CO_CODIGO` |
| `DD` | `DD_DIA` |
| `DE` | `DE_DESCRICAO` |
| `DH` | `DH_DATA_HORA` |
| `DT` | `DT_DATA` |
| `ED` | `ED_ENDERECO` |
| `GE` | `GE_GEOMETRIA` |
| `GE` | `GF_GEOGRAFIA` |
| `HH` | `HH_HORA` |
| `IC` | `IC_INDICADOR` |
| `ID` | `ID_IDENTIFICADOR` |
| `MM` | `MM_MES` |
| `NO` | `NO_NOME` |
| `NU` | `NU_NUMERO` |
| `PC` | `PC_PERCENTUAL` |
| `QT` | `QT_QUANTIDADE` |
| `SG` | `SG_SIGLA` |
| `TX` | `TX_TEXTO` |
| `VR` | `VR_VALOR` |

## 8. Guia rápido de nomes

| Objeto | Padrão | Composição e restrições |
|---|---|---|
| Chave primária | `PK_TT` | PK = Constante<br>TT – Nome da tabela |
| Check Constraint | `CC_XX_YY_99` | CC = Identificador de Check Constraint<br>XX = Nome da tabela<br>YY = Coluna (Opcional quando for o caso de uma geral para a tabela)<br>99 = Sequencial da Constrant, iniciando do 01 |
| Coluna  | `CC_XX` | CC = Classe de Valor<br>XX = Nome da coluna<br>32 Caracteres no máximo |
| Default Constraint | `DF_XX_YY_99` | DF = Identificador de Default Constraint<br>XX = Nome da tabela<br>YY = Nome do campo<br>99 = Sequencial da Constrant, iniciando do 01 |
| Function  | `FN_XX` | FN = Constante<br>XX = Nome da function |
| Índice | `IX_TT_99` | IX = Constante<br>TT = Nome da Tabela<br>99 = Sequencial, iniciando do 01 |
| Modelo de dados | `PDM_XXX` | PDM = Constante de Modelo<br>XXX = Banco de dados (transacionais) ou Tabela Fato (analíticos) |
| Relacionamento | `FK_TF_TP_XX` | FK = Constante<br>TF = Tabela Filho<br>TP = Tabela Pai<br>XX – Qualificador da relação |
| Stored Procedure | `SP_XX` | SP = Constante<br>XX = Nome da procedure |
| Tabela | `CC_XX` | CC = Classe de tabela<br>XX = Nome da tabela<br>32 Caracteres no máximo |
| Trigger | `TRG_XX_YY_9` | TRG = Constante<br>XX = Nome da tabela<br>YY = Nome negocial da trigger<br>9 = Sequencial, iniciando do 1 |
| View | `VW_XX` | IX = Identificador de view<br>XX = Nome da visualização |
