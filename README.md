# Pipeline de Dados — Meios de Pagamento (BCB)

> Projeto de Engenharia de Dados para ingestão, transformação e armazenamento de dados públicos do Banco Central do Brasil sobre meios de pagamento eletrônico.

---

## Sumário

1. [Contexto e Objetivo](#1-contexto-e-objetivo)
2. [Stack Tecnológico](#2-stack-tecnológico)
3. [Arquitetura do Sistema](#3-arquitetura-do-sistema)
4. [Modelagem de Dados](#4-modelagem-de-dados)
5. [Fontes de Dados e Endpoints](#5-fontes-de-dados-e-endpoints)
6. [Estrutura do Projeto](#6-estrutura-do-projeto)
7. [Fluxo de Carga — Jobs e Transformações](#7-fluxo-de-carga--jobs-e-transformações)
8. [Infraestrutura e Docker](#8-infraestrutura-e-docker)
9. [Configuração e Execução](#9-configuração-e-execução)
10. [Segurança e Acesso](#10-segurança-e-acesso)

---

## 1. Contexto e Objetivo

Este pipeline consome dados trimestrais da API pública do Banco Central do Brasil (BCB) referentes ao mercado de meios de pagamento eletrônico no país. Os dados são extraídos, transformados e carregados em um Data Warehouse dimensional (Star Schema) hospedado em um banco PostgreSQL containerizado.

**Dados coletados:**
- Quantidade e valor de transações com cartões (por bandeira, função, produto e modalidade)
- Quantidade de terminais POS e PDV por estado
- Quantidade de estabelecimentos credenciados por estado e tipo de captura
- Tarifas de anuidade média e programas de pontos/recompensas de cartões

**Cobertura temporal:** 2019 Q1 até o trimestre anterior ao da execução (dados ficam disponíveis ~90 dias após o encerramento do trimestre).

---

## 2. Stack Tecnológico

| Componente | Tecnologia | Versão |
|---|---|---|
| Orquestração e ETL | Pentaho Data Integration (PDI / Kettle) | 9.4 |
| Banco de Dados | PostgreSQL | 18 |
| Containerização | Docker | — |
| Acesso Remoto | WireGuard VPN | — |
| Versionamento | Git / GitHub | — |
| Linguagem de Script | JavaScript (Rhino Engine — PDI) | — |

---

## 3. Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                        FONTES EXTERNAS                          │
│                                                                 │
│  API BCB (OData)                       API IBGE (REST)          │
│  olinda.bcb.gov.br                     servicodados.ibge.gov.br │
└───────────────────┬────────────────────────────┬────────────────┘
                    │                            │
                    ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PENTAHO DATA INTEGRATION (PDI)                 │
│                                                                 │
│  ┌───────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ job_carga_    │    │ job_fato_    │    │  job_fato_       │  │
│  │ dados.kjb     │───►│ transacoes   │    │  terminais.kjb   │  │
│  │ (orquestrador)│    │ .kjb         │    │                  │  │
│  └───────┬───────┘    └──────────────┘    └──────────────────┘  │
│          │                                                       │
│  ┌───────▼──────────────────────────────────────────────────┐   │
│  │              TRANSFORMAÇÕES (.ktr)                        │   │
│  │  dim_tempo  │ dim_local  │ dim_cartao  │ fato_*          │   │
│  └──────────────────────────────────────┬────────────────────┘  │
└─────────────────────────────────────────┼───────────────────────┘
                                          │
                    ┌─────────────────────▼──────────────────────┐
                    │        PostgreSQL 18 (Docker)               │
                    │          schema: dm_financeiro              │
                    │                                             │
                    │  DIMENSÕES          FATOS         STAGING   │
                    │  dim_tempo          fato_         stg_      │
                    │  dim_cartao         transacoes    portadorda │
                    │  dim_local          fato_         stg_      │
                    │                     terminais    terminais_ │
                    │                     fato_         estab     │
                    │                     movimentacoes           │
                    └─────────────────────────────────────────────┘
                                          │
                    ┌─────────────────────▼──────────────────────┐
                    │         Acesso via WireGuard VPN            │
                    │         Host: <HOSTVPN>  Porta: <PORT>        │
                    └─────────────────────────────────────────────┘
```

---

## 4. Modelagem de Dados

O modelo segue o padrão **Star Schema** com fato e dimensão no schema `dm_financeiro`.

### Dimensões

#### `dim_tempo`
| Coluna | Tipo | Descrição |
|---|---|---|
| `tempo_sk` | SERIAL PK | Surrogate key |
| `temp_ano_trimestre` | INTEGER | Período no formato YYYYQ (ex: 20241) |
| `temp_ano` | INTEGER | Ano |
| `temp_trimestre` | INTEGER | Número do trimestre (1–4) |
| `temp_flag_primeiro_trimestre` | BOOLEAN | Verdadeiro se Q1 |
| `temp_flag_ultimo_trimestre` | BOOLEAN | Verdadeiro se Q4 |
| `temp_inicio_trimestre_estimado` | DATE | Início estimado do trimestre |
| `temp_fim_trimestre_estimado` | DATE | Fim estimado do trimestre |

#### `dim_cartao`
| Coluna | Tipo | Descrição |
|---|---|---|
| `cart_sk` | SERIAL PK | Surrogate key |
| `car_bandeira` | VARCHAR | Nome da bandeira (ex: VISA, MASTERCARD) |
| `car_funcao` | VARCHAR | Função do cartão (CRÉDITO, DÉBITO, etc.) |
| `car_produto` | VARCHAR | Tipo de produto do cartão |
| `car_modalidade` | VARCHAR | Modalidade de uso |

#### `dim_local`
| Coluna | Tipo | Descrição |
|---|---|---|
| `loc_sk` | SERIAL PK | Surrogate key |
| `loc_uf` | VARCHAR(2) | Sigla do estado (ex: SP, RJ) |
| `loc_estado` | VARCHAR | Nome completo do estado |
| `loc_latidude` | NUMERIC | Latitude do centroide do estado |
| `loc_longitude` | NUMERIC | Longitude do centroide do estado |
| `loc_cod_ibge` | INTEGER | Código IBGE do estado |
| `loc_regicao` | VARCHAR | Região geográfica (Norte, Nordeste, etc.) |

### Fatos

#### `fato_transacoes`
| Coluna | Tipo | Descrição |
|---|---|---|
| `tra_sk_cartao` | INT FK | Referência a `dim_cartao` |
| `tra_sk_tempo` | INT FK | Referência a `dim_tempo` |
| `tra_qtdCartoesEmitidos` | INT | Quantidade de cartões emitidos |
| `tra_qtdCartoesAtivos` | INT | Quantidade de cartões ativos |
| `tra_qtdTransacoesNacionais` | INT | Qtd. de transações nacionais |
| `tra_valorTransacoesNacionais` | NUMERIC | Valor total de transações nacionais |
| `tra_qtdTransacoesInternacionais` | INT | Qtd. de transações internacionais |
| `tra_valorTransacoesInternacionais` | NUMERIC | Valor total de transações internacionais |
| `tra_tarifaAnuidadeMedia` | NUMERIC | Tarifa média de anuidade (via API PORTADORDA) |
| `tra_qtdPontosAcumulados` | INT | Pontos de fidelidade acumulados |
| `tra_qtdPontosAdquiridos` | INT | Pontos adquiridos no período |
| `tra_qtdPontosConvertidos` | INT | Pontos convertidos em benefício |
| `tra_qtdPontosExpirados` | INT | Pontos expirados |
| `tra_valorGastoProgramaRecompra` | NUMERIC | Valor gasto em programas de recompra |
| `s_t_a_m_p` | DATE | Data de carga |

#### `fato_terminais`
| Coluna | Tipo | Descrição |
|---|---|---|
| `ter_sk_tempo` | INT FK | Referência a `dim_tempo` |
| `ter_sk_local` | INT FK | Referência a `dim_local` |
| `ter_qtdTermPOS` | INT | Quantidade de terminais POS |
| `ter_qtdTermPOScompartilhados` | INT | POS compartilhados entre credenciadoras |
| `ter_qtdTermPOSchip` | INT | POS com tecnologia chip |
| `ter_qtdTermPDV` | INT | Quantidade de terminais PDV |
| `ter_qtdEstabTotal` | INT | Total de estabelecimentos credenciados (via API INFRESTADA) |
| `ter_qtdEstabCapturaEletronica` | INT | Estabelecimentos com captura eletrônica |
| `ter_qtdEstabCapturaRemota` | INT | Estabelecimentos com captura remota |
| `s_t_a_m_p` | DATE | Data de carga |

### Tabelas de Staging

#### `stg_portadorda`
Staging intermediária para dados de tarifas e pontos da API PORTADORDA. Truncada a cada execução e usada para o `Bulk UPDATE` da `fato_transacoes`.

#### `stg_terminais_estab`
Staging intermediária para dados de estabelecimentos credenciados da API INFRESTADA. Apagada por trimestre e usada para o `Bulk UPDATE` da `fato_terminais`.

```sql
CREATE TABLE dm_financeiro.stg_terminais_estab (
    ter_sk_tempo              int4,
    ter_sk_local              int4,
    qtdEstabTotal             int4,
    qtdEstabCapturaEletronica int4,
    qtdEstabCapturaRemota     int4,
    s_t_a_m_p                 date DEFAULT CURRENT_DATE
);
```

---

## 5. Fontes de Dados e Endpoints

Todas as APIs são públicas, sem autenticação, no padrão **OData v4** do BCB.

**Base URL:** `https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/versao/v1/odata/`

### API BCB — Endpoints Utilizados

| Endpoint | Parâmetro | Usado em | Descrição |
|---|---|---|---|
| `Quantidadeetransacoesdecartoes(trimestre=@trimestre)` | `@trimestre` (YYYYQ) | `fato_transacoes.ktr`, `dim_cartao.ktr` | Quantidade e valor de transações com cartões por bandeira/função/produto/modalidade |
| `PORTADORDA(trimestre=@trimestre)` | `@trimestre` (YYYYQ) | `fato_transacoes_portadorda.ktr` | Tarifas de anuidade e dados de programas de pontos/recompensas por portador |
| `INFRTERMDA(trimestre=@trimestre)` | `@trimestre` (YYYYQ) | `fato_terminais.ktr`, `dim_local.ktr` | Quantidade de terminais POS e PDV por UF |
| `INFRESTADA(trimestre=@trimestre)` | `@trimestre` (YYYYQ) | `fato_terminais_estab.ktr` | Quantidade de estabelecimentos credenciados por UF e tipo de captura |

### Parâmetros OData suportados

| Parâmetro | Exemplo | Descrição |
|---|---|---|
| `$format` | `json` | Formato da resposta |
| `$top` | `10000` | Número máximo de registros retornados |
| `$filter` | `trimestre eq '20241'` | Filtro de seleção — **só funciona em `Quantidadeetransacoesdecartoes`** (ver nota abaixo) |
| `$orderby` | `UFTerminal asc` | Ordenação |

**Exemplo de URL completa:**
```
https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/versao/v1/odata/
INFRTERMDA(trimestre=@trimestre)?@trimestre='20241'&$format=json&$top=10000
```

> ⚠️ **Comportamento cumulativo do parâmetro `@trimestre`**
>
> O parâmetro de função `(trimestre=@trimestre)` **não filtra um único trimestre** — ele
> retorna o trimestre informado **e todos os posteriores** até o mais recente disponível
> (ex.: `@trimestre='20191'` traz de 20191 até hoje). Como o pipeline itera período a período,
> sem recorte cada trimestre seria contado várias vezes (somatórios inflados nas fatos de
> terminais, staging/inserts duplicados nas demais).
>
> O recorte para manter **apenas o trimestre da chamada** depende do endpoint:
>
> | Endpoint | `trimestre` | `$filter` no servidor | Estratégia de recorte |
> |---|---|---|---|
> | `Quantidadeetransacoesdecartoes` | string | ✅ `$filter=trimestre eq '20241'` | servidor (`dim_cartao`, `fato_transacoes`) |
> | `PORTADORDA` | int | ❌ HTTP 400 | filtro client-side no PDI (`fato_transacoes_portadorda`) |
> | `INFRTERMDA` | int | ❌ HTTP 400/500 | filtro client-side no PDI (`fato_terminais`, `dim_local`) |
> | `INFRESTADA` | int | ❌ HTTP 400 | filtro client-side no PDI (`fato_terminais_estab`) |
>
> O filtro client-side é um passo `Filter Rows` (`trimestre = periodo`) logo após o `JSON Input`,
> mantendo somente as linhas do trimestre solicitado.

### API IBGE — Enriquecimento Geográfico

| Endpoint | Usado em | Descrição |
|---|---|---|
| `https://servicodados.ibge.gov.br/api/v1/localidades/estados/{sigla}` | `dim_local.ktr` | Nome, código e região do estado a partir da sigla UF |

**Campos extraídos do IBGE:** `id` (código numérico), `sigla`, `nome`, `regiao.nome`.

**Campos complementados manualmente:** latitude e longitude (centroides pré-calculados para todos os 27 estados).

### Cobertura Temporal

Os endpoints exigem um trimestre no formato `YYYYQ` (ex: `20241` = 2024 Q1). O pipeline gera automaticamente todos os períodos de `20191` até o **trimestre anterior à data de execução**, respeitando o atraso de ~90 dias de disponibilidade dos dados do BCB.

---

## 6. Estrutura do Projeto

```
pipeline_data_sad/
│
├── jobs/                              # Orquestração ETL (Kettle Jobs)
│   ├── job_carga_dados.kjb            # Job principal — executa toda a carga
│   ├── job_fato_transacoes.kjb        # Job de transações (API1 + API PORTADORDA)
│   └── job_fato_terminais.kjb         # Job de terminais (INFRTERMDA + INFRESTADA)
│
├── transformations/
│   ├── dim/                           # Dimensões
│   │   ├── dim_tempo.ktr              # Calendário trimestral
│   │   ├── dim_cartao.ktr             # Produtos de cartão
│   │   └── dim_local.ktr              # Estados brasileiros (BCB + IBGE)
│   └── fato/                          # Fatos e staging
│       ├── fato_transacoes.ktr        # Transações — DELETE+INSERT (API principal)
│       ├── fato_transacoes_portadorda.ktr  # Tarifas/pontos — staging
│       ├── fato_terminais.ktr         # Terminais POS/PDV — DELETE+INSERT
│       └── fato_terminais_estab.ktr   # Estabelecimentos — staging
│
├── docker/
│   ├── data_base.Dockerfile           # Imagem PostgreSQL 18 com dump inicial
│   └── init-db.sh                     # Script de restore do dump no container
│
├── backup/
│   └── <DATABASE>                          # Dump binário do banco (pg_restore)
│
├── .env                               # Credenciais do banco (não versionado)
├── .gitignore
├── sad.sh                             # Script de gerenciamento do Docker
├── LICENSE
└── README.md
```

---

## 7. Fluxo de Carga — Jobs e Transformações

### 7.1 Job Principal: `job_carga_dados.kjb`

Orquestra toda a carga na seguinte ordem sequencial. Cada etapa só inicia se a anterior for bem-sucedida.

```
START
  │
  ├─► dim_tempo          (dimensão temporal — insert incremental)
  │
  ├─► dim_local          (dimensão geográfica — insert incremental)
  │
  ├─► dim_cartao         (dimensão de cartões — insert incremental)
  │
  ├─► job_fato_transacoes  ──► (ver 7.2)
  │
  ├─► job_fato_terminais   ──► (ver 7.3)
  │
  └─► SUCCESS
```

> As dimensões são carregadas primeiro para garantir que as chaves surrogate (`SK`) existam antes da carga dos fatos.

---

### 7.2 Job: `job_fato_transacoes.kjb`

Parâmetro: `TRIMESTRE` (opcional — vazio carrega todos os períodos pendentes)

```
START
  │
  ├─► fato_transacoes.ktr
  │     Endpoint: Quantidadeetransacoesdecartoes(trimestre=@trimestre)
  │     Operação: DELETE por trimestre → INSERT em fato_transacoes
  │     Lookups: dim_cartao (bandeira/função/produto/modalidade)
  │              dim_tempo  (temp_ano_trimestre)
  │
  ├─► fato_transacoes_portadorda.ktr
  │     Endpoint: PORTADORDA(trimestre=@trimestre)
  │     Operação: DELETE por trimestre em stg_portadorda → INSERT stg_portadorda
  │     Lookups: dim_cartao, dim_tempo
  │
  ├─► Bulk UPDATE fato_transacoes FROM stg_portadorda
  │     SQL:
  │       UPDATE dm_financeiro.fato_transacoes f
  │       SET tra_tarifaAnuidadeMedia        = s.tarifaAnuidadeMedia,
  │           tra_qtdPontosAcumulados        = s.qtdPontosAcumulados,
  │           tra_qtdPontosAdquiridos        = s.qtdPontosAdquiridos,
  │           tra_qtdPontosConvertidos       = s.qtdPontosConvertidos,
  │           tra_qtdPontosExpirados         = s.qtdPontosExpirados,
  │           tra_valorGastoProgramaRecompra = s.valorGastoProgramaRecompra
  │       FROM dm_financeiro.stg_portadorda s
  │       WHERE f.tra_sk_cartao = s.tra_sk_cartao
  │         AND f.tra_sk_tempo  = s.tra_sk_tempo
  │
  └─► SUCCESS
```

---

### 7.3 Job: `job_fato_terminais.kjb`

Parâmetro: `TRIMESTRE` (opcional)

```
START
  │
  ├─► fato_terminais.ktr
  │     Endpoint: INFRTERMDA(trimestre=@trimestre)
  │     Operação: DELETE por trimestre → INSERT em fato_terminais
  │     Campos inseridos: ter_qtdTermPOS, ter_qtdTermPOScompartilhados,
  │                       ter_qtdTermPOSchip, ter_qtdTermPDV
  │     Lookups: dim_local (loc_uf = UFTerminal → loc_sk)
  │              dim_tempo (temp_ano_trimestre = trimestre → tempo_sk)
  │
  ├─► fato_terminais_estab.ktr
  │     Endpoint: INFRESTADA(trimestre=@trimestre)
  │     Operação: DELETE por trimestre em stg_terminais_estab → INSERT stg
  │     Campos inseridos: qtdEstabTotal, qtdEstabCapturaEletronica,
  │                       qtdEstabCapturaRemota
  │     (qtdEstabCapturaManual NAO e carregado: o BCB nao publica esse campo — sempre null)
  │     Lookups: dim_local (loc_uf = UFEstabelecimento → loc_sk)
  │              dim_tempo (temp_ano_trimestre = trimestre → tempo_sk)
  │
  ├─► Bulk UPDATE fato_terminais FROM stg_terminais_estab
  │     SQL:
  │       UPDATE dm_financeiro.fato_terminais f
  │       SET ter_qtdEstabTotal             = s.qtdEstabTotal,
  │           ter_qtdEstabCapturaEletronica = s.qtdEstabCapturaEletronica,
  │           ter_qtdEstabCapturaRemota     = s.qtdEstabCapturaRemota
  │       FROM dm_financeiro.stg_terminais_estab s
  │       WHERE f.ter_sk_local = s.ter_sk_local
  │         AND f.ter_sk_tempo = s.ter_sk_tempo
  │
  └─► SUCCESS
```

---

### 7.4 Transformação: `dim_tempo.ktr`

Gera automaticamente o calendário trimestral sem dependência de API.

```
Geração de Datas (30 linhas, início 01/01/2019)
  → Sequencia (ID 1–30)
  → Campo 01 (calcula data, trimestre, fim estimado)
  → Campos 02 (flags: primeiro/último trimestre)
  → Calculator (extrai ano)
  → Select values (renomeia, remove campos)
  → Concatenando Campo (gera temp_ano_trimestre = ano || trimestre)
  → Tratamento de Campos Final (converte tipos, remove ID)
  → Database Lookup - Verifica Existencia
      (SELECT tempo_sk FROM dm_financeiro.dim_tempo
       WHERE temp_ano_trimestre = ?)
  → Filter - Apenas Novos Trimestres (existe_sk IS NULL)
  → Gravando dim_tempo (INSERT dm_financeiro.dim_tempo)
```

> **Idempotente:** execuções repetidas não duplicam registros.

---

### 7.5 Transformação: `dim_local.ktr`

Combina dados da API BCB e da API IBGE para montar a dimensão geográfica.

```
Gera Periodos (60 linhas)
  → Sequence - Indice
  → JavaScript - Calcula Periodo (gera YYYYQ, valida limite)
  → Filter - Apenas Periodos Validos
  → Calculator - Monta URL BCB
      URL: INFRTERMDA(trimestre=@trimestre)?...&$select=UFTerminal
  → REST Client - BCB API
  → JSON Input - Parseia UFTerminal (extrai $.value[*].UFTerminal)
  → Sort Rows - Para Unique UF
  → Unique Rows - Deduplica UF
  → Calculator - Monta URL IBGE
      URL: https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}
  → REST Client - IBGE API
  → JSON Input - Parseia Estado IBGE (id, sigla, nome, regiao.nome)
  → Filter - Apenas UF Valida (sigla NOT NULL)
  → JavaScript - Add Lat Lon (centroide pré-calculado para cada UF)
  → Select Values - Renomeia Campos (loc_uf, loc_estado, loc_cod_ibge, loc_regicao)
  → Database Lookup - Verifica Existente
      (SELECT loc_sk FROM dm_financeiro.dim_local WHERE loc_uf = ?)
  → Filter - Apenas Novos (existe_flag IS NULL)
  → Table Output - dim_local (INSERT dm_financeiro.dim_local)
```

---

### 7.6 Transformação: `dim_cartao.ktr`

Extrai combinações únicas de cartão (bandeira × função × produto × modalidade) da API de transações.

```
Gera Periodos (60 linhas)
  → Sequence - Indice
  → JavaScript - Calcula Periodo
  → Filter - Apenas Periodos Validos
  → Calculator - Monta URL BCB
      URL: Quantidadeetransacoesdecartoes(trimestre=@trimestre)?...
  → REST Client - BCB API
  → JSON Input - Parseia Response
      (nomeBandeira, nomeFuncao, produto, modalidade)
  → Select Values - Renomeia Campos (car_bandeira, car_funcao, car_produto, car_modalidade)
  → Sort Rows - Para Unique
  → Unique Rows - Deduplica
  → Database Lookup - Verifica Existente
      (SELECT car_bandeira FROM dm_financeiro.dim_cartao
       WHERE car_bandeira=? AND car_funcao=? AND car_produto=? AND car_modalidade=?)
  → Filter - Apenas Novos DC (existe_flag IS NULL)
  → Table Output - dim_cartao (INSERT dm_financeiro.dim_cartao)
```

---

### 7.7 Estratégia de Carga por Fato

| Estratégia | Aplicação | Detalhe |
|---|---|---|
| **DELETE + INSERT** | `fato_transacoes`, `fato_terminais` | Remove registros do trimestre antes de reinserir. Controle via parâmetro `TRIMESTRE`. |
| **TRUNCATE + INSERT** | `stg_portadorda`, `stg_terminais_estab` | Staging limpa a cada execução do trimestre processado. |
| **Bulk UPDATE** | `fato_transacoes`, `fato_terminais` | Atualiza colunas de uma segunda API usando a staging como origem, com join por SK. |
| **INSERT incremental** | `dim_tempo`, `dim_cartao`, `dim_local` | Lookup verifica existência; insere apenas registros novos. Nunca apaga dados existentes. |

---

## 8. Infraestrutura e Docker

### Dockerfile (`docker/data_base.Dockerfile`)

```dockerfile
FROM postgres:18
LABEL project="pipeline-sad" environment="producao" database="<DATABASE>"

COPY backup/<DATABASE> /tmp/<DATABASE>.dump
COPY docker/init-db.sh /docker-entrypoint-initdb.d/01_init-db.sh
RUN chmod +x /docker-entrypoint-initdb.d/01_init-db.sh

VOLUME ["/var/lib/postgresql"]
EXPOSE 5432
```

### Script de Inicialização (`docker/init-db.sh`)

```bash
#!/bin/bash
set -e
pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --no-owner --role="$POSTGRES_USER" /tmp/<DATABASE>.dump
```

### Gerenciamento com `sad.sh`

```bash
./sad.sh -up       # Build da imagem, cria volume e inicia o container
./sad.sh -stop     # Para o container (dados preservados no volume)
./sad.sh -remove   # Remove container, imagem e volume (destrutivo)
```

**Pré-requisitos validados pelo script:**
1. Docker daemon ativo
2. Interface WireGuard `wg0` ativa
3. Arquivo `.env` presente

### Configuração Docker

| Item | Valor |
|---|---|
| Nome do container | `pipeline-sad-db` |
| Volume | `pipeline-sad-pgdata` → `/var/lib/postgresql` |
| Porta exposta | `<PORT>` (host) → `5432` (container) |
| IP de bind | `<HOSTVPN>` (WireGuard) |
| Política de restart | `unless-stopped` |

---

## 9. Configuração e Execução

### Variáveis de Ambiente

Criar o arquivo `.env` na raiz do projeto (nunca versionar):

```env
POSTGRES_DB=<DATABASE>
POSTGRES_USER=<USER>
POSTGRES_PASSWORD=<senha_segura>
```

Configurar no Pentaho via `kettle.properties` (em `~/.kettle/`):

```properties
DB_HOST_SAD=<HOSTVPN>
DB_NAME_SAD=<DATABASE>
DB_PORT_SAD=<PORT>
DB_USER_SAD=<USER>
DB_PASSWORD_SAD=<senha_segura>
```

### Pré-requisitos

- Pentaho Data Integration 9.x instalado
- Java 11+ no PATH
- Docker instalado e em execução
- WireGuard configurado com interface `wg0` ativa
- Arquivo `.env` presente na raiz do projeto

### Iniciar o Banco de Dados

```bash
./sad.sh -up
```

### Executar o Pipeline

**Via Spoon (GUI):**
1. Abrir o Pentaho Spoon
2. Carregar `jobs/job_carga_dados.kjb`
3. Definir variáveis de ambiente se necessário
4. Clicar em **Run**

**Via Kitchen (CLI):**
```bash
# Linux/macOS
kitchen.sh -file="jobs/job_carga_dados.kjb" -level=Basic

# Windows
kitchen.bat /file:"jobs\job_carga_dados.kjb" /level:Basic
```

**Carga de um trimestre específico:**
```bash
kitchen.sh -file="jobs/job_fato_transacoes.kjb" -param:TRIMESTRE=20241 -level=Basic
```

**Parâmetro `TRIMESTRE`:**
- Vazio (padrão): carrega todos os períodos de 20191 até o trimestre anterior
- Preenchido (ex: `20241`): carrega ou recarrega apenas aquele trimestre

### Conexão Direta ao Banco

```bash
# Via Docker (na VPS)
docker exec -it pipeline-sad-db psql -U <USER> -d <DATABASE>

# Via psql local (requer WireGuard ativo)
psql -h <HOSTVPN> -p <PORT> -U <USER> -d <DATABASE>
```

---

## 10. Segurança e Acesso

### WireGuard VPN

O banco de dados **não é acessível pela internet**. Toda conexão passa pela VPN WireGuard:

- Rede: `10.0.0.0/24`
- IP do servidor: `<HOSTVPN>`
- O container expõe a porta `<PORT>` apenas no IP WireGuard

### Variáveis de Ambiente

- Credenciais nunca são hardcoded nos arquivos `.kjb`/`.ktr`
- Senhas usam placeholders `${DB_PASSWORD_SAD}` resolvidos em runtime pelo PDI
- O arquivo `.env` está no `.gitignore`

### Senhas no PDI

Senhas armazenadas no Pentaho são criptografadas com o prefixo `Encrypted ` (algoritmo proprietário Kettle).

---

## Diagrama de Dependências das Transformações

```
                     ┌──────────────┐
                     │  dim_tempo   │
                     └──────┬───────┘
                             │ tempo_sk
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────────┐  ┌──────────────┐
│  dim_cartao  │    │  fato_transacoes │  │  fato_       │
│              │───►│                  │  │  terminais   │
└──────────────┘    └──────────────────┘  └──────────────┘
        car_sk              ▲                    ▲
                             │                    │
                    ┌────────┘             ┌──────┘
                    │                      │ loc_sk
              ┌─────┴────────┐      ┌──────┴───────┐
              │ stg_portadorda│      │  dim_local   │
              └──────────────┘      └──────────────┘
                                           ▲
                                    ┌──────┘
                              ┌─────┴────────────────┐
                              │  stg_terminais_estab  │
                              └──────────────────────┘
```
