# Documentação Técnica — Pipeline de Dados: Meios de Pagamento (BCB)

> Projeto de Engenharia de Dados desenvolvido como trabalho acadêmico/profissional.  
> Apresentação: [Slides de Arquitetura e Decisões de Projeto](https://docs.google.com/presentation/d/1wqZ1Qz7q3WXKUcJgXXYAGh_k9-uwdDZO2D5WIIVncx4/edit?usp=sharing)

---

## Sumário

1. [Contexto e Objetivo](#1-contexto-e-objetivo)
2. [Visão Arquitetural](#2-visão-arquitetural)
3. [Stack Tecnológico](#3-stack-tecnológico)
4. [Fonte de Dados](#4-fonte-de-dados)
5. [Modelagem do Data Warehouse](#5-modelagem-do-data-warehouse)
6. [Pipeline ETL — Detalhamento](#6-pipeline-etl--detalhamento)
7. [Orquestração](#7-orquestração)
8. [Infraestrutura e DevOps](#8-infraestrutura-e-devops)
9. [Segurança e Acesso](#9-segurança-e-acesso)
10. [Configuração e Execução](#10-configuração-e-execução)
11. [Decisões de Projeto](#11-decisões-de-projeto)

---

## 1. Contexto e Objetivo

O **Sistema de Análise de Dados (SAD)** é um pipeline de engenharia de dados que coleta, transforma e armazena informações sobre meios de pagamento eletrônico no Brasil, a partir da **API pública do Banco Central do Brasil (BCB)**.

O projeto endereça um problema recorrente em análise econômica: dados de meios de pagamento estão disponíveis em APIs públicas governamentais em formato não estruturado para consumo analítico. O pipeline os transforma em um modelo dimensional consultável, base para relatórios e análises de Business Intelligence.

**Objetivos específicos:**
- Consumir dados trimestrais de quantidade de transações com cartão da API do BCB de forma automatizada
- Estruturar os dados em um Data Warehouse com modelo Star Schema
- Garantir carga incremental idempotente — sem reprocessamento de períodos já carregados
- Disponibilizar o banco de forma segura em infraestrutura cloud via VPN

---

## 2. Visão Arquitetural

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTE DE DADOS                               │
│                                                                     │
│   API REST — Banco Central do Brasil (dados abertos, trimestral)    │
│   Arquivos CSV locais (dim_cartao, dim_local)                       │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     CAMADA ETL — Pentaho PDI                        │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐    │
│  │  dim_tempo   │   │  dim_cartao  │   │      dim_local       │    │
│  │    .ktr      │   │    .ktr      │   │        .ktr          │    │
│  └──────────────┘   └──────────────┘   └──────────────────────┘    │
│                                                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────┐        │
│  │   fato_movimentacoes     │  │     fato_terminais       │        │
│  │         .ktr             │  │         .ktr             │        │
│  └──────────────────────────┘  └──────────────────────────┘        │
│                                                                     │
│              Orquestrado por: jobs/Fatos.kjb                        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CAMADA DE ARMAZENAMENTO                           │
│                                                                     │
│   PostgreSQL 18 — banco bcbdb                                       │
│   Rodando em container Docker em VPS Linux                          │
│   Acesso restrito via túnel WireGuard (10.0.0.1:8594)              │
└─────────────────────────────────────────────────────────────────────┘
```

**Padrão arquitetural adotado:** ETL clássico com separação clara entre camadas de extração, transformação e carga, com modelo dimensional de destino.

---

## 3. Stack Tecnológico

| Componente | Tecnologia | Versão | Função |
|---|---|---|---|
| Orquestração ETL | Pentaho Data Integration (PDI/Kettle) | 9.x | Criação e execução do pipeline |
| Interface ETL | Spoon (GUI) / Kitchen (CLI) | 9.x | Desenvolvimento e operação |
| Banco de dados | PostgreSQL | 18 | Data Warehouse |
| Containerização | Docker | — | Portabilidade do banco |
| VPN | WireGuard | — | Acesso seguro ao banco |
| Servidor | VPS Linux (Ubuntu) | — | Hospedagem do container |
| Versionamento | Git + GitHub | — | Controle de versão |
| Encoding | UTF-8 sem BOM | — | Compatibilidade cross-platform |

---

## 4. Fonte de Dados

### API do Banco Central do Brasil

| Atributo | Detalhe |
|---|---|
| Provedor | Banco Central do Brasil |
| Tipo | API REST pública (dados abertos) |
| Série consumida | Quantidade de transações com instrumentos de pagamento |
| Granularidade | Trimestral |
| Período mínimo | 1º trimestre de 2019 (`20191`) |
| Máximo dinâmico | Trimestre imediatamente anterior ao atual |
| Autenticação | Não requerida (API pública) |

A codificação de período adotada usa o formato `YYYYQ`, onde `YYYY` é o ano e `Q` é o número do trimestre (1 a 4). Exemplo: `20232` representa o 2º trimestre de 2023.

### Arquivos CSV

Duas dimensões são populadas a partir de arquivos CSV locais:

| Arquivo | Dimensão | Descrição |
|---|---|---|
| `cartoes.csv` | `dim_cartao` | Atributos de produtos de cartão (bandeira, função, produto, modalidade, parcelamento) |
| `estados_brasil.csv` | `dim_local` | Dados geográficos dos estados brasileiros (UF, nome, coordenadas, IBGE, região) |

---

## 5. Modelagem do Data Warehouse

O modelo segue o padrão **Star Schema** (Esquema Estrela), com duas tabelas fato e três dimensões.

```
                        ┌───────────────┐
                        │   dim_tempo   │
                        │─────────────  │
                        │ tempo_sk (PK) │
                        │ ano           │
                        │ trimestre     │
                        │ temp_periodo  │
                        │ dt_ini_trim   │
                        │ dt_fim_trim   │
                        └───────┬───────┘
                                │
           ┌────────────────────┼────────────────────┐
           │                    │                    │
┌──────────┴──────┐   ┌─────────┴────────┐  ┌───────┴──────────┐
│   dim_cartao    │   │ fat_movimentacoes │  │    dim_local     │
│─────────────────│   │──────────────────│  │──────────────────│
│ car_bandeira    │◄──│ fk_tempo         │  │ loc_uf (PK)      │
│ car_funcao      │   │ fk_cartao        │──►loc_estado        │
│ car_produto     │   │ fk_local         │  │ loc_latidude     │
│ car_modalidade  │   │ [métricas]       │  │ loc_longitude    │
│ car_num_parcela │   └──────────────────┘  │ loc_cod_ibge     │
└─────────────────┘                         │ loc_regicao      │
                                            └──────────────────┘
                          ┌──────────────────┐
                          │  fat_terminais   │
                          │──────────────────│
                          │ fk_tempo         │
                          │ fk_cartao        │
                          │ fk_local         │
                          │ [métricas]       │
                          └──────────────────┘
```

### Tabelas de Dimensão

#### `dim_tempo` — Dimensão Temporal
Gerada automaticamente pelo pipeline. Cobre 30 trimestres a partir de 01/01/2019.

| Coluna | Tipo | Descrição |
|---|---|---|
| `tempo_sk` | SERIAL (PK) | Surrogate key gerada por sequência |
| `ano` | INTEGER | Ano (ex: 2023) |
| `trimestre` | INTEGER | Número do trimestre (1–4) |
| `temp_periodo` | VARCHAR | Período no formato YYYYQ (ex: 20231) |
| `temp_flag_primeiro_trimestre` | BOOLEAN | Verdadeiro se trimestre = 1 |
| `temp_flag_ultimo_trimestre` | BOOLEAN | Verdadeiro se trimestre = 4 |
| `dt_ini_trim` | DATE | Data de início do trimestre |
| `dt_fim_trim` | DATE | Data estimada de fim do trimestre |

> A sequência é resetada a cada carga (`RESTART WITH 1`) garantindo surrogate keys consistentes após truncate.

#### `dim_cartao` — Dimensão de Cartões
Carregada a partir de CSV. Descreve os atributos de produtos de cartão.

| Coluna | Tipo | Descrição |
|---|---|---|
| `car_bandeira` | VARCHAR(10) | Bandeira do cartão (ex: Visa, Mastercard) |
| `car_funcao` | VARCHAR(7) | Função (ex: Crédito, Débito) |
| `car_produto` | VARCHAR(8) | Produto (ex: Básico, Gold) |
| `car_modalidade` | VARCHAR(9) | Modalidade de uso |
| `car_numero_parcela` | VARCHAR(6) | Configuração de parcelamento |

#### `dim_local` — Dimensão Geográfica
Carregada a partir de CSV. Cobre todos os estados do Brasil com dados do IBGE.

| Coluna | Tipo | Descrição |
|---|---|---|
| `loc_uf` | VARCHAR(2) | Sigla do estado (ex: SP, RJ) |
| `loc_estado` | VARCHAR(8) | Nome do estado |
| `loc_latidude` | DOUBLE | Latitude central do estado |
| `loc_longitude` | DOUBLE | Longitude central do estado |
| `loc_cod_ibge` | INTEGER | Código IBGE do estado |
| `loc_regicao` | VARCHAR(8) | Macrorregião (ex: Sudeste) |

### Tabelas de Fato

#### `fat_movimentacoes` — Fato de Movimentações
Armazena o volume de transações de meios de pagamento por período trimestral.

#### `fat_terminais` — Fato de Terminais
Armazena dados de terminais de pagamento (PDVs) ativos por período trimestral.

Ambas as tabelas fato se relacionam com as três dimensões através de chaves estrangeiras e contêm as métricas quantitativas provindas da API do BCB.

---

## 6. Pipeline ETL — Detalhamento

### 6.1 `dim_tempo.ktr` — Geração da Dimensão Temporal

**Objetivo:** Gerar automaticamente o calendário trimestral sem dependência de fonte externa.

**Fluxo interno de steps:**

```
RowGenerator (30 linhas) 
    → Sequence (ID 1–30) 
    → Formula: data_calc, trimestre, dt_fim_estimado 
    → Formula: flags booleanos (primeiro/último trimestre) 
    → Calculator: extrai ano 
    → SelectValues: renomeia para prefixo "temp_" 
    → ConcatFields: gera temp_periodo (YYYYQ) 
    → SelectValues: converte tipos finais + descarta ID 
    → ExecuteSQL: TRUNCATE + RESET SEQUENCE 
    → TableOutput: INSERT em dim_tempo (batch 1000)
```

**Decisão de design:** O truncate prévio garante que a dimensão seja sempre reconstruída integralmente, evitando inconsistências por reprocessamento parcial. O reset da sequência (`ALTER SEQUENCE ... RESTART WITH 1`) assegura que as surrogate keys sejam estáveis entre execuções.

---

### 6.2 `dim_cartao.ktr` — Carga de Cartões

**Objetivo:** Popular a dimensão de produtos de cartão a partir de arquivo CSV.

**Fluxo interno de steps:**

```
CsvInput (cartoes.csv, separador ";") 
    → TableOutput: INSERT em dim_cartao (batch 1000)
```

**Campos processados:** `car_bandeira`, `car_funcao`, `car_produto`, `car_modalidade`, `car_numero_parcela`

---

### 6.3 `dim_local.ktr` — Carga de Localização

**Objetivo:** Popular a dimensão geográfica com dados dos estados brasileiros.

**Fluxo interno de steps:**

```
CsvInput (estados_brasil.csv, separador ";") 
    → TableOutput: INSERT em dim_local (batch 1000)
```

**Campos processados:** `loc_uf`, `loc_estado`, `loc_latidude`, `loc_longitude`, `loc_cod_ibge`, `loc_regicao`

> Os campos de coordenadas geográficas (latitude/longitude) permitem futura integração com ferramentas de visualização geoespacial.

---

### 6.4 `fato_movimentacoes.ktr` — Carga de Movimentações

**Objetivo:** Realizar a carga incremental de dados de transações da API do BCB.

**Lógica de carga incremental:**

```
1. Consulta o último trimestre carregado em fat_movimentacoes
2. Determina períodos pendentes:
   - Mínimo: 20191 (1º trimestre de 2019)
   - Máximo: trimestre anterior ao atual
3. Para cada período pendente:
   a. Consome a API do BCB com o período como parâmetro
   b. Transforma e normaliza os dados recebidos
   c. Realiza o relacionamento com as dimensões
   d. Insere os registros em fat_movimentacoes
```

**Idempotência:** Como o pipeline verifica o último período carregado antes de processar, execuções repetidas não causam duplicação de dados.

---

### 6.5 `fato_terminais.ktr` — Carga de Terminais

**Objetivo:** Carregar dados de terminais de pagamento da API do BCB com a mesma lógica incremental de `fato_movimentacoes.ktr`, adaptada para a série de terminais.

---

### Conexão com o Banco de Dados

Todas as transformações utilizam a conexão nomeada **"Banco SAD"**, configurada com variáveis de ambiente para portabilidade entre ambientes:

| Parâmetro PDI | Variável de Ambiente | Valor de Produção |
|---|---|---|
| Server | `${DB_HOST_SAD}` | `10.0.0.1` (via WireGuard) |
| Database | `${DB_NAME_SAD}` | `bcbdb` |
| Port | `${DB_PORT_SAD}` | `8594` |
| Username | `${DB_USER_SAD}` | `caraveianame` |
| Password | `${DB_PASSWORD_SAD}` | configurado no ambiente |

> O uso de variáveis de ambiente em vez de credenciais hardcoded permite que o mesmo arquivo `.ktr` funcione em ambientes de desenvolvimento (localhost) e produção (VPS) sem modificações.

---

## 7. Orquestração

### `jobs/Fatos.kjb` — Job Orquestrador

O job garante a execução sequencial e condicional de todas as transformações. Cada passo só é executado se o anterior foi concluído com sucesso.

```
Start
  │
  ▼ (sucesso)
dim_tempo.ktr          ← Dimensão temporal (pré-requisito para os fatos)
  │
  ▼ (sucesso)
dim_local.ktr          ← Dimensão geográfica
  │
  ▼ (sucesso)
dim_cartao.ktr         ← Dimensão de cartões
  │
  ▼ (sucesso)
fato_movimentacoes.ktr ← Fato principal (depende de todas as dimensões)
  │
  ▼ (sucesso)
fato_terminais.ktr     ← Fato secundário
  │
  ▼ (sucesso)
Success
```

**Configuração de execução:**
- Cada job entry aguarda conclusão do passo anterior (`Wait for completion: Y`)
- Log Level: Basic
- Todas as variáveis de ambiente são passadas para as sub-transformações
- Run configuration: Pentaho Local

**Decisão de design:** A ordem de execução garante integridade referencial — as dimensões são sempre populadas antes das tabelas fato que delas dependem.

---

## 8. Infraestrutura e DevOps

### Container Docker

O banco de dados roda em container Docker para garantir portabilidade e facilidade de provisionamento.

**Dockerfile (`docker/data_base.Dockerfile`):**

```dockerfile
FROM postgres:18

LABEL project="pipeline-sad" \
      environment="producao" \
      database="bcbdb"

COPY backup/bcbdb /tmp/bcbdb.dump
COPY docker/init-db.sh /docker-entrypoint-initdb.d/01_init-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/01_init-db.sh

VOLUME ["/var/lib/postgresql"]
EXPOSE 5432
```

> **Nota técnica:** O volume é montado em `/var/lib/postgresql` (não `/var/lib/postgresql/data`), compatível com a mudança de estrutura de diretórios introduzida no PostgreSQL 18+. Versões anteriores do Postgres usavam o subdiretório `/data`, mas o PG18 adota nomes versionados internamente.

**Script de inicialização (`docker/init-db.sh`):**

```bash
pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --no-owner --role="$POSTGRES_USER" /tmp/bcbdb.dump
```

O script é executado automaticamente pelo mecanismo `docker-entrypoint-initdb.d` do PostgreSQL na primeira inicialização do container, restaurando o schema e os dados base a partir do dump incluído na imagem.

### Script de Operação (`sad.sh`)

Interface CLI para gerenciamento do container em produção:

| Comando | Ação |
|---|---|
| `./sad.sh -up` | Build da imagem, criação do volume e inicialização do container |
| `./sad.sh -stop` | Para o container (dados preservados no volume) |
| `./sad.sh -remove` | Remove container, imagem e volume |

**Validações executadas pelo `-up` antes de iniciar:**
1. Docker daemon ativo (`systemctl is-active docker`)
2. Interface WireGuard `wg0` ativa (`ip link show wg0`)
3. Arquivo `.env` presente no diretório

**Configuração de runtime:**

| Parâmetro | Valor |
|---|---|
| Nome da imagem | `pipeline-sad-postgres` |
| Nome do container | `pipeline-sad-db` |
| Volume persistente | `pipeline-sad-pgdata` |
| Porta no host | `8594` (WireGuard) |
| Política de restart | `unless-stopped` |

---

## 9. Segurança e Acesso

### Arquitetura de Rede

O banco de dados **não está exposto à internet pública**. O acesso ocorre exclusivamente através de túnel VPN WireGuard.

```
Máquina Local (10.0.0.2)
        │
        │  WireGuard (UDP 51820, criptografado)
        │
VPS — Interface wg0 (10.0.0.1)
        │
        │  Rede interna Docker
        │
Container PostgreSQL (porta 8594 → 5432)
```

**Regras de firewall (iptables) aplicadas pelo WireGuard:**

```bash
iptables -A INPUT -i wg0 -p tcp --dport 8594 -j ACCEPT
iptables -A INPUT -i wg0 -j DROP
```

Apenas tráfego TCP na porta 8594 oriundo da interface WireGuard é aceito. Todo o restante é descartado.

### Peers WireGuard Configurados

| Peer | IP no Túnel | Uso |
|---|---|---|
| Peer 1 | `10.0.0.2/32` | Desenvolvedor (Thiago) |
| Peer 2 | `10.0.0.3/32` | Desenvolvedor (Guilherme) |
| VPS | `10.0.0.1/32` | Servidor / banco de dados |

### Gestão de Credenciais

- Credenciais do banco armazenadas em `.env` (não versionado — listado no `.gitignore`)
- Senhas nas transformações PDI protegidas com criptografia do Pentaho (`Encrypted ...`)
- Variáveis de ambiente utilizadas nos arquivos `.ktr` para separação entre config e código
- Arquivo `.env` nunca exposto no repositório

---

## 10. Configuração e Execução

### Pré-requisitos

- Pentaho Data Integration (PDI) 9.x
- Docker (para banco em produção) ou PostgreSQL 13+ local
- WireGuard configurado (para acesso ao banco em VPS)
- Acesso à internet (API do BCB)

### Variáveis de Ambiente (kettle.properties ou equivalente)

```properties
DB_HOST_SAD=10.0.0.1
DB_NAME_SAD=bcbdb
DB_PORT_SAD=8594
DB_USER_SAD=caraveianame
DB_PASSWORD_SAD=<senha>
```

### Inicialização da Infraestrutura (VPS)

```bash
# 1. Garantir permissão de execução
chmod +x sad.sh

# 2. Subir o banco de dados
sudo ./sad.sh -up

# 3. Verificar status
docker ps | grep pipeline-sad-db
```

### Execução do Pipeline

**Via interface gráfica (Spoon):**
1. Abrir Spoon
2. Carregar `jobs/Fatos.kjb`
3. Configurar variáveis de ambiente com as credenciais
4. Executar

**Via linha de comando (Kitchen):**

```bash
# Linux/macOS
kitchen.sh -file="jobs/Fatos.kjb" -level=Basic

# Windows
kitchen.bat /file:"jobs\Fatos.kjb" /level:Basic
```

### Acesso ao Banco para Consultas

**DBeaver / pgAdmin:**

| Campo | Valor |
|---|---|
| Host | `10.0.0.1` |
| Porta | `8594` |
| Banco | `bcbdb` |
| Usuário | `caraveianame` |

> Requer WireGuard ativo na máquina local antes de conectar.

**Via psql (na VPS):**

```bash
docker exec -it pipeline-sad-db psql -U caraveianame -d bcbdb
```

---

## 11. Decisões de Projeto

### Por que Pentaho PDI?

Pentaho oferece interface visual para construção de pipelines ETL com suporte nativo a transformações complexas, consumo de APIs REST e conexão com bancos relacionais. A abordagem low-code/no-code para steps comuns (CSV input, Table output, Formula) reduz o tempo de desenvolvimento e facilita a manutenção por membros da equipe com perfis distintos.

### Por que Star Schema?

O modelo estrela é a abordagem canônica para Data Warehouses analíticos (Kimball). Ele:
- Simplifica queries analíticas (joins diretos entre fato e dimensões)
- Melhora performance de leitura com menos joins que Snowflake
- Facilita a compreensão do modelo por analistas de negócio

### Por que PostgreSQL 18?

Versão LTS moderna com excelente suporte a operações analíticas, particionamento, e compatibilidade com ferramentas BI. O uso em container elimina o acoplamento com o sistema operacional do servidor.

### Por que WireGuard?

WireGuard é uma VPN moderna com implementação enxuta, alta performance e criptografia de estado da arte (ChaCha20, Poly1305, Curve25519). Comparado a OpenVPN, oferece configuração mais simples, menor latência e melhor auditabilidade de código. A escolha garante que o banco nunca precise de porta pública exposta.

### Por que Carga Incremental?

A API do BCB pode retornar séries históricas longas. Truncar e recarregar integralmente a cada execução seria custoso em tempo e largura de banda. A lógica incremental — verificar o último trimestre carregado e processar apenas os períodos novos — torna o pipeline eficiente para execuções periódicas (ex: agendamento trimestral).

### Separação de Variáveis de Ambiente vs. Credenciais Hardcoded

O uso de `${DB_HOST_SAD}`, `${DB_NAME_SAD}`, etc. nos arquivos `.ktr` em vez de valores fixos foi uma decisão deliberada para permitir que o mesmo conjunto de transformações funcione em diferentes ambientes (desenvolvimento local, produção em VPS) sem modificação dos artefatos ETL — princípio equivalente ao 12-Factor App aplicado a pipelines de dados.

---

*Documentação gerada em 2026-06-04.*
