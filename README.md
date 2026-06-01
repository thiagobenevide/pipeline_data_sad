# Pipeline de Dados — Meios de Pagamento (Banco Central do Brasil)

Pipeline ETL desenvolvido com **Pentaho Data Integration (PDI/Kettle)** para ingestão, transformação e carga de dados de meios de pagamento publicados pela API do Banco Central do Brasil. Os dados são armazenados em um Data Warehouse modelado em esquema estrela (Star Schema) em PostgreSQL.

---

## Visão Geral

```
API Banco Central do Brasil
        │
        ▼
┌───────────────────┐
│   EXTRACT         │  Consumo da API pública do BCB
│   (Pentaho PDI)   │  Série: Quantidade de Transações com Cartão
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   TRANSFORM       │  Normalização por trimestre
│   (Pentaho PDI)   │  Carga de dimensões (tempo)
│                   │  Relacionamentos dim → fato
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   LOAD            │  PostgreSQL — banco bcbdb
│   (PostgreSQL)    │  fat_movimentacoes + dim_tempo
└───────────────────┘
```

---

## Arquitetura do Data Warehouse

O modelo segue o padrão **Star Schema**:

| Tabela             | Tipo      | Descrição                                              |
|--------------------|-----------|--------------------------------------------------------|
| `fat_movimentacoes`| Fato      | Transações de meios de pagamento por período           |
| `fat_terminais`| Fato      | Transações de meios de pagamento por período           |
| `dim_tempo`        | Dimensão  | Calendário trimestral (ano, trimestre, período)        |
| `dim_cartao`        | Dimensão  | Calendário trimestral (ano, trimestre, período)        |
| `dim_local`        | Dimensão  | Calendário trimestral (ano, trimestre, período)        |


---

## Estrutura do Projeto

```
pipeline_data_sad/
├── Jobs/
│   └── fat_movimentacoes.kjb           # Job orquestrador principal
|   └── fat_terminais.kjb
└── Transforms/
    ├── dim_tempo.ktr        # Transformação: dimensão tempo
    └── dim_cartao.ktr  # Transformação: quantidade de transações com cartão
    └── dim_local.ktr
```

### Jobs

**`Fatos.kjb`** — orquestra a execução sequencial das transformações:

```
Start → dimensoes → fato → Success
```

1. **dimensoes**: executa `dim_tempo.ktr` para popular/atualizar a dimensão tempo
2. **fato**: executa `QTD_TRANS_CART.ktr` para carregar os dados de transações

### Transformações

**`dim_tempo.ktr`**

Popula a dimensão de tempo com granularidade trimestral. Garante que todos os períodos necessários para os fatos existam antes da carga.

**`QTD_TRANS_CART.ktr`**

Carrega os dados de quantidade de transações com cartão da API do BCB. Lógica de execução:

1. Busca o último trimestre já cadastrado em `fat_movimentacoes`
2. Monta tabela virtual com os períodos pendentes (mínimo: `20191`, máximo: trimestre anterior ao atual)
3. Carrega os dados da API a partir do menor trimestre pendente
4. Separa e grava os dados de dimensão no banco
5. Carrega a dimensão e realiza o relacionamento com a tabela fato
6. Executa o insert na tabela fato

---

## Fonte de Dados

| Atributo       | Detalhe                                                             |
|----------------|---------------------------------------------------------------------|
| Provedor       | Banco Central do Brasil (BCB)                                       |
| Tipo           | API REST pública (dados abertos)                                    |
| Série          | Quantidade de transações com instrumentos de pagamento              |
| Granularidade  | Trimestral                                                          |
| Período mínimo | 1º trimestre de 2019 (`20191`)                                     |

---

## Banco de Dados

| Parâmetro  | Valor        |
|------------|--------------|
| SGBD       | PostgreSQL   |
| Database   | `bcbdb`      |
| Porta      | `5432`       |
| Usuário    | `postgres`   |
| Conexão    | ODBC         |

> **Atenção:** a senha da conexão está criptografada no arquivo `.ktr`. Não exponha credenciais em texto plano.

---

## Pré-requisitos

- [Pentaho Data Integration (PDI) 9.x](https://www.hitachivantara.com/en-us/products/pentaho-platform/data-integration-analytics.html)
- PostgreSQL 13+
- Driver ODBC do PostgreSQL configurado
- Acesso à internet para consumo da API do BCB

---

## Como Executar

### Via interface gráfica (Spoon)

1. Abra o **Spoon** (PDI)
2. Carregue o arquivo `Jobs/Fatos.kjb`
3. Configure a conexão `Banco SAD` com os dados do ambiente
4. Clique em **Run**

### Via linha de comando (Kitchen)

```bash
kitchen.sh -file="Jobs/Fatos.kjb" -level=Basic
```

No Windows:

```bat
kitchen.bat /file:"Jobs\Fatos.kjb" /level:Basic
```

---

## Fluxo de Carga Incremental

O pipeline é **incremental**: a cada execução, verifica o último trimestre carregado e processa apenas os períodos novos até o trimestre imediatamente anterior ao atual. Isso evita reprocessamento desnecessário e garante idempotência.

```
Última carga: 20231
Período atual: 20242
Períodos a processar: 20232, 20233, 20234, 20241
```

---

## Apresentação

Slides com detalhamento da arquitetura e decisões de projeto:
[Acessar apresentação](https://docs.google.com/presentation/d/1wqZ1Qz7q3WXKUcJgXXYAGh_k9-uwdDZO2D5WIIVncx4/edit?usp=sharing)

---

## Licença

Distribuído sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para detalhes.
