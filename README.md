# RastroPúblico

Plataforma analítica batch para explorar compras públicas brasileiras de
tecnologia, relacionando órgãos, fornecedores, itens e contratos. Os indicadores
são descritivos: apontam concentração, recorrência e cobertura para investigação,
sem classificar fraude ou irregularidade.

## O que foi processado

Os números publicados pela auditoria anterior foram retirados do README: o script
usava uma implementação simplificada, diferente da Silver/Gold. A nova auditoria
chama as mesmas transformações produtivas; seus totais só devem voltar após uma
execução externa sobre os snapshots anuais.

Os totais monetários e as comparações de preço não são publicados. A auditoria
encontrou valores extremos sem atributos suficientes para resolver unidade, lote
e semântica; aplicar um corte arbitrário produziria uma conclusão frágil.

## Arquitetura e decisões

```text
fontes oficiais → landing imutável → staging Delta reconstruível
                → Silver tipada e deduplicada → Gold elegível
                → Databricks SQL / apresentação
```

- Python comum faz HTTP; Spark processa arquivos conjuntos, joins, janelas,
  deduplicação e agregações.
- A coleta e `silver.contratacoes` possuem comportamento incremental. O núcleo
  Silver e as Gold atuais são reconstruções integrais idempotentes da janela.
- A carga Bronze por arquivo usa `check → append` e só é idempotente quando
  executada pelo Job serializado (`max_concurrent_runs: 1`); execução manual
  concorrente não é suportada.
- A primeira carga migra idempotentemente `total_linhas` nas tabelas Delta
  históricas e recompõe as contagens a partir das Bronze existentes.
- Qualidade é específica por indicador: ausência de unidade bloqueia preço, mas
  não remove automaticamente um registro válido para recorrência.
- Identificadores só permanecem públicos para tipos reconhecidos de pessoa
  jurídica; casos desconhecidos são pseudonimizados.
- A lista órgão–fornecedor representa arestas agregadas, não graph analytics.

## Evidência de Spark

O benchmark formal comparou estratégias no mesmo snapshot e ambiente:

| Estratégia | Mediana |
| --- | ---: |
| plano natural com AQE | 3,19 s |
| broadcast por hint | 3,23 s |
| sort-merge por hint | 6,95 s |

Foram lidas 4.791.466 linhas em 39 arquivos, com 270,53 MB de shuffle e zero
spill. A decisão foi manter o plano natural: broadcast não trouxe ganho material
e forçar sort-merge foi pior. O checksum prova equivalência entre estratégias,
não correção semântica do dado.

## Como verificar

Pré-requisitos locais: Python 3.12, Java 20 ou 21 e `uv`.

```bash
uv sync --locked --group dev
uv run ruff check .
uv run pytest --cov=rastro_publico --cov-report=term-missing
```

A CI em `.github/workflows/ci.yml` executa esses gates em Linux. Chamadas reais às
fontes e payloads de 9 GB não fazem parte da CI; contagens históricas de testes não
são usadas como evidência desta revisão.

Para recriar os Jobs, use o Databricks Asset Bundle:

```bash
databricks bundle validate -t dev --profile rastro-publico \
  --var "landing_root=<volume>,contexto_root=<volume>"
```

## Mapa técnico

- [arquitetura e operação](docs/03-arquitetura-e-operacao.md);
- [modelo e métricas](docs/04-modelo-e-metricas.md);
- [runbook](docs/21-runbook-operacional.md);
- o recálculo de KPIs e a auditoria monetária precisam ser reexecutados; os JSONs
  e exports históricos não fazem parte do repositório público;
- [índice completo da documentação](docs/00-indice-documentacao.md).

## Limitações atuais

- A cobertura de modalidades e o vínculo contratação–contrato são parciais.
- O snapshot Delta anterior à auditoria contém KPIs invalidados; a Gold corrigida
  precisa ser rematerializada quando houver compute disponível.
- O runtime local é Spark 4.2; o serverless observado era Spark 4.1.
- O Query History foi exportado anteriormente, mas o JSON detalhado do Query
  Profile não; esses exports históricos não são publicados neste repositório.

Código, testes e documentação técnica atual são as fontes de verdade deste
repositório. Relatórios e artefatos históricos foram removidos.
