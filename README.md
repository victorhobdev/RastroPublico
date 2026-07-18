# RastroPúblico

Plataforma analítica batch para explorar compras públicas brasileiras de
tecnologia, relacionando órgãos, fornecedores, itens e contratos. Os indicadores
são descritivos: apontam concentração, recorrência e cobertura para investigação,
sem classificar fraude ou irregularidade.

[Abrir o case study auditado em PDF](deliverables/RastroPublico-case-study.pdf)

![Arquitetura do RastroPúblico](deliverables/assets/arquitetura.png)

## O que foi processado

Na janela de 18/07/2025 a 17/07/2026, o recálculo PySpark auditado identificou:

- 12.252 compras classificadas como tecnologia;
- 29.207 itens tecnológicos distintos;
- 20.664 resultados ligados a esses itens;
- 4.246 fornecedores tecnológicos distintos;
- 828 relações recorrentes, cada uma com ao menos duas contratações distintas;
- 1.537 contratos e 1.755 eventos vinculados a itens tecnológicos.

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
- Escritas concorrentes de ingestão usam `MERGE` Delta atômico.
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

Resultado local auditado: **113 testes aprovados e 82,97% de cobertura**. A CI em
`.github/workflows/ci.yml` repete esses gates em Linux. Chamadas reais às fontes e
payloads de 9 GB não fazem parte da CI.

Para recriar os Jobs, use o Databricks Asset Bundle:

```bash
databricks bundle validate -t dev --profile rastro-publico \
  --var "landing_root=<volume>,contexto_root=<volume>"
```

## Mapa de evidências

- [auditoria e baseline atual](docs/25-auditoria-e-baseline-portfolio.md);
- [arquitetura e operação](docs/03-arquitetura-e-operacao.md);
- [modelo e métricas](docs/04-modelo-e-metricas.md);
- [runbook](docs/21-runbook-operacional.md);
- [KPIs corrigidos](evidence/data/corrected-kpis.json);
- [auditoria monetária](evidence/data/value-semantics-summary.json);
- [evidências sanitizadas do Databricks](evidence/databricks/);
- [índice completo da documentação](docs/00-indice-documentacao.md).

## Limitações atuais

- A cobertura de modalidades e o vínculo contratação–contrato são parciais.
- O snapshot Delta anterior à auditoria contém KPIs invalidados; a Gold corrigida
  precisa ser rematerializada quando houver compute disponível.
- O runtime local é Spark 4.2; o serverless observado era Spark 4.1.
- O Query History foi exportado, mas o JSON detalhado do Query Profile não.
- As capturas antigas do dashboard são históricas. Para divulgação, use o
  [case study auditado](deliverables/RastroPublico-case-study.pdf).

Relatórios de execução antigos permanecem no repositório como trilha de decisão,
mas não são fonte de verdade para os KPIs atuais.
