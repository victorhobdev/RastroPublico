# RastroPúblico — índice da documentação

Este índice complementa o README e separa contratos atuais de registros
históricos para que números invalidados não sejam confundidos com o produto
corrigido.

## Leia primeiro

1. `25-auditoria-e-baseline-portfolio.md` — veredito atual, correções, evidências
   e pendências;
2. `01-visao-e-escopo.md` — problema, recorte e limites;
3. `03-arquitetura-e-operacao.md` — fluxo executado e semântica das camadas;
4. `04-modelo-e-metricas.md` — grãos, definições e baseline corrigida;
5. `21-runbook-operacional.md` — reprodução local e no Databricks;
6. `23-guia-portfolio-e-entrevista.md` — alegações permitidas e roteiro.

## Evidências verificáveis

- `evidence/data/corrected-kpis.json` — contagens do recorte tecnológico;
- `evidence/data/value-semantics-summary.json` — auditoria que bloqueou totais
  monetários;
- `evidence/databricks/` — definições sanitizadas de jobs, dashboard, execução e
  Query History;
- `.github/workflows/ci.yml` — lint, testes e cobertura em ambiente limpo;
- `databricks.yml` — configuração reproduzível dos jobs;
- `tests/` — contratos executáveis das transformações.

## Registros históricos

Os documentos `06` a `19` e `24` registram decisões e execuções por bloco. Eles
não são fontes de verdade para KPIs atuais. Quando houver divergência, prevalecem
o código testado, as evidências acima e o documento `25`.

## Limitações que não devem ser omitidas

- a ingestão e uma entidade Silver usam incrementalidade; o núcleo restante é
  reconstruído integralmente de forma idempotente;
- o landing é imutável, mas `workspace.staging.*` é um snapshot reconstruível;
- o vínculo contratação–contrato é parcial (`C3`);
- preços e totais monetários não são publicados;
- a Gold de relações é uma lista de arestas, não análise de grafos;
- o snapshot Delta corrigido ainda precisa ser rematerializado em compute.
