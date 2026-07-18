# RastroPúblico — índice da documentação

Este índice complementa o README e aponta somente para documentação técnica
atual. Registros históricos e números invalidados não fazem parte do
repositório público.

## Leia primeiro

1. `01-visao-e-escopo.md` — problema, recorte e limites;
2. `03-arquitetura-e-operacao.md` — fluxo executado e semântica das camadas;
3. `04-modelo-e-metricas.md` — grãos e definições;
4. `21-runbook-operacional.md` — reprodução local e no Databricks;
5. `22-consultas-validacao.sql` — consultas de validação operacional.

## Evidências verificáveis

- `scripts/audit_corrected_kpis.py` — recálculo pela mesma lógica da Gold;
- `scripts/audit_value_semantics.py` — gate do valor homologado; os JSONs de
  saída precisam ser recriados externamente;
- `.github/workflows/ci.yml` — lint, testes e cobertura em ambiente limpo;
- `databricks.yml` — configuração reproduzível dos jobs;
- `tests/` — contratos executáveis das transformações.

## Limitações que não devem ser omitidas

- a ingestão e uma entidade Silver usam incrementalidade; o núcleo restante é
  reconstruído integralmente de forma idempotente;
- o landing é imutável, mas `workspace.staging.*` é um snapshot reconstruível;
- o vínculo contratação–contrato é parcial (`C3`);
- preços e totais monetários não são publicados;
- a Gold de relações é uma lista de arestas, não análise de grafos;
- o snapshot Delta corrigido ainda precisa ser rematerializado em compute.
