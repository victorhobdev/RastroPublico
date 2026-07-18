-- RastroPublico — consultas de validação da versão 1
-- Executar no Databricks SQL Warehouse após o job da janela.

-- 1. Contagens principais Silver
SELECT 'contratacoes' AS entidade, count(*) AS linhas FROM workspace.silver.contratacoes
UNION ALL SELECT 'itens', count(*) FROM workspace.silver.itens_contratacao
UNION ALL SELECT 'resultados', count(*) FROM workspace.silver.resultados_itens
UNION ALL SELECT 'fornecedores', count(*) FROM workspace.silver.fornecedores
UNION ALL SELECT 'orgaos', count(*) FROM workspace.silver.orgaos
UNION ALL SELECT 'unidades', count(*) FROM workspace.silver.unidades_compradoras
UNION ALL SELECT 'contratos', count(*) FROM workspace.silver.contratos
UNION ALL SELECT 'itens_contrato', count(*) FROM workspace.silver.itens_contrato
UNION ALL SELECT 'eventos_contrato', count(*) FROM workspace.silver.eventos_contrato;

-- 2. Limites temporais do recorte analítico
SELECT min(to_date(publicado_em)) AS inicio,
       max(to_date(publicado_em)) AS fim,
       count(*) AS contratacoes
FROM workspace.silver.contratacoes;

SELECT min(data_publicacao) AS inicio,
       max(data_publicacao) AS fim,
       count(*) AS contratos
FROM workspace.silver.contratos;

-- 3. Unicidade das chaves correntes: todas as diferenças devem ser zero
SELECT 'contratacoes' AS entidade, count(*) - count(DISTINCT contratacao_id) AS duplicadas
FROM workspace.silver.contratacoes
UNION ALL SELECT 'itens', count(*) - count(DISTINCT item_id)
FROM workspace.silver.itens_contratacao
UNION ALL SELECT 'resultados', count(*) - count(DISTINCT resultado_id)
FROM workspace.silver.resultados_itens
UNION ALL SELECT 'contratos', count(*) - count(DISTINCT contrato_id)
FROM workspace.silver.contratos;

-- 4. Integridade referencial materializada: todas as contagens devem ser zero
SELECT 'item_sem_contratacao' AS regra, count(*) AS violacoes
FROM workspace.silver.itens_contratacao i
LEFT ANTI JOIN workspace.silver.contratacoes c USING (contratacao_id)
UNION ALL
SELECT 'resultado_sem_item', count(*)
FROM workspace.silver.resultados_itens r
LEFT ANTI JOIN workspace.silver.itens_contratacao i USING (item_id)
UNION ALL
SELECT 'item_contrato_sem_contrato', count(*)
FROM workspace.silver.itens_contrato i
LEFT ANTI JOIN workspace.silver.contratos c USING (contrato_id)
UNION ALL
SELECT 'evento_sem_contrato', count(*)
FROM workspace.silver.eventos_contrato e
LEFT ANTI JOIN workspace.silver.contratos c USING (contrato_id);

-- 5. Resultado das regras operacionais do último run
WITH ultimo AS (
  SELECT run_id FROM workspace.ops.quality_results
  ORDER BY registrado_em DESC LIMIT 1
)
SELECT q.*
FROM workspace.ops.quality_results q
JOIN ultimo u USING (run_id)
ORDER BY severidade DESC, regra;

-- 6. Estado final dos indicadores
SELECT 'qualidade_cobertura' AS tabela, status_publicacao, count(*) AS linhas
FROM workspace.gold.qualidade_cobertura GROUP BY status_publicacao
UNION ALL
SELECT 'concentracao_fornecedores', status_publicacao, count(*)
FROM workspace.gold.concentracao_fornecedores GROUP BY status_publicacao
UNION ALL
SELECT 'recorrencia_orgao_fornecedor', status_publicacao, count(*)
FROM workspace.gold.recorrencia_orgao_fornecedor GROUP BY status_publicacao
UNION ALL
SELECT 'presenca_fornecedores', status_publicacao, count(*)
FROM workspace.gold.presenca_fornecedores GROUP BY status_publicacao
UNION ALL
SELECT 'variacao_precos', status_publicacao, count(*)
FROM workspace.gold.variacao_precos GROUP BY status_publicacao
UNION ALL
SELECT 'evolucao_contratual', status_publicacao, count(*)
FROM workspace.gold.evolucao_contratual GROUP BY status_publicacao
UNION ALL
SELECT 'arestas_orgao_fornecedor', status_publicacao, count(*)
FROM workspace.gold.arestas_orgao_fornecedor GROUP BY status_publicacao;

-- 7. Proteções semânticas de concentração
SELECT count(*) AS valores_invalidos
FROM workspace.gold.concentracao_fornecedores
WHERE top_1 < 0 OR top_1 > 1
   OR top_3 < 0 OR top_3 > 1
   OR hhi < 0 OR hhi > 1
   OR top_3 < top_1;

-- 8. Cobertura do vínculo contratual C3
SELECT * FROM workspace.gold.vinculo_contrato_cobertura;
SELECT * FROM workspace.gold.contratos_cobertura;

-- 9. Histórico Delta e organização física
DESCRIBE HISTORY workspace.silver.itens_contratacao;
DESCRIBE DETAIL workspace.silver.itens_contratacao;
DESCRIBE DETAIL workspace.silver.resultados_itens;

-- 10. Distribuição candidata a skew
WITH por_contratacao AS (
  SELECT contratacao_id, count(*) AS itens
  FROM workspace.silver.itens_contratacao
  GROUP BY contratacao_id
)
SELECT percentile_approx(itens, array(0.50, 0.95, 0.99), 10000) AS percentis,
       max(itens) AS maximo
FROM por_contratacao;

-- 11. Linguagem e privacidade: inspeção de colunas publicadas, não de payloads
SHOW COLUMNS IN workspace.gold.fornecedores_contexto;
SELECT status_publicacao, count(*)
FROM workspace.gold.fornecedores_contexto
GROUP BY status_publicacao;
