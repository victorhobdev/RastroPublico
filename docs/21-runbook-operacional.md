# RastroPublico — runbook operacional

## 1. Pré-requisitos

- Windows com PowerShell, Python 3.10–3.12 e Git;
- ambiente criado por `uv sync --dev`;
- Databricks CLI autenticada no perfil `rastro-publico`;
- acesso ao workspace, Unity Catalog, Jobs, SQL Warehouse e AI/BI;
- espaço de dados em `D:\RastroPublico`; não usar o SSD para payloads grandes;
- nenhum token, cookie ou payload bruto versionado no Git.

Validação inicial:

```powershell
uv sync --dev
.\.venv\Scripts\python.exe -c "import rastro_publico; print('ok')"
databricks auth profiles
databricks current-user me --profile rastro-publico
```

## 2. Parâmetros da execução de referência

| Parâmetro | Valor |
| --- | --- |
| início | `2025-07-18` |
| fim | `2026-07-17` |
| run | `b11-janela-20260718` |
| anos de snapshot | `2025 2026` |
| dados locais | `D:\RastroPublico\data\block11` |
| landing | `/Volumes/workspace/rastro_publico_dev/landing/block11/janela-20250718-20260717` |
| warehouse | `c8b5924d51fcc1e2` |

## 3. Coleta local

Baixar os seis conjuntos anuais de Compras.gov e Comprasnet para 2025 e 2026:

```powershell
.\.venv\Scripts\python.exe -m rastro_publico.coleta.janela `
  --destino D:\RastroPublico\data\block11\anual\2025-2026 `
  --run-id b11-janela-20260718 --anos 2025 2026 --workers 3
```

O comando é retomável: arquivo e manifesto já presentes não são baixados de
novo. Cada artefato deve possuir tamanho e SHA-256 no manifesto.

Fragmentar CSVs grandes preservando registros com quebras de linha internas:

```powershell
@'
from pathlib import Path
from rastro_publico.coleta.janela import fragmentar_csv_logico
origem = Path(r"D:\RastroPublico\data\block11\anual\2025-2026")
destino = Path(r"D:\RastroPublico\data\block11\fragmentado\2025-2026")
for arquivo in origem.glob("*.csv"):
    fragmentar_csv_logico(arquivo, destino)
'@ | .\.venv\Scripts\python.exe -
```

Para fontes contextuais, usar os módulos `fontes_contextuais` e `contexto`.
O recorte CNPJ/QSA deve ser feito localmente e conter apenas os fornecedores
observados; nomes de sócios não são publicados.

## 4. Transferência ao Volume

Criar o diretório remoto uma vez e copiar somente CSVs e manifestos:

```powershell
$landing = "dbfs:/Volumes/workspace/rastro_publico_dev/landing/block11/janela-20250718-20260717"
databricks fs mkdir $landing --profile rastro-publico
databricks fs cp --recursive `
  D:\RastroPublico\data\block11\fragmentado\2025-2026 $landing `
  --profile rastro-publico
databricks fs ls $landing --profile rastro-publico
```

Antes de iniciar o job, reconciliar nome, quantidade e tamanho dos objetos entre
HD e Volume. Na execução de referência foram 78 objetos e 9.028.123.353 bytes.

## 5. Código no workspace

Os notebooks importam os módulos de `src`. Atualizar ambos sem enviar `.venv`,
dados ou caches:

```powershell
databricks workspace import-dir src `
  /Workspace/Users/<usuario>/rastro_publico_block2/src `
  --overwrite --profile rastro-publico
databricks workspace import-dir notebooks `
  /Workspace/Users/<usuario>/rastro_publico_block2 `
  --overwrite --profile rastro-publico
```

Substituir `<usuario>` pelo usuário do workspace e ajustar o parâmetro
`source_root` dos jobs.

## 6. Execução e acompanhamento

Job definitivo da janela:

```powershell
databricks jobs run-now 399795155769573 --profile rastro-publico
databricks jobs list-runs --job-id 399795155769573 --limit 5 `
  --profile rastro-publico
databricks jobs get-run <run-id> --profile rastro-publico
```

Ordem esperada: `preparar_bronze_janela` → `silver_contratacoes` →
`silver_nucleo` → `silver_contratos` → `gold_base` → `gold_completa`.
Cada tarefa possui retry e timeout; o job aceita somente um run concorrente.

Benchmark final:

```powershell
databricks jobs run-now 79177278313280 --profile rastro-publico
databricks jobs get-run 159978813347835 --profile rastro-publico
```

## 7. Tratamento de falha

1. identificar a primeira tarefa vermelha e preservar as anteriores;
2. ler o erro e o output da task, sem assumir causa pela mensagem do job pai;
3. confirmar parâmetros, arquivos no Volume e schema da tabela de entrada;
4. corrigir código ou dado recuperável;
5. reenviar somente os arquivos alterados ao workspace;
6. reparar o run a partir da tarefa falha, quando permitido, ou reexecutar o job
   com o mesmo recorte e novo `run_id`;
7. confirmar que `ops.pipeline_state` não avançou antes das regras bloqueantes;
8. executar as consultas de `22-consultas-validacao.sql`.

Não apagar Bronze, manifestos ou versões Delta para “corrigir” contagem. Não
usar `VACUUM` como procedimento de recuperação.

## 8. Aceite operacional

- todas as tarefas em `SUCCESS`;
- datas Silver dentro da janela declarada;
- zero violação bloqueante no run corrente;
- duplicidades e quarentena reconciliadas como alertas, não ocultadas;
- Gold com `status_publicacao` preenchido;
- dashboard recarregado sem erro;
- benchmark com checksum equivalente entre estratégias;
- nenhum dado de `D:` ou segredo presente no Git.

## 9. Artefatos de referência

| Artefato | Identificador |
| --- | --- |
| job da janela | `399795155769573` |
| run definitivo da janela | `907362552312516` |
| job Gold/contexto anterior | `677476072044521` |
| job benchmark final | `79177278313280` |
| run benchmark final | `159978813347835` |
| dashboard | `01f182507d3519de8cd5931bef2d613f` |

Esses identificadores são evidências da conta atual, não configuração portátil.
Em outro workspace, os novos IDs devem ser registrados no relatório da execução.
