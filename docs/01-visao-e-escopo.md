# RastroPublico — visão e escopo

## 1. Estado do documento

- **Status:** baseline revisada após parecer externo e reanálise interna.
- **Versão:** 1.3.
- **Data-base:** 17 de julho de 2026.
- **Fonte de contexto profissional:** `inventario-competencias-evidencias.md`.
- **Escopo deste documento:** problema, público, perguntas, limites e critérios de sucesso.
- **Fora deste documento:** schema físico definitivo, código, notebooks e configuração de infraestrutura.

## 2. Definição do produto

> Plataforma analítica batch para explorar relações entre órgãos, fornecedores, contratos e itens nas compras públicas brasileiras de tecnologia, identificando concentração, recorrência, variações de preço, evolução contratual e qualidade dos dados, sem classificar fraude ou irregularidade.

O produto deve apoiar investigação exploratória e auditável. Um indicador representa um recorte dos dados publicados e não uma conclusão jurídica sobre a contratação, o órgão ou o fornecedor.

## 3. Objetivo profissional

O projeto deve produzir evidência defendível para uma vaga de Engenharia de Dados Júnior e funções próximas que combinem SQL, Python, pipelines e modelagem.

### 3.1 Competências já comprovadas e reaproveitadas

O inventário apresenta evidência forte ou suficiente de:

- Python, SQL e PostgreSQL;
- ETL/ELT, Airflow, dbt e Great Expectations;
- modelagem relacional e dimensional;
- incrementalidade, backfill, retries e upserts idempotentes;
- normalização de entidades e qualidade de dados;
- FastAPI, testes, Docker e GitHub Actions;
- Power BI, DAX, documentação e deploy de projeto.

Essas competências serão usadas apenas onde forem necessárias. O projeto não repetirá a arquitetura do Football Analytics para aumentar a lista de tecnologias.

### 3.2 Competências parcialmente demonstradas

- investigação de performance sem benchmark completo antes/depois;
- índices e particionamento sem planos de execução documentados;
- operação cloud em projeto, sem arquitetura gerenciada ou observabilidade centralizada;
- monitoramento de rotinas em escala básica;
- Power BI sem Service, Embedded ou refresh corporativo.

### 3.3 Competências ainda não comprovadas

- Apache Spark e PySpark;
- Databricks;
- Spark SQL em processamento distribuído;
- Delta Lake e histórico de tabelas;
- análise de planos físicos Spark;
- shuffle, broadcast join, Adaptive Query Execution e data skew;
- otimização e reprocessamento de jobs Spark em volume relevante.

### 3.4 Prioridade de desenvolvimento

O núcleo técnico será Spark + PySpark + Databricks + Delta Lake. Airflow, dbt, PostgreSQL, FastAPI, Kafka, Kubernetes, Terraform e frontend próprio não fazem parte da arquitetura inicial.

## 4. Usuários e usos

### 4.1 Usuários primários

- analistas e pesquisadores que exploram compras públicas;
- profissionais de controle ou gestão que precisam encontrar recortes para investigação;
- o autor do projeto, para demonstrar decisões de Engenharia de Dados em portfólio e entrevista.

### 4.2 Usos permitidos

- comparar concentração e recorrência em recortes equivalentes;
- observar distribuição e variação de preços quando houver comparabilidade suficiente;
- acompanhar alterações contratuais;
- avaliar cobertura e qualidade das informações publicadas;
- gerar hipóteses que exigem investigação adicional na fonte oficial.

### 4.3 Usos proibidos

- classificar automaticamente fraude, corrupção ou irregularidade;
- produzir ranking reputacional de órgãos ou fornecedores;
- afirmar superfaturamento apenas por diferença de preço;
- comparar itens ou serviços semanticamente diferentes;
- ocultar baixa cobertura, dados sigilosos ou campos ausentes.

## 5. Perguntas analíticas principais

1. Quem compra de quem nas contratações públicas de tecnologia?
2. Qual é a participação dos maiores fornecedores por órgão, categoria, modalidade e período?
3. Quais relações órgão–fornecedor se repetem e por quanto tempo?
4. Quais fornecedores possuem presença distribuída ou concentrada entre órgãos e regiões?
5. Como preços unitários variam dentro de grupos realmente comparáveis?
6. Quais contratos sucedem contratações anteriores ou mantêm relações recorrentes?
7. Como valores, vigência e fornecedores mudam ao longo do histórico contratual?
8. Qual é a cobertura de CNPJ, unidade, fornecedor, quantidade, unidade de medida e preço?
9. Quais registros não puderam ser normalizados ou comparados e por quê?
10. Como modalidade e distribuição geográfica alteram os padrões observados?

## 6. Escopo da versão 1 completa

### 6.1 Escopo-alvo

- **abrangência:** nacional onde comprovada pela fonte; cobertura por sistema, esfera e geografia sempre publicada;
- **janela:** janela móvel de 12 meses encerrada em D-1;
- **modalidades:** todas as modalidades presentes nos canais oficiais selecionados, preservando o domínio e a identificação de origem;
- **domínio:** equipamentos e serviços de tecnologia;
- **entidades:** contratações, itens, resultados, órgãos, unidades, fornecedores, contratos, termos e eventos contratuais disponíveis;
- **fontes transacionais:** PNCP e arquivos oficiais Compras.gov/Comprasnet Contratos, tratados como canais distintos até reconciliação;
- **enriquecimentos aprovados:** CNPJ/QSA, IBGE Geociências, IPCA e CEIS/CNEP, cada um com finalidade e cobertura próprias;
- **processamento:** batch diário;
- **plataforma principal:** Databricks Free;
- **desenvolvimento:** módulos Python testáveis localmente e execução Spark/Delta no Databricks;
- **consumo:** Databricks SQL e um dashboard Databricks AI/BI; Power BI é opcional.

Para uma execução com data de referência em 17/07/2026, a janela móvel inicial sugerida é 17/07/2025 a 16/07/2026. As datas serão parâmetros, não constantes no código.

### 6.2 Marcos internos de construção

Não haverá uma versão reduzida lançada antes da versão 1. A versão 1 só será considerada completa quando o escopo desta seção estiver encerrado. Isso não autoriza uma construção monolítica: os marcos abaixo são gates internos de engenharia, não produtos alternativos nem redução silenciosa do escopo.

1. uma data e poucas modalidades para validar contratos de fonte;
2. sete dias, cobrindo todas as modalidades ativas e calibrando primeiro as categorias de equipamentos;
3. trinta dias, com núcleo de contratações, itens, resultados, incrementalidade, qualidade e primeiro Gold;
4. a partir da Gold validada, dashboard Databricks AI/BI e benchmark Spark inicial seguem como trilhas independentes;
5. incorporação de serviços, contratos, termos, eventos e indicadores restantes;
6. expansão por janelas até completar 12 meses e benchmark final no volume integral.

Cada marco depende de reconciliação, idempotência, cobertura e custo aceitáveis na etapa anterior. Um marco pode exigir correção antes do próximo, mas não transforma capacidades posteriores em opcionais.

### 6.3 Famílias tecnológicas iniciais

**Equipamentos**

- notebooks e computadores;
- monitores;
- impressoras e scanners;
- servidores;
- equipamentos de rede.

**Serviços**

- suporte técnico;
- licenciamento de software;
- desenvolvimento e manutenção de software;
- outsourcing de tecnologia;
- infraestrutura de tecnologia;
- serviços de cloud.

A lista é uma taxonomia inicial, não uma classificação definitiva. Códigos de catálogo e campos estruturados terão prioridade sobre palavras-chave. Registros ambíguos serão identificados como tal.

Equipamentos entram primeiro na calibração porque oferecem melhor chance de unidade e preço comparáveis. Serviços continuam obrigatórios na versão 1 para concentração, recorrência, presença e relações contratuais; comparação de preço para serviços só será publicada nos grupos que comprovarem comparabilidade suficiente.

## 7. Fora da versão 1

- ingestão em tempo real;
- índice de fraude ou irregularidade;
- processamento de documentos PDF e anexos;
- classificação por modelo de linguagem ou machine learning;
- frontend próprio;
- autenticação e gestão de usuários;
- orquestrador externo ao Databricks;
- Data Warehouse em PostgreSQL;
- mensageria, microsserviços ou infraestrutura como código;
- garantia de serviço, SLA comercial ou operação crítica.

Também ficam fora integrações sem pergunta analítica aprovada. Portal da Transparência e SIAFI podem servir à reconciliação federal; DOU, TCU e TCEs/TCMs podem apoiar investigação externa. Eles não entram como ingestões permanentes na versão 1 sem um gate específico. Bases eleitorais, financeiras, sanitárias, educacionais, ambientais e regulatórias permanecem fora do produto.

## 8. Atributos de qualidade

| Prioridade | Atributo | Requisito inicial |
| --- | --- | --- |
| 1 | Correção | Métricas devem respeitar grão, denominador e filtros documentados |
| 2 | Rastreabilidade | Todo registro transformado deve apontar para coleta, endpoint e `run_id` |
| 3 | Idempotência | Reexecutar a mesma entrada não pode criar duplicatas lógicas |
| 4 | Recuperabilidade | Silver e Gold devem ser reconstruíveis a partir da Bronze |
| 5 | Transparência | Cobertura, quarentena e limitações acompanham os indicadores |
| 6 | Observabilidade | Jobs registram parâmetros, contagens, duração, status e versões Delta |
| 7 | Desempenho | Decisões Spark dependem de planos e métricas no mesmo ambiente |
| 8 | Custo | O pipeline deve operar dentro dos limites do Databricks Free |
| 9 | Privacidade | CPF e identificadores de pessoa física não são expostos no consumo |

Não existe requisito de tempo real. O objetivo de duração dos jobs será definido após o primeiro baseline medido.

### 8.1 Estados formais das capacidades

- **`publicada`:** capacidade implementada, validada e exposta para consumo.
- **`não publicável`:** os dados necessários foram coletados e processados e a análise foi efetivamente avaliada, mas o resultado foi suprimido porque cobertura, comparabilidade ou semântica não permitem publicação defensável.
- **`bloqueada`:** a capacidade não pôde ser executada ou avaliada porque fonte, endpoint, vínculo essencial, ambiente ou quota estão indisponíveis.

`Não publicável` é um estado final válido da versão 1. `Bloqueada` impede concluir a versão 1 até a remoção do impedimento ou revisão explícita do requisito.

## 9. Critérios de sucesso da versão 1

A versão 1 estará concluída quando existir evidência reproduzível de:

- ingestão da janela móvel de 12 meses para o recorte tecnológico aprovado, com cobertura por fonte, esfera e geografia medida e exposta, sem apresentar cobertura parcial como nacional completa;
- Bronze imutável e rastreável;
- controle operacional separado dos payloads Bronze;
- Silver tipada, deduplicada e atualizada por Delta `MERGE`;
- tratamento de correções e cancelamentos;
- reprocessamento parametrizado sem duplicação;
- quarentena e métricas de cobertura;
- classificação de equipamentos e serviços com método, versão e cobertura;
- Gold de qualidade, concentração, recorrência, presença, variação de preços elegíveis, evolução contratual e rede órgão–fornecedor;
- estado explícito `não publicável` para indicador efetivamente avaliado cuja cobertura, comparabilidade ou semântica impeça resultado defensável, acompanhado da evidência que sustenta a decisão;
- contratos, termos e eventos processados no nível suportado pela fonte, sem apresentar ausência de cobertura como funcionalidade entregue;
- Jobs com histórico, parâmetros e retries;
- uso real de PySpark, Spark SQL e Delta Lake;
- benchmark inicial e benchmark final entre estratégias Spark com resultado lógico equivalente;
- dashboard Databricks AI/BI apoiado por consultas do Databricks SQL;
- documentação de decisões, evidências e limitações.

Os enriquecimentos aprovados atendem a finalidades limitadas: CNPJ/QSA normaliza identidade cadastral; IBGE organiza geografia; IPCA permite valores reais identificados separadamente dos nominais; CEIS/CNEP acrescentam contexto correcional datado. Presença ou ausência em cadastro correcional não classifica fraude, irregularidade ou risco.

Se fonte, quota ou ambiente impedirem um item obrigatório, a versão 1 permanece incompleta até existir correção, mudança de ambiente ou revisão explícita de escopo aprovada. A documentação da limitação, isoladamente, não transforma o requisito em concluído.

## 10. Linguagem responsável

Termos recomendados:

- padrão incomum;
- concentração;
- recorrência;
- variação;
- indicador;
- sinal para investigação;
- qualidade ou cobertura da informação.

Termos como fraude, corrupção, superfaturamento ou irregularidade só podem aparecer para explicar o que o produto **não** conclui.
