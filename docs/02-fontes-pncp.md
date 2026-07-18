# RastroPublico — fontes oficiais e contratos preliminares

## 1. Objetivo

Registrar o que foi confirmado nas fontes oficiais, separar documentação de comportamento observado e definir o protocolo de validação antes da implementação.

Nenhum campo, endpoint ou limite deste documento deve ser tratado como contrato definitivo sem uma chamada controlada e uma amostra preservada na Bronze.

## 2. Fontes oficiais consultadas

- [Swagger da API PNCP Consulta](https://pncp.gov.br/api/consulta/swagger-ui/index.html)
- [Manual de Integração do PNCP v2.5](https://pncp.gov.br/manual/pt-br/latest/singlehtml/)
- [Histórico de versões do manual](https://pncp.gov.br/manual/pt-br/latest/historico_de_versoes/)
- [Entidades de domínio do PNCP](https://pncp.gov.br/app/entidades-dominio)
- [Portal de Dados Abertos do Compras.gov](https://www.gov.br/compras/pt-br/cidadao/portal-de-dados-abertos/portal-de-dados-abertos)
- [Repositório CSV do Compras.gov](https://repositorio.dados.gov.br/seges/comprasgov/)
- [Repositório CSV do Comprasnet Contratos](https://repositorio.dados.gov.br/seges/comprasnet_contratos/)

Consulta realizada em 17/07/2026. A versão e o conteúdo devem ser verificados novamente no início de cada fase que altere o contrato de ingestão.

### 2.1 Fontes aprovadas e função

| Fonte | Papel aprovado | Uso inicial | Limite |
| --- | --- | --- | --- |
| PNCP API de Consulta | canal canônico nacional planejado | monitoramento e reconciliação | consulta de contratações indisponível no Bloco 2; detalhe de item voltou a responder `200` no Bloco 4 |
| Compras.gov CSV | fonte transacional ativa | contratações, itens e resultados; bootstrap e incremental | cobertura deve ser medida, não presumida equivalente ao PNCP inteiro |
| Comprasnet Contratos CSV | fonte contratual ativa | contratos, itens e históricos | vínculo com a contratação não é explícito em todas as linhas |
| CNPJ/QSA | enriquecimento cadastral | identidade, razão social e quadro societário com data de referência | não substitui o fornecedor publicado na contratação |
| IBGE Geociências | dimensão geográfica | códigos e hierarquias territoriais | divergências de código seguem para qualidade |
| IPCA/IBGE | deflator analítico | valor real em série separada do valor nominal | não corrige comparabilidade de item |
| CEIS/CNEP | contexto correcional | presença cadastral datada e rastreável | não é indicador de fraude, irregularidade ou risco |

Portal da Transparência fica aprovado somente para reconciliação federal. SIAFI, DOU, TCU e TCEs/TCMs exigem pergunta e gate próprios antes de qualquer ingestão. As demais bases avaliadas não pertencem ao escopo atual.

### 2.2 Evidência dos arquivos Compras.gov e Comprasnet

Probes locais em 17/07/2026, armazenados fora do Git em `D:\RastroPublico\data\source-probes`, confirmaram:

| Recorte | Entidade | Linhas | Bytes | Observação |
| --- | --- | ---: | ---: | --- |
| diário | compras | 1.531 | 1.609.052 | 27 UFs, CNPJ e controle PNCP completos |
| diário | itens de compra | 40.969 | 24.445.232 | 24.334 chaves de item distintas; expansão por resultado exige grão composto |
| diário | resultados | 8.611 | 3.294.989 | identificador e nome do fornecedor completos na amostra |
| diário | contratos | 1.344 | 978.651 | chave `id` única |
| diário | itens contratuais | 2.243 | 777.755 | 53 referências sem pai no mesmo arquivo diário |
| diário | históricos contratuais | 2.236 | 1.737.416 | 30 referências sem pai no mesmo arquivo diário |
| mensal | compras | 13.870 | 14.533.015 | 13.864 `id_compra` distintos; seis repetições observadas |
| mensal | itens de compra | 502.945 | 295.385.144 | 206.023 `id_compra_item` distintos; grão físico não é um item único |
| mensal | resultados | 89.563 | 34.218.110 | chave técnica observada sem repetição |
| mensal | contratos | 12.254 | 8.560.912 | cobertura federal, estadual e municipal observada |
| mensal | itens contratuais | 24.252 | 7.622.550 | 269 referências sem pai no recorte mensal |
| mensal | históricos contratuais | 19.684 | 15.119.700 | uma referência sem pai no recorte mensal |

Os arquivos diário e mensal são recortes de publicação/atualização, não snapshots referencialmente fechados. Ausência do pai no mesmo arquivo não autoriza quarentena definitiva: primeiro deve ocorrer reconciliação contra o histórico Silver. O bootstrap usa arquivos anuais por período; arquivos diários formam o incremental; arquivos mensais servem à reconciliação e ao reprocessamento delimitado.

## 3. Distinção entre APIs

O PNCP apresenta:

- uma API pública de consulta, descrita no Swagger de consulta;
- serviços de integração e manutenção, descritos no Manual de Integração.

O RastroPublico é consumidor somente leitura. Serviços `POST`, `PUT`, `PATCH` e `DELETE` não fazem parte do projeto. Endpoints `GET` encontrados no Manual só serão usados após confirmar que são acessíveis no ambiente público de produção sem credencial de plataforma integradora.

## 4. Endpoints documentados

### 4.1 Descoberta e cargas incrementais

| Recurso | Endpoint documentado | Uso pretendido | Estado |
| --- | --- | --- | --- |
| Contratações publicadas | `GET /api/consulta/v1/contratacoes/publicacao` | Bootstrap por publicação | Observado `200`, paginado |
| Contratações atualizadas | `GET /api/consulta/v1/contratacoes/atualizacao` | Incremental e captura de correções | Observado `200`, paginado |
| Contratos publicados | `GET /api/consulta/v1/contratos` | Bootstrap de contratos/empenhos | Observado `200`, paginado |
| Contratos atualizados | `GET /api/consulta/v1/contratos/atualizacao` | Incremental e captura de correções | Observado `200`, paginado |
| Modalidades | `GET /api/pncp/v1/modalidades?statusAtivo=true` | Descobrir códigos ativos | Observado `200`; 19 modalidades ativas em 17/07/2026 |

### 4.2 Detalhes de contratação

| Recurso | Endpoint documentado | Chave candidata |
| --- | --- | --- |
| Contratação | `GET /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` | CNPJ + ano + sequencial |
| Itens | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens` | contratação + `numeroItem` |
| Item específico | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}` | contratação + item |
| Resultados do item | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados` | item + `sequencialResultado` |
| Resultado específico | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados/{sequencialResultado}` | item + resultado |
| Histórico | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/historico` | contratação + evento |

O detalhe da contratação na base `/api/pncp` devolveu `301` em JSON, sem cabeçalho `Location`, apontando para `/api/consulta`. Itens, resultados e histórico permaneceram acessíveis na base `/api/pncp` nos testes do Bloco 0. A base não deve ser substituída globalmente.

### 4.3 Detalhes de contrato

| Recurso | Endpoint documentado | Chave candidata |
| --- | --- | --- |
| Contrato/empenho | `GET /api/pncp/v1/orgaos/{cnpj}/contratos/{ano}/{sequencial}` | CNPJ + ano + sequencial |
| Contratos de uma contratação | `GET /api/pncp/v1/orgaos/{cnpj}/contratos/contratacao/{anoContratacao}/{sequencialContratacao}` | contratação + contrato |
| Histórico contratual | `GET /api/pncp/v1/orgaos/{cnpj}/contratos/{ano}/{sequencial}/historico` | contrato + evento |
| Termos | `GET /api/pncp/v1/orgaos/{cnpj}/contratos/{ano}/{sequencial}/termos` | contrato + termo |

O histórico contratual documenta eventos de inclusão, retificação e exclusão de contrato, termo e documento. O Bloco 0 confirmou listagem, detalhe, vínculo paginado, histórico e pelo menos um termo real, classificando a capacidade preliminar como `C1`. A cobertura nacional e o vínculo entre termo e alteração de valor/vigência ainda precisam ser medidos.

## 5. Fatos já sustentados pela documentação

- o PNCP utiliza identificadores de controle para contratações e contratos;
- itens possuem `numeroItem`, indicação de material ou serviço, descrição, quantidade, unidade, valores e datas de inclusão/atualização;
- itens podem ter orçamento sigiloso, situação que altera a interpretação de valores iguais a zero;
- resultados contêm fornecedor e valores homologados e podem ser cancelados;
- registros podem ser retificados depois da publicação;
- históricos registram categorias e tipos de operação;
- modalidades e outros domínios podem evoluir e devem ser descobertos, não codificados como lista fixa.

## 6. Janela de extração

O escopo usa uma **janela móvel de 12 meses encerrada em D-1**. O job recebe `data_referencia`; a janela-alvo é:

- `data_fim = data_referencia - 1 dia`;
- `data_inicio = data_fim - 12 meses de calendário + 1 dia`.

O bootstrap será quebrado em janelas menores. O tamanho inicial será de um dia por modalidade, com `tamanhoPagina=50`; poderá aumentar somente depois de medir páginas, duração, erros e tamanho dos payloads. O valor 50 foi aceito e 100 foi rejeitado com `400` nos testes realizados.

Para atualizações diárias, o Bloco 5 fixou uma sobreposição inicial de três dias. A escolha é operacional e provisória: será reavaliada após observar pelo menos 30 dias de atraso de correções reais. O watermark só avança após conclusão e validação integral da janela.

## 7. Estratégia para todas as modalidades

Todas as modalidades significam todas as modalidades ativas retornadas pelo domínio oficial e aceitas pelo endpoint consultado na data da execução.

O coletor não manterá uma enumeração manual permanente. Ele registrará:

- código e nome da modalidade;
- data em que o domínio foi consultado;
- status ativo;
- modalidades processadas em cada `run_id`;
- modalidades rejeitadas pelo endpoint e respectivo erro.

O endpoint de publicação foi executado com uma modalidade por consulta. O coletor criará uma janela lógica por modalidade e período e registrará as 19 modalidades ativas observadas, sem fixá-las permanentemente no código.

## 8. Identificação do recorte de tecnologia

O PNCP não será tratado como se fornecesse uma categoria única e confiável de “tecnologia”. A seleção seguirá esta ordem:

1. código e descrição de catálogo, NCM ou NBS, quando preenchidos e validados;
2. indicação material/serviço e categoria estruturada;
3. vocabulário controlado sobre objeto, descrição e informação complementar;
4. marcação como `incerto` quando houver evidência insuficiente ou conflitante.

A filtragem em duas etapas passou pelo gate preliminar do Bloco 0:

- usar objeto e campos estruturados da listagem para selecionar candidatos;
- buscar itens, resultados e contratos somente para candidatos.

Essa escolha reduz chamadas, mas pode perder contratações cujo objeto genérico só revela tecnologia nos itens. Na amostra de 50 pregões, 18% foram candidatos; itens de oito candidatos e oito não candidatos não revelaram falso negativo entre os não candidatos. Isso confirma viabilidade, não recall nacional. Se o recall posterior for inadequado, o recorte ou a estratégia de coleta deverá ser revisto.

Catálogo estruturado estava ausente nos 89 itens observados e NCM/NBS preenchido em apenas 7,9%. Esses campos ajudam quando presentes, mas não podem ser filtros obrigatórios.

A calibração começa por equipamentos porque seus campos estruturados e unidades tendem a oferecer validação mais objetiva. Serviços permanecem no escopo completo, inicialmente para concentração, recorrência e presença; preço de serviço exige gate próprio de comparabilidade.

## 9. Protocolo de validação da fonte

Para cada endpoint, executar e registrar:

1. chamada mínima válida;
2. resposta vazia;
3. resposta com múltiplas páginas;
4. repetição da mesma chamada;
5. janela com atualização conhecida;
6. erro de parâmetro;
7. latência e tamanho aproximado;
8. cabeçalhos e possíveis sinais de rate limit;
9. campos ausentes, nulos e tipos observados;
10. chaves duplicadas e datas fora da janela.

### 9.1 Evidência mínima por endpoint

- URL e parâmetros sem segredo;
- timestamp UTC da coleta;
- status HTTP;
- hash do payload;
- número da página;
- total de páginas e registros, se informado;
- amostra original preservada;
- perfil de campos e nulidade;
- conclusão: aprovado, aprovado com ressalvas ou bloqueado.

### 9.2 Gate antecipado de volume e adequação do Spark

O spike deve estimar, por dia, mês e modalidade:

- contratações;
- itens por contratação;
- resultados por item;
- tamanho médio e total dos payloads;
- volume Bronze e Silver estimado para 30 dias e 12 meses;
- quantidade e distribuição de arquivos;
- cardinalidade dos joins principais;
- distribuição por órgão, modalidade, categoria e fornecedor;
- concentração das maiores chaves e potencial de skew;
- shuffle esperado nas agregações e joins candidatos ao benchmark.

O gate responde: **o recorte produz volume e operações distribuídas suficientes para demonstrar Spark de forma defensável?**

Além das estimativas, o spike executa pelo menos um probe Spark descartável com:

- dados reais coletados;
- mais de uma partição de entrada;
- um join ou agregação candidato;
- plano físico inicial;
- bytes e linhas observados em cada lado do join, quando disponíveis;
- distribuição das chaves;
- exchange/shuffle observável;
- duração de referência e evidência da execução.

O probe não é o benchmark formal. Ele existe para confirmar comportamento distribuído real antes da implementação durável.

Resultados possíveis:

| Resultado | Decisão |
| --- | --- |
| Suficiente em até 30 dias | Executar o primeiro benchmark após o primeiro Gold |
| Insuficiente em 30 dias, suficiente em janela maior | Expandir apenas o volume necessário antes do benchmark inicial |
| Recorte tecnológico insuficiente | Usar a listagem nacional completa na etapa de classificação e detalhar apenas candidatos tecnológicos |
| Ainda insuficiente | Rever recorte ou hipótese do benchmark antes de construir o restante; não fabricar volume duplicando linhas |

Não haverá um número arbitrário de linhas como critério isolado. O gate exige plano distribuído, múltiplas partições, shuffle/join mensurável e duração suficiente para comparar estratégias no mesmo ambiente.

### 9.3 Gate de contratos, termos e eventos

Contratos fazem parte da versão 1 completa, mas o nível de análise depende do que a fonte realmente entrega:

| Código | Evidência da fonte | Capacidade da versão 1 |
| --- | --- | --- |
| `C1` | Listagem, detalhe, vínculo, histórico e termos disponíveis | Contratos, evolução de valor/vigência e linha do tempo completos dentro da cobertura |
| `C2` | Listagem e detalhe disponíveis, histórico/termos incompletos | Contratos entram em concentração, recorrência e presença; linha do tempo pode receber estado `não publicável` após medição |
| `C3` | Vínculo contratação–contrato ausente em parte dos dados | Manter análises de contrato separadas e publicar taxa de vínculo; não inferir relação pelo texto |
| `C4` | Endpoints e arquivos públicos oficiais indisponíveis | Capacidade `bloqueada`; a versão 1 não pode ser concluída sem remover o impedimento ou revisar explicitamente o requisito |

`C2` e `C3` podem ocorrer simultaneamente. O gate condiciona a forma da entrega, não remove contratos silenciosamente do escopo.

## 10. Ordem de validação

1. modalidades ativas;
2. contratações por publicação;
3. contratação específica;
4. itens;
5. resultados;
6. contratações por atualização;
7. contratos por publicação;
8. contrato específico e vínculo com contratação;
9. contratos por atualização;
10. históricos e termos.

## 11. Política de erros HTTP

| Situação | Tratamento inicial |
| --- | --- |
| `2xx` com payload válido | Persistir payload na Bronze e requisição em `ops.ingestion_requests` |
| `2xx` com schema essencial inesperado | Preservar corpo recuperável na Bronze, registrar falha operacional e bloquear downstream |
| `400` ou `422` | Registrar em `ops.ingestion_requests`; não repetir automaticamente |
| `404` em detalhe previamente listado | Registrar ausência operacional e enviar a chave para investigação/quarentena |
| `429` | Registrar tentativa, respeitar `Retry-After` quando presente e aplicar backoff |
| `5xx` ou timeout | Registrar tentativa e executar retry limitado com backoff e jitter |
| Falha após retries | Marcar run/janela incompleta em `ops.*`; não avançar watermark |

O Bloco 0 observou `200`, `204`, `301`, `400`, `404`, `422` e `429`. O `429` não apresentou `Retry-After` nem cabeçalhos explícitos de quota; por isso, o coletor começa sequencial e usa backoff com jitter quando necessário.

## 12. Pontos ainda não confirmados

- limite máximo formal de página e de intervalo, além dos valores testados;
- limite de requisições e política de throttling;
- estabilidade do total de páginas durante paginação;
- completude do campo de atualização global;
- representação de exclusões nas consultas incrementais;
- estabilidade e unicidade das chaves candidatas;
- cobertura de histórico, termos e vínculo para toda a população contratual;
- cobertura de catálogos, NCM/NBS, unidade e preço fora da amostra;
- capacidade de transferir e processar 12 meses dentro da quota do Databricks Free.

O acesso de saída foi resolvido: `pncp.gov.br` não é resolvido pelo compute serverless atual, enquanto domínio Databricks permitido resolve normalmente. A coleta será local e a transferência ao workspace será validada no Bloco 1. Os itens restantes não devem ser resolvidos por suposição.
