# CLAUDE.md — Guia do Projeto DATAFLOW para Claude Code

Este arquivo é o ponto de entrada para qualquer sessão do Claude Code (ou de outro
assistente de IA) que trabalhe neste repositório. Leia-o por completo antes de
sugerir código, abrir um PR ou tomar qualquer decisão de arquitetura.

O DATAFLOW é um **projeto de estudo e portfólio**, construído por um estudante de
Engenharia de Computação em final de curso, com experiência prévia em Backend
(Python/Django) e conhecimento acadêmico — hoje enferrujado — em Engenharia de
Dados (PySpark, MinIO, ETL, Data Lake). O objetivo do projeto não é "usar muitas
tecnologias": é construir, com profundidade real, uma pequena plataforma de dados
e, no processo, reconstruir e consolidar fundamentos. Isso muda como a IA deve se
comportar aqui — ver a seção [Como a IA deve ajudar neste projeto](#como-a-ia-deve-ajudar-neste-projeto).

Os documentos detalhados estão em `dataflow-docs/docs/`. Este arquivo é um resumo operacional;
quando houver dúvida sobre regra de negócio, arquitetura ou convenção, **leia o
documento correspondente antes de assumir ou inventar comportamento**.

---

## 1. O que é o DATAFLOW

Uma plataforma de dados pequena, mas arquiteturalmente séria, que:

1. recebe arquivos de dados via API (FastAPI);
2. valida e armazena o dado original (camada Bronze, em MinIO);
3. processa o dado com PySpark, aplicando limpeza e padronização (camada Silver);
4. gera dados agregados para consumo (camada Gold);
5. registra metadados, status de execução e resultados estruturados no PostgreSQL;
6. trata falhas de forma explícita (retry, idempotência, reprocessamento);
7. possui testes automatizados e pipeline de CI/CD;
8. roda localmente via Docker Compose;
9. documenta toda decisão arquitetural relevante como ADR (`dataflow-docs/docs/adr/`).

Componentes como Celery/Redis, Kafka, Databricks e deploy em nuvem **não fazem
parte do núcleo inicial**. Eles entram apenas em versões posteriores do roadmap,
e somente se houver um problema real que os justifique. Ver `dataflow-docs/docs/roadmap.md` e
`dataflow-docs/docs/architecture.md`.

### Arquitetura em uma linha

```
Cliente → FastAPI (ingestão) → MinIO/Bronze → PySpark → Silver → Gold
                                                              ↓
                                                   PostgreSQL (metadados/status)
```

Detalhe completo, com justificativa de cada seta e de cada componente opcional,
em `dataflow-docs/docs/architecture.md`.

---

## 2. Princípios do projeto (não negociáveis)

Estes princípios têm prioridade sobre qualquer sugestão de "melhor prática
genérica" trazida de outro contexto:

1. **Simplicidade antes de sofisticação.** Nenhuma tecnologia entra no projeto
   sem um problema concreto que ela resolva. "Pode ser útil no futuro" não é
   justificativa. Ver `dataflow-docs/docs/adr/` para o padrão de decisão.
2. **MinIO ≠ Spark ≠ Databricks.** São três papéis diferentes (armazenamento,
   engine de processamento, plataforma gerenciada). Nunca tratar como
   intercambiáveis nem sugerir que um substitui o outro por padrão.
3. **Arquitetura Medalhão é um contrato, não um detalhe de implementação.**
   Bronze preserva o dado original e nunca é sobrescrito. Silver é dado limpo e
   validado. Gold é dado agregado para consumo. Ver `dataflow-docs/docs/business-rules.md`.
2. **PostgreSQL guarda metadados e estado estruturado — não arquivos.** Dataset,
   execução de job, status, métricas de qualidade: PostgreSQL. Arquivo bruto ou
   processado (CSV/Parquet): MinIO. Nunca sugerir salvar arquivo binário/Parquet
   como BYTEA ou campo de tabela.
4. **Toda falha é tratada explicitamente.** Nenhum componente deve assumir que
   a rede, o worker ou o storage nunca falham. Ver `dataflow-docs/docs/business-rules.md`
   (seção de confiabilidade) antes de implementar qualquer job.
5. **Idempotência é obrigatória em qualquer operação que escreve dado.**
   Reexecutar um job com a mesma entrada não pode duplicar dado nem corromper
   estado. Ver `dataflow-docs/docs/business-rules.md`.
6. **Decisão de arquitetura relevante = ADR.** Se uma mudança troca uma peça da
   arquitetura (ex.: trocar Celery por outra fila, introduzir Kafka, mudar
   formato de arquivo), ela precisa de um ADR em `dataflow-docs/docs/adr/` antes do merge.
7. **Não fazer overengineering.** Kubernetes, Terraform, microsserviços,
   Elasticsearch, MongoDB, GraphQL, cluster Spark real: fora de escopo do
   núcleo. Ver a lista explícita em `dataflow-docs/docs/roadmap.md`.

---

## 3. Stack técnica

| Camada                     | Tecnologia                         | Status no projeto |
|----------------------------|-------------------------------------|--------------------|
| API / Ingestão              | FastAPI (Python)                   | Núcleo (V0)        |
| Banco relacional / metadados| PostgreSQL                         | Núcleo (V0)        |
| Object storage               | MinIO (compatível com S3)        | Núcleo (V1)        |
| Formato de dado              | Parquet (Silver/Gold), CSV/JSON possíveis na entrada (Bronze) | Núcleo (V2) |
| Processamento distribuído    | PySpark (modo local, 1 nó)       | Núcleo (V3)        |
| Processamento assíncrono     | Celery + Redis                   | Opcional (V4)      |
| Mensageria com replay/streaming | Kafka                          | Experimento futuro (V9), não é dependência padrão |
| Plataforma gerenciada        | Databricks                        | Experimento futuro (V8), complementar ao MinIO, não substituto |
| Orquestração de containers   | Docker + Docker Compose          | Núcleo (V0/V6)     |
| CI/CD                        | GitHub Actions                    | Núcleo (V6)        |
| Orquestração de pipelines    | (futuro) Airflow                 | Fora do núcleo, avaliar apenas se o agendamento manual se tornar insuficiente |

Não assuma versões de biblioteca sem checar `pyproject.toml`/`requirements.txt`
do repositório. Não afirme limites, preços ou recursos de ferramentas (em
especial Databricks Free Edition) sem verificar a documentação oficial atual.

---

## 4. Estrutura do repositório (referência)

```
dataflow/
├── CLAUDE.md
├── docker-compose.yml
├── dataflow-docs/
│   └── docs/
│       ├── README.md                 # índice da documentação
│       ├── business-rules.md         # regras de negócio e de domínio
│       ├── architecture.md           # arquitetura técnica detalhada
│       ├── coding-standards.md       # convenções de código e testes
│       ├── roadmap.md                # versões V0 a V10
│       ├── ai-usage-policy.md        # como usar IA durante o projeto
│       └── adr/                      # Architecture Decision Records
├── services/
│   ├── api/                      # FastAPI: ingestão, consulta de status
│   ├── worker/                   # Celery worker (quando V4 existir)
│   └── processing/               # jobs PySpark (bronze→silver→gold)
├── libs/
│   └── dataflow_common/          # código compartilhado (schemas, clients)
├── infra/
│   └── docker/                   # Dockerfiles por serviço
├── tests/
│   ├── unit/
│   └── integration/
└── .github/workflows/            # pipelines de CI
```

Esta estrutura é um ponto de partida. Se o repositório real divergir, **confie
no repositório, não neste arquivo**, e sinalize a divergência.

---

## 5. Convenções de código

Resumo — detalhes completos em `dataflow-docs/docs/coding-standards.md`.

- Python 3.11+, type hints em funções públicas, `black`/`ruff` para
  formatação e lint.
- Um módulo = uma responsabilidade. Lógica de domínio não deve depender
  diretamente de FastAPI, Celery ou Spark (facilita teste e troca de
  infraestrutura).
- Toda função que lê/escreve em MinIO, PostgreSQL ou dispara um job PySpark
  precisa de teste (unitário com mocks, ou de integração com
  Docker Compose).
- Nunca commitar segredo, chave ou credencial. Usar `.env` (fora do git) e
  `dataflow-docs/docs/business-rules.md` para quais variáveis existem.
- Commits pequenos e descritivos. Mudança de arquitetura referencia o ADR
  correspondente na mensagem de commit ou no PR.
- Mensagens de commit: curtas, claras e profissionais. Sem co-autoria de IA
  (`Co-Authored-By`), sem texto gerado automático.

---

## 6. Como a IA deve ajudar neste projeto

Isto é tão importante quanto a stack técnica. O usuário deste repositório está
construindo o DATAFLOW **para aprender a raciocinar como engenheiro**, não para
colecionar código gerado. A política completa está em `dataflow-docs/docs/ai-usage-policy.md`;
regras operacionais para quem está codando aqui:

1. **Antes de gerar código para um componente novo**, pergunte (ou verifique no
   PR/issue) se já existe uma hipótese do usuário sobre a solução. Se não
   existir, ofereça primeiro as perguntas de design (`dataflow-docs/docs/business-rules.md`
   tem o checklist de decisão), não a implementação pronta.
2. **Prefira explicar o "porquê" antes do "como".** Ao sugerir uma tecnologia
   ou padrão, aponte o problema que resolve, a alternativa mais simples
   considerada e o trade-off — não apenas o snippet.
3. **Nunca introduza Kafka, Databricks, Kubernetes, Airflow ou qualquer
   componente fora do núcleo sem checar `dataflow-docs/docs/roadmap.md` e sem propor um ADR
   primeiro.** Se o usuário pedir diretamente, tudo bem — mas aponte o que
   isso adiciona de complexidade antes de implementar.
4. **Toda decisão de arquitetura relevante vira ADR.** Se você (IA) tomar ou
   sugerir uma decisão estrutural, proponha o rascunho do ADR junto com o
   código.
5. **Não resolva o exercício de raciocínio por completo.** Se o usuário está
   claramente estudando um conceito (ex.: "por que usar Parquet aqui?"),
   responda com explicação e, se fizer sentido, devolva uma pergunta que o
   ajude a concluir sozinho — em vez de só entregar a resposta pronta.
6. **Sinalize quando uma afirmação depende de versão, plano ou preço** (em
   especial Databricks, GitHub Actions, MinIO) e, quando possível, verifique a
   documentação oficial em vez de assumir.
7. **Teste de sanidade antes de fechar qualquer tarefa:** o usuário deveria
   conseguir explicar o que foi implementado e por quê, sem depender da IA.
   Se a resposta for "não", prefira deixar um resumo de decisão/trade-off no
   PR em vez de apenas fechar a tarefa.

---

## 7. Onde encontrar cada coisa

| Pergunta | Documento |
|---|---|
| O que cada camada (Bronze/Silver/Gold) deve conter, status de job, regras de validação, convenções de nome/path | `dataflow-docs/docs/business-rules.md` |
| Por que cada componente existe, alternativas consideradas, diagrama completo | `dataflow-docs/docs/architecture.md` |
| Como estruturar código, testes, lint, commits | `dataflow-docs/docs/coding-standards.md` |
| O que implementar em cada versão (V0...V10), o que fica de fora por enquanto | `dataflow-docs/docs/roadmap.md` |
| Como pedir ajuda de IA sem perder aprendizado | `dataflow-docs/docs/ai-usage-policy.md` |
| Por que escolhemos X em vez de Y, para uma decisão específica | `dataflow-docs/docs/adr/` |

---

## 8. Checklist rápido antes de abrir um PR

- [ ] A mudança respeita a separação Bronze/Silver/Gold?
- [ ] Arquivo vai para MinIO e metadado/estado vai para PostgreSQL (não o
      inverso)?
- [ ] A operação é idempotente? O que acontece se rodar duas vezes?
- [ ] Falhas (timeout, indisponibilidade de MinIO/Postgres, worker morto)
      foram consideradas, mesmo que o tratamento seja simples nesta versão?
- [ ] Testes cobrem o caminho feliz e pelo menos um caminho de falha?
- [ ] Alguma tecnologia nova foi introduzida? Se sim, há um ADR?
- [ ] Este PR está alinhado com a versão atual do roadmap (`dataflow-docs/docs/roadmap.md`)
      ou está adiantando algo que deveria esperar?
