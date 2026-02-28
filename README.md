# Intelligent Multi-Agent Task Orchestration Platform

A production-style, containerized multi-agent orchestration platform:
- **API** accepts user tasks
- **Decomposer** plans tasks (with reuse via Qdrant + embeddings)
- **Orchestrator** schedules a DAG of subtasks and retries failures
- **Workers** execute subtasks by role (research/analysis/code)
- **Synthesizer** aggregates results and writes a final response (Postgres + Kafka + Qdrant)
- **Observability** via Prometheus + Grafana + Kafka Exporter

## Architecture (high level)
**Flow**
1. `api` receives `POST /tasks` and publishes to Kafka `task.requests`
2. `decomposer` consumes `task.requests`
   - reuse: embeds + searches Qdrant; may publish final immediately to `task.final` and write Postgres `task_final`
   - otherwise: publishes a plan to `task.plan`
3. `orchestrator` consumes `task.plan` and dispatches subtasks to `task.work`
4. `worker` services consume `task.work` and publish results to `task.results`
5. `synthesizer` consumes `task.results`, writes final to Postgres `task_final`, publishes to `task.final`, and stores embeddings/final in Qdrant

Architecture details are described inline above.

## Services and Ports
- API: `http://localhost:8000`
- Decomposer metrics/ops: `http://localhost:8001`
- Orchestrator metrics/ops: `http://localhost:8002`
- Synthesizer metrics/ops: `http://localhost:8003`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001` (admin password: `admin`)
- Kafka Exporter: `http://localhost:9308`
- Postgres: `localhost:5432`
- Qdrant: `http://localhost:6333`

## Kafka Topics
- `task.requests` (API -> Decomposer)
- `task.plan` (Decomposer -> Orchestrator)
- `task.work` (Orchestrator -> Workers)
- `task.results` (Workers -> Orchestrator + Synthesizer)
- `task.final` (Synthesizer/Decomposer -> downstream/visibility)
- `task.dlq` (dead-letter queue)

## Quickstart
### 1) Start the stack
```bash
docker compose up -d --build
# IMPORTANT: start worker agents (required for progress beyond s1 dispatch)
docker compose up -d research_agent analysis_agent code_agent