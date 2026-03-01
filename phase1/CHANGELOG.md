# Changelog

All notable changes for this project are documented here.

This project follows a simple semantic versioning convention:
- **MAJOR**: breaking changes
- **MINOR**: new features / platform capability
- **PATCH**: fixes, small improvements

## v0.7.0 — Productionization & Release (Phase 7)
### Added
- Health and readiness endpoints across services (`/health`, `/ready`) for production-style dependency checks.
- End-to-end smoke test script (`scripts/smoke_test.sh`) to validate task submission → orchestration → final output.
- Demo bring-up helper (`scripts/demo_up.sh`) to boot stack + worker agents and verify API readiness.
- Prometheus + Grafana observability wiring (Prometheus scrape config, Grafana provisioning, dashboard JSON).
- Kafka consumer lag visibility via `kafka_exporter` and Prometheus rules group(s).

### Changed
- Standardized Postgres schema expectations across services:
  - `task_final.final_text` as the canonical final output column.
- Improved resilience via schema initialization (tables created if missing after volume reset).
- Code quality gate via Ruff (repo-wide lint and autofix).

### Fixed
- Kafka metadata timeouts caused by missing topics when `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
- API 500s caused by missing Postgres tables after `docker compose down -v`.
- Inconsistent final output column naming (`final` vs `final_text`).

### Notes
- Worker agents must be running for tasks to progress beyond `DISPATCHED`:
  - `research_agent`, `analysis_agent`, `code_agent`
