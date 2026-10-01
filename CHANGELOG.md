# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Planned
- Phase 1: Ingestion layer & Poisson event producer simulator.
- Phase 2: Redis-backed stateful sliding window aggregations.
- Phase 3: Cost-sensitive XGBoost training pipeline and ONNX export.
- Phase 4: Sub-10ms FastAPI inference microservice with TreeSHAP.
- Phase 5: Continuous PSI and KS-test drift observability worker.

## [0.1.0] - 2026-10-01
### Added
- Phase 0: Baseline container orchestration with Docker Compose.
- Multi-container topology: Redpanda (Kafka v24.1), Redpanda Console, Redis 7.2, and Prometheus v2.51.
- Project folder layout: `config`, `data`, `models`, `src`, and `tests`.
- Initial environment dependencies specification via `requirements.txt`.
- Prometheus configuration scraper (`config/prometheus.yml`).


## [0.2.0] - 2026-10-01
### Added
- Phase 1: High-throughput ingestion layer and Poisson transaction simulator.
- Pydantic contract definition (`src/ingestion/schemas.py`).
- Synthetic event generator with card-testing velocity bursts, outlier spikes, and geo-hops (`src/ingestion/producer_simulator.py`).
- 3-partition Redpanda topic with `account_id` key affinity.