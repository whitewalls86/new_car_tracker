"""Prometheus collector for Plan 142 coordination and admission-gate health."""

from datetime import datetime, timezone

from prometheus_client.core import GaugeMetricFamily

from ops.coordination_drain import _airflow_gate_observations
from shared.db import db_cursor


class CoordinationCollector:
    """Read authoritative state at scrape time so stale cached health is impossible."""

    def describe(self):
        """Describe series without querying PostgreSQL during registration."""
        yield GaugeMetricFamily(
            "cartracker_coordination_state_readable",
            "Whether the authoritative coordination row can be read",
        )
        yield GaugeMetricFamily(
            "cartracker_coordination_gate_evidence_known",
            "Whether current-generation Airflow admission evidence is readable",
        )
        yield GaugeMetricFamily(
            "cartracker_coordination_state_info",
            "Current coordination kind and phase",
            labels=["kind", "phase"],
        )
        yield GaugeMetricFamily(
            "cartracker_coordination_state_age_seconds",
            "Seconds since the current coordination state last changed",
            labels=["kind", "phase"],
        )
        yield GaugeMetricFamily(
            "cartracker_coordination_gate_unobserved_runs",
            "Active affected DAG runs that have not observed the current drain",
        )

    def collect(self):
        readable = GaugeMetricFamily(
            "cartracker_coordination_state_readable",
            "Whether the authoritative coordination row can be read",
        )
        gate_known = GaugeMetricFamily(
            "cartracker_coordination_gate_evidence_known",
            "Whether current-generation Airflow admission evidence is readable",
        )
        try:
            with db_cursor(
                error_context="Coordination-Metrics", dict_cursor=True
            ) as cur:
                cur.execute(
                    """SELECT kind, phase, generation, scope, updated_at
                         FROM coordination_state WHERE id = 1"""
                )
                row = cur.fetchone()
            if row is None:
                raise RuntimeError("coordination row missing")

            state = dict(row)
            phase = state["phase"]
            kind = state["kind"] or "none"
            updated_at = state["updated_at"]
            readable.add_metric([], 1)

            state_info = GaugeMetricFamily(
                "cartracker_coordination_state_info",
                "Current coordination kind and phase",
                labels=["kind", "phase"],
            )
            state_info.add_metric([kind, phase], 1)
            yield state_info

            age = GaugeMetricFamily(
                "cartracker_coordination_state_age_seconds",
                "Seconds since the current coordination state last changed",
                labels=["kind", "phase"],
            )
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            age.add_metric(
                [kind, phase],
                max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds()),
            )
            yield age

            gate_count = 0
            gate_is_known = True
            if phase == "draining":
                evidence = _airflow_gate_observations(
                    frozenset(state["scope"]), state["generation"]
                )
                gate_is_known = evidence["status"] == "known"
                gate_count = evidence["count"] if gate_is_known else 0
            gate_known.add_metric([], int(gate_is_known))
            unobserved = GaugeMetricFamily(
                "cartracker_coordination_gate_unobserved_runs",
                "Active affected DAG runs that have not observed the current drain",
            )
            unobserved.add_metric([], gate_count)
            yield unobserved
        except Exception:
            readable.add_metric([], 0)
            gate_known.add_metric([], 0)

        yield readable
        yield gate_known


COORDINATION_COLLECTOR = CoordinationCollector()
