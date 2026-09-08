"""The closed vocabularies the database owns, declared once in Python.

Plan 162 Stage W / CAR-82.

Every value in this module is also written in a ``CHECK (<column> IN (...))``
in ``db/migrations/``, and the migration is the owner: Postgres rejects a write
outside the constraint, so the constraint is what is true. Python's copy is
here because a service cannot ask a migration file at runtime, and it is a copy
in exactly one place instead of one per call site.

**The pair of rules that makes a copy safe**, both in
``tests/test_testing_contract.py`` and both derived from ``db/migrations/``
rather than from a list:

* ``test_every_check_constrained_column_has_one_declared_vocabulary`` compares
  this module to the migrations in both directions -- a new ``CHECK`` with no
  vocabulary here fails, a vocabulary naming no ``CHECK`` fails, and unequal
  values fail. So a migration that renames a value cannot land without this
  module moving with it.
* ``test_no_module_retypes_a_database_vocabulary_it_could_import`` fails a
  module that compares against a bare literal which is a member of a
  vocabulary owned by a table that module's own statements touch. So the
  rename propagates: every call site reads the member from here, and none of
  them has its own copy to leave behind.

**Neither rule works alone**, which is why they are written as a pair. The
second one alone is vacuous under a rename -- the old literal stops being a
member, so the guard stops being in scope and passes silently. The first one
alone leaves the call sites free to keep typing the string.

**Why ``StrEnum``.** A member *is* a ``str``, so ``state["phase"] ==
CoordinationPhase.DRAINING`` compares against the row exactly as the literal
did, psycopg2 adapts it with no cast, and JSON serialises it unchanged. And
iterating the class gives the whole vocabulary, so the set and its members are
one declaration rather than two that can disagree.
"""
from __future__ import annotations

from enum import StrEnum


class RequestableRole(StrEnum):
    """``access_requests.requested_role`` -- ``admin`` is deliberately absent."""

    OBSERVER = "observer"
    POWER_USER = "power_user"
    VIEWER = "viewer"


class AccessRequestStatus(StrEnum):
    """``access_requests.status``."""

    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


class ArtifactType(StrEnum):
    """``artifacts_queue.artifact_type``."""

    DETAIL_PAGE = "detail_page"
    RESULTS_PAGE = "results_page"


class ArtifactStatus(StrEnum):
    """``artifacts_queue.status``."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    RETRY = "retry"
    SKIP = "skip"


class UserRole(StrEnum):
    """``authorized_users.role``."""

    ADMIN = "admin"
    POWER_USER = "power_user"
    OBSERVER = "observer"
    VIEWER = "viewer"


class BlockedCooldownEvent(StrEnum):
    """``blocked_cooldown_events.event_type``."""

    BLOCKED = "blocked"
    INCREMENTED = "incremented"
    CLEARED = "cleared"


class CoordinationKind(StrEnum):
    """``coordination_state.kind`` and ``coordination_state_events.kind``."""

    DEPLOY = "deploy"
    HOST_MAINTENANCE = "host_maintenance"
    SERVICE_MAINTENANCE = "service_maintenance"


class CoordinationPhase(StrEnum):
    """The coordination state machine's phases.

    Owned by three columns with the same constraint:
    ``coordination_state.phase``, ``coordination_state_events.phase`` and
    ``coordination_state_events.prior_phase``.
    """

    NONE = "none"
    REQUESTED = "requested"
    DRAINING = "draining"
    ACTIVE = "active"
    VALIDATING = "validating"


class DetailScrapeClaimEvent(StrEnum):
    """``detail_scrape_claim_events.status``."""

    CLAIMED = "claimed"
    RELEASED = "released"
    EXPIRED = "expired"
    PROCESSED = "processed"


class PriceObservationEvent(StrEnum):
    """``price_observation_events.event_type``."""

    UPSERTED = "upserted"
    DELETED = "deleted"


class ObservationSource(StrEnum):
    """``price_observation_events.source`` and ``silver_observations.source``."""

    SRP = "srp"
    DETAIL = "detail"
    CAROUSEL = "carousel"


class SilverObservationEvent(StrEnum):
    """``silver_observation_events.event_type``."""

    FLUSHED = "flushed"
    FAILED = "failed"


class TrackedModelEvent(StrEnum):
    """``tracked_model_events.event_type``."""

    ADDED = "added"
    REMOVED = "removed"


class VinToListingEvent(StrEnum):
    """``vin_to_listing_events.event_type``."""

    MAPPED = "mapped"
    REMAPPED = "remapped"


#: Which column each vocabulary is a copy of. Asserted against
#: ``db/migrations/`` in both directions, so this mapping cannot go stale in
#: either direction: an unlisted ``CHECK`` fails, and a listed column that no
#: longer carries one fails too. It is keyed by ``(table, column)`` rather than
#: by table because three of the pairs below share a table.
DB_VOCABULARIES: dict[tuple[str, str], type[StrEnum]] = {
    ("access_requests", "requested_role"): RequestableRole,
    ("access_requests", "status"): AccessRequestStatus,
    ("artifacts_queue", "artifact_type"): ArtifactType,
    ("artifacts_queue", "status"): ArtifactStatus,
    ("authorized_users", "role"): UserRole,
    ("blocked_cooldown_events", "event_type"): BlockedCooldownEvent,
    ("coordination_state", "kind"): CoordinationKind,
    ("coordination_state", "phase"): CoordinationPhase,
    ("coordination_state_events", "kind"): CoordinationKind,
    ("coordination_state_events", "phase"): CoordinationPhase,
    ("coordination_state_events", "prior_phase"): CoordinationPhase,
    ("detail_scrape_claim_events", "status"): DetailScrapeClaimEvent,
    ("price_observation_events", "event_type"): PriceObservationEvent,
    ("price_observation_events", "source"): ObservationSource,
    ("silver_observation_events", "event_type"): SilverObservationEvent,
    ("silver_observations", "source"): ObservationSource,
    ("tracked_model_events", "event_type"): TrackedModelEvent,
    ("vin_to_listing_events", "event_type"): VinToListingEvent,
}
