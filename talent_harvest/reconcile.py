from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .contracts import Record


@dataclass(frozen=True)
class Candidate:
    record: Record
    entity_id: str
    status: str
    reason: str


def semantic_signature(record: Record) -> tuple[str, str, int, str]:
    return record.discipline.casefold(), record.published_at, record.quantity, record.reference


def describe_conflict(records: Iterable[Record]) -> str:
    items = list(records)
    if not items:
        return ""
    first = items[0]
    if any(item.kind != first.kind for item in items):
        return "kind_mismatch"
    if any(item.discipline.casefold() != first.discipline.casefold() for item in items):
        return "discipline_mismatch"
    if any(item.quantity != first.quantity for item in items):
        return "quantity_mismatch"
    if any(item.published_at != first.published_at for item in items):
        return "date_mismatch"
    return ""


def choose_preferred(rows: list[dict]) -> dict:
    return sorted(rows, key=lambda row: (-row["trust"], row["source"],
                                         row["record_id"], -row["version"]))[0]


def select_current(rows: Iterable[dict]) -> list[dict]:
    """Pick the current observation per (source, record_id) by business order.

    Events are ordered by (occurred_at, event_id), never by arrival: a
    backfilled page with an earlier occurred_at must not displace a newer
    state. Intake guarantees that distinct events for one record_id never
    share an occurred_at unless their content is identical, so the event_id
    tie-break only chooses between equivalent statements. When an event was
    observed more than once, its first arrival represents it.
    """
    current: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = row["source"], row["record_id"]
        winner = current.get(key)
        if winner is None:
            current[key] = row
            continue
        candidate_order = (row["occurred_at"], row["event_id"])
        winner_order = (winner["occurred_at"], winner["event_id"])
        if candidate_order > winner_order or (candidate_order == winner_order and row["id"] < winner["id"]):
            current[key] = row
    return list(current.values())


def reconcile_group(rows: list[dict]) -> tuple[str, str, dict | None]:
    if not rows:
        return "empty", "", None
    if any(row["status"] != "accepted" for row in rows):
        rows = [row for row in rows if row["status"] == "accepted"]
    if not rows:
        return "quarantined", "all_records_quarantined", None
    records = [Record(**row["record"]) for row in rows]
    conflict = describe_conflict(records)
    if conflict:
        return "conflict", conflict, None
    return "accepted", "", choose_preferred(rows)


def duplicate_ids(records: Iterable[Record]) -> set[str]:
    seen: set[str] = set()
    repeated: set[str] = set()
    for record in records:
        if record.source_record_id in seen:
            repeated.add(record.source_record_id)
        seen.add(record.source_record_id)
    return repeated
