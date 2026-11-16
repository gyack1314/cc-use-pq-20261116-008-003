from __future__ import annotations

import unittest

from talent_harvest.audit import Auditor
from talent_harvest.contracts import HarvestError
from talent_harvest.export import Delivery
from talent_harvest.inventory import Inventory
from talent_harvest.lineage import Lineage
from tests.support import Workspace, manifest, record

T1 = "2026-08-01T09:00:00+00:00"
T2 = "2026-09-01T09:00:00+00:00"
T3 = "2026-10-01T09:00:00+00:00"
T4 = "2026-10-15T09:00:00+00:00"


def event(event_id, occurred_at, quantity=5, **kwargs):
    return record(event_id=event_id, occurred_at=occurred_at, quantity=quantity, **kwargs)


class EventOrderingTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.work.register()

    def tearDown(self):
        self.work.close()

    def test_out_of_order_backfill_keeps_business_order(self):
        self.work.ingest(pages=[[event("evt-open", T1)]])
        # the freeze page is declared but its mirror file is missing
        path = manifest(self.work.path, batch="batch-2", captured_at="2026-09-02T08:00:00+00:00",
                        pages=[[event("evt-freeze", T2, quantity=0)]])
        (path.parent / "page-1.json").unlink()
        failed = self.work.harvester.ingest(path)
        self.assertEqual(failed["state"], "failed")
        # the reopen is captured and ingested before the freeze page is backfilled
        self.work.ingest(batch="batch-3", captured_at="2026-10-02T08:00:00+00:00",
                         pages=[[event("evt-reopen", T3, quantity=6)]])
        self.assertEqual(Delivery(self.work.ledger).current()["accepted"][0]["record"]["quantity"], 6)
        # the missing freeze page is restored and the failed batch resumed
        restored = manifest(self.work.path, batch="batch-2", captured_at="2026-09-02T08:00:00+00:00",
                            pages=[[event("evt-freeze", T2, quantity=0)]])
        self.assertEqual(self.work.harvester.resume(restored)["state"], "complete")
        # business order wins over arrival order: the reopen stays current
        current = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(current["counts"], {"accepted": 1, "conflicts": 0, "quarantined": 0})
        self.assertEqual(current["accepted"][0]["record"]["quantity"], 6)
        self.assertEqual(current["accepted"][0]["selected"]["event_id"], "evt-reopen")
        self.assertEqual(current["accepted"][0]["selected"]["occurred_at"], T3)
        # ingestion history keeps arrival order, versions and quality states
        history = Delivery(self.work.ledger).history(source="company")["observations"]
        self.assertEqual([row["event_id"] for row in history], ["evt-open", "evt-reopen", "evt-freeze"])
        self.assertEqual([row["version"] for row in history], [1, 2, 3])
        self.assertEqual(history[-1]["status"], "quarantined")
        self.assertEqual(history[-1]["occurred_at"], T2)
        # the supply summary carries the reopen quantity alone, never both
        summary = Delivery(self.work.ledger).summary()
        self.assertEqual(summary["by_kind"]["position"], {"entities": 1, "quantity": 6})
        lineage = Lineage(self.work.ledger).record("company", "role-1")
        self.assertEqual(len(lineage["versions"]), 3)
        self.assertEqual(lineage["latest_status"], "accepted")
        kinds = [item["event_type"] for item in self.work.ledger.events(source="company")]
        self.assertIn("page_failed", kinds)
        self.assertEqual(kinds[-1], "batch_completed")
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_freeze_as_latest_event_removes_supply(self):
        self.work.ingest(pages=[[event("evt-open", T1)]])
        self.work.ingest(batch="batch-2", captured_at="2026-09-02T08:00:00+00:00",
                         pages=[[event("evt-freeze", T2, quantity=0)]])
        current = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(current["counts"]["accepted"], 0)
        self.assertEqual(current["counts"]["quarantined"], 1)
        self.assertEqual(current["quarantined"][0]["event_id"], "evt-freeze")
        self.assertEqual(Delivery(self.work.ledger).summary()["by_kind"], {})

    def test_duplicate_restatement_and_retry_are_idempotent_in_effect(self):
        self.work.ingest(pages=[[event("evt-open", T1)]])
        # a later batch restates the known event on page 1 and a newer event on page 2
        path, result = self.work.ingest(batch="batch-2", captured_at="2026-09-20T08:00:00+00:00",
                                        pages=[[event("evt-open", T1)],
                                               [event("evt-raise", T2, quantity=8)]])
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["pages"][0]["duplicates"], 1)
        self.assertEqual(result["pages"][1]["duplicates"], 0)
        # every arrival is stored; versions follow arrival order
        history = Delivery(self.work.ledger).history(source="company")["observations"]
        self.assertEqual([row["event_id"] for row in history], ["evt-open", "evt-open", "evt-raise"])
        self.assertEqual([row["version"] for row in history], [1, 2, 3])
        # the current state comes from the newest event only; quantities never add up
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["accepted"][0]["record"]["quantity"], 8)
        self.assertEqual(current["accepted"][0]["selected"]["event_id"], "evt-raise")
        self.assertEqual(Delivery(self.work.ledger).summary()["by_kind"]["position"]["quantity"], 8)
        # retrying the completed batch stores nothing and emits no events
        events_before = len(self.work.ledger.events())
        retry = self.work.harvester.ingest(path)
        self.assertTrue(all(page["unchanged"] for page in retry["pages"]))
        self.assertEqual(len(self.work.ledger.observations()), 3)
        self.assertEqual(len(self.work.ledger.events()), events_before)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_contradictory_event_fails_page_and_recovers_with_new_batch(self):
        self.work.ingest(pages=[[event("evt-open", T1)]])
        # the same event id arrives again with different content
        _, result = self.work.ingest(batch="batch-2", captured_at="2026-09-20T08:00:00+00:00",
                                     pages=[[event("evt-open", T1, quantity=9)]])
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.work.ledger.page("company", "batch-2", 1)["error_code"], "event_conflict")
        self.assertEqual(len(self.work.ledger.observations()), 1)
        self.assertEqual(Inventory(self.work.ledger).recovery_plan()["retryable"], 1)
        # registered manifests are immutable, so the correction uses a new batch
        _, recovered = self.work.ingest(batch="batch-3", captured_at="2026-09-21T08:00:00+00:00",
                                        pages=[[event("evt-open", T1)],
                                               [event("evt-raise", T2, quantity=8)]])
        self.assertEqual(recovered["state"], "complete")
        self.assertEqual(recovered["pages"][0]["duplicates"], 1)
        self.assertEqual(Delivery(self.work.ledger).current()["accepted"][0]["record"]["quantity"], 8)
        self.assertEqual(len(self.work.ledger.observations()), 3)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_conflicting_state_at_same_moment_is_rejected(self):
        self.work.ingest(pages=[[event("evt-open", T1)]])
        _, result = self.work.ingest(batch="batch-2", captured_at="2026-09-20T08:00:00+00:00",
                                     pages=[[event("evt-other", T1, quantity=7)]])
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.work.ledger.page("company", "batch-2", 1)["error_code"], "event_conflict")
        # the same instant written in another timezone is still the same moment
        _, again = self.work.ingest(batch="batch-3", captured_at="2026-09-21T08:00:00+00:00",
                                    pages=[[event("evt-other", "2026-08-01T17:00:00+08:00", quantity=7)]])
        self.assertEqual(self.work.ledger.page("company", "batch-3", 1)["error_code"], "event_conflict")
        self.assertEqual(len(self.work.ledger.observations()), 1)

    def test_conflicting_event_within_page_is_rejected(self):
        rows = [event("evt-shared", T1),
                record(record_id="role-2", title="Analyst", event_id="evt-shared",
                       occurred_at=T2, quantity=3)]
        _, result = self.work.ingest(pages=[rows])
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.work.ledger.page("company", "batch-1", 1)["error_code"], "event_conflict")
        self.assertEqual(len(self.work.ledger.observations()), 0)

    def test_event_fields_are_required(self):
        for field, code in (("event_id", "invalid_identifier"), ("occurred_at", "invalid_time")):
            with self.subTest(field=field):
                broken = record()
                del broken[field]
                _, result = self.work.ingest(batch=f"batch-{field}", pages=[[broken]])
                self.assertEqual(result["state"], "failed")
                self.assertEqual(self.work.ledger.page("company", f"batch-{field}", 1)["error_code"], code)
        self.assertEqual(len(self.work.ledger.observations()), 0)

    def test_occurred_at_is_normalized_to_utc(self):
        self.work.ingest(pages=[[event("evt-open", "2026-08-01T17:00:00+08:00")]])
        row = Delivery(self.work.ledger).history()["observations"][0]
        self.assertEqual(row["occurred_at"], "2026-08-01T09:00:00+00:00")

    def test_opposing_sources_follow_existing_dispute_rules(self):
        self.work.register(key="campus", trust=80)
        # company freezes then reopens; only the reopen is current
        self.work.ingest(pages=[[event("evt-open", T1)]])
        self.work.ingest(batch="batch-2", captured_at="2026-09-02T08:00:00+00:00",
                         pages=[[event("evt-freeze", T2, quantity=0)]])
        self.work.ingest(batch="batch-3", captured_at="2026-10-02T08:00:00+00:00",
                         pages=[[event("evt-reopen", T3, quantity=6)]])
        # campus claims the same position with a matching state
        self.work.ingest(key="campus",
                         pages=[[event("evt-campus-open", T3, quantity=6, record_id="role-9")]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["selected"]["source"], "campus")
        self.assertEqual(len(current["accepted"][0]["evidence"]), 2)
        self.assertEqual(current["accepted"][0]["record"]["quantity"], 6)
        # campus later claims a different quantity: the dispute rule excludes the entity
        self.work.ingest(key="campus", batch="batch-2", captured_at="2026-10-20T08:00:00+00:00",
                         pages=[[event("evt-campus-raise", T4, quantity=11, record_id="role-9")]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 0)
        self.assertEqual(current["counts"]["conflicts"], 1)
        self.assertEqual(current["conflicts"][0]["reason"], "quantity_mismatch")
        self.assertEqual(Delivery(self.work.ledger).summary()["by_kind"], {})

    def test_withdrawal_scopes_all_events_of_a_source(self):
        self.work.register(key="campus", trust=80)
        self.work.ingest(pages=[[event("evt-open", T1)]])
        self.work.ingest(batch="batch-2", captured_at="2026-10-02T08:00:00+00:00",
                         pages=[[event("evt-reopen", T3, quantity=6)]])
        self.work.ingest(key="campus",
                         pages=[[event("evt-campus-open", T3, quantity=6, record_id="role-9")]])
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["accepted"], 1)
        # withdrawing the company removes its whole event stream from the current view
        self.assertFalse(self.work.harvester.withdraw("company", "mirror retired")["unchanged"])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["selected"]["source"], "campus")
        self.assertEqual(len(current["accepted"][0]["evidence"]), 1)
        self.assertEqual(Delivery(self.work.ledger).summary()["by_kind"]["position"]["quantity"], 6)
        # history and audit events are preserved; withdrawal is idempotent
        history = Delivery(self.work.ledger).history()["observations"]
        self.assertEqual(len(history), 3)
        company_rows = [row for row in history if row["source"] == "company"]
        self.assertEqual(len(company_rows), 2)
        self.assertTrue(all(row["withdrawn_at"] for row in company_rows))
        self.assertTrue(self.work.harvester.withdraw("company", "again")["unchanged"])
        with self.assertRaises(HarvestError):
            self.work.ingest(batch="batch-3")
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])
