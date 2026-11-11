"""Application orchestration: Runner wires domain decisions to the ports.

The Runner performs no business decisions of its own: phases, validation,
version merges and response handling live in :mod:`harvest.domain`. It only
executes the domain's declarative effects against the Repository and Portal
ports, owns transaction boundaries and reproduces the historical fault
injection points and step budget.
"""

from . import domain
from .ports import HarvestError


class Runner:
    """Compatible run/state/snapshot/page engine over injected ports."""

    def __init__(self, repository, portal):
        self.repository = repository
        self.portal = portal

    # -- public read-only surface ---------------------------------------

    def state(self):
        return self.repository.read_control()

    def snapshot(self):
        return self.repository.snapshot()

    def page(self, raw, cursor):
        """Validate a page against history without changing the checkpoint."""
        page, digest = domain.decode_page(raw, cursor)
        old = self.repository.read_receipt(page["page"])
        if old is not None and old["digest"] != digest:
            raise HarvestError("PAGE_CHANGED")
        return page, digest

    # -- main loop ------------------------------------------------------

    def run(self, steps=100, fault=None):
        def crash(point):
            if point == fault:
                raise HarvestError("CRASH_" + point)

        if self.repository.read_control()["done"]:
            return {"status": "done", "steps": 0}
        for number in range(steps):
            control = self.repository.read_control()
            plan = domain.plan_step(control)
            if plan["kind"] == "commit":
                self._commit(control, crash)
            else:
                self._remote(plan["op"], crash)
            if self.repository.read_control()["done"]:
                return {"status": "done", "steps": number + 1}
        return {"status": "running", "steps": steps}

    # -- commit branch --------------------------------------------------

    def _commit(self, control, crash):
        pending = control["pending"]
        with self.repository.transaction():
            stored = {}
            for row in pending["page"]["rows"]:
                record = self.repository.read_record(row["id"])
                if record is not None:
                    stored[row["id"]] = record
            for row, _old in domain.merge_decision(pending["page"], stored):
                self.repository.write_record(row)
            # Historical fault point: after page rows, before receipt/cursor.
            crash("in_commit")
            self.repository.write_receipt(domain.commit_receipt(control))
            self.repository.write_control(domain.commit_control(control))
            self.repository.append_audit(domain.commit_event(control))

    # -- remote branches ------------------------------------------------

    def _remote(self, op, crash):
        """One remote-driven iteration; every decision reads fresh state."""
        if op == "login":
            response = self.portal.request({"op": "login"})
            session = domain.login_decision(response)
            self._persist(
                domain.login_effect(self.repository.read_control(), session))
            return

        # Every other request needs the live session and live pending phase.
        control = self.repository.read_control()
        plan = domain.plan_step(control)
        if op == "ack":
            # The staged->uncertain intent is durable before the ack leaves
            # the process, so a lost response still replays as an uncertain
            # ack. The request itself is built from the pre-intent snapshot,
            # matching the legacy loop; only the response reads fresh state.
            self._persist(domain.intent_effect(control))
        try:
            response = self.portal.request(plan["request"])
        except HarvestError as error:
            if str(error) == "EXPIRED":
                # Legacy ordering: the checkpoint is re-read inside the
                # exception handler, expiry is persisted atomically, and the
                # iteration ends there; the next iteration logs in afresh.
                latest = self.repository.read_control()
                self._persist(domain.expire_effect(latest, op))
                return
            raise

        # Null responses historically consume a step without touching state;
        # login never had that allowance.
        if response is None:
            return

        # Response handling runs against the freshest checkpoint: an earlier
        # expiry in the same run() call may already have advanced the phase.
        control = self.repository.read_control()
        if op == "fetch":
            # Crash point sits after decoding but before the refreshed-page
            # digest comparison, exactly like the legacy loop.
            page, digest = domain.inspect_fetch(control, response,
                                                plan["cursor"])
            crash("after_fetch")
            self._persist(domain.fetch_effect(control, page, digest))
            crash("after_stage")
        elif op == "ack":
            crash("after_ack")
            self._persist(domain.receipt_effect(control, response))
        elif op == "status":
            if response == {"missing": True}:
                self._persist(domain.missing_effect(control))
            else:
                self._persist(domain.receipt_effect(control, response))

    def _persist(self, effect):
        """Apply one declarative domain effect in a single transaction."""
        with self.repository.transaction():
            self.repository.write_control(effect["control"])
            for event in effect["events"]:
                self.repository.append_audit(event)
