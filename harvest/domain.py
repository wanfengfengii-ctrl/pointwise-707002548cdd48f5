"""Pure domain core: page validation, phases, response decisions, merges.

Every function takes plain JSON values and returns plain values, declarative
effect descriptors or HarvestError. Nothing here opens a connection, calls a
repository or portal, reads clocks/randomness or mutates caller input.
"""

import copy
import hashlib
import json

from .ports import HarvestError

PAGE_KEYS = ("page", "cursor", "next", "rows")
ROW_KEYS = frozenset(("id", "rev", "deleted", "value"))


def canonical(value):
    """Canonical JSON encoding used for digests and stored control/bodies."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def initial_control():
    """Fresh checkpoint content for an empty repository."""
    return {"cursor": "0", "done": False, "session": None, "pending": None}


def page_semantics(page):
    """Strip the delivery-specific token; row order stays significant."""
    return {key: page[key] for key in PAGE_KEYS}


def page_digest(page):
    """SHA-256 over the canonical token-free page."""
    return hashlib.sha256(
        canonical(page_semantics(page)).encode("utf-8")).hexdigest()


def decode_page(raw, cursor):
    """Validate a fetched page; return (normalized deep copy, digest).

    Mirrors the historical boundary checks exactly, including the exact key
    set and the strict bool/int typing of rev and deleted.
    """
    try:
        if set(raw) != {"page", "cursor", "next", "token", "rows"}:
            raise ValueError()
        for key in ("page", "cursor", "token"):
            if not isinstance(raw[key], str) or not raw[key]:
                raise ValueError()
        if raw["cursor"] != cursor or not (
                raw["next"] is None
                or isinstance(raw["next"], str) and raw["next"]):
            raise ValueError()
        if not isinstance(raw["rows"], list):
            raise ValueError()
        for row in raw["rows"]:
            if set(row) != ROW_KEYS:
                raise ValueError()
            if not isinstance(row["id"], str) or not row["id"]:
                raise ValueError()
            if type(row["rev"]) is not int or row["rev"] < 0:
                raise ValueError()
            if type(row["deleted"]) is not bool or not isinstance(
                    row["value"], dict):
                raise ValueError()
            canonical(row["value"])
        digest = page_digest(raw)
    except (TypeError, ValueError, KeyError):
        raise HarvestError("BAD_PAGE") from None
    return json.loads(canonical(raw)), digest


def login_decision(response):
    """Validate a login response; return the session string."""
    if (not isinstance(response, dict)
            or not isinstance(response.get("session"), str)
            or not response["session"]):
        raise HarvestError("BAD_LOGIN")
    return response["session"]


def receipt_decision(response):
    """Validate an ack/status response carrying a receipt."""
    if (not isinstance(response, dict) or set(response) != {"receipt"}
            or not isinstance(response["receipt"], str)
            or not response["receipt"]):
        raise HarvestError("BAD_RECEIPT")
    return response["receipt"]


def fetch_cursor(control):
    """Cursor for the next fetch: refreshed pages refetch their own cursor."""
    pending = control["pending"]
    if pending is None:
        return control["cursor"]
    return pending["page"]["cursor"]


def login_effect(control, session):
    """Bind a fresh session while preserving cursor/pending checkpoint work."""
    next_control = copy.deepcopy(control)
    next_control["session"] = session
    return {"kind": "persist",
            "control": next_control,
            "events": [{"event": "login", "session": session}]}


def expire_effect(control, op):
    """Session expiry clears the session and records the op that saw it."""
    next_control = copy.deepcopy(control)
    next_control["session"] = None
    return {"kind": "persist",
            "control": next_control,
            "events": [{"event": "expired", "op": op}]}


def inspect_fetch(control, response, cursor):
    """Pure first half of fetch handling: validate the page and digest it.

    Kept separate from :func:`fetch_effect` so the application can reproduce
    the historical after-fetch fault point, which sits between decoding and
    the refreshed-page digest comparison.
    """
    page, digest = decode_page(response, cursor)
    return page, digest


def fetch_effect(control, page, digest):
    """Bind a decoded page, or raise PAGE_CHANGED on a refreshed mismatch."""
    pending = control["pending"]
    next_control = copy.deepcopy(control)
    if pending is not None:
        # A rebind must keep the same semantic digest; only the token may move.
        if digest != pending["digest"]:
            raise HarvestError("PAGE_CHANGED")
        next_control["pending"]["page"] = page
        next_control["pending"]["refresh"] = False
        event = {"event": "rebind", "page": page["page"]}
    else:
        next_control["pending"] = {"phase": "staged", "page": page,
                                   "digest": digest, "refresh": False}
        event = {"event": "stage", "page": page["page"]}
    return {"kind": "persist", "control": next_control, "events": [event]}


def intent_effect(control):
    """staged -> uncertain before the ack request leaves the process."""
    next_control = copy.deepcopy(control)
    next_control["pending"]["phase"] = "uncertain"
    return {"kind": "persist",
            "control": next_control,
            "events": [{"event": "intent",
                        "page": control["pending"]["page"]["page"]}]}


def receipt_effect(control, response):
    """Accept a receipt: uncertain/staged -> receipted (durable locally)."""
    receipt = receipt_decision(response)
    next_control = copy.deepcopy(control)
    pending = next_control["pending"]
    pending["phase"] = "receipted"
    pending["receipt"] = receipt
    return {"kind": "persist",
            "control": next_control,
            "events": [{"event": "receipt",
                        "page": control["pending"]["page"]["page"],
                        "receipt": receipt}]}


def missing_effect(control):
    """Remote confirms the ack never landed: mark for a same-digest refetch."""
    next_control = copy.deepcopy(control)
    pending = next_control["pending"]
    pending["phase"] = "staged"
    pending["refresh"] = True
    return {"kind": "persist",
            "control": next_control,
            "events": [{"event": "status_missing",
                        "page": control["pending"]["page"]["page"]}]}


def merge_decision(pending_page, stored):
    """Per-row version merge for a receipted page.

    Returns a list of (row, stored_or_none) pairs that need writing plus the
    revision conflict marker. Higher rev replaces, lower is ignored; equal rev
    with identical content is a no-op, equal rev with different content raises
    REVISION_CONFLICT. Tombstones keep their value and compare like any row.
    """
    writes = []
    for row in pending_page["rows"]:
        old = stored.get(row["id"])
        if old is not None and row["rev"] < old["rev"]:
            continue
        if old is not None and row["rev"] == old["rev"]:
            if (old["deleted"], canonical(old["value"])) != (
                    row["deleted"], canonical(row["value"])):
                raise HarvestError("REVISION_CONFLICT")
            continue
        writes.append((row, old))
    return writes


def commit_control(control):
    """Control state after a successful local commit of a receipted page."""
    pending = control["pending"]
    next_control = copy.deepcopy(control)
    next_control["cursor"] = pending["page"]["next"]
    next_control["done"] = next_control["cursor"] is None
    next_control["pending"] = None
    return next_control


def commit_receipt(control):
    pending = control["pending"]
    return {"page": pending["page"]["page"], "digest": pending["digest"],
            "receipt": pending["receipt"]}


def commit_event(control):
    return {"event": "commit", "page": control["pending"]["page"]["page"]}


def plan_step(control):
    """Declarative plan for one loop iteration of the given checkpoint.

    Returns either a commit descriptor (all work happens inside one local
    transaction) or a remote descriptor naming the request and the pure
    response handler. No I/O happens here.
    """
    pending = control["pending"]
    if pending is not None and pending["phase"] == "receipted":
        return {"kind": "commit"}
    if control["session"] is None:
        return {"kind": "remote", "op": "login",
                "request": {"op": "login"}}
    if pending is None or pending.get("refresh", False):
        cursor = fetch_cursor(control)
        return {"kind": "remote", "op": "fetch",
                "request": {"op": "fetch", "session": control["session"],
                            "cursor": cursor},
                "cursor": cursor}
    if pending["phase"] == "staged":
        return {"kind": "remote", "op": "ack",
                "request": {"op": "ack", "session": control["session"],
                            "page": pending["page"]["page"],
                            "digest": pending["digest"],
                            "token": pending["page"]["token"]}}
    if pending["phase"] == "uncertain":
        return {"kind": "remote", "op": "status",
                "request": {"op": "status", "session": control["session"],
                            "page": pending["page"]["page"],
                            "digest": pending["digest"]}}
    raise HarvestError("BAD_CHECKPOINT")
