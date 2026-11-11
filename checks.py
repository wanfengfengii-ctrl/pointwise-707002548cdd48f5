import copy
import hashlib
import json
import sqlite3
import sys

from harvest.legacy import Collector, HarvestError
from harvest.replay import ScriptedPortal, load, replay


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest_page(page):
    return hashlib.sha256(canonical({k: page[k] for k in
        ("page", "cursor", "next", "rows")}).encode("utf-8")).hexdigest()


def checksum(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def regression(name):
    result = replay(load("samples/" + name + ".json"))
    expected = load("samples/expected.json")[name]
    assert checksum(result) == expected, (name, checksum(result), expected)
    final = result["runs"][-1]["snapshot"]
    assert final["control"]["done"] is True
    assert final["records"] == [
        {"id": "a", "rev": 2, "deleted": True, "value": {}},
        {"id": "b", "rev": 2, "deleted": False, "value": {"name": "乙"}},
        {"id": "c", "rev": 0, "deleted": False,
         "value": {"name": "丙", "tags": ["公开", "年报"]}}]
    if name == "recovery":
        runs = result["runs"]
        assert [r["calls"] for r in runs] == [2,3,4,5,7,7,8,11,13,14,18,18,18]
        assert runs[4]["snapshot"] == runs[5]["snapshot"]
        assert runs[2]["snapshot"]["control"]["pending"]["phase"] == "uncertain"
        assert runs[2]["snapshot"]["records"] == []
        assert runs[8]["snapshot"]["control"]["pending"]["page"]["token"] == "t4"
        assert runs[10]["snapshot"]["control"]["pending"]["phase"] == "receipted"
    return result


def expect_error(fn, code):
    try:
        fn()
    except HarvestError as error:
        assert str(error) == code, str(error)
    else:
        raise AssertionError("missing " + code)


def edge_checks():
    p = load("samples/normal.json")["actions"][1]["return"]
    db = sqlite3.connect(":memory:")
    portal = ScriptedPortal([])
    collector = Collector(db, portal)
    # Strict normalization is independent of the session protocol.
    for path, bad in [("rev", True), ("rev", -1), ("deleted", 1), ("value", [])]:
        page = copy.deepcopy(p)
        page["rows"][0][path] = bad
        before = collector.snapshot()
        expect_error(lambda: collector.page(page, "0"), "BAD_PAGE")
        assert collector.snapshot() == before
    expect_error(lambda: collector.page(p, "wrong"), "BAD_PAGE")
    # Tokens are delivery-specific; ordered rows and semantic content are not.
    a, da = collector.page(p, "0")
    b = copy.deepcopy(p)
    b["token"] = "replacement"
    assert collector.page(b, "0")[1] == da
    b["rows"].reverse()
    assert collector.page(b, "0")[1] != da
    # A previously committed page id cannot silently acquire new content.
    db.execute("INSERT INTO receipts VALUES(?,?,?)", ("p1", da, "R1"))
    b = copy.deepcopy(p)
    b["next"] = "different"
    expect_error(lambda: collector.page(b, "0"), "PAGE_CHANGED")
    db.close()
    # One accepted receipt can still fail its local merge: every table rolls back,
    # including records already written earlier in the page.
    page = {"page": "conflict", "cursor": "0", "next": None, "token": "z", "rows": [
        {"id": "new", "rev": 1, "deleted": False, "value": {"n": 1}},
        {"id": "old", "rev": 4, "deleted": False, "value": {"n": 99}}]}
    db = sqlite3.connect(":memory:")
    portal = ScriptedPortal([
        {"expect": {"op": "login"}, "return": {"session": "S"}},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": page},
        {"expect": {"op": "ack", "session": "S", "page": "conflict",
                    "digest": digest_page(page), "token": "z"}, "return": {"receipt": "R"}}])
    collector = Collector(db, portal)
    db.execute("INSERT INTO records VALUES(?,?,?,?)", ("old", 4, 0, canonical({"n": 1})))
    assert collector.run(steps=3) == {"status": "running", "steps": 3}
    before = collector.snapshot()
    for _ in range(2):
        expect_error(lambda: collector.run(), "REVISION_CONFLICT")
        assert collector.snapshot() == before
        assert len(portal.calls) == 3
    db.close()
    # Malformed ack replies also leave a durable uncertain outcome, not staged.
    page = copy.deepcopy(p)
    page["next"] = None
    portal = ScriptedPortal([
        {"expect": {"op": "login"}, "return": {"session": "S"}},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": page},
        {"expect": {"op": "ack", "session": "S", "page": "p1",
                    "digest": digest_page(page), "token": "t1"}, "return": {"receipt": ""}},
        {"expect": {"op": "status", "session": "S", "page": "p1",
                    "digest": digest_page(page)}, "return": {"receipt": "R"}}])
    db = sqlite3.connect(":memory:")
    collector = Collector(db, portal)
    expect_error(lambda: collector.run(), "BAD_RECEIPT")
    assert collector.state()["pending"]["phase"] == "uncertain"
    assert collector.run()["status"] == "done"
    db.close()

    # Failure precedence is observable: a crash after decoding wins over a
    # refreshed page's pending-digest mismatch, which is checked afterwards.
    page = copy.deepcopy(p)
    changed = copy.deepcopy(p)
    changed["rows"][0]["value"] = {"name": "changed"}
    portal = ScriptedPortal([
        {"expect": {"op": "login"}, "return": {"session": "S"}},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": page},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": changed},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": changed}])
    db = sqlite3.connect(":memory:")
    collector = Collector(db, portal)
    collector.run(steps=2)
    state = collector.state()
    state["pending"]["refresh"] = True
    db.execute("UPDATE control SET body=? WHERE id=1", (canonical(state),))
    before = collector.snapshot()
    expect_error(lambda: collector.run(fault="after_fetch"), "CRASH_after_fetch")
    assert collector.snapshot() == before
    expect_error(lambda: collector.run(), "PAGE_CHANGED")
    assert collector.snapshot() == before
    db.close()
    # Null responses historically consume a step without expiring a session.
    # Refactoring must not silently tighten this into a protocol validation fix.
    portal = ScriptedPortal([
        {"expect": {"op": "login"}, "return": {"session": "S", "ignored": 1}},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"}, "return": None}])
    db = sqlite3.connect(":memory:")
    collector = Collector(db, portal)
    assert collector.run(steps=0) == {"status": "running", "steps": 0}
    assert collector.run(steps=2) == {"status": "running", "steps": 2}
    assert collector.state()["session"] == "S"
    assert collector.snapshot()["audit"] == [{"event": "login", "session": "S"}]
    db.close()


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if mode == "normal":
        regression("normal")
        print("NORMAL_REGRESSION_OK")
    elif mode == "verify":
        regression("normal")
        regression("recovery")
        edge_checks()
        print("BEHAVIOR_REGRESSION_OK")
    elif mode == "demo":
        result = regression("recovery")
        print(json.dumps({
            "recovery": [{"result": r["result"], "remote_calls": r["calls"],
                          "phase": (r["snapshot"]["control"]["pending"] or {}).get("phase")}
                         for r in result["runs"]],
            "final": result["runs"][-1]["snapshot"],
            "trace": result["trace"]}, ensure_ascii=False, indent=2))
    else:
        raise SystemExit("unknown mode: " + mode)


if __name__ == "__main__":
    main()
