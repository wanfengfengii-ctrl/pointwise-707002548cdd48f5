"""Structural and boundary verification for the refactored harvest package.

Runs four real checks rather than inspecting file existence:
1. AST/import proof of the dependency direction; no dynamic imports,
   eval/exec or escape hatches back to the legacy executor.
2. An in-memory Repository double implementing only the fixed port, replaying
   normal/recovery/conflict scenarios through Runner with sqlite3 disabled.
3. Domain determinism and input non-mutation across normal and failure cases.
4. Real on-disk SQLite reopen recovery at staged/uncertain/receipted and
   after an in_commit rollback, plus legacy-schema acceptance.

Entry: python3 verify_structure.py
"""

import ast
import copy
import hashlib
import importlib
import json
import os
import sqlite3
import sys
import tempfile

from harvest import domain
from harvest.adapters.sqlite import SqliteRepository
from harvest.application import Runner
from harvest.ports import HarvestError, Repository
from harvest.replay import ScriptedPortal, load, replay

HARVEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "harvest")
PORT_METHODS = ("read_control", "read_record", "read_receipt", "write_control",
                "write_record", "write_receipt", "append_audit",
                "transaction", "snapshot")


# --------------------------------------------------------------------------
# In-memory Repository double: exactly the fixed port, no sqlite anywhere.
# --------------------------------------------------------------------------

class InMemoryRepository:
    """Value-isolated, transactional Repository port implementation."""

    __slots__ = ("_control", "_records", "_receipts", "_audit",
                 "_transaction", "_backup")

    def __init__(self):
        self._control = domain.initial_control()
        self._records = {}
        self._receipts = {}
        self._audit = []
        self._transaction = False
        self._backup = None

    def read_control(self):
        return copy.deepcopy(self._control)

    def read_record(self, identifier):
        row = self._records.get(identifier)
        return None if row is None else copy.deepcopy(row)

    def read_receipt(self, page):
        receipt = self._receipts.get(page)
        return None if receipt is None else copy.deepcopy(receipt)

    def write_control(self, control):
        self._control = copy.deepcopy(control)

    def write_record(self, row):
        self._records[row["id"]] = copy.deepcopy(row)

    def write_receipt(self, receipt):
        self._receipts[receipt["page"]] = copy.deepcopy(receipt)

    def append_audit(self, event):
        self._audit.append(copy.deepcopy(event))

    def snapshot(self):
        return {
            "control": copy.deepcopy(self._control),
            "records": [copy.deepcopy(self._records[key])
                        for key in sorted(self._records)],
            "receipts": [copy.deepcopy(self._receipts[key])
                         for key in sorted(self._receipts)],
            "audit": copy.deepcopy(self._audit),
        }

    def transaction(self):
        return _MemoryTransaction(self)


class _MemoryTransaction:
    def __init__(self, repository):
        self.repository = repository

    def __enter__(self):
        if self.repository._transaction:
            raise RuntimeError("nested transactions are not supported")
        self.repository._backup = (
            copy.deepcopy(self.repository._control),
            copy.deepcopy(self.repository._records),
            copy.deepcopy(self.repository._receipts),
            copy.deepcopy(self.repository._audit),
        )
        self.repository._transaction = True
        return None

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is not None:
            control, records, receipts, audit = self.repository._backup
            self.repository._control = control
            self.repository._records = records
            self.repository._receipts = receipts
            self.repository._audit = audit
        self.repository._transaction = False
        self.repository._backup = None
        return False


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def checksum(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def digest_page(page):
    return hashlib.sha256(canonical(
        {key: page[key] for key in ("page", "cursor", "next", "rows")}
    ).encode("utf-8")).hexdigest()


def drive_with_sqlite(scenario):
    """Replay a scenario through Runner + SqliteRepository (replay semantics)."""
    portal = ScriptedPortal(scenario["actions"])
    db = sqlite3.connect(":memory:")
    repository = SqliteRepository(db)
    outputs = []
    for invocation in scenario["runs"]:
        image = "\n".join(db.iterdump())
        db.close()
        db = sqlite3.connect(":memory:")
        db.executescript(image)
        repository = SqliteRepository(db)
        runner = Runner(repository, portal)
        try:
            result = runner.run(**invocation)
        except HarvestError as error:
            result = {"error": str(error)}
        outputs.append({"result": result,
                        "snapshot": repository.snapshot(),
                        "calls": len(portal.calls)})
    assert len(portal.calls) == len(portal.actions)
    db.close()
    return {"runs": outputs, "trace": portal.calls}


def drive_in_memory(scenario, snapshots):
    """Same replay through the port-only double; no connection to reopen."""
    portal = ScriptedPortal(scenario["actions"])
    repository = InMemoryRepository()
    outputs = []
    for invocation in scenario["runs"]:
        # A fresh Runner must continue purely from persisted repository state.
        runner = Runner(repository, portal)
        try:
            result = runner.run(**invocation)
        except HarvestError as error:
            result = {"error": str(error)}
        outputs.append({"result": result,
                        "snapshot": repository.snapshot(),
                        "calls": len(portal.calls)})
        snapshots.append(repository.snapshot())
    assert len(portal.calls) == len(portal.actions)
    return {"runs": outputs, "trace": portal.calls}


def conflict_scenario():
    page = {"page": "conflict", "cursor": "0", "next": None, "token": "z",
            "rows": [
                {"id": "new", "rev": 1, "deleted": False,
                 "value": {"n": 1}},
                {"id": "old", "rev": 4, "deleted": False,
                 "value": {"n": 99}}]}
    return {
        "actions": [
            {"expect": {"op": "login"}, "return": {"session": "S"}},
            {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
             "return": page},
            {"expect": {"op": "ack", "session": "S", "page": "conflict",
                        "digest": digest_page(page), "token": "z"},
             "return": {"receipt": "R"}}],
        "runs": [{"steps": 3}, {}, {}]}


# --------------------------------------------------------------------------
# Check 1: AST dependency direction
# --------------------------------------------------------------------------

ALLOWED_HARVEST_IMPORTS = {
    "domain.py": set(),
    "ports.py": set(),
    "application.py": {"harvest.domain", "harvest.ports"},
    "adapters/sqlite.py": set(),
    "legacy.py": {"harvest.adapters.sqlite", "harvest.application",
                  "harvest.ports"},
    "replay.py": {"harvest.adapters.sqlite", "harvest.application",
                  "harvest.ports"},
    "__main__.py": {"harvest.replay"},
    "__init__.py": set(),
    "adapters/__init__.py": set(),
}
FORBIDDEN_CALLS = {"eval", "exec", "__import__", "compile"}
FORBIDDEN_DYNAMIC = {"importlib"}
CORE_FILES = ("domain.py", "ports.py", "application.py",
              "adapters/sqlite.py")


def harvest_module_name(level, module, current):
    if level == 0:
        return module
    parts = current.split("/")
    # relative import: climb `level - 1` packages from the file's package
    base = parts[:-1]
    for _ in range(level - 1):
        base.pop()
    return ".".join(base + ((module or "").split(".") if module else []))


def check_dependency_direction():
    seen = set()
    for root, _dirs, files in os.walk(HARVEST_DIR):
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            rel = os.path.relpath(path, HARVEST_DIR)
            seen.add(rel)
            tree = ast.parse(open(path, encoding="utf-8").read())
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name.split(".")[0])
                        if alias.name.startswith("harvest"):
                            imports.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.level:
                        resolved = harvest_module_name(
                            node.level, node.module or "", rel)
                        imports.add(resolved)
                    elif node.module:
                        imports.add(node.module)
                        if node.module.startswith("harvest"):
                            imports.add(node.module)
                elif isinstance(node, ast.Call):
                    func = node.func
                    called = getattr(func, "id", None) or getattr(
                        func, "attr", None)
                    assert called not in FORBIDDEN_CALLS, (
                        rel, "forbidden call", called)
                    if called == "import_module":
                        raise AssertionError((rel, "dynamic import"))
                elif isinstance(node, ast.Attribute):
                    assert node.attr not in FORBIDDEN_DYNAMIC, (
                        rel, "importlib use")
            harvest_imports = {
                item for item in imports
                if item == "harvest" or item.startswith("harvest.")}
            allowed = ALLOWED_HARVEST_IMPORTS[rel]
            extra = harvest_imports - allowed - {"harvest"}
            assert not extra, (rel, "unexpected imports", sorted(extra))
            text = open(path, encoding="utf-8").read()
            if rel in ("domain.py", "ports.py", "application.py",
                       "adapters/sqlite.py"):
                for banned in ("sqlite3", "socket", "urllib", "requests",
                               "subprocess"):
                    assert banned not in imports, (rel, banned)
                if rel == "adapters/sqlite.py":
                    continue
                assert "open" not in {
                    node.id for node in ast.walk(tree)
                    if isinstance(node, ast.Name)}, (rel, "open() call")
            if rel == "domain.py":
                for banned in ("os", "sys", "time", "random", "datetime"):
                    assert banned not in imports, (rel, banned)
                assert not any(
                    isinstance(node, ast.Global) for node in ast.walk(tree)), (
                    rel, "global statement")
            if rel == "legacy.py":
                # Docstrings may explain the boundary; code must stay a
                # forwarding facade: no SQL, no phase state machine, no loop.
                code_names = {
                    node.id for node in ast.walk(tree)
                    if isinstance(node, ast.Name)}
                code_attrs = {
                    node.attr for node in ast.walk(tree)
                    if isinstance(node, ast.Attribute)}
                for banned in ("execute", "executescript", "iterdump",
                               "phase", "pending", "sqlite3", "db"):
                    assert banned not in code_names and banned not in code_attrs, (
                        rel, "legacy retains", banned)
                assert not any(
                    isinstance(node, (ast.For, ast.While))
                    for node in ast.walk(tree)), "legacy loop"
                assert "sqlite3" not in imports
            if rel == "application.py":
                for banned in ("legacy", "replay", "adapters", "sqlite3"):
                    assert banned not in imports, (rel, "app dependency",
                                                   banned)
    # Ensure the contract modules all exist and parse.
    for required in ("domain.py", "ports.py", "application.py",
                     "adapters/sqlite.py", "legacy.py"):
        assert required in seen, required
    # No second executor: legacy facade only forwards the four old methods.
    legacy_tree = ast.parse(open(
        os.path.join(HARVEST_DIR, "legacy.py"), encoding="utf-8").read())
    methods = {
        node.name for node in ast.walk(legacy_tree)
        if isinstance(node, ast.FunctionDef)}
    assert methods <= {"__init__", "run", "state", "snapshot", "page"}, methods
    print("1. dependency direction: OK")


# --------------------------------------------------------------------------
# Check 2: in-memory double equivalence with sqlite3 disabled
# --------------------------------------------------------------------------

def check_in_memory_double():
    # The double exposes only the fixed Repository port surface.
    public = {name for name in dir(InMemoryRepository)
              if not name.startswith("_")}
    assert public == set(PORT_METHODS), public
    assert isinstance(InMemoryRepository(), Repository)

    # Transaction rollback restores every table including audit.
    double = InMemoryRepository()
    before = double.snapshot()
    try:
        with double.transaction():
            double.write_control({"cursor": "x", "done": False,
                                  "session": None, "pending": None})
            double.append_audit({"event": "login", "session": "S"})
            raise HarvestError("rollback-probe")
    except HarvestError:
        pass
    assert double.snapshot() == before
    # Nested transactions are rejected.
    try:
        with double.transaction():
            with double.transaction():
                raise AssertionError("nested transaction accepted")
    except RuntimeError:
        pass

    scenarios = {
        "normal": load("samples/normal.json"),
        "recovery": load("samples/recovery.json"),
        "conflict": conflict_scenario(),
    }
    expected_results = {}
    sqlite_snapshots, memory_snapshots = {}, {}
    for name, scenario in scenarios.items():
        expected = drive_with_sqlite(copy.deepcopy(scenario))
        expected_results[name] = expected
        mem_snaps = []
        actual = drive_in_memory(copy.deepcopy(scenario), mem_snaps)
        sql_snaps = []
        drive_with_sqlite_snapshots(scenario, sql_snaps)
        assert checksum(actual) == checksum(expected), name
        assert actual["trace"] == expected["trace"], name
        assert mem_snaps == sql_snaps, name
        sqlite_snapshots[name] = sql_snaps
        memory_snapshots[name] = mem_snaps

    # Now disable sqlite3 entirely and the SqliteRepository constructor:
    # Runner + double must still finish all scenarios.
    real_connect = sqlite3.connect
    real_init = SqliteRepository.__init__

    def blocked_connect(*_args, **_kwargs):
        raise AssertionError("sqlite3.connect must not be needed")

    def blocked_init(*_args, **_kwargs):
        raise AssertionError("SqliteRepository must not be constructed")

    sqlite3.connect = blocked_connect
    SqliteRepository.__init__ = blocked_init
    try:
        for name, scenario in scenarios.items():
            snaps = []
            result = drive_in_memory(copy.deepcopy(scenario), snaps)
            assert checksum(result) == checksum(expected_results[name]), name
            assert snaps == memory_snapshots[name], name
    finally:
        sqlite3.connect = real_connect
        SqliteRepository.__init__ = real_init
    print("2. in-memory repository double (sqlite3 disabled): OK")


def drive_with_sqlite_snapshots(scenario, snapshots):
    portal = ScriptedPortal(scenario["actions"])
    db = sqlite3.connect(":memory:")
    repository = SqliteRepository(db)
    for invocation in scenario["runs"]:
        image = "\n".join(db.iterdump())
        db.close()
        db = sqlite3.connect(":memory:")
        db.executescript(image)
        repository = SqliteRepository(db)
        try:
            Runner(repository, portal).run(**invocation)
        except HarvestError:
            pass
        snapshots.append(repository.snapshot())
    db.close()


# --------------------------------------------------------------------------
# Check 3: domain determinism and input non-mutation
# --------------------------------------------------------------------------

def check_domain_purity():
    page = load("samples/normal.json")["actions"][1]["return"]
    control = domain.initial_control()
    staged_control = {"cursor": "0", "done": False, "session": "S",
                      "pending": {"phase": "staged",
                                  "page": copy.deepcopy(page),
                                  "digest": digest_page(page),
                                  "refresh": False}}

    def repeated(builder):
        first = builder()
        second = builder()
        assert canonical(first) == canonical(second)
        return first

    # Normal page decode and stage effect.
    repeated(lambda: domain.decode_page(copy.deepcopy(page), "0"))
    stage = repeated(lambda: domain.fetch_effect(
        copy.deepcopy(control),
        *domain.inspect_fetch(copy.deepcopy(control),
                              copy.deepcopy(page), "0")))

    # Unknown confirmation (arbitrary receipt response) is a receipt decision.
    repeated(lambda: domain.receipt_effect(
        copy.deepcopy(staged_control), {"receipt": "Rx"}))

    # Session expiry effect.
    expired = repeated(lambda: domain.expire_effect(
        copy.deepcopy(staged_control), "ack"))
    assert expired["control"]["session"] is None
    assert expired["control"]["pending"] == staged_control["pending"]

    # Refresh whose semantic digest changed must raise PAGE_CHANGED.
    changed = copy.deepcopy(page)
    changed["token"] = "new-token"
    same_digest = domain.fetch_effect(
        copy.deepcopy(staged_control),
        *domain.inspect_fetch(copy.deepcopy(staged_control),
                              copy.deepcopy(changed), "0"))
    assert same_digest["control"]["pending"]["page"]["token"] == "new-token"
    changed["rows"][0]["value"] = {"name": "changed"}
    try:
        domain.fetch_effect(
            copy.deepcopy(staged_control),
            *domain.inspect_fetch(copy.deepcopy(staged_control),
                                  copy.deepcopy(changed), "0"))
    except HarvestError as error:
        assert str(error) == "PAGE_CHANGED"
    else:
        raise AssertionError("PAGE_CHANGED not raised")

    # Commit-time revision conflict (equal rev, different content).
    pending_page = copy.deepcopy(page)
    stored = {row["id"]: copy.deepcopy(row) for row in page["rows"]}
    stored["b"]["value"] = {"name": "different"}
    try:
        domain.merge_decision(pending_page, stored)
    except HarvestError as error:
        assert str(error) == "REVISION_CONFLICT"
    else:
        raise AssertionError("REVISION_CONFLICT not raised")
    # Higher/lower rev merge decisions are stable too.
    lower = copy.deepcopy(stored)
    lower["a"]["rev"] = 99  # incoming a rev 1 is now lower -> ignored
    lower["b"]["value"] = copy.deepcopy(
        next(row for row in page["rows"] if row["id"] == "b")["value"])
    writes = repeated(lambda: domain.merge_decision(
        copy.deepcopy(pending_page), copy.deepcopy(lower)))
    # a loses on rev (ignored), b is equal-rev identical content (no-op).
    assert writes == []
    # An incoming higher rev is accepted for writing.
    higher = copy.deepcopy(stored)
    higher["a"]["rev"] = 0
    higher["b"]["value"] = copy.deepcopy(
        next(row for row in page["rows"] if row["id"] == "b")["value"])
    writes = repeated(lambda: domain.merge_decision(
        copy.deepcopy(pending_page), copy.deepcopy(higher)))
    assert [row["id"] for row, _ in writes] == ["a"]

    # Inputs must be untouched before and after a deep copy round trip.
    for label, builder in (
            ("decode", lambda: domain.decode_page(copy.deepcopy(page), "0")),
            ("stage", lambda: domain.fetch_effect(
                copy.deepcopy(control),
                *domain.inspect_fetch(copy.deepcopy(control),
                                      copy.deepcopy(page), "0"))),
            ("plan", lambda: domain.plan_step(copy.deepcopy(staged_control)))):
        snapshot = canonical(builder())
        again = canonical(builder())
        assert snapshot == again, label
    raw_page = copy.deepcopy(page)
    raw_ctl = copy.deepcopy(staged_control)
    domain.decode_page(raw_page, "0")
    domain.fetch_effect(raw_ctl, *domain.inspect_fetch(
        raw_ctl, raw_page, "0"))
    domain.plan_step(raw_ctl)
    assert raw_page == page
    assert raw_ctl == staged_control

    # Domain module imports nothing beyond stdlib + ports (re-assert by import
    # graph) and exposes no I/O helpers.
    assert not hasattr(domain, "sqlite3")
    print("3. domain determinism and purity: OK")


# --------------------------------------------------------------------------
# Check 4: real on-disk SQLite reopen recovery
# --------------------------------------------------------------------------

def _open(path):
    connection = sqlite3.connect(path)
    return connection, SqliteRepository(connection)


def _portal_for_page(page):
    digest = digest_page(page)
    return ScriptedPortal([
        {"expect": {"op": "login"}, "return": {"session": "S"}},
        {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
         "return": page},
        {"expect": {"op": "ack", "session": "S", "page": page["page"],
                    "digest": digest, "token": page["token"]},
         "return": {"receipt": "R"}},
        # Reopening on a staged checkpoint resumes with an ack, then commit.
        {"expect": {"op": "ack", "session": "S", "page": page["page"],
                    "digest": digest, "token": page["token"]},
         "return": {"receipt": "R"}}])


def check_disk_recovery():
    page = {"page": "p1", "cursor": "0", "next": None, "token": "t1",
            "rows": [{"id": "a", "rev": 1, "deleted": False,
                      "value": {"name": "甲"}}]}

    with tempfile.TemporaryDirectory() as tmp:
        # -- staged: crash after_fetch leaves no pending; after_stage does --
        path = os.path.join(tmp, "staged.db")
        portal = _portal_for_page(page)
        connection, repository = _open(path)
        try:
            Runner(repository, portal).run(steps=2, fault="after_stage")
        except HarvestError as error:
            assert str(error) == "CRASH_after_stage"
        assert repository.read_control()["pending"]["phase"] == "staged"
        connection.close()
        connection, repository = _open(path)
        Runner(repository, portal).run()
        assert repository.read_control()["done"] is True
        connection.close()

        # -- uncertain: crash after_ack; reopen and finish via status path --
        path = os.path.join(tmp, "uncertain.db")
        portal = ScriptedPortal([
            {"expect": {"op": "login"}, "return": {"session": "S"}},
            {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
             "return": page},
            {"expect": {"op": "ack", "session": "S", "page": "p1",
                        "digest": digest_page(page), "token": "t1"},
             "return": {"receipt": "R"}},
            {"expect": {"op": "status", "session": "S", "page": "p1",
                        "digest": digest_page(page)},
             "return": {"receipt": "R"}}])
        connection, repository = _open(path)
        try:
            Runner(repository, portal).run(fault="after_ack")
        except HarvestError as error:
            assert str(error) == "CRASH_after_ack"
        assert repository.read_control()["pending"]["phase"] == "uncertain"
        connection.close()
        connection, repository = _open(path)
        Runner(repository, portal).run()
        assert repository.read_control()["done"] is True
        connection.close()

        # -- uncertain across session expiry on a reopened database -------
        path = os.path.join(tmp, "uncertain-expired.db")
        portal = ScriptedPortal([
            {"expect": {"op": "login"}, "return": {"session": "S"}},
            {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
             "return": page},
            {"expect": {"op": "ack", "session": "S", "page": "p1",
                        "digest": digest_page(page), "token": "t1"},
             "return": {"receipt": "R"}},
            {"expect": {"op": "status", "session": "S", "page": "p1",
                        "digest": digest_page(page)},
             "raise": "EXPIRED"},
            {"expect": {"op": "login"}, "return": {"session": "T"}},
            {"expect": {"op": "status", "session": "T", "page": "p1",
                        "digest": digest_page(page)},
             "return": {"receipt": "R"}}])
        connection, repository = _open(path)
        try:
            Runner(repository, portal).run(fault="after_ack")
        except HarvestError as error:
            assert str(error) == "CRASH_after_ack"
        connection.close()
        connection, repository = _open(path)
        Runner(repository, portal).run()
        assert repository.read_control()["done"] is True
        assert [event["event"] for event in
                repository.snapshot()["audit"]] == [
            "login", "stage", "intent", "expired", "login", "receipt",
            "commit"]
        connection.close()

        # -- receipted: persist receipt then reopen and commit -------------
        path = os.path.join(tmp, "receipted.db")
        portal = ScriptedPortal([
            {"expect": {"op": "login"}, "return": {"session": "S"}},
            {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
             "return": page},
            {"expect": {"op": "ack", "session": "S", "page": "p1",
                        "digest": digest_page(page), "token": "t1"},
             "return": {"receipt": "R"}}])
        connection, repository = _open(path)
        Runner(repository, portal).run(steps=3)
        assert repository.read_control()["pending"]["phase"] == "receipted"
        before_audit = len(repository.snapshot()["audit"])
        connection.close()
        connection, repository = _open(path)
        Runner(repository, portal).run()
        assert repository.read_control()["done"] is True
        assert repository.read_record("a") == page["rows"][0]
        assert repository.read_receipt("p1") == {
            "page": "p1", "digest": digest_page(page), "receipt": "R"}
        assert len(repository.snapshot()["audit"]) == before_audit + 1
        connection.close()

        # -- in_commit rollback: nothing from the failed transaction stays --
        path = os.path.join(tmp, "commit-rollback.db")
        portal = ScriptedPortal([
            {"expect": {"op": "login"}, "return": {"session": "S"}},
            {"expect": {"op": "fetch", "session": "S", "cursor": "0"},
             "return": page},
            {"expect": {"op": "ack", "session": "S", "page": "p1",
                        "digest": digest_page(page), "token": "t1"},
             "return": {"receipt": "R"}}])
        connection, repository = _open(path)
        Runner(repository, portal).run(steps=3)
        checkpoint = repository.snapshot()
        try:
            Runner(repository, portal).run(fault="in_commit")
        except HarvestError as error:
            assert str(error) == "CRASH_in_commit"
        else:
            raise AssertionError("in_commit crash missing")
        assert repository.snapshot() == checkpoint
        connection.close()
        connection, repository = _open(path)
        assert repository.snapshot() == checkpoint
        Runner(repository, portal).run()
        assert repository.read_control()["done"] is True
        connection.close()

        # -- legacy schema/control acceptance: hand-built old checkpoint,
        # no new-version "migration" may rewrite it. ----------------------
        path = os.path.join(tmp, "legacy.db")
        connection = sqlite3.connect(path)
        connection.executescript("""
        CREATE TABLE control(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
        CREATE TABLE records(id TEXT PRIMARY KEY, rev INTEGER NOT NULL,
                             deleted INTEGER NOT NULL, body TEXT NOT NULL);
        CREATE TABLE receipts(page TEXT PRIMARY KEY, digest TEXT NOT NULL,
                              receipt TEXT NOT NULL);
        CREATE TABLE audit(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
        """)
        legacy_body = canonical({"cursor": "0", "done": False,
                                 "session": None, "pending": None})
        connection.execute("INSERT INTO control VALUES(1,?)", (legacy_body,))
        connection.commit()
        connection.close()
        connection = sqlite3.connect(path)
        repository = SqliteRepository(connection)
        assert connection.execute(
            "SELECT body FROM control WHERE id=1").fetchone()[0] == legacy_body
        assert repository.read_control() == json.loads(legacy_body)
        # Pre-existing non-default rows survive adapter construction.
        connection.execute(
            "INSERT INTO records VALUES(?,?,?,?)",
            ("kept", 7, 1, canonical({"v": 1})))
        connection.commit()
        connection.close()
        connection, repository = _open(path)
        assert repository.read_record("kept") == {
            "id": "kept", "rev": 7, "deleted": True, "value": {"v": 1}}
        connection.close()
    print("4. on-disk reopen recovery and legacy acceptance: OK")


def main():
    check_dependency_direction()
    check_in_memory_double()
    check_domain_purity()
    check_disk_recovery()
    print("STRUCTURE_VERIFY_OK")


if __name__ == "__main__":
    main()
