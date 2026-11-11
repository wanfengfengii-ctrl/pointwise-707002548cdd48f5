import hashlib
import json
import sqlite3


class HarvestError(Exception):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class Collector:
    """Production runner: SQLite checkpoint and synchronous portal transport."""
    def __init__(self, connection, transport):
        self.db = connection
        self.transport = transport
        self.db.isolation_level = None
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS control(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, rev INTEGER NOT NULL,
                                               deleted INTEGER NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS receipts(page TEXT PRIMARY KEY, digest TEXT NOT NULL,
                                                receipt TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
        """)
        if not self.db.execute("SELECT 1 FROM control").fetchone():
            self.db.execute("INSERT INTO control VALUES(1,?)", (canonical({
                "cursor": "0", "done": False, "session": None, "pending": None}),))

    def state(self):
        return json.loads(self.db.execute("SELECT body FROM control WHERE id=1").fetchone()[0])

    def save(self, state):
        self.db.execute("UPDATE control SET body=? WHERE id=1", (canonical(state),))

    def log(self, event, **fields):
        self.db.execute("INSERT INTO audit(body) VALUES(?)",
                        (canonical(dict(event=event, **fields)),))

    def atomic(self, change):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            change()
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def snapshot(self):
        return {
            "control": self.state(),
            "records": [{"id": i, "rev": r, "deleted": bool(d), "value": json.loads(b)}
                        for i, r, d, b in self.db.execute(
                            "SELECT id,rev,deleted,body FROM records ORDER BY id")],
            "receipts": [{"page": p, "digest": d, "receipt": r} for p, d, r in
                         self.db.execute("SELECT page,digest,receipt FROM receipts ORDER BY page")],
            "audit": [json.loads(r[0]) for r in self.db.execute("SELECT body FROM audit ORDER BY seq")]
        }

    def page(self, raw, cursor):
        try:
            if set(raw) != {"page", "cursor", "next", "token", "rows"}:
                raise ValueError()
            for key in ("page", "cursor", "token"):
                if not isinstance(raw[key], str) or not raw[key]:
                    raise ValueError()
            if raw["cursor"] != cursor or not (
                    raw["next"] is None or isinstance(raw["next"], str) and raw["next"]):
                raise ValueError()
            if not isinstance(raw["rows"], list):
                raise ValueError()
            for row in raw["rows"]:
                if set(row) != {"id", "rev", "deleted", "value"}:
                    raise ValueError()
                if not isinstance(row["id"], str) or not row["id"]:
                    raise ValueError()
                if type(row["rev"]) is not int or row["rev"] < 0:
                    raise ValueError()
                if type(row["deleted"]) is not bool or not isinstance(row["value"], dict):
                    raise ValueError()
                canonical(row["value"])
            semantic = {k: raw[k] for k in ("page", "cursor", "next", "rows")}
            digest = hashlib.sha256(canonical(semantic).encode("utf-8")).hexdigest()
        except (TypeError, ValueError, KeyError):
            raise HarvestError("BAD_PAGE") from None
        old = self.db.execute("SELECT digest FROM receipts WHERE page=?", (raw["page"],)).fetchone()
        if old and old[0] != digest:
            raise HarvestError("PAGE_CHANGED")
        return json.loads(canonical(raw)), digest

    def request(self, op, state, **fields):
        try:
            return self.transport.request(dict(op=op, session=state["session"], **fields))
        except HarvestError as error:
            if str(error) == "EXPIRED":
                state["session"] = None
                def expire():
                    self.save(state)
                    self.log("expired", op=op)
                self.atomic(expire)
                return None
            raise

    def run(self, steps=100, fault=None):
        def crash(point):
            if point == fault:
                raise HarvestError("CRASH_" + point)
        if self.state()["done"]:
            return {"status": "done", "steps": 0}
        for number in range(steps):
            state = self.state()
            pending = state["pending"]
            if pending and pending["phase"] == "receipted":
                def commit():
                    for row in pending["page"]["rows"]:
                        old = self.db.execute("SELECT rev,deleted,body FROM records WHERE id=?",
                                              (row["id"],)).fetchone()
                        body = canonical(row["value"])
                        if old and row["rev"] < old[0]:
                            continue
                        if old and row["rev"] == old[0]:
                            if (bool(old[1]), old[2]) != (row["deleted"], body):
                                raise HarvestError("REVISION_CONFLICT")
                            continue
                        self.db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?,?)",
                                        (row["id"], row["rev"], row["deleted"], body))
                    crash("in_commit")
                    self.db.execute("INSERT OR REPLACE INTO receipts VALUES(?,?,?)",
                                    (pending["page"]["page"], pending["digest"], pending["receipt"]))
                    state["cursor"] = pending["page"]["next"]
                    state["done"] = state["cursor"] is None
                    state["pending"] = None
                    self.save(state)
                    self.log("commit", page=pending["page"]["page"])
                self.atomic(commit)
            elif state["session"] is None:
                response = self.transport.request({"op": "login"})
                if not isinstance(response, dict) or not isinstance(response.get("session"), str) or not response["session"]:
                    raise HarvestError("BAD_LOGIN")
                state["session"] = response["session"]
                def login():
                    self.save(state)
                    self.log("login", session=state["session"])
                self.atomic(login)
            elif pending is None or pending.get("refresh", False):
                cursor = state["cursor"] if pending is None else pending["page"]["cursor"]
                response = self.request("fetch", state, cursor=cursor)
                if response is None:
                    continue
                page, digest = self.page(response, cursor)
                crash("after_fetch")
                if pending is not None:
                    if digest != pending["digest"]:
                        raise HarvestError("PAGE_CHANGED")
                    pending["page"] = page
                    pending["refresh"] = False
                    event = "rebind"
                else:
                    state["pending"] = {"phase": "staged", "page": page,
                                        "digest": digest, "refresh": False}
                    event = "stage"
                def stage():
                    self.save(state)
                    self.log(event, page=page["page"])
                self.atomic(stage)
                crash("after_stage")
            elif pending["phase"] == "staged":
                pending["phase"] = "uncertain"
                def intent():
                    self.save(state)
                    self.log("intent", page=pending["page"]["page"])
                self.atomic(intent)
                response = self.request("ack", state, page=pending["page"]["page"],
                                        digest=pending["digest"], token=pending["page"]["token"])
                if response is None:
                    continue
                crash("after_ack")
                self.accept_receipt(state, response)
            elif pending["phase"] == "uncertain":
                response = self.request("status", state, page=pending["page"]["page"],
                                        digest=pending["digest"])
                if response is None:
                    continue
                if response == {"missing": True}:
                    pending["phase"] = "staged"
                    pending["refresh"] = True
                    def missing():
                        self.save(state)
                        self.log("status_missing", page=pending["page"]["page"])
                    self.atomic(missing)
                else:
                    self.accept_receipt(state, response)
            else:
                raise HarvestError("BAD_CHECKPOINT")
            if self.state()["done"]:
                return {"status": "done", "steps": number + 1}
        return {"status": "running", "steps": steps}

    def accept_receipt(self, state, response):
        if not isinstance(response, dict) or set(response) != {"receipt"} or not isinstance(response["receipt"], str) or not response["receipt"]:
            raise HarvestError("BAD_RECEIPT")
        pending = state["pending"]
        pending["phase"] = "receipted"
        pending["receipt"] = response["receipt"]
        def accept():
            self.save(state)
            self.log("receipt", page=pending["page"]["page"], receipt=response["receipt"])
        self.atomic(accept)
