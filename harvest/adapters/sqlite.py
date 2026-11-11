"""SQLite adapter for the harvest Repository port.

Owns schema creation, SQL, atomic commit/rollback and DTO encoding only.
It never drives the harvest flow and never calls the portal. Empty
connections receive the four historical tables and initial control;
connections that already contain content are preserved verbatim.
"""

import json


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS control(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, rev INTEGER NOT NULL,
                                   deleted INTEGER NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipts(page TEXT PRIMARY KEY, digest TEXT NOT NULL,
                                    receipt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
"""


class _Transaction:
    """BEGIN IMMEDIATE transaction; any BaseException rolls back."""

    def __init__(self, connection):
        self.connection = connection
        self.active = False

    def __enter__(self):
        if self.active:
            raise RuntimeError("nested transactions are not supported")
        self.active = True
        self.connection.execute("BEGIN IMMEDIATE")
        return None

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.connection.execute("COMMIT")
        else:
            self.connection.execute("ROLLBACK")
        self.active = False
        return False


class SqliteRepository:
    """Repository port implementation backed by a sqlite3 connection."""

    def __init__(self, connection):
        self.connection = connection
        # Historical construction semantics: manual transaction control.
        self.connection.isolation_level = None
        self.connection.executescript(_SCHEMA)
        if not self.connection.execute("SELECT 1 FROM control").fetchone():
            self.connection.execute(
                "INSERT INTO control VALUES(1,?)",
                (_canonical({"cursor": "0", "done": False,
                             "session": None, "pending": None}),))

    def transaction(self):
        return _Transaction(self.connection)

    # -- control --------------------------------------------------------

    def read_control(self):
        body = self.connection.execute(
            "SELECT body FROM control WHERE id=1").fetchone()[0]
        return json.loads(body)

    def write_control(self, control):
        self.connection.execute(
            "UPDATE control SET body=? WHERE id=1", (_canonical(control),))

    # -- records --------------------------------------------------------

    def read_record(self, identifier):
        row = self.connection.execute(
            "SELECT rev,deleted,body FROM records WHERE id=?",
            (identifier,)).fetchone()
        if row is None:
            return None
        return {"id": identifier, "rev": row[0], "deleted": bool(row[1]),
                "value": json.loads(row[2])}

    def write_record(self, row):
        self.connection.execute(
            "INSERT OR REPLACE INTO records VALUES(?,?,?,?)",
            (row["id"], row["rev"], row["deleted"],
             _canonical(row["value"])))

    # -- receipts -------------------------------------------------------

    def read_receipt(self, page):
        row = self.connection.execute(
            "SELECT digest,receipt FROM receipts WHERE page=?",
            (page,)).fetchone()
        if row is None:
            return None
        return {"page": page, "digest": row[0], "receipt": row[1]}

    def write_receipt(self, receipt):
        self.connection.execute(
            "INSERT OR REPLACE INTO receipts VALUES(?,?,?)",
            (receipt["page"], receipt["digest"], receipt["receipt"]))

    # -- audit ----------------------------------------------------------

    def append_audit(self, event):
        self.connection.execute(
            "INSERT INTO audit(body) VALUES(?)", (_canonical(event),))

    # -- public snapshot ------------------------------------------------

    def snapshot(self):
        return {
            "control": self.read_control(),
            "records": [
                {"id": identifier, "rev": rev, "deleted": bool(deleted),
                 "value": json.loads(body)}
                for identifier, rev, deleted, body in self.connection.execute(
                    "SELECT id,rev,deleted,body FROM records ORDER BY id")],
            "receipts": [
                {"page": page, "digest": digest, "receipt": receipt}
                for page, digest, receipt in self.connection.execute(
                    "SELECT page,digest,receipt FROM receipts ORDER BY page")],
            "audit": [
                json.loads(body) for (body,) in self.connection.execute(
                    "SELECT body FROM audit ORDER BY seq")],
        }
