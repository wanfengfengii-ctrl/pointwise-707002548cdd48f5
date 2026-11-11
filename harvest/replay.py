import json
import sqlite3

from .adapters.sqlite import SqliteRepository
from .application import Runner
from .ports import HarvestError


class ScriptedPortal:
    """Deterministic recording of the remote boundary, outside local transactions."""
    def __init__(self, actions):
        self.actions = actions
        self.calls = []

    def request(self, request):
        index = len(self.calls)
        self.calls.append(json.loads(json.dumps(request)))
        if index >= len(self.actions):
            raise AssertionError("unexpected request: " + repr(request))
        action = self.actions[index]
        if request != action["expect"]:
            raise AssertionError({"index": index, "expected": action["expect"], "actual": request})
        if "raise" in action:
            raise HarvestError(action["raise"])
        return action["return"]


def replay(scenario):
    portal = ScriptedPortal(scenario["actions"])
    db = sqlite3.connect(":memory:")
    outputs = []
    repository = SqliteRepository(db)
    runner = Runner(repository, portal)
    for invocation in scenario["runs"]:
        # Reopen the identical persisted database; the remote system is not rewound.
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
        outputs.append({"result": result, "snapshot": repository.snapshot(),
                        "calls": len(portal.calls)})
    if len(portal.calls) != len(portal.actions):
        raise AssertionError("unconsumed portal actions")
    db.close()
    return {"runs": outputs, "trace": portal.calls}


def load(path):
    with open(path, encoding="utf-8") as source:
        return json.load(source)
