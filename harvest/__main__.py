import json
import sys
from .replay import load, replay

print(json.dumps(replay(load(sys.argv[1])), ensure_ascii=False, sort_keys=True))
