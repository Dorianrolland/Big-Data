import sys
from pathlib import Path

# Each service has its own copilot_events_pb2.py and runs with its directory as WORKDIR
# in Docker. Add service dirs to sys.path so tests can import service modules directly.
_root = Path(__file__).parent
for _svc in ("cold_path", "hot_path", "api", "context_poller", "copilot_features",
             "driver_ingest", "tlc_replay"):
    _p = str(_root / _svc)
    if _p not in sys.path:
        sys.path.insert(0, _p)
