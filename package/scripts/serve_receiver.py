"""Fixed reviewed-source entry point; no cwd or inherited PYTHONPATH imports."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pebble_bridge.receiver import ReceiverSettings, create_receiver
import uvicorn

if __name__ == '__main__':
    settings = ReceiverSettings.from_env()
    uvicorn.run(create_receiver(settings), host='127.0.0.1', port=settings.port,
                access_log=False, log_level='warning')
