"""The receiver refuses unauthenticated or malformed captures before storage."""
from starlette.testclient import TestClient

from pebble_bridge.receiver import ReceiverSettings, create_receiver


def test_receiver_requires_bearer_and_multipart(tmp_path):
    settings = ReceiverSettings(data_dir=tmp_path / 'data',
        webhook_token='A' * 32, allowed_hosts=frozenset({'ring.example.test'}))
    with TestClient(create_receiver(settings)) as client:
        missing = client.post('/pebble', headers={'host': 'ring.example.test'}, content=b'x')
        assert missing.status_code == 401
        wrong_host = client.post('/pebble', headers={
            'host': 'wrong.example.test', 'authorization': 'Bearer ' + 'A' * 32}, content=b'x')
        assert wrong_host.status_code == 403
        malformed = client.post('/pebble', headers={
            'host': 'ring.example.test', 'authorization': 'Bearer ' + 'A' * 32}, content=b'x')
        assert malformed.status_code == 415
