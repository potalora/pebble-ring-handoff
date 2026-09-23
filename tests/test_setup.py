"""First-run setup keeps credentials and state outside the plugin package."""

import json
import sqlite3
from pathlib import Path

import pytest

from pebble_bridge import setup


def fake_installer(venv: Path, requirements: Path):
    assert requirements.name == 'receiver-requirements.txt'
    (venv / 'bin').mkdir(parents=True, exist_ok=True)
    (venv / 'bin' / 'python').write_text('synthetic receiver python')


def test_first_run_and_repeat_preserve_token_and_database(tmp_path):
    root = tmp_path / 'runtime'
    cloudflared = tmp_path / 'cloudflared'
    cloudflared.write_text('synthetic executable')
    cloudflared.chmod(0o700)

    first = setup.prepare_runtime(root, cloudflared=cloudflared,
                                  install_receiver=fake_installer)
    secret = root / '.pebble-receiver.json'
    token = json.loads(secret.read_text())['webhook_token']
    database = root / 'data' / 'bridge.sqlite3'
    with sqlite3.connect(database) as connection:
        connection.execute('CREATE TABLE setup_probe(value TEXT)')
        connection.execute("INSERT INTO setup_probe VALUES ('preserved')")
    second = setup.prepare_runtime(root, cloudflared=cloudflared,
                                   install_receiver=fake_installer)

    assert first['status'] == second['status'] == 'runtime-prepared'
    assert token == json.loads(secret.read_text())['webhook_token']
    assert token not in json.dumps(first)
    assert json.loads(secret.read_text()) == {
        'data_dir': str(root / 'data'), 'webhook_token': token, 'port': 8765}
    assert (root / '.pebble-runtime.env').read_text() == (
        'PEBBLE_ALLOWED_HOSTS=bootstrap.trycloudflare.com\n')
    assert (root / '.pebble-watchdog-runtime' / 'watchdog.lock').is_file()
    for directory in (root, root / 'data', root / '.pebble-watchdog-runtime'):
        assert directory.stat().st_mode & 0o777 == 0o700
    for path in (secret, root / '.pebble-runtime.env',
                 root / '.pebble-watchdog-runtime' / 'watchdog.lock', database):
        assert path.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(database) as connection:
        assert connection.execute('SELECT value FROM setup_probe').fetchone() == ('preserved',)


def test_missing_cloudflared_stops_before_creating_state(tmp_path):
    root = tmp_path / 'runtime'
    with pytest.raises(setup.SetupError, match='cloudflared-unavailable'):
        setup.prepare_runtime(root, cloudflared=tmp_path / 'missing',
                              install_receiver=fake_installer)
    assert not root.exists()


def test_system_cloudflared_can_be_used_without_a_fixed_data_path(tmp_path):
    binary = tmp_path / 'cloudflared'
    binary.write_text('synthetic executable')
    binary.chmod(0o700)
    assert setup.select_cloudflared([tmp_path / 'missing', binary]) == binary


def test_existing_credentials_and_symlinks_are_not_replaced(tmp_path):
    root = tmp_path / 'runtime'
    cloudflared = tmp_path / 'cloudflared'
    cloudflared.write_text('synthetic executable')
    cloudflared.chmod(0o700)
    setup.prepare_runtime(root, cloudflared=cloudflared,
                          install_receiver=fake_installer)
    secret = root / '.pebble-receiver.json'
    original = secret.read_bytes()
    value = json.loads(original)
    value['data_dir'] = str(tmp_path / 'wrong')
    secret.write_text(json.dumps(value))
    with pytest.raises(setup.SetupError, match='existing-state-mismatch'):
        setup.prepare_runtime(root, cloudflared=cloudflared,
                              install_receiver=fake_installer)
    assert json.loads(secret.read_text())['data_dir'] == str(tmp_path / 'wrong')
    secret.unlink()
    secret.symlink_to(tmp_path / 'wrong')
    with pytest.raises(setup.SetupError, match='unsafe-state-path'):
        setup.prepare_runtime(root, cloudflared=cloudflared,
                              install_receiver=fake_installer)
    assert (tmp_path / 'wrong').exists() is False


def test_runtime_root_cannot_overlap_package(tmp_path):
    cloudflared = tmp_path / 'cloudflared'
    cloudflared.write_text('synthetic executable')
    cloudflared.chmod(0o700)
    package_root = Path(setup.__file__).resolve().parents[1]
    with pytest.raises(setup.SetupError, match='unsafe-state-path'):
        setup.prepare_runtime(package_root / 'data', cloudflared=cloudflared,
                              install_receiver=fake_installer)


def test_scope_validation_rejects_wildcards_and_non_snowflakes():
    with pytest.raises(setup.SetupError, match='invalid-scope'):
        setup.validate_scope('123456789012345678', '*', '323456789012345678')
    assert setup.validate_scope('123456789012345678', '223456789012345678',
                                '323456789012345678') == {
        'guild_id': '123456789012345678',
        'channel_id': '223456789012345678',
        'approver_id': '323456789012345678'}
