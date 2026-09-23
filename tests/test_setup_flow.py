"""Guided setup writes one Hermes setting and one durable watchdog job."""

import argparse
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace as NS

import pytest

from pebble_bridge import management, plugin, setup


SCOPE = dict(guild_id='323456789012345678', channel_id='223456789012345678',
             approver_id='123456789012345678')


def test_unconfigured_plugin_exposes_setup_without_wiring_discord(monkeypatch, capsys):
    commands, handlers = {}, {}
    ctx = NS(get_config=lambda _key: None,
             register_cli_command=lambda **kwargs: commands.update({kwargs['name']: kwargs}),
             register_tool=lambda **_kwargs: None,
             register_skill=lambda **_kwargs: None,
             register_hook=lambda *_args: None,
             register_platform_handler=lambda *args: handlers.update({args[0]: args[1]}))
    monkeypatch.setattr(setup, 'guided_setup', lambda **_kwargs: {
        'status': 'setup-prepared', 'config': 'written', 'watchdog': 'created',
        'ingress_url': 'https://synthetic.trycloudflare.com/pebble',
        'token_file': '/tmp/synthetic/.pebble-receiver.json'})
    plugin.register(ctx)
    parser = argparse.ArgumentParser()
    commands['pebble']['setup_fn'](parser)
    args = parser.parse_args(['setup', '--guild-id', SCOPE['guild_id'],
                              '--channel-id', SCOPE['channel_id'],
                              '--approver-id', SCOPE['approver_id']])
    assert commands['pebble']['handler_fn'](args) == 0
    output = capsys.readouterr().out
    assert 'synthetic.trycloudflare.com/pebble' in output
    assert '.pebble-receiver.json' in output
    assert 'webhook_token' not in output
    assert not handlers


def test_one_atomic_hermes_config_write_and_managed_fallback(monkeypatch, capsys):
    module = ModuleType('hermes_cli.config')
    calls = []
    module.is_managed = lambda: False
    module.set_config_value = lambda key, value, force=False: (
        calls.append((key, json.loads(value), force)), print('synthetic private config'))
    package = ModuleType('hermes_cli')
    package.config = module
    monkeypatch.setitem(sys.modules, 'hermes_cli', package)
    monkeypatch.setitem(sys.modules, 'hermes_cli.config', module)
    settings = dict(data_dir='/tmp/synthetic/data', **SCOPE,
                    audio_retention_ms=604800000)
    assert setup.configure_hermes(settings) == 'written'
    assert calls == [('plugins.entries.pebble-ring-handoff.settings', settings, False)]
    assert capsys.readouterr().out == ''
    module.is_managed = lambda: True
    assert setup.configure_hermes(settings) == 'managed-manual'
    assert len(calls) == 1


def test_watchdog_schedule_is_idempotent(tmp_path):
    home = tmp_path / 'hermes'
    home.mkdir()
    (home / 'cron').mkdir()
    jobs = home / 'cron' / 'jobs.json'
    calls = []

    def create(command):
        calls.append(command)
        jobs.write_text(json.dumps({'jobs': [{
            'id': 'synthetic', 'name': 'Pebble Ring ingress watchdog',
            'script': 'pebble-ring-handoff-watchdog.py', 'enabled': True,
            'no_agent': True,
            'schedule': {'kind': 'interval', 'minutes': 5, 'display': 'every 5m'},
            'deliver': f"discord:{SCOPE['channel_id']}",
            'failure_deliver': f"discord:{SCOPE['channel_id']}"}]}))

    assert setup.schedule_watchdog(home, channel_id=SCOPE['channel_id'],
                                   create_job=create) == 'created'
    assert setup.schedule_watchdog(home, channel_id=SCOPE['channel_id'],
                                   create_job=create) == 'existing'
    assert len(calls) == 1
    assert (home / 'scripts' / 'pebble-ring-handoff-watchdog.py').is_file()
    assert '--no-agent' in calls[0] and '--deliver' in calls[0]
    assert f"discord:{SCOPE['channel_id']}" in calls[0]


def test_watchdog_does_not_adopt_wrong_delivery(tmp_path):
    home = tmp_path / 'hermes'
    home.mkdir()
    (home / 'cron').mkdir()
    jobs = home / 'cron' / 'jobs.json'
    jobs.write_text(json.dumps({'jobs': [{
        'name': 'Pebble Ring ingress watchdog',
        'script': 'pebble-ring-handoff-watchdog.py', 'enabled': True,
        'no_agent': True,
        'schedule': {'kind': 'interval', 'minutes': 5, 'display': 'every 5m'},
        'deliver': 'discord:999999999999999999',
        'failure_deliver': 'discord:999999999999999999'}]}))
    with pytest.raises(setup.SetupError, match='watchdog-job-conflict'):
        setup.schedule_watchdog(home, channel_id=SCOPE['channel_id'])


def test_watchdog_script_reports_only_changed_url():
    from scripts import pebble_watchdog_job
    unchanged = {'status': 'ingress-verified', 'ring_url_rebind': 'unknown'}
    changed = {'status': 'ingress-verified', 'ring_url_rebind': 'required',
               'ingress_url': 'https://synthetic.trycloudflare.com/pebble'}
    assert pebble_watchdog_job.render(unchanged) == ''
    assert pebble_watchdog_job.render(changed) == (
        'Pebble Ring URL changed: https://synthetic.trycloudflare.com/pebble. '
        'Update the app webhook.\n')


def test_managed_config_leaves_scheduling_to_operator(monkeypatch, tmp_path):
    root = tmp_path / 'runtime'
    token = root / '.pebble-receiver.json'
    monkeypatch.setattr(setup, 'prepare_runtime', lambda _root: {
        'data_dir': str(root / 'data'), 'token_file': str(token)})
    monkeypatch.setattr(setup, 'configure_hermes', lambda _settings: 'managed-manual')
    monkeypatch.setattr(management, 'recover', lambda *_args: {
        'status': 'ingress-verified',
        'ingress_url': 'https://synthetic.trycloudflare.com/pebble'})
    monkeypatch.setattr(setup, 'schedule_watchdog',
                        lambda _home: pytest.fail('managed config scheduled a job'))
    result = setup.guided_setup(**SCOPE, runtime_root=root)
    assert result['status'] == 'setup-needs-operator'
    assert result['config'] == 'managed-manual'
    assert result['watchdog'] == 'manual'


def test_existing_scope_mismatch_stops_before_runtime_change(monkeypatch, tmp_path):
    monkeypatch.setattr(setup, 'prepare_runtime',
                        lambda _root: pytest.fail('changed configured runtime'))
    with pytest.raises(setup.SetupError, match='configured-scope-mismatch'):
        setup.guided_setup(**SCOPE, runtime_root=tmp_path / 'new',
                           existing_data_dir=tmp_path / 'old' / 'data',
                           existing_scope=SCOPE)


def test_repeat_setup_displays_current_verified_ingress_url(monkeypatch, tmp_path):
    root = tmp_path / 'runtime'
    monkeypatch.setattr(setup, 'prepare_runtime', lambda _root: {
        'data_dir': str(root / 'data'),
        'token_file': str(root / '.pebble-receiver.json')})
    monkeypatch.setattr(setup, 'configure_hermes', lambda _settings: 'written')
    monkeypatch.setattr(management, 'recover', lambda *_args: {
        'status': 'ingress-verified', 'ring_url_rebind': 'unknown'})
    monkeypatch.setattr(management, 'status', lambda *_args: {
        'ingress_url': 'https://current.trycloudflare.com/pebble'})
    monkeypatch.setattr(setup, 'schedule_watchdog', lambda *_args, **_kwargs: 'existing')
    result = setup.guided_setup(**SCOPE, runtime_root=root)
    assert result['status'] == 'setup-prepared'
    assert result['ingress_url'] == 'https://current.trycloudflare.com/pebble'
