"""Small native CLI wrapper around existing Pebble observations and recovery.

Status is metadata only. Recovery belongs to the existing watchdog and its lock;
only an explicit terminal command invokes it. Neither path proves a Ring turn.
"""
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

from . import worker_health

WATCHDOG = 'pebble_quick_tunnel_watchdog.py'
CODE_ROOT = Path(__file__).resolve().parents[1]


def external_state_path(path):
    """Reject state in the replaceable code tree, including resolved aliases."""
    if not isinstance(path, Path) or not path.is_absolute():
        return False
    def overlap(left, right):
        return left.is_relative_to(right) or right.is_relative_to(left)
    try:
        lexical_code = Path(__file__).absolute().parents[1]
        return (not overlap(path, CODE_ROOT) and not overlap(path, lexical_code)
                and not overlap(path.resolve(), CODE_ROOT.resolve()))
    except (OSError, RuntimeError):
        return False


def _configured(data_dir, scope):
    return (isinstance(data_dir, Path) and data_dir.is_absolute() and data_dir.name == 'data'
            and external_state_path(data_dir.parent) and external_state_path(data_dir)
            and type(scope) is dict and set(scope) == {'guild_id', 'channel_id', 'approver_id'}
            and all(type(value) is str and re.fullmatch(r'[0-9]{17,20}', value)
                    for value in scope.values()))


def _load_watchdog(data_dir):
    """Load only bundled controller code; durable state remains outside the plugin."""
    data_dir = Path(data_dir)
    if (not data_dir.is_absolute() or data_dir.name != 'data'
            or not external_state_path(data_dir.parent)):
        raise ValueError('runtime unavailable')
    root = data_dir.parent
    if root.resolve() != root or data_dir.is_symlink():
        raise ValueError('runtime unavailable')
    for path in (root, data_dir, CODE_ROOT, CODE_ROOT/'scripts'):
        metadata = path.lstat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o022):
            raise ValueError('runtime unavailable')
    source = None
    for name in (WATCHDOG, 'run_receiver.py', 'serve_receiver.py'):
        path = CODE_ROOT/'scripts'/name
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(fd)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                    or metadata.st_nlink != 1 or metadata.st_mode & 0o022):
                raise ValueError('runtime unavailable')
            if name == WATCHDOG:
                source = os.read(fd, 262145)
                if len(source) > 262144 or len(source) != metadata.st_size:
                    raise ValueError('runtime unavailable')
                after = os.fstat(fd)
                if (metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns) != (
                        after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise ValueError('runtime unavailable')
        finally:
            os.close(fd)
    # Execute the bytes read from the checked descriptor, not a second path open.
    name = '_pebble_management_watchdog_' + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(name, CODE_ROOT/'scripts'/WATCHDOG)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolves annotations through this slot.
    try:
        exec(compile(source, str(CODE_ROOT/'scripts'/WATCHDOG), 'exec'), module.__dict__)
    finally:
        if sys.modules.get(name) is module:
            sys.modules.pop(name, None)
    return module


def _quiet():
    # Existing helpers may raise with private context; never render it publicly.
    stack = contextlib.ExitStack()
    output = stack.enter_context(open(os.devnull, 'w'))
    stack.enter_context(contextlib.redirect_stdout(output))
    stack.enter_context(contextlib.redirect_stderr(output))
    return stack


def status(data_dir, scope):
    """Observe worker freshness and owned readiness without DB or HTTP access."""
    result = dict(status='unknown', worker='unknown', ingress='unknown',
                  end_to_end='unverified', ring_url_rebind='unknown',
                  ingress_url=None, ingress_generation=None,
                  unresolved_records='not-inspected', current_http_health='not-probed',
                  last_verified_age_seconds=None)
    if not _configured(data_dir, scope):
        return dict(result, status='configuration-unavailable')
    try:
        now = time.time()
        health = worker_health.read_health(data_dir, scope, int(now*1000))
        result['worker'] = health['status'] if health['status'] in {'ok', 'degraded', 'unknown'} else 'unknown'
        result['creation_stopped'] = health['creation_stopped']
        wd = _load_watchdog(data_dir)
        snapshot = wd.prepare_controller(wd.Paths.for_root(data_dir.parent))
        manifest, ready = snapshot.manifest, snapshot.readiness
        if (snapshot.database_present and manifest and ready
                and snapshot.bridge_class is wd.ProcessClass.OWNED
                and snapshot.supervisor_class is wd.ProcessClass.OWNED
                and snapshot.runtime_hostname == manifest.hostname == ready.hostname
                and ready.nonce == manifest.supervisor.nonce and ready.status == 'ready'):
            result['ingress'] = 'owned-readiness'
            result['ingress_url'] = f'https://{manifest.hostname}/pebble'
            result['ingress_generation'] = manifest.generation
            # The manifest is written on startup/recovery, not a heartbeat.
            if type(manifest.verified_at) is int and 0 <= manifest.verified_at <= now:
                result['last_verified_age_seconds'] = int(now-manifest.verified_at)
        result['status'] = ('observed-ready' if result['worker'] == 'ok'
                            and result['ingress'] == 'owned-readiness' else 'degraded')
    except BaseException:
        pass
    return result

def recover(data_dir, scope):
    """Invoke the existing controller once, verifying within its existing lock."""
    result = dict(status='recovery-unverified', recovery_performed=None,
                  ring_url_rebind='unknown', end_to_end='unverified')
    if not _configured(data_dir, scope):
        return dict(result, status='configuration-unavailable')
    try:
        with _quiet():
            wd = _load_watchdog(data_dir)
            paths = wd.Paths.for_root(data_dir.parent)
            actions = wd.controller_actions(paths)
            acquired = busy = verified = changed = False
            hostname = None

            def acquire(path):
                nonlocal acquired, busy
                lock = actions['acquire'](path)
                acquired, busy = lock is not None, lock is None
                return lock

            def healthy(snapshot):
                nonlocal verified
                verified = actions['healthy'](snapshot) is True
                return verified

            def bridge(snapshot):
                nonlocal changed
                done = actions['bridge_only'](snapshot)
                if done:
                    changed = True
                    if not healthy(actions['prepare'](paths)):
                        raise ValueError('recovery unverified')
                return done

            def full(snapshot):
                nonlocal changed, hostname
                hostname = actions['full'](snapshot)
                changed = True
                # Validate the only public runtime value before including it.
                wd.format_recovery_output(hostname)
                result.update(ring_url_rebind='required', ingress_url=f'https://{hostname}/pebble')
                current = actions['prepare'](paths)
                if current.runtime_hostname != hostname or not healthy(current):
                    raise ValueError('recovery unverified')
                return hostname

            wd.run_watchdog(paths, acquire=acquire, prepare=actions['prepare'],
                            healthy=healthy, bridge_only=bridge, full=full)
            if busy:
                result['status'] = 'busy'
            elif acquired and verified:
                result.update(status='ingress-verified', recovery_performed=changed)
                if hostname:
                    result.update(ring_url_rebind='required', ingress_url=f'https://{hostname}/pebble')
    except BaseException:
        pass
    return result


def stop_ingress(data_dir, scope):
    """Stop one owned generation once; preserve its evidence for update review."""
    result = dict(status='refused', stop_attempted=[],
                  groups_gone=dict(bridge=None, supervisor=None))
    if not _configured(data_dir, scope):
        return result
    try:
        with _quiet():
            wd = _load_watchdog(data_dir)
            paths = wd.Paths.for_root(data_dir.parent)
            lock = wd.try_acquire_watchdog_lock(paths.lock)
            if lock is None:
                return dict(result, status='busy')
            try:
                snapshot = wd.prepare_controller(paths)
                if snapshot.manifest is None:
                    return result
                records = ((snapshot.manifest.bridge, snapshot.bridge_class),
                           (snapshot.manifest.supervisor, snapshot.supervisor_class))
                if any(state not in (wd.ProcessClass.OWNED, wd.ProcessClass.GONE)
                       for _, state in records):
                    return result
                result['status'] = 'stop-unverified'
                for record, state in records:
                    if state is wd.ProcessClass.OWNED:
                        result['stop_attempted'].append(record.role)
                        wd.stop_process(paths, record)
                # A vanished leader may leave cloudflared or another child in
                # the original group. Observe the group; never signal it again.
                deadline = time.monotonic() + wd.CHILD_STOP_SECONDS
                while True:
                    for record, _ in records:
                        try:
                            os.killpg(record.pid, 0)
                        except ProcessLookupError:
                            result['groups_gone'][record.role] = True
                        else:
                            result['groups_gone'][record.role] = False
                    if all(result['groups_gone'].values()):
                        result['status'] = 'ingress-stopped'
                        return result
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return result
                    time.sleep(min(wd.STOP_POLL_SECONDS, remaining))
            finally:
                os.close(lock)
    except BaseException:
        pass
    return result


def register_management(ctx, data_dir, scope):
    """Register operator CLI and a read-only discovery tool after config validation."""
    def setup(parser):
        parser.add_argument('action', choices=('status', 'recover', 'stop-ingress'))

    def command(args):
        action = {'status': status, 'recover': recover, 'stop-ingress': stop_ingress}[args.action]
        result = action(data_dir, scope)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['status'] in {'observed-ready', 'ingress-verified', 'ingress-stopped'} else 1

    def tool(args, **_kwargs):
        if type(args) is not dict or args:
            return json.dumps({'status': 'invalid-arguments'})
        return json.dumps(status(data_dir, scope), sort_keys=True)

    ctx.register_cli_command(name='pebble', help='Observe Pebble, recover ingress, or stop owned ingress for an update',
                             setup_fn=setup, handler_fn=command)
    ctx.register_tool(name='pebble_status', toolset='pebble', handler=tool,
        schema={'name': 'pebble_status',
                'description': 'Read-only Pebble worker and ingress ownership/readiness metadata. '
                               'Does not prove a Ring-to-thread turn. For supported management, '
                               'read skill pebble-ring-handoff:pebble.',
                'parameters': {'type': 'object', 'properties': {}, 'additionalProperties': False}})
