"""Read one private config, then exec the receive-only server with a minimal environment.

Invoke with the reviewed virtualenv Python and -I. The supervising parent must
also use an explicit clean spawn environment; this is not OS user isolation.
"""
import json
import os
from pathlib import Path
import re
import stat
import sys


def read_protected(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > 16384):
            raise ValueError('invalid config')
        data = os.read(fd, 16385)
        after = os.fstat(fd)
        stable = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_nlink',
                  'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if (len(data) != before.st_size
                or any(getattr(before, field) != getattr(after, field) for field in stable)):
            raise ValueError('changed config')
    finally:
        os.close(fd)
    return data


def load_config(path, runtime_path=None):
    data = read_protected(path)

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate config key')
            result[key] = value
        return result
    config = json.loads(data, object_pairs_hook=unique)
    keys = {'data_dir', 'webhook_token', 'port'}
    if runtime_path is None:
        keys.add('allowed_hosts')
    if set(config) != keys:
        raise ValueError('invalid config keys')
    if runtime_path is not None:
        runtime = read_protected(runtime_path).decode('ascii')
        match = re.fullmatch(r'PEBBLE_ALLOWED_HOSTS=([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.trycloudflare\.com)\n', runtime)
        if match is None or match[1] == 'api.trycloudflare.com':
            raise ValueError('invalid hostname trust anchor')
        config['allowed_hosts'] = [match[1]]
    if (not isinstance(config['data_dir'], str) or not Path(config['data_dir']).is_absolute()
            or not isinstance(config['webhook_token'], str)
            or re.fullmatch(r'[A-Za-z0-9_-]{32,128}', config['webhook_token']) is None
            or type(config['port']) is not int or not 1 <= config['port'] <= 65535
            or type(config['allowed_hosts']) is not list or not config['allowed_hosts']
            or len(config['allowed_hosts']) > 8):
        raise ValueError('invalid config values')
    for host in config['allowed_hosts']:
        if (not isinstance(host, str) or len(host) > 253
                or any(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) is None
                       for label in host.split('.'))):
            raise ValueError('invalid hostname')
    return config


def main():
    try:
        if len(sys.argv) not in (2, 3) or not sys.flags.isolated:
            raise ValueError('isolated Python invocation required')
        config = load_config(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else None)
        env = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
               'PEBBLE_DATA_DIR': config['data_dir'], 'PEBBLE_WEBHOOK_TOKEN': config['webhook_token'],
               'PEBBLE_ALLOWED_HOSTS': ','.join(config['allowed_hosts']), 'PORT': str(config['port'])}
        entry = Path(__file__).resolve().with_name('serve_receiver.py')
        os.execve(sys.executable, [sys.executable, '-I', '-B', str(entry)], env)
    except (OSError, ValueError, TypeError, KeyError):
        print('FAIL: receiver configuration or launch rejected', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
