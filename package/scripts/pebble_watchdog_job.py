"""No-agent cron wrapper for a configured Pebble Ring ingress."""

import json
from pathlib import Path
import re
import subprocess
import sys


URL = re.compile(r'https://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.trycloudflare\.com/pebble')


def render(result: dict) -> str:
    if result.get('status') != 'ingress-verified':
        raise ValueError('unverified ingress')
    if result.get('ring_url_rebind') != 'required':
        return ''
    url = result.get('ingress_url')
    if type(url) is not str or URL.fullmatch(url) is None:
        raise ValueError('unverified ingress URL')
    return f'Pebble Ring URL changed: {url}. Update the app webhook.\n'


def main() -> int:
    try:
        cli = Path(sys.executable).with_name('hermes')
        completed = subprocess.run([str(cli), 'pebble', 'recover'],
                                   capture_output=True, text=True, timeout=180,
                                   check=False)
        if completed.returncode != 0:
            raise ValueError('recovery failed')
        output = render(json.loads(completed.stdout))
        if output:
            sys.stdout.write(output)
        return 0
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        sys.stderr.write('Pebble ingress check failed; review hermes pebble status.\n')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
