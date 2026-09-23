"""The published package manifest covers the reviewed runtime payload."""
import hashlib
import json
from pathlib import Path
import re


def test_manifest_is_accepted_by_hermes_0212_installer():
    root = Path(__file__).resolve().parents[1] / 'package'
    assert re.search(r'^manifest_version: 1$',
                     (root / 'plugin.yaml').read_text(), re.MULTILINE)


def test_manifest_hashes_match_runtime_files():
    root = Path(__file__).resolve().parents[1] / 'package'
    manifest = json.loads((root / 'manifest.json').read_text())['sha256']
    for relative, digest in manifest.items():
        path = root / relative
        assert path.is_file(), relative
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, relative
    runtime = {str(path.relative_to(root)) for path in root.rglob('*')
               if path.is_file() and '__pycache__' not in path.parts
               and path.name not in {'README.md', 'LICENSE', 'manifest.json'}}
    assert runtime == set(manifest)
