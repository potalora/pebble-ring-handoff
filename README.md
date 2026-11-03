# Pebble Ring handoff for Hermes

The installable Hermes plugin is in [`package/`](package/). It receives Ring captures and asks one Discord user to approve each Hermes turn. The `hermes pebble setup` command prepares the local receiver, token, configuration, tunnel, and watchdog. Read the [setup guide](package/README.md) for the install command and app binding steps.

Development tests are in `tests/`. The plugin catalog pins a reviewed commit and uses `subdir: package`.

Version 3.0.5 supports Discord.py 2.7.1 with aiohttp 3.14.3 or 3.14.5. Other
SDK pairs fail before Ring workers start. Failed and uncertain captures keep
their existing recovery rules and do not retry after an update.

Run the transport regression in each supported SDK environment without network
access or credentials:

```sh
PYTHONPATH=package python -m unittest discover -s tests -p test_native_sdk_compatibility.py -v
```
