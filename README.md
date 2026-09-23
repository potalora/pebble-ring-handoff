# Pebble Ring handoff for Hermes

The installable Hermes plugin is in [`package/`](package/). It receives Ring captures and asks one Discord user to approve each Hermes turn. The `hermes pebble setup` command prepares the local receiver, token, configuration, tunnel, and watchdog. Read the [setup guide](package/README.md) for the install command and app binding steps.

Development tests are in `tests/`. The plugin catalog pins a reviewed commit and uses `subdir: package`.
