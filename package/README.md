# Pebble Ring handoff for Hermes

This Hermes plugin receives Pebble Ring captures through a separate local receiver and posts each transcript to a private Discord channel. A configured human can approve one capture for a normal Hermes turn or reject it. Approval has a 30-minute deadline and does not authorize a later or different capture. Audio playback and local audio retrieval have separate controls.

The plugin also routes authenticated typed messages in the Ring channel into Discord threads. A direct reply to an expired capture can use that capture's transcript as context without running it again. Failed or uncertain capture execution requires manual review.

## Status

This repository contains the Hermes plugin, receiver, and Quick Tunnel controller. Setup is manual on Linux. The receiver needs its own Python environment, a Discord webhook, a Cloudflare Quick Tunnel, and protected runtime files. Keep all state and secrets outside the installed plugin directory. The plugin does not create Discord channels, webhooks, a Cloudflare account, or Pebble app settings.

## Requirements

- Hermes Agent 0.21.2 or newer with its Discord platform enabled.
- A private regular Discord channel, a channel-owned incoming webhook, and the Hermes bot in the same guild. The configured approver must be one exact Discord user ID.
- A Linux host that can run the receiver and `cloudflared`. The current controller expects `cloudflared` at `/opt/data/bin/cloudflared` and serves the receiver at `127.0.0.1:8765`.
- Python 3.11 or newer for the receiver. Install its exact, hash-checked dependencies from `receiver-requirements.txt` in a separate virtual environment. Hermes provides the native plugin's Discord dependencies.
- A Pebble Ring app gesture configured for multipart audio and transcript delivery, with a Bearer authorization header. The Quick Tunnel URL may change after recovery and must then be saved in the app.

The receiver and controller are designed for one operator scope and one owned Quick Tunnel. They do not manage a shared multi-user Ring service. Review the file ownership and process controls before installing on a different host layout.

## Install and configure

1. Create a private runtime root outside Hermes's plugin directory, such as `/opt/data/pebble-ring-webhook`, owned by the account that runs the receiver. Set the root, its `data` directory, and `.pebble-watchdog-runtime` to mode `0700`. Keep the receiver SQLite database and virtual environment there.
2. Create a separate receiver virtual environment at `<runtime-root>/.venv`. Install `receiver-requirements.txt` with hash checking using `uv pip sync --require-hashes --python <runtime-root>/.venv/bin/python receiver-requirements.txt`. Review dependency changes before updating the lock.
3. Put `cloudflared` at `/opt/data/bin/cloudflared`. Provide `<runtime-root>/.pebble-receiver.json` as an owner-only `0600` file with exactly `data_dir`, `webhook_token`, and `port`. The token must be a 32–128 character base64url value and the port must be `8765`. Do not put the token in Hermes config.
4. Provide `<runtime-root>/.pebble-runtime.env` as an owner-only `0600` file containing exactly one line, `PEBBLE_ALLOWED_HOSTS=<hostname>.trycloudflare.com`. On a new install, use `bootstrap.trycloudflare.com` as an initial placeholder. The controller replaces it with the verified tunnel hostname before starting the receiver. Create `<runtime-root>/.pebble-watchdog-runtime/watchdog.lock` as an owner-only `0600` regular file.
5. Install this repository at a reviewed commit with `hermes plugins install https://github.com/potalora/pebble-ring-handoff#package --ref <full-40-character-sha>`. The plugin does not install or start the receiver environment. Set these values under `plugins.entries.pebble-ring-handoff.settings` in Hermes `config.yaml`:

   ```yaml
   data_dir: /opt/data/pebble-ring-webhook/data
   guild_id: "<Discord guild ID>"
   channel_id: "<Ring channel ID>"
   approver_id: "<approver user ID>"
   audio_retention_ms: 604800000
   ```

6. Initialize an empty receiver database in `<runtime-root>/data` with `pebble_bridge.repository.Repository` from the installed package, then close the connection. The controller requires this database before its first run. For the example paths above, run:

   ```sh
   PYTHONPATH="$HOME/.hermes/plugins/pebble-ring-handoff" /opt/data/pebble-ring-webhook/.venv/bin/python -c 'from pathlib import Path; from pebble_bridge.repository import Repository; Repository(Path("/opt/data/pebble-ring-webhook/data")).connection.close()'
   ```

7. Use your host supervisor to run the Quick Tunnel controller on a schedule; it starts the owned receiver and tunnel. Its CLI is `python scripts/pebble_quick_tunnel_watchdog.py --state-root <runtime-root>`. Keep the watchdog schedule, Hermes gateway lifecycle, and backups under host management. Run `hermes pebble status` to verify observed ownership and worker freshness, then put its current ingress URL into the Pebble app webhook setting.
8. Send a harmless physical capture. Check that one card appears in the configured Discord channel, approve it once, and confirm one response in its original thread. Test a normal typed message separately. Do not use an old expired card as the first delivery check.

The setup above describes the package's current path and ownership checks. A different filesystem layout needs code and test changes before use. Do not copy a running database or process manifest between hosts as setup.

## Safety behavior

The receiver validates the Bearer token, host, payload type, size, and capture identity before storing a capture. It cannot call a model or Hermes tools. The Hermes plugin checks the exact Discord guild, channel, approver, message, and durable approval state before one dispatch. A restart does not extend approval or automatically retry an uncertain thread creation or dispatch. The audio tool returns a local recording only after a separate authenticated request for that capture.

`hermes pebble recover` may stop and restart owned receiver processes and replace the Quick Tunnel. It can change the public URL. Use `hermes pebble status` for read-only checks. Read [`skills/pebble/SKILL.md`](skills/pebble/SKILL.md) before maintenance.

## Development

Run `hermes plugins validate .` against the package and use the included synthetic tests before a release. Never commit runtime roots, databases, recordings, `.env` files, process manifests, credentials, or user captures. The catalog pins an exact Git commit, so each update requires a new reviewed pin.

## Catalog submission

The Hermes plugin catalog accepts community entries through a maintainer-reviewed pull request. The entry must point to a public release commit with a full 40-character SHA and accurately declare this plugin's tools and hook. See the [Hermes catalog policy](https://github.com/NousResearch/hermes-agent/blob/main/plugin-catalog/README.md).

## License

MIT. See [`LICENSE`](LICENSE).
