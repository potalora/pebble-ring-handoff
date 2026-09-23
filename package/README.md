# Pebble Ring handoff for Hermes

This plugin receives Pebble Ring captures and posts them in a private Discord channel. One configured person can approve a capture for a Hermes turn or reject it. Approval applies to that capture once. A typed message in the Ring channel starts a separate thread.

The receiver stores recordings locally. Setup generates a Bearer token for the Ring app. Hermes's Discord bot posts the cards and replies.

## Set up

You need Hermes Agent 0.21.2 or newer with Discord enabled, a private regular Discord channel where the bot can post and create public threads, and `cloudflared` installed on Linux. The plugin finds `cloudflared` at `/opt/data/bin/cloudflared`, `/usr/local/bin/cloudflared`, or `/usr/bin/cloudflared`. The Quick Tunnel needs no Cloudflare account. Its URL can change after recovery.

1. Install the reviewed package and enable it:

   ```sh
   hermes plugins install https://github.com/potalora/pebble-ring-handoff#package --ref <full-40-character-commit-sha>
   hermes plugins enable pebble-ring-handoff
   ```

   Once the catalog entry is merged, `hermes plugins install pebble-ring-handoff` can replace the first command.

2. Run setup with the Discord guild, channel, and approver user IDs:

   ```sh
   hermes pebble setup --guild-id <guild-id> --channel-id <ring-channel-id> --approver-id <user-id>
   ```

   Add `--runtime-root /absolute/path` to keep receiver state somewhere other than `$HERMES_HOME/pebble-ring-webhook` (or `~/.hermes/pebble-ring-webhook` when `HERMES_HOME` is unset).

   Setup creates an owner-only receiver directory, generates its Bearer token, installs the receiver's hash-checked Python dependencies in a separate virtual environment, initializes an empty database, saves the Hermes plugin settings, starts and checks one Quick Tunnel, and creates one five-minute no-agent watchdog job. Running setup again preserves the token, database, and existing job. It never approves or replays a capture.

3. Restart the Hermes gateway with your host's service manager so it loads the new settings. Copy the setup command's webhook URL into the Pebble app gesture. Read `webhook_token` from the owner-only `.pebble-receiver.json` path shown by setup and use it as the app's `Authorization: Bearer ...` header. Keep the gesture's multipart audio and transcript fields. Do not paste the token into chat or Git.

4. Send a harmless physical capture. Check that one card appears in the Ring channel, approve it once, and confirm one response in its original thread. Then send a typed message in the parent channel and confirm that it starts a new thread.

Use `hermes pebble status` for a read-only check. A fresh `observed-ready` result confirms recent worker health and owned receiver and tunnel state; the physical test confirms delivery from the app. The watchdog sends a notice to the configured Discord channel if recovery changes the URL. Update the app URL when that happens.

## Managed Hermes configuration

On a managed Hermes install, setup leaves `config.yaml` to the host's configuration owner and reports `managed-manual`. Add this mapping through that owner's normal workflow, restart the gateway, then rerun setup to finish the watchdog step:

```yaml
plugins:
  entries:
    pebble-ring-handoff:
      settings:
        data_dir: <runtime-root>/data
        guild_id: "<guild-id>"
        channel_id: "<ring-channel-id>"
        approver_id: "<user-id>"
        audio_retention_ms: 604800000
```

If setup reports `cloudflared-unavailable`, install `cloudflared` in one of the paths above and rerun it. If it reports `ingress-unverified` or a manual watchdog step, check `hermes pebble status` and the host's service and cron setup before changing the Ring app URL.

## Operation and updates

The plugin checks the exact Discord guild, channel, approver, message, and durable approval state before one dispatch. A restart does not extend approval or retry an uncertain thread creation or model turn. Audio retrieval requires a separate explicit request for that capture. Recordings, database, token, process state, and the receiver virtual environment stay outside the replaceable plugin directory.

`hermes pebble recover` may stop and restart owned receiver processes and replace the Quick Tunnel. Read [`skills/pebble/SKILL.md`](skills/pebble/SKILL.md) before maintenance. A plugin update needs a reviewed commit pin, a receiver backup, a paused watchdog, a verified `hermes pebble stop-ingress`, a controlled gateway restart, and one recovery. The plugin does not update its own code.

## Development

Run `hermes plugins validate .` and the synthetic test suite before a release. The public repository keeps runtime data and credentials out of Git. Each catalog update pins a new reviewed commit; see the [Hermes catalog policy](https://github.com/NousResearch/hermes-agent/blob/main/plugin-catalog/README.md).

MIT. See [`LICENSE`](LICENSE).
