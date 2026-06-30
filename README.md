# Streammirrarr

Exact-match stream consolidation for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).

## What it does

When you add the same IPTV provider as several M3U accounts (each capped at one
connection) so multiple people can stream at once, one account auto-syncs the
channel list and the others are pure duplicates. Streammirrarr attaches those
duplicate streams onto the existing channels as **failover** streams, so
Dispatcharr can fail over between accounts when a connection is in use.

It does this by **exact matching**, not fuzzy matching:

- The channel's identity is its **name**.
- For each channel it finds the **primary** account's stream whose name exactly
  matches → that stream's provider `stream_id` is the channel's key.
- It fans that `stream_id` out to each **failover** account to find the twin
  stream, attaching them as failover (order 1, 2, …) with the primary at order 0.
- Mismatched streams from managed accounts are removed (cleans up prior fuzzy
  over-matching). Streams from other accounts are never touched. A zero-match
  never wipes a channel.

Because every account is the same provider feed, matching is a dictionary
lookup — it runs in **seconds**, not hours, and is safe to run daily.

## Install

**Via plugin repo (recommended):** In Dispatcharr go to **Plugins → Repos → Add
repo** and paste:

```
https://raw.githubusercontent.com/roydufek/streammirrarr/main/manifest.json
```

Then find Streammirrarr in the available plugins and click install. Updates show
up automatically when a new version is published.

**Manual zip upload:** Download a release zip (or run `bash build.sh`) and use
**Plugins → Import** to upload it.

**Manual copy:** Copy this folder to `…/dispatcharr/data/plugins/streammirrarr/`,
then **Plugins → reload**.

After installing, enable Streammirrarr and configure:

- **Primary (source-of-truth) account** — auto-detected from `auto_created_by`;
  the account whose auto-sync owns your channel list.
- **Failover accounts** — comma-separated, priority order.
- **Limit to channel groups** — optional.
- **Remove mismatched managed streams** — leave on for cleanup.
- **Daily run time (HH:MM, UTC)** — blank disables the schedule.
- **Gotify** — optional notifications for scheduled runs (off / on-failure /
  on-completion). Set your **Gotify server URL** and **app token** to enable.

## Actions

- **Preview (dry-run)** — reports exactly what would change, writes nothing.
- **Run exact match now** — performs the reconcile.
- **View last results** — the report from the most recent run.
- **Clear operation lock** — recover from an interrupted run.

## Notes

- The container runs in UTC; the schedule time is UTC.
- No external dependencies (pure stdlib + Django ORM).

## Publishing a new version

1. Bump `__version__` in `plugin.py` and `version` in `plugin.json`, add a
   `CHANGELOG.md` entry, commit.
2. `git push github main` then `git tag vX.Y.Z && git push github vX.Y.Z`.
3. The `release` workflow builds `streammirrarr-X.Y.Z.zip`, computes its sha256,
   refreshes `manifest.json` + `plugin-manifest.json`, and creates the GitHub
   Release. Dispatcharr instances see the update on their next repo refresh.

The tag (`vX.Y.Z`) must match the version in `plugin.json`, or the workflow fails.
