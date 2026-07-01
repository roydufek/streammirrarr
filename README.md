<p align="center">
  <img src="logo.png" alt="Streammirrarr" width="96" height="96" />
</p>

<h1 align="center">Streammirrarr</h1>

<p align="center">
  <strong>Exact-match stream consolidation plugin for Dispatcharr.</strong>
</p>

<p align="center">
  <a href="#what-it-does">What it does</a> · <a href="#install">Install</a> · <a href="#settings">Settings</a> · <a href="#actions">Actions</a> · <a href="#publishing-a-new-version">Publishing</a>
</p>

<p align="center">
  <img alt="License" src="https://img.shields.io/badge/license-MIT-blue" />
  <img alt="Python" src="https://img.shields.io/badge/python-3-3776AB?logo=python&logoColor=white" />
  <img alt="Dispatcharr" src="https://img.shields.io/badge/dispatcharr-plugin-1d9bf0?logo=plex&logoColor=white" />
  <img alt="Manifest" src="https://img.shields.io/badge/manifest-GPG--signed-brightgreen" />
</p>

---

## What it does

When you add the same IPTV provider as several M3U accounts (each capped at one
connection) so multiple people can stream at once, one account auto-syncs the
channel list and the others are pure duplicates. Streammirrarr attaches those
duplicate streams onto the existing channels as **failover** streams, so
Dispatcharr can fail over between accounts when a connection is in use.

It does this by **exact matching** on provider `stream_id`, not fuzzy matching:

- Each channel is anchored on the **primary-account stream already attached to
  it** (the one auto-sync created the channel from) — never on the channel name.
- It reads that stream's provider `stream_id` and fans it out to each **failover**
  account to find the twin stream, attaching them as failover (order 1, 2, …) with
  the primary at order 0.
- Other managed streams that aren't a match are removed. Streams from non-managed
  accounts are never touched. A channel with no primary stream is skipped (never
  wiped).

Anchoring on `stream_id` (not name) keeps every channel mapped to its own distinct
stream, so duplicate-named channels never collapse onto a shared stream and the
auto-sync can still prune stale channels. Matching is a dictionary lookup — it runs
in **seconds**, not hours, and is safe to run daily.

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

## Settings

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
