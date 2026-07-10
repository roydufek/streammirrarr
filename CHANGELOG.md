# Changelog

## v0.5.0 — 2026-07-10T17:30:00Z
- **Channel-drop safeguard.** Dispatcharr auto-deletes auto-created channels when
  a provider M3U refresh comes back empty — a transient provider 404 during the
  nightly refresh can wipe thousands of channels in one sync (streams get a stale
  grace period; channels do not). Streammirrarr now tracks the primary account's
  channel count between runs and, if it collapses (drops to ≤50% of the baseline,
  above a 50-channel floor):
  - **fires a HIGH-priority Gotify alert** (even when notifications are set to Off),
    with recovery steps — a heads-up so you find out in minutes, not days;
  - **skips the scheduled duplicate cleanup** and **refuses a manual dedup**, so the
    plugin never deletes channels during the churn of a half-recreated state;
  - surfaces the count + baseline in "View last results" and the run report.
  New **"Channel-drop safeguard"** setting (on by default). The plugin still never
  deletes channels itself — this only detects Dispatcharr's own behavior and warns.
  Diagnosed from a real incident: a trex-3 provider 404 at 00:29 UTC made
  Dispatcharr's auto channel sync delete all 12,074 channels; recovery was a single
  account refresh.

## v0.4.4 — 2026-07-09T20:20:00Z
- Backups (`backup_channelstream_*.json`, `backup_deleted_channels_*.json`) now
  write to a persistent **`streammirrarr-backups/`** dir beside the plugins folder
  (Dispatcharr's bind-mounted `/data`) instead of inside the plugin folder — so they
  **survive plugin updates** (the repo-managed install atomic-swaps the plugin folder
  and was wiping them). Still rotate, keeping the last 7. Falls back to the plugin
  dir if that location isn't writable.

## v0.4.3 — 2026-07-09T20:00:00Z
- Fold channel-profile selection into the **"Which channels to process"** dropdown
  (options now include `Profile: <name>` per profile) and remove the separate
  `channel_profile` select. That second dropdown was dynamically populated, and
  Dispatcharr's settings form persisted it as `null` when left untouched — which
  made `profile` scope ambiguous (and once crashed scheduled runs). One select
  can't go null. Old `profile` + `channel_profile` configs still work
  (backward-compatible).

## v0.4.2 — 2026-07-09T19:30:00Z
- Fix scheduler-greenlet leak: `_ensure_scheduler` now checks live threads by name
  (survives module re-import on reload) instead of a class attr that reset on every
  reload, which spawned a new scheduler greenlet each time.
- Scheduler tick no longer calls `self.fields` (which ran a ~12k-row Channel
  aggregate for primary-account auto-detect in every worker every 30s). It now
  merges static defaults, so the tick is a single small `PluginConfig` lookup.
  Both reduce needless per-worker DB load under uWSGI+gevent.

## v0.4.1 — 2026-07-09T18:45:00Z
- Fix: manual actions (Preview/Run/dedup) now run **synchronously** and return the
  real result, instead of spawning a background thread. On Dispatcharr's
  uWSGI+gevent server a greenlet spawned from the request handler and left
  detached isn't reliably scheduled after the response is sent, so the job never
  ran (no status written, nothing changed). Jobs take seconds and uWSGI
  http-timeout is 600s, so inline execution is safe. The daily scheduler
  (a long-lived greenlet) was unaffected and still works.

## v0.4.0 — 2026-07-02T00:00:00Z
- **Duplicate-channel cleanup.** Dispatcharr's auto-sync creates duplicate
  channels when a provider's 24/7/event feeds rotate their stream_id (several
  channels end up sharing the same primary stream). New:
  - **Preview duplicate cleanup** action — reports duplicates, deletes nothing.
  - **Remove duplicate channels** action (confirm-required) — for each primary
    stream on >1 channel, keeps one (EPG-bound, else lowest id) and deletes the
    rest. Only touches true duplicates (same stream_id); distinct feeds with the
    same name are left alone. A backup of deleted channels is written first
    (`backup_deleted_channels_*.json`, last 7 kept).
  - **"Also remove duplicate channels after the daily run"** setting (off by
    default) to keep them from accumulating.

## v0.3.0 — 2026-07-01T18:30:00Z
- Matching now anchors each channel on **its own attached primary-account stream
  (by `stream_id`)** instead of by channel name. Name matching collapsed
  duplicate-named channels onto one shared stream, creating redundant "zombie"
  channels and blocking Dispatcharr's auto-sync from pruning stale ones (channel
  count drifted above the stream count). stream_id anchoring keeps every channel
  mapped to its own distinct stream; the plugin now only ever *adds* failovers and
  never reassigns the primary. Removes the fuzzy/name code entirely.
- To reset an instance already affected by the old behavior: delete all channels,
  re-sync the source account, then run this plugin.

## v0.2.4 — 2026-07-01T17:30:00Z
- Fix the actual root cause behind the v0.2.3 crash: the Dispatcharr UI saved
  `channel_profile` as JSON `null`, and the scheduled-settings merge let that
  null clobber the field's default. Now non-None saved values override defaults;
  a null falls back to the default. (v0.2.3's single-profile fallback stays as a
  second line of defense.)

## v0.2.3 — 2026-07-01T16:00:00Z
- Fix: `profile` channel-scope no longer fails the run when the saved profile
  value arrives blank (a select-serialization quirk). If exactly one channel
  profile exists it's used automatically; otherwise a clear error names the
  available profiles. Fixes daily scheduled-run failures with
  `ValueError: Channel scope is 'profile' but no profile is selected`.

## v0.2.2 — 2026-06-30T22:00:00Z
- Release manifests (`manifest.json`, `plugin-manifest.json`) are now
  GPG-signed in CI, so Dispatcharr can verify them (the "verified" badge). The
  signing step is optional/guarded — skipped cleanly if no key is configured.

## v0.2.1 — 2026-06-30T19:40:00Z
- Gotify config split into separate **server URL** + **app token** fields
  (clearer for general users); the message URL is assembled internally. The old
  single `gotify_url` still works as a fallback.

## v0.2.0 — 2026-06-30T19:30:00Z
Hardening pass for unattended daily operation:
- **Cross-process file lock** guards every run (manual + scheduled) across all
  gunicorn workers, with stale-lock auto-recovery; "Clear lock" clears it.
- **Scheduled run retries on failure** within a 6h window (15-min cooldown),
  and writes a success marker only after success — a failed run self-heals.
- **Skip dead/stale streams** (`is_stale`) so failovers are never attached to
  streams the provider dropped. New toggle, default on.
- **Auto rotating pre-run backup** of managed ChannelStream rows (keeps last 7)
  before any real write — every run is one-command reversible.
- **Channel scope** selectable: all channels / auto-created only / a named
  channel profile (+ optional group filter).
- **Gotify notifications** for scheduled runs (off / on-failure / on-completion).
- Run summary logged to the container log for a full audit trail.
- Added LICENSE (MIT) and a logo.

## v0.1.1 — 2026-06-30T19:10:00Z
- Log on plugin init and scheduler-thread start (observability for the unattended
  daily run, parity with other Dispatcharr plugins).

## v0.1.0 — 2026-06-30T18:55:00Z
Initial build. Exact-match stream consolidation plugin for Dispatcharr:
- Anchors each channel to its source-of-truth account stream by exact name,
  fans out to failover accounts by provider `stream_id`.
- Preview (dry-run) and Run actions; idempotent reconcile (adds exact matches,
  removes mismatched managed streams, re-promotes the primary to order 0).
- Background-threaded execution with progress + status file, cancel via `stop()`.
- Optional daily UTC schedule with multi-worker-safe file locking.
- No external dependencies.
