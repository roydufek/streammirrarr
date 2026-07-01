# Changelog

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
