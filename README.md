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

Copy this folder to `…/dispatcharr/data/plugins/streammirrarr/`, then in
Dispatcharr go to **Plugins → reload**, enable Streammirrarr, and configure:

- **Primary (source-of-truth) account** — auto-detected from `auto_created_by`;
  the account whose auto-sync owns your channel list.
- **Failover accounts** — comma-separated, priority order.
- **Limit to channel groups** — optional.
- **Remove mismatched managed streams** — leave on for cleanup.
- **Daily run time (HH:MM, UTC)** — blank disables the schedule.

## Actions

- **Preview (dry-run)** — reports exactly what would change, writes nothing.
- **Run exact match now** — performs the reconcile.
- **View last results** — the report from the most recent run.
- **Clear operation lock** — recover from an interrupted run.

## Notes

- The container runs in UTC; the schedule time is UTC.
- No external dependencies (pure stdlib + Django ORM).
