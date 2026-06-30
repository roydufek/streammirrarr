# Changelog

## v0.1.0 — 2026-06-30T18:55:00Z
Initial build. Exact-match stream consolidation plugin for Dispatcharr:
- Anchors each channel to its source-of-truth account stream by exact name,
  fans out to failover accounts by provider `stream_id`.
- Preview (dry-run) and Run actions; idempotent reconcile (adds exact matches,
  removes mismatched managed streams, re-promotes the primary to order 0).
- Background-threaded execution with progress + status file, cancel via `stop()`.
- Optional daily UTC schedule with multi-worker-safe file locking.
- No external dependencies.
