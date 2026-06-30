"""
Streammirrarr — exact-match stream consolidation for Dispatcharr.

Problem this solves
-------------------
You add the same IPTV provider as several M3U accounts (each capped at one
connection) so multiple people can stream at once. One account auto-syncs the
channel list; the others are pure duplicates. To get failover you want the
duplicate streams attached to the existing channels.

Stream-Mapparr does this with fuzzy name matching, which is O(channels x streams)
edit-distance scoring — ~6 hours for ~11k streams, and it both over-matches
(wrong streams attached) and under-matches.

Because every account is the *same* provider feed, the duplicates are identical:
same stream name, same provider ``stream_id``. So matching collapses to a plain
dictionary lookup and runs in seconds.

How it matches
--------------
* The channel's identity is its **name** (the only stable channel attribute).
* For each channel we find the **primary** account stream whose name exactly
  equals the channel name -> that stream's ``stream_id`` is the channel's key.
* We fan out that ``stream_id`` to each **failover** account to find the twin
  stream, and attach them as failover (order 1, 2, ...) with the primary at
  order 0.
* Anything else from a managed account that isn't an exact match is removed
  (cleans up prior fuzzy mistakes). Streams from non-managed accounts are left
  untouched. A zero-match never wipes a channel.

No external dependencies — pure stdlib + Django ORM.
"""

import datetime
import glob
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request

from django.db import close_old_connections, transaction

from apps.channels.models import (
    Channel,
    ChannelProfile,
    ChannelProfileMembership,
    ChannelStream,
    Stream,
)
from apps.m3u.models import M3UAccount

try:
    from core.utils import send_websocket_update
except Exception:  # pragma: no cover - defensive: never block on websocket import
    def send_websocket_update(*_a, **_k):
        return None

__version__ = "0.2.0"

logger = logging.getLogger("plugins.streammirrarr")

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGIN_KEY = os.path.basename(PLUGIN_DIR).replace(" ", "_").lower()
STATUS_FILE = os.path.join(PLUGIN_DIR, "last_run.json")
SCHED_DIR = os.path.join(PLUGIN_DIR, ".sched")
RUN_LOCK = os.path.join(SCHED_DIR, "run.lock")
ATTEMPT_FILE = os.path.join(SCHED_DIR, "attempt.ts")
BACKUP_GLOB = os.path.join(PLUGIN_DIR, "backup_channelstream_*.json")

# Scheduler tuning.
SCHED_TICK_SECS = 30          # how often the scheduler thread wakes
SCHED_WINDOW_SECS = 6 * 3600  # keep retrying for this long after the target time
SCHED_COOLDOWN_SECS = 15 * 60  # minimum gap between scheduled attempts
LOCK_STALE_SECS = 30 * 60     # a run lock older than this (dead holder) is stolen
BACKUP_KEEP = 7               # rotating pre-run backups to retain


def _norm(name):
    """Normalize a stream/channel name for exact comparison.

    Collapses runs of whitespace and casefolds. Deliberately *not* fuzzy — same
    provider feed means names are byte-identical apart from trivial whitespace.
    """
    if not name:
        return ""
    return " ".join(str(name).split()).casefold()


def _now():
    # Container runs UTC; keep everything UTC and label it as such in the UI.
    # tz-aware -> naive UTC (avoids the deprecated datetime.utcnow()).
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def _iso(dt):
    return dt.replace(microsecond=0).isoformat() + "Z"


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True  # exists but owned by another uid — alive
    except ProcessLookupError:
        return False
    except OSError:
        return False
    except Exception:
        return True  # can't tell — assume alive (don't steal the lock)


class Plugin:
    name = "Streammirrarr"
    version = __version__
    description = (
        "Exact-match stream consolidation. Attaches duplicate streams from "
        "failover M3U accounts onto the channels created by your source-of-truth "
        "account, matching by exact name + provider stream_id. No fuzzy matching "
        "— runs in seconds, safe to run daily."
    )
    author = "Roy Dufek"

    _job_thread = None
    _cancel = threading.Event()
    _sched_thread = None
    _sched_stop = threading.Event()

    actions = [
        {
            "id": "preview",
            "label": "🔍 Preview (dry-run)",
            "description": "Compute exactly what would change and report it. Writes nothing.",
            "button_label": "Preview",
            "button_variant": "outline",
        },
        {
            "id": "run",
            "label": "▶️ Run exact match now",
            "description": "Reconcile failover streams onto channels.",
            "button_label": "Run now",
            "button_variant": "filled",
            "confirm": {
                "required": True,
                "title": "Run exact-match reconcile?",
                "message": (
                    "Rebuilds failover streams on matched channels: attaches exact "
                    "stream_id matches and removes mismatched managed streams. A "
                    "backup is taken first. Proceed?"
                ),
            },
        },
        {
            "id": "view_last",
            "label": "📄 View last results",
            "description": "Show the report from the most recent run.",
            "button_label": "View",
            "button_variant": "subtle",
        },
        {
            "id": "clear_lock",
            "label": "🧹 Clear operation lock",
            "description": "Force-clear a stuck run lock if a previous run was interrupted.",
            "button_label": "Clear lock",
            "button_variant": "subtle",
        },
    ]

    def __init__(self):
        try:
            self._ensure_scheduler()
        except Exception:
            logger.debug("scheduler start deferred", exc_info=True)
        logger.info("[Streammirrarr] v%s initialized", __version__)

    # ------------------------------------------------------------------ fields
    @property
    def fields(self):
        accounts = []
        profiles = []
        try:
            accounts = list(
                M3UAccount.objects.filter(is_active=True)
                .values("id", "name", "account_type")
                .order_by("name")
            )
        except Exception:
            logger.debug("could not list M3U accounts for fields", exc_info=True)
        try:
            profiles = list(ChannelProfile.objects.values("name").order_by("name"))
        except Exception:
            logger.debug("could not list channel profiles for fields", exc_info=True)

        names = [a["name"] for a in accounts]
        acct_options = [{"value": n, "label": n} for n in names]
        profile_options = [{"value": p["name"], "label": p["name"]} for p in profiles]

        primary_default = self._detect_primary_name(accounts)
        failover_default = ",".join(
            a["name"]
            for a in accounts
            if a["name"] != primary_default and a["account_type"] == "XC"
        )

        return [
            {
                "id": "primary_account",
                "label": "Primary (source-of-truth) account",
                "type": "select",
                "options": acct_options,
                "default": primary_default,
                "help_text": (
                    "The account whose auto-sync owns the channel list. Channels "
                    "anchor to this account's streams by exact name."
                ),
            },
            {
                "id": "failover_accounts",
                "label": "Failover accounts (comma-separated, priority order)",
                "type": "string",
                "default": failover_default,
                "placeholder": "trex-1,trex-2",
                "help_text": (
                    "Accounts to exact-match and attach as failover streams, in "
                    "priority order (first = order 1)."
                ),
            },
            {
                "id": "channel_scope",
                "label": "Which channels to process",
                "type": "select",
                "options": [
                    {"value": "all", "label": "All channels"},
                    {"value": "auto_created", "label": "Auto-created channels only"},
                    {"value": "profile", "label": "A specific channel profile"},
                ],
                "default": "all",
                "help_text": "Limit the channels this plugin touches.",
            },
            {
                "id": "channel_profile",
                "label": "Channel profile (when scope = profile)",
                "type": "select",
                "options": profile_options,
                "default": (profiles[0]["name"] if profiles else ""),
                "help_text": "Only used when 'Which channels' is set to a profile.",
            },
            {
                "id": "channel_groups",
                "label": "Further limit to channel groups (optional, comma-separated)",
                "type": "string",
                "default": "",
                "help_text": "Optional extra filter, ANDed with the scope above.",
            },
            {
                "id": "remove_mismatched",
                "label": "Remove mismatched managed streams",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Delete streams from managed accounts that don't exact-match the "
                    "channel. Cleans up prior fuzzy over-matching. Leave on."
                ),
            },
            {
                "id": "skip_stale",
                "label": "Skip dead/stale streams",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Ignore streams Dispatcharr has flagged stale (provider dropped "
                    "them), so you never attach a dead failover."
                ),
            },
            {
                "id": "schedule_time",
                "label": "Daily run time (HH:MM, UTC — blank = off)",
                "type": "string",
                "default": "",
                "placeholder": "10:00",
                "help_text": (
                    "Server time is UTC. 10:00 UTC ≈ 3:00 AM US-Pacific. Blank "
                    "disables the daily schedule. Retries for 6h on failure."
                ),
            },
            {
                "id": "gotify_notify",
                "label": "Gotify notification for scheduled runs",
                "type": "select",
                "options": [
                    {"value": "off", "label": "Off"},
                    {"value": "on_failure", "label": "On failure only"},
                    {"value": "on_completion", "label": "On every completion"},
                ],
                "default": "off",
                "help_text": "Notify a Gotify endpoint after the daily scheduled run.",
            },
            {
                "id": "gotify_url",
                "label": "Gotify message URL (with ?token=…)",
                "type": "string",
                "input_type": "password",
                "default": "",
                "placeholder": "https://gotify.example.com/message?token=…",
                "help_text": "Full Gotify message URL including the app token. Stored in the DB, never committed.",
            },
        ]

    # --------------------------------------------------------------- dispatch
    def run(self, action, params, context):
        settings = (context or {}).get("settings", {}) or {}
        if action == "view_last":
            return self._action_view_last()
        if action == "clear_lock":
            return self._action_clear_lock()
        if action in ("preview", "run"):
            return self._action_start(dry_run=(action == "preview"), settings=settings)
        if action == "stop":
            self._cancel.set()
            return {"status": "ok", "message": "Cancellation requested."}
        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        self._cancel.set()
        self._sched_stop.set()

    # --------------------------------------------------------------- actions
    def _action_start(self, dry_run, settings):
        if self._job_thread is not None and self._job_thread.is_alive():
            return {"status": "error", "message": "A run is already in progress."}
        if not self._acquire_lock():
            return {
                "status": "error",
                "message": "A run is already in progress (locked). Use 'Clear lock' if it's stuck.",
            }
        self._cancel.clear()
        try:
            t = threading.Thread(
                target=self._job_guarded,
                args=(dry_run, dict(settings)),
                name="streammirrarr-job",
                daemon=True,
            )
            Plugin._job_thread = t
            t.start()
        except Exception as exc:
            # Thread never started -> _job_guarded's finally won't release the lock.
            self._release_lock()
            Plugin._job_thread = None
            logger.exception("streammirrarr could not start job thread")
            return {"status": "error", "message": f"Could not start run: {exc}"}
        mode = "Preview (dry-run)" if dry_run else "Exact-match reconcile"
        return {
            "status": "queued",
            "message": f"{mode} started. Watch notifications, then check ‘View last results’.",
        }

    def _action_view_last(self):
        data = self._read_status()
        if not data:
            return {"status": "ok", "message": "No runs recorded yet."}
        return {"status": "ok", "message": self._format_report(data), "result": data}

    def _action_clear_lock(self):
        existed = os.path.exists(RUN_LOCK)
        self._release_lock()
        Plugin._job_thread = None
        self._cancel.clear()
        return {"status": "ok", "message": "Lock cleared." if existed else "No lock was held."}

    # --------------------------------------------------------- cross-proc lock
    def _acquire_lock(self):
        """Atomic cross-process lock. Steals a stale lock (dead holder/too old).

        The steal is done by deleting the stale file and re-attempting the
        O_EXCL create, so if several workers race only the one whose exclusive
        create succeeds wins — there is no truncate-write that two workers could
        both perform.
        """
        os.makedirs(SCHED_DIR, exist_ok=True)
        payload = f"{os.getpid()}|{time.time()}".encode()
        for _ in range(3):
            try:
                fd = os.open(RUN_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, payload)
                os.close(fd)
                return True
            except FileExistsError:
                if not self._lock_is_stale():
                    return False
                # Stale: remove and retry the exclusive create. Only one racer's
                # unlink+create pair wins; the others see the new lock and bail.
                try:
                    os.remove(RUN_LOCK)
                    logger.warning("[Streammirrarr] removing a stale run lock")
                except FileNotFoundError:
                    pass  # someone else already removed it; retry create
                except Exception:
                    return False
        return False

    def _lock_is_stale(self):
        try:
            with open(RUN_LOCK, "r") as fh:
                pid_s, ts_s = fh.read().strip().split("|")
            pid, ts = int(pid_s), float(ts_s)
        except Exception:
            return True  # unreadable -> treat as stale
        if (time.time() - ts) > LOCK_STALE_SECS:
            return True
        return not _pid_alive(pid)

    def _release_lock(self):
        try:
            os.remove(RUN_LOCK)
        except FileNotFoundError:
            pass
        except Exception:
            logger.debug("could not remove run lock", exc_info=True)

    # --------------------------------------------------------------- job body
    def _job_guarded(self, dry_run, settings):
        try:
            self._run_job(dry_run, settings)
        except Exception as exc:
            logger.exception("streammirrarr job failed")
            self._write_status({
                "status": "error",
                "dry_run": dry_run,
                "finished": _iso(_now()),
                "error": str(exc),
                "message": f"Run failed: {exc}",
            })
            send_websocket_update(
                "updates", "update",
                {"type": "streammirrarr", "status": "error", "message": str(exc)},
            )
        finally:
            close_old_connections()
            self._release_lock()
            Plugin._job_thread = None

    def _run_job(self, dry_run, settings):
        started = _now()
        primary, failovers, managed_ids = self._resolve_accounts(settings)
        remove_mismatched = bool(settings.get("remove_mismatched", True))
        skip_stale = bool(settings.get("skip_stale", True))
        group_names = [g.strip() for g in (settings.get("channel_groups") or "").split(",") if g.strip()]

        self._write_status({
            "status": "running",
            "dry_run": dry_run,
            "started": _iso(started),
            "primary": primary.name,
            "failovers": [f.name for f in failovers],
            "message": "Building lookup maps…",
        })

        primary_by_name, fo_by_sid, fo_by_name = self._build_maps(primary, failovers, skip_stale)

        cs_rows = ChannelStream.objects.filter(
            stream__m3u_account_id__in=managed_ids
        ).values_list("channel_id", "stream_id", "order", "id")
        current = {}
        for ch_id, stream_pk, order, cs_id in cs_rows.iterator():
            current.setdefault(ch_id, []).append((order, stream_pk, cs_id))

        channels = list(self._scope_channels(settings, group_names).values("id", "name"))

        stats = {
            "channels_total": len(channels),
            "matched": 0,
            "unmatched": 0,
            "channels_changed": 0,
            "streams_added": 0,
            "streams_removed": 0,
            "streams_reordered": 0,
        }
        unmatched_examples = []
        worst_overmatched = []

        to_create = []
        to_update = []
        delete_cs_ids = []

        total = len(channels)
        for idx, ch in enumerate(channels):
            if self._cancel.is_set():
                return self._finish_cancelled(dry_run, started, stats)

            key = _norm(ch["name"])
            p = primary_by_name.get(key)
            if not p:
                stats["unmatched"] += 1
                if len(unmatched_examples) < 25:
                    unmatched_examples.append(ch["name"])
                continue
            stats["matched"] += 1

            primary_pk, sid = p
            desired = [primary_pk]
            for i, _f in enumerate(failovers):
                fo = fo_by_sid[i].get(sid) if sid else None
                if fo is None:
                    fo = fo_by_name[i].get(key)
                if fo is not None and fo not in desired:
                    desired.append(fo)

            rows = sorted(current.get(ch["id"], []))  # (order, stream_pk, cs_id)
            existing = {stream_pk: (order, cs_id) for order, stream_pk, cs_id in rows}
            desired_set = set(desired)

            # Decide the final ordered set of managed streams for this channel.
            removed_here = 0
            if remove_mismatched:
                for order, stream_pk, cs_id in rows:
                    if stream_pk not in desired_set:
                        delete_cs_ids.append(cs_id)
                        removed_here += 1
                final_order = list(desired)
            else:
                # Keep mismatched managed streams, but append them so orders stay
                # contiguous (no duplicate/ambiguous order values).
                leftover = [pk for _o, pk, _c in rows if pk not in desired_set]
                final_order = list(desired) + leftover

            added_here = reordered_here = 0
            for new_order, stream_pk in enumerate(final_order):
                if stream_pk in existing:
                    cur_order, cs_id = existing[stream_pk]
                    if cur_order != new_order:
                        to_update.append(ChannelStream(id=cs_id, order=new_order))
                        reordered_here += 1
                else:
                    to_create.append(
                        ChannelStream(channel_id=ch["id"], stream_id=stream_pk, order=new_order)
                    )
                    added_here += 1

            if removed_here or added_here or reordered_here:
                stats["channels_changed"] += 1
                stats["streams_removed"] += removed_here
                stats["streams_added"] += added_here
                stats["streams_reordered"] += reordered_here
                if removed_here >= 3:
                    worst_overmatched.append((removed_here, ch["name"]))

            if idx % 1500 == 0:
                self._progress(idx, total, dry_run)

        worst_overmatched.sort(reverse=True)
        report = {
            "status": "done",
            "dry_run": dry_run,
            "started": _iso(started),
            "finished": _iso(_now()),
            "primary": primary.name,
            "failovers": [f.name for f in failovers],
            "remove_mismatched": remove_mismatched,
            "skip_stale": skip_stale,
            "scope": settings.get("channel_scope", "all"),
            "groups": group_names or "ALL",
            "stats": stats,
            "samples": {
                "worst_overmatched": [
                    {"removed": n, "channel": name} for n, name in worst_overmatched[:15]
                ],
                "unmatched_examples": unmatched_examples,
            },
        }

        if dry_run:
            report["message"] = (
                f"DRY-RUN: would change {stats['channels_changed']} channels "
                f"(+{stats['streams_added']} added, -{stats['streams_removed']} removed, "
                f"~{stats['streams_reordered']} reordered). Nothing written."
            )
            self._write_status(report)
            logger.info("[Streammirrarr] %s", report["message"])
            send_websocket_update(
                "updates", "update",
                {"type": "streammirrarr", "status": "done", "dry_run": True,
                 "message": report["message"]},
            )
            return report

        has_changes = bool(delete_cs_ids or to_create or to_update)
        backup_path = None
        if has_changes:
            backup_path = self._backup_managed(managed_ids)
            with transaction.atomic():
                if delete_cs_ids:
                    for chunk in _chunks(delete_cs_ids, 5000):
                        ChannelStream.objects.filter(id__in=chunk).delete()
                if to_create:
                    # By invariant every desired stream is from a managed account,
                    # so any pre-existing (channel, stream) row is already in
                    # `existing` and routed to update — to_create rows are new.
                    # ignore_conflicts guards only a sub-second TOCTOU edge; it
                    # self-heals on the next run.
                    ChannelStream.objects.bulk_create(
                        to_create, batch_size=2000, ignore_conflicts=True
                    )
                if to_update:
                    ChannelStream.objects.bulk_update(to_update, ["order"], batch_size=2000)

        report["backup"] = backup_path
        report["message"] = (
            f"Done: {stats['channels_changed']} channels changed "
            f"(+{stats['streams_added']} added, -{stats['streams_removed']} removed, "
            f"~{stats['streams_reordered']} reordered), "
            f"{stats['unmatched']} channels had no primary match."
        )
        self._write_status(report)
        logger.info("[Streammirrarr] %s (backup: %s)", report["message"], backup_path or "none")
        send_websocket_update(
            "updates", "update",
            {"type": "streammirrarr", "status": "done", "message": report["message"]},
        )
        send_websocket_update("updates", "update", {"type": "channels_refresh"})
        return report

    def _finish_cancelled(self, dry_run, started, stats):
        report = {
            "status": "cancelled",
            "dry_run": dry_run,
            "started": _iso(started),
            "finished": _iso(_now()),
            "stats": stats,
            "message": "Run cancelled before writing.",
        }
        self._write_status(report)
        logger.info("[Streammirrarr] run cancelled")
        return report

    # ------------------------------------------------------------ channel scope
    def _scope_channels(self, settings, group_names):
        scope = (settings.get("channel_scope") or "all").strip()
        qs = Channel.objects.all()
        if scope == "auto_created":
            qs = qs.filter(auto_created=True)
        elif scope == "profile":
            pname = (settings.get("channel_profile") or "").strip()
            if not pname:
                raise ValueError("Channel scope is 'profile' but no profile is selected.")
            ch_ids = ChannelProfileMembership.objects.filter(
                channel_profile__name=pname, enabled=True
            ).values_list("channel_id", flat=True)
            qs = qs.filter(id__in=ch_ids)
        if group_names:
            qs = qs.filter(channel_group__name__in=group_names)
        return qs

    # ----------------------------------------------------------- map building
    def _build_maps(self, primary, failovers, skip_stale):
        def base_qs(account):
            qs = Stream.objects.filter(m3u_account=account)
            if skip_stale:
                qs = qs.exclude(is_stale=True)
            return qs.values_list("id", "name", "stream_id")

        primary_by_name = {}
        for pk, name, sid in base_qs(primary).iterator():
            key = _norm(name)
            if not key:
                continue
            cur = primary_by_name.get(key)
            if cur is None or (sid is not None and (cur[1] is None or sid < cur[1])):
                primary_by_name[key] = (pk, sid)

        fo_by_sid = []
        fo_by_name = []
        for f in failovers:
            by_sid = {}
            by_name = {}
            for pk, name, sid in base_qs(f).iterator():
                if sid is not None and sid not in by_sid:
                    by_sid[sid] = pk
                key = _norm(name)
                if key and key not in by_name:
                    by_name[key] = pk
            fo_by_sid.append(by_sid)
            fo_by_name.append(by_name)
        return primary_by_name, fo_by_sid, fo_by_name

    # ------------------------------------------------------------- accounts
    def _resolve_accounts(self, settings):
        all_accounts = {}
        dup_names = set()
        for a in M3UAccount.objects.all():
            if a.name in all_accounts:
                dup_names.add(a.name)
            all_accounts[a.name] = a

        def require(nm):
            if nm in dup_names:
                raise ValueError(
                    f"Multiple M3U accounts are named '{nm}'. Rename them uniquely — "
                    "this plugin targets accounts by name."
                )
            return all_accounts.get(nm)

        primary_name = (settings.get("primary_account") or "").strip()
        if not primary_name:
            primary_name = self._detect_primary_name(
                M3UAccount.objects.all().values("id", "name", "account_type")
            )
        primary = require(primary_name)
        if not primary:
            raise ValueError(
                f"Primary account '{primary_name}' not found. Set it in plugin settings."
            )

        failovers = []
        raw = (settings.get("failover_accounts") or "").strip()
        for nm in [x.strip() for x in raw.split(",") if x.strip()]:
            acct = require(nm)
            if not acct:
                raise ValueError(f"Failover account '{nm}' not found.")
            if acct.id == primary.id or acct in failovers:
                continue
            failovers.append(acct)
        if not failovers:
            raise ValueError("No valid failover accounts configured.")

        managed_ids = {primary.id} | {f.id for f in failovers}
        return primary, failovers, managed_ids

    def _detect_primary_name(self, accounts):
        """The source-of-truth account = the one most channels were auto-created by."""
        try:
            from django.db.models import Count
            row = (
                Channel.objects.exclude(auto_created_by__isnull=True)
                .values("auto_created_by")
                .annotate(n=Count("id"))
                .order_by("-n")
                .first()
            )
            if row:
                acct = M3UAccount.objects.filter(id=row["auto_created_by"]).values("name").first()
                if acct:
                    return acct["name"]
        except Exception:
            logger.debug("primary auto-detect failed", exc_info=True)
        acc_list = list(accounts)
        for a in sorted(acc_list, key=lambda x: x["name"]):
            if a.get("account_type") == "XC":
                return a["name"]
        return acc_list[0]["name"] if acc_list else ""

    # ------------------------------------------------------------- backups
    def _backup_managed(self, managed_ids):
        try:
            rows = list(
                ChannelStream.objects.filter(stream__m3u_account_id__in=managed_ids)
                .values_list("channel_id", "stream_id", "order")
            )
            ts = _now().strftime("%Y%m%d-%H%M%S")
            path = os.path.join(PLUGIN_DIR, f"backup_channelstream_{ts}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(rows, fh)
            existing = sorted(glob.glob(BACKUP_GLOB))
            for old in existing[:-BACKUP_KEEP]:
                try:
                    os.remove(old)
                except Exception:
                    pass
            return path
        except Exception:
            logger.exception("[Streammirrarr] backup failed (continuing)")
            return None

    # ----------------------------------------------------------- status I/O
    def _write_status(self, data):
        try:
            tmp = STATUS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            os.replace(tmp, STATUS_FILE)
        except Exception:
            logger.debug("could not write status file", exc_info=True)

    def _read_status(self):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None

    def _progress(self, idx, total, dry_run):
        msg = f"{'Preview' if dry_run else 'Reconcile'}: scanned {idx}/{total} channels…"
        send_websocket_update(
            "updates", "update",
            {"type": "streammirrarr", "status": "running", "progress": idx,
             "total": total, "message": msg},
        )

    def _format_report(self, data):
        s = data.get("stats", {})
        lines = [
            f"[{data.get('status', '?')}{' / dry-run' if data.get('dry_run') else ''}] "
            f"{data.get('message', '')}",
        ]
        if s:
            lines.append(
                f"channels: {s.get('channels_total','?')} total, "
                f"{s.get('matched','?')} matched, {s.get('unmatched','?')} unmatched, "
                f"{s.get('channels_changed','?')} changed"
            )
        if data.get("backup"):
            lines.append(f"backup: {data['backup']}")
        worst = (data.get("samples", {}) or {}).get("worst_overmatched", [])
        if worst:
            lines.append("worst over-matched cleaned up:")
            for w in worst[:10]:
                lines.append(f"  -{w['removed']}  {w['channel']}")
        return "\n".join(lines)

    # --------------------------------------------------------------- gotify
    def _notify_gotify(self, settings, ok, message):
        mode = (settings.get("gotify_notify") or "off").strip()
        if mode == "off":
            return
        if mode == "on_failure" and ok:
            return
        url = (settings.get("gotify_url") or "").strip()
        if not url:
            return
        title = "Streammirrarr ✅" if ok else "Streammirrarr ❌ FAILED"
        priority = 3 if ok else 7
        try:
            data = urllib.parse.urlencode(
                {"title": title, "message": message[:1800], "priority": priority}
            ).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            urllib.request.urlopen(req, timeout=10).read()
        except Exception:
            logger.warning("[Streammirrarr] gotify notify failed", exc_info=True)

    # ------------------------------------------------------------- scheduler
    def _ensure_scheduler(self):
        if Plugin._sched_thread is not None and Plugin._sched_thread.is_alive():
            return
        Plugin._sched_stop.clear()
        t = threading.Thread(target=self._scheduler_loop, name="streammirrarr-sched", daemon=True)
        Plugin._sched_thread = t
        t.start()
        logger.info("[Streammirrarr] scheduler thread active (checks schedule_time every %ss)", SCHED_TICK_SECS)

    def _scheduler_loop(self):
        os.makedirs(SCHED_DIR, exist_ok=True)
        while not Plugin._sched_stop.wait(SCHED_TICK_SECS):
            try:
                self._scheduler_tick()
            except Exception:
                logger.exception("streammirrarr scheduler tick failed")

    def _scheduler_tick(self):
        close_old_connections()  # this thread touches the ORM every tick
        cfg = self._scheduled_settings()
        target = self._parse_hhmm(cfg.get("schedule_time"))
        if not target:
            return
        now = _now()
        # Most recent occurrence of the target time at or before now. Using
        # "today or yesterday" keeps a late-evening schedule's retry window valid
        # across the midnight boundary, and keys the success marker off the
        # target's date (not now's date).
        target_today = now.replace(hour=target[0], minute=target[1], second=0, microsecond=0)
        target_dt = target_today if target_today <= now else target_today - datetime.timedelta(days=1)
        if (now - target_dt).total_seconds() > SCHED_WINDOW_SECS:
            return  # outside the retry window for the last scheduled occurrence
        success_marker = os.path.join(SCHED_DIR, f"success-{target_dt.strftime('%Y%m%d')}.marker")
        if os.path.exists(success_marker):
            return
        # cooldown between attempts (any worker)
        last = self._read_attempt()
        if last and (time.time() - last) < SCHED_COOLDOWN_SECS:
            return
        self._write_attempt(time.time())

        if self._job_thread is not None and self._job_thread.is_alive():
            return
        if not self._acquire_lock():
            return
        self._cancel.clear()
        ok = False
        message = ""
        try:
            logger.info("[Streammirrarr] scheduled run firing (target %02d:%02d UTC)", *target)
            report = self._run_job(False, cfg)
            ok = bool(report and report.get("status") == "done")
            message = (report or {}).get("message", "no report")
            if ok:
                _touch(success_marker)
                self._cleanup_markers()
        except Exception as exc:
            logger.exception("streammirrarr scheduled run failed")
            message = f"{exc}"
        finally:
            close_old_connections()
            self._release_lock()
            self._notify_gotify(cfg, ok, message)

    def _scheduled_settings(self):
        from apps.plugins.models import PluginConfig
        from apps.plugins.loader import PluginManager
        cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).values("settings").first()
        saved = (cfg or {}).get("settings", {}) or {}
        # Merge field defaults so a never-saved setting still has its default.
        try:
            defaults = {f["id"]: f.get("default") for f in self.fields}
            merged = dict(defaults)
            merged.update(saved)
            return merged
        except Exception:
            return saved

    def _parse_hhmm(self, val):
        val = (val or "").strip()
        if not val:
            return None
        try:
            hh, mm = val.split(":")
            hh, mm = int(hh), int(mm)
            if 0 <= hh < 24 and 0 <= mm < 60:
                return (hh, mm)
        except Exception:
            pass
        return None

    def _read_attempt(self):
        try:
            with open(ATTEMPT_FILE, "r") as fh:
                return float(fh.read().strip())
        except Exception:
            return None

    def _write_attempt(self, ts):
        _atomic_write(ATTEMPT_FILE, str(ts))

    def _cleanup_markers(self):
        try:
            cutoff = time.time() - 8 * 86400
            for fn in os.listdir(SCHED_DIR):
                fp = os.path.join(SCHED_DIR, fn)
                if fn.startswith("success-") and os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                    os.remove(fp)
        except Exception:
            pass


def _atomic_write(path, text):
    try:
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except Exception:
        pass


def _touch(path):
    _atomic_write(path, _iso(_now()))


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]
