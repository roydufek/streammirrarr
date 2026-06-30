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
import json
import logging
import os
import threading
import time

from django.db import close_old_connections, transaction
from django.db.models import Count

from apps.channels.models import Channel, ChannelStream, Stream
from apps.m3u.models import M3UAccount

try:
    from core.utils import send_websocket_update
except Exception:  # pragma: no cover - defensive: never block on websocket import
    def send_websocket_update(*_a, **_k):
        return None

__version__ = "0.1.1"

logger = logging.getLogger("plugins.streammirrarr")

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGIN_KEY = os.path.basename(PLUGIN_DIR).replace(" ", "_").lower()
STATUS_FILE = os.path.join(PLUGIN_DIR, "last_run.json")
SCHED_DIR = os.path.join(PLUGIN_DIR, ".sched")


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
    return datetime.datetime.utcnow()


def _iso(dt):
    return dt.replace(microsecond=0).isoformat() + "Z"


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

    # Shared across instances/processes within a worker.
    _job_lock = threading.Lock()
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
                    "stream_id matches and removes mismatched managed streams. "
                    "Proceed?"
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
        # Kick the scheduler on load; it idles unless a schedule_time is set.
        try:
            self._ensure_scheduler()
        except Exception:
            logger.debug("scheduler start deferred", exc_info=True)
        logger.info("[Streammirrarr] v%s initialized", __version__)

    # ------------------------------------------------------------------ fields
    @property
    def fields(self):
        accounts = []
        try:
            accounts = list(
                M3UAccount.objects.filter(is_active=True)
                .values("id", "name", "account_type")
                .order_by("name")
            )
        except Exception:
            logger.debug("could not list M3U accounts for fields", exc_info=True)

        names = [a["name"] for a in accounts]
        options = [{"value": n, "label": n} for n in names]

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
                "options": options,
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
                "id": "channel_groups",
                "label": "Limit to channel groups (optional, comma-separated)",
                "type": "string",
                "default": "",
                "help_text": "Leave blank to process every channel.",
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
                "id": "schedule_time",
                "label": "Daily run time (HH:MM, UTC — blank = off)",
                "type": "string",
                "default": "",
                "placeholder": "10:00",
                "help_text": (
                    "Server time is UTC. 10:00 UTC ≈ 3:00 AM US-Pacific. Blank "
                    "disables the daily schedule."
                ),
            },
        ]

    # --------------------------------------------------------------- dispatch
    def run(self, action, params, context):
        settings = (context or {}).get("settings", {}) or {}
        log = (context or {}).get("logger", logger)

        if action == "view_last":
            return self._action_view_last()
        if action == "clear_lock":
            return self._action_clear_lock()
        if action in ("preview", "run"):
            return self._action_start(dry_run=(action == "preview"), settings=settings, log=log)
        if action == "stop":
            self._cancel.set()
            return {"status": "ok", "message": "Cancellation requested."}
        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        self._cancel.set()
        self._sched_stop.set()

    # --------------------------------------------------------------- actions
    def _action_start(self, dry_run, settings, log):
        if self._job_thread is not None and self._job_thread.is_alive():
            return {"status": "error", "message": "A run is already in progress."}
        if not self._job_lock.acquire(blocking=False):
            return {"status": "error", "message": "A run is already in progress."}

        self._cancel.clear()
        t = threading.Thread(
            target=self._job_guarded,
            args=(dry_run, dict(settings)),
            name="streammirrarr-job",
            daemon=True,
        )
        Plugin._job_thread = t
        t.start()
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
        released = False
        try:
            self._job_lock.release()
            released = True
        except RuntimeError:
            pass
        Plugin._job_thread = None
        self._cancel.clear()
        return {
            "status": "ok",
            "message": "Lock cleared." if released else "No lock was held.",
        }

    # --------------------------------------------------------------- job body
    def _job_guarded(self, dry_run, settings):
        try:
            self._run_job(dry_run, settings)
        except Exception as exc:  # report, never crash the thread silently
            logger.exception("streammirrarr job failed")
            self._write_status(
                {
                    "status": "error",
                    "dry_run": dry_run,
                    "finished": _iso(_now()),
                    "error": str(exc),
                    "message": f"Run failed: {exc}",
                }
            )
            send_websocket_update(
                "updates", "update",
                {"type": "streammirrarr", "status": "error", "message": str(exc)},
            )
        finally:
            close_old_connections()
            try:
                self._job_lock.release()
            except RuntimeError:
                pass
            Plugin._job_thread = None

    def _run_job(self, dry_run, settings):
        started = _now()
        primary, failovers, managed_ids = self._resolve_accounts(settings)
        remove_mismatched = bool(settings.get("remove_mismatched", True))
        group_names = [g.strip() for g in (settings.get("channel_groups") or "").split(",") if g.strip()]

        self._write_status({
            "status": "running",
            "dry_run": dry_run,
            "started": _iso(started),
            "primary": primary.name,
            "failovers": [f.name for f in failovers],
            "message": "Building lookup maps…",
        })

        primary_by_name, fo_by_sid, fo_by_name = self._build_maps(primary, failovers)

        # Pull every managed Channel<->Stream link in one query.
        cs_rows = ChannelStream.objects.filter(
            stream__m3u_account_id__in=managed_ids
        ).values_list("channel_id", "stream_id", "order", "id")
        current = {}  # channel_id -> list[(order, stream_pk, cs_id)]
        for ch_id, stream_pk, order, cs_id in cs_rows.iterator():
            current.setdefault(ch_id, []).append((order, stream_pk, cs_id))

        ch_qs = Channel.objects.all()
        if group_names:
            ch_qs = ch_qs.filter(channel_group__name__in=group_names)
        channels = list(ch_qs.values("id", "name"))

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
        worst_overmatched = []  # (extra_removed, channel_name)

        to_create = []           # ChannelStream objects
        to_update = []           # ChannelStream(id=, order=)
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

            rows = sorted(current.get(ch["id"], []))  # by order
            desired_set = set(desired)

            # Removals: managed rows that aren't exact matches.
            removed_here = 0
            kept = {}  # stream_pk -> (order, cs_id)
            for order, stream_pk, cs_id in rows:
                if stream_pk in desired_set:
                    kept[stream_pk] = (order, cs_id)
                elif remove_mismatched:
                    delete_cs_ids.append(cs_id)
                    removed_here += 1

            # Additions + reordering to the desired sequence.
            added_here = reordered_here = 0
            for new_order, stream_pk in enumerate(desired):
                if stream_pk in kept:
                    cur_order, cs_id = kept[stream_pk]
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
            send_websocket_update(
                "updates", "update",
                {"type": "streammirrarr", "status": "done", "dry_run": True,
                 "message": report["message"]},
            )
            return report

        # Apply for real, in one transaction.
        with transaction.atomic():
            if delete_cs_ids:
                for chunk in _chunks(delete_cs_ids, 5000):
                    ChannelStream.objects.filter(id__in=chunk).delete()
            if to_create:
                ChannelStream.objects.bulk_create(to_create, batch_size=2000)
            if to_update:
                ChannelStream.objects.bulk_update(to_update, ["order"], batch_size=2000)

        report["message"] = (
            f"Done: {stats['channels_changed']} channels changed "
            f"(+{stats['streams_added']} added, -{stats['streams_removed']} removed, "
            f"~{stats['streams_reordered']} reordered), "
            f"{stats['unmatched']} channels had no primary match."
        )
        self._write_status(report)
        send_websocket_update(
            "updates", "update",
            {"type": "streammirrarr", "status": "done", "message": report["message"]},
        )
        # Nudge the channels UI to refresh.
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
        return report

    # ----------------------------------------------------------- map building
    def _build_maps(self, primary, failovers):
        """primary_by_name: norm(name) -> (stream_pk, stream_id).

        On duplicate names within the primary account, keep the lowest
        stream_id deterministically.
        """
        primary_by_name = {}
        for pk, name, sid in (
            Stream.objects.filter(m3u_account=primary)
            .values_list("id", "name", "stream_id")
            .iterator()
        ):
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
            for pk, name, sid in (
                Stream.objects.filter(m3u_account=f)
                .values_list("id", "name", "stream_id")
                .iterator()
            ):
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
        all_accounts = {a.name: a for a in M3UAccount.objects.all()}
        primary_name = (settings.get("primary_account") or "").strip()
        if not primary_name:
            primary_name = self._detect_primary_name(
                M3UAccount.objects.all().values("id", "name", "account_type")
            )
        primary = all_accounts.get(primary_name)
        if not primary:
            raise ValueError(
                f"Primary account '{primary_name}' not found. Set it in plugin settings."
            )

        failovers = []
        raw = (settings.get("failover_accounts") or "").strip()
        for nm in [x.strip() for x in raw.split(",") if x.strip()]:
            acct = all_accounts.get(nm)
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
        # Fallback: first XC account by name.
        acc_list = list(accounts)
        for a in sorted(acc_list, key=lambda x: x["name"]):
            if a.get("account_type") == "XC":
                return a["name"]
        return acc_list[0]["name"] if acc_list else ""

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
        worst = (data.get("samples", {}) or {}).get("worst_overmatched", [])
        if worst:
            lines.append("worst over-matched cleaned up:")
            for w in worst[:10]:
                lines.append(f"  -{w['removed']}  {w['channel']}")
        return "\n".join(lines)

    # ------------------------------------------------------------- scheduler
    def _ensure_scheduler(self):
        if Plugin._sched_thread is not None and Plugin._sched_thread.is_alive():
            return
        Plugin._sched_stop.clear()
        t = threading.Thread(target=self._scheduler_loop, name="streammirrarr-sched", daemon=True)
        Plugin._sched_thread = t
        t.start()
        logger.info("[Streammirrarr] scheduler thread active (checks schedule_time every 30s)")

    def _scheduler_loop(self):
        os.makedirs(SCHED_DIR, exist_ok=True)
        while not Plugin._sched_stop.wait(30):
            try:
                target = self._scheduled_time()
                if not target:
                    continue
                now = _now()
                if now.strftime("%H:%M") != target:
                    continue
                marker = os.path.join(SCHED_DIR, f"ran-{now.strftime('%Y%m%d')}-{target.replace(':','')}.lock")
                try:
                    fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    os.close(fd)
                except FileExistsError:
                    continue  # another worker already claimed this minute
                self._cleanup_markers()
                if self._job_lock.acquire(blocking=False):
                    try:
                        logger.info("streammirrarr scheduled run firing at %s UTC", target)
                        self._run_job(False, self._scheduled_settings())
                    finally:
                        close_old_connections()
                        try:
                            self._job_lock.release()
                        except RuntimeError:
                            pass
            except Exception:
                logger.exception("streammirrarr scheduler tick failed")

    def _scheduled_settings(self):
        from apps.plugins.models import PluginConfig
        cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).values("settings").first()
        return (cfg or {}).get("settings", {}) or {}

    def _scheduled_time(self):
        val = (self._scheduled_settings().get("schedule_time") or "").strip()
        if not val:
            return ""
        try:
            hh, mm = val.split(":")
            return f"{int(hh):02d}:{int(mm):02d}"
        except Exception:
            return ""

    def _cleanup_markers(self):
        try:
            cutoff = time.time() - 2 * 86400
            for fn in os.listdir(SCHED_DIR):
                fp = os.path.join(SCHED_DIR, fn)
                if os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                    os.remove(fp)
        except Exception:
            pass


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]
