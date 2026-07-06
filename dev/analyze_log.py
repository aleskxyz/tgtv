#!/usr/bin/env python3
"""Analyze TGTV JSON log files (tgtv.log).

Unified ingest health report: pacing/drift, duplicate segments, live-edge
probes (raw + excess lag), FLOOD_WAIT bursts, 60s emit windows, segment flow,
resync cadence, timeline gaps, ffmpeg discontinuities, and a pass/fail verdict.

Usage:
  python3 dev/analyze_log.py tgtv.log
  python3 dev/analyze_log.py tgtv.log --stream 5a7c9e17a644
  python3 dev/analyze_log.py tgtv.log --json
  python3 dev/analyze_log.py tgtv.log --verdict-only
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


def parse_ts(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def fmt_ts(dt: Optional[datetime]) -> str:
    if dt is None:
        return "?"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_dur(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes:.0f}m {sec:.0f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours:.0f}h {minutes:.0f}m"


def stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    out = {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
    }
    if len(values) >= 2:
        out["stdev"] = statistics.stdev(values)
        out["median"] = statistics.median(values)
    return out


def pct(n: int, total: int) -> str:
    if total == 0:
        return "0%"
    return f"{100 * n / total:.1f}%"


FLOOD_WAIT_RE = re.compile(r"FLOOD_WAIT\s*\((\d+)\)")
DISCONTINUITY_RE = re.compile(
    r"timestamp discontinuity \(stream id=\d+\):\s*(-?\d+),\s*new offset=\s*(-?\d+)"
)

# From internal/stream/constants.go — keep in sync for verdict thresholds.
REBUFFER_MS = 3000
CATCHUP_EXCESS_THRESHOLD_MS = 2000
CATCHUP_NEAR_EXCESS_MS = 1500
TARGET_PUBLISH_RATE = 1.0
PUBLISH_RATE_TOLERANCE = 0.01  # ±1%
DRIFT_WARN_PER_12MIN_S = 2.0


@dataclass
class StreamSession:
    stream_id: str
    title: str = ""
    chat_id: Optional[int] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    ingest_started: bool = False
    unified: Optional[bool] = None
    bootstrap: str = ""

    parts_in: int = 0
    segments_out: int = 0
    max_parts_gap: int = 0
    since_last_part: list[float] = field(default_factory=list)

    telegram_resyncs: list[datetime] = field(default_factory=list)
    scheduler_resyncs: list[datetime] = field(default_factory=list)
    live_edge_catchups: list[dict[str, Any]] = field(default_factory=list)
    live_edge_lags: list[int] = field(default_factory=list)
    live_edge_excess_lags: list[int] = field(default_factory=list)
    live_edge_cooldown_skips: int = 0
    probes: list[dict[str, Any]] = field(default_factory=list)
    parts: list[tuple[datetime, int]] = field(default_factory=list)
    heartbeats: list[tuple[datetime, int, int]] = field(default_factory=list)
    getfile_failures: Counter = field(default_factory=Counter)
    getfile_failures_detail: list[dict[str, Any]] = field(default_factory=list)
    getfile_requested: list[tuple[datetime, int]] = field(default_factory=list)
    getfile_ok: list[tuple[datetime, int]] = field(default_factory=list)
    flood_waits: list[int] = field(default_factory=list)
    timeline_gaps_ms: list[int] = field(default_factory=list)
    ffmpeg_discontinuities: list[int] = field(default_factory=list)
    stale_drops: int = 0
    recovery_deferred: Counter = field(default_factory=Counter)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    last_part_ms: Optional[int] = None
    last_heartbeat_parts: int = 0
    last_heartbeat_segments: int = 0


@dataclass
class LogAnalysis:
    path: str
    lines_total: int = 0
    lines_parsed: int = 0
    parse_errors: int = 0
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    message_counts: Counter = field(default_factory=Counter)
    level_counts: Counter = field(default_factory=Counter)
    discovered: dict[str, str] = field(default_factory=dict)
    sessions: list[StreamSession] = field(default_factory=list)
    global_warnings: list[str] = field(default_factory=list)
    global_errors: list[str] = field(default_factory=list)


def active_ingest_session(
    sessions: list[StreamSession], ts: Optional[datetime]
) -> Optional[StreamSession]:
    """Return the ingest session that was active at ts (from ingest started boundaries)."""
    if ts is None:
        return None
    for sess in reversed(sessions):
        if not sess.ingest_started or sess.started_at is None:
            continue
        if sess.started_at <= ts and (sess.ended_at is None or ts <= sess.ended_at):
            return sess
    return None


def open_session_for_stream(
    sessions: list[StreamSession], stream_id: str
) -> Optional[StreamSession]:
    for sess in reversed(sessions):
        if sess.stream_id == stream_id and sess.ingest_started and sess.ended_at is None:
            return sess
    return None


def close_other_sessions(
    sessions: list[StreamSession], keep: StreamSession, ts: Optional[datetime]
) -> None:
    if ts is None:
        return
    for sess in sessions:
        if sess is not keep and sess.ingest_started and sess.ended_at is None:
            if sess.started_at and sess.started_at < ts:
                sess.ended_at = ts


def finalize_sessions(sessions: list[StreamSession], last_ts: Optional[datetime]) -> list[StreamSession]:
    """Close open sessions and drop phantom sessions never started via ingest."""
    for sess in sessions:
        if sess.ingest_started and sess.ended_at is None and last_ts:
            sess.ended_at = last_ts

    real = [s for s in sessions if s.ingest_started]
    real.sort(key=lambda s: s.started_at or datetime.min.replace(tzinfo=timezone.utc))

    merged: list[StreamSession] = []
    for sess in real:
        if not merged:
            merged.append(sess)
            continue
        prev = merged[-1]
        if prev.stream_id == sess.stream_id and prev.ended_at and sess.started_at:
            gap = (sess.started_at - prev.ended_at).total_seconds()
            if gap < 5 and not sess.scheduler_resyncs and not sess.live_edge_catchups:
                # Brief overlap/restart artifact — fold metrics into previous session.
                _merge_session(prev, sess)
                continue
        merged.append(sess)
    return merged


def _merge_session(dst: StreamSession, src: StreamSession) -> None:
    if src.ended_at and (dst.ended_at is None or src.ended_at > dst.ended_at):
        dst.ended_at = src.ended_at
    dst.parts_in = max(dst.parts_in, src.parts_in)
    dst.segments_out = max(dst.segments_out, src.segments_out)
    dst.max_parts_gap = max(dst.max_parts_gap, src.max_parts_gap)
    dst.since_last_part.extend(src.since_last_part)
    dst.telegram_resyncs.extend(src.telegram_resyncs)
    dst.scheduler_resyncs.extend(src.scheduler_resyncs)
    dst.live_edge_catchups.extend(src.live_edge_catchups)
    dst.live_edge_lags.extend(src.live_edge_lags)
    dst.live_edge_excess_lags.extend(src.live_edge_excess_lags)
    dst.live_edge_cooldown_skips += src.live_edge_cooldown_skips
    dst.probes.extend(src.probes)
    dst.parts.extend(src.parts)
    dst.heartbeats.extend(src.heartbeats)
    dst.getfile_failures.update(src.getfile_failures)
    dst.getfile_failures_detail.extend(src.getfile_failures_detail)
    dst.getfile_requested.extend(src.getfile_requested)
    dst.getfile_ok.extend(src.getfile_ok)
    dst.flood_waits.extend(src.flood_waits)
    dst.timeline_gaps_ms.extend(src.timeline_gaps_ms)
    dst.ffmpeg_discontinuities.extend(src.ffmpeg_discontinuities)
    dst.stale_drops += src.stale_drops
    dst.recovery_deferred.update(src.recovery_deferred)
    dst.warnings.extend(src.warnings)
    dst.errors.extend(src.errors)


def parse_lag_ms(rec: dict[str, Any]) -> Optional[int]:
    lag = rec.get("lag_ms")
    if isinstance(lag, bool) or lag is None:
        return None
    if isinstance(lag, (int, float)):
        return int(lag)
    return None


def ingest_event(
    rec: dict[str, Any],
    ts: Optional[datetime],
    sessions: list[StreamSession],
    discovered: dict[str, str],
) -> None:
    msg = rec.get("message", "")
    stream = rec.get("stream")
    if not stream:
        return

    if msg == "discovered live":
        discovered[stream] = rec.get("title", "")
        return

    if msg == "ingest started":
        sess = StreamSession(
            stream_id=stream,
            title=discovered.get(stream, ""),
            started_at=ts,
            ingest_started=True,
            chat_id=rec.get("chat"),
        )
        sessions.append(sess)
        close_other_sessions(sessions, sess, ts)
        return

    sess = open_session_for_stream(sessions, stream)
    if sess is None:
        return

    if msg == "stream mode":
        sess.unified = rec.get("unified")
        sess.bootstrap = rec.get("bootstrap", "")
        return

    if msg == "ingest heartbeat":
        parts = int(rec.get("parts_in", 0))
        segs = int(rec.get("segments_out", 0))
        gap = parts - segs
        if gap > sess.max_parts_gap:
            sess.max_parts_gap = gap
        if rec.get("since_last_part") is not None:
            sess.since_last_part.append(float(rec["since_last_part"]))
        sess.parts_in = max(sess.parts_in, parts)
        sess.segments_out = max(sess.segments_out, segs)
        sess.last_heartbeat_parts = parts
        sess.last_heartbeat_segments = segs
        if ts is not None:
            sess.heartbeats.append((ts, parts, segs))
        return

    if msg == "part received":
        time_ms = rec.get("time_ms")
        if isinstance(time_ms, int):
            if sess.last_part_ms is not None:
                delta = time_ms - sess.last_part_ms
                if delta > 1000:
                    sess.timeline_gaps_ms.append(delta)
                elif delta < 0:
                    sess.timeline_gaps_ms.append(delta)
            sess.last_part_ms = time_ms
            if ts is not None:
                sess.parts.append((ts, time_ms))
        return

    if msg == "telegram resync":
        if ts:
            sess.telegram_resyncs.append(ts)
        return

    if msg in ("dropping stale part after resync", "dropping stale part after resync (post-assemble)"):
        sess.stale_drops += 1
        return

    if msg == "recovery deferred":
        reason = rec.get("reason", "unknown")
        sess.recovery_deferred[reason] += 1
        return

    if msg == "ffmpeg":
        stderr = rec.get("stderr", "")
        m = DISCONTINUITY_RE.search(stderr)
        if m:
            sess.ffmpeg_discontinuities.append(abs(int(m.group(1))))
        return

    if rec.get("level") == "warn":
        sess.warnings.append(f"{fmt_ts(ts)} {msg}")
    elif rec.get("level") == "error":
        sess.errors.append(f"{fmt_ts(ts)} {msg}")


def scheduler_event(
    rec: dict[str, Any],
    ts: Optional[datetime],
    sessions: list[StreamSession],
) -> None:
    msg = rec.get("message", "")
    sess = active_ingest_session(sessions, ts)
    if sess is None:
        return

    if msg == "resync" and ts:
        sess.scheduler_resyncs.append(ts)
        return

    if msg == "live edge catch-up":
        sess.live_edge_catchups.append(
            {
                "ts": ts,
                "reason": rec.get("reason", ""),
                "lag_ms": parse_lag_ms(rec),
                "excess_lag_ms": rec.get("excess_lag_ms"),
                "next_ms": rec.get("next_ms"),
                "adjusted_ms": rec.get("adjusted_ms"),
                "gen": rec.get("gen"),
            }
        )
        lag = parse_lag_ms(rec)
        if lag is not None:
            sess.live_edge_lags.append(lag)
        excess = rec.get("excess_lag_ms")
        if isinstance(excess, (int, float)):
            sess.live_edge_excess_lags.append(int(excess))
        return

    if msg == "live edge ok":
        lag = parse_lag_ms(rec)
        excess = rec.get("excess_lag_ms")
        head_ms = rec.get("head_ms")
        live_ms = rec.get("live_ms")
        if lag is not None:
            sess.live_edge_lags.append(lag)
        if isinstance(excess, (int, float)):
            sess.live_edge_excess_lags.append(int(excess))
        if ts is not None and lag is not None:
            sess.probes.append(
                {
                    "ts": ts,
                    "lag_ms": lag,
                    "excess_lag_ms": int(excess) if isinstance(excess, (int, float)) else None,
                    "head_ms": head_ms,
                    "live_ms": live_ms,
                }
            )
        return

    if msg == "live edge lag above threshold; cooldown":
        sess.live_edge_cooldown_skips += 1
        lag = parse_lag_ms(rec)
        if lag is not None:
            sess.live_edge_lags.append(lag)
        excess = rec.get("excess_lag_ms")
        if isinstance(excess, (int, float)):
            sess.live_edge_excess_lags.append(int(excess))
        return

    if msg == "getFile requested":
        time_ms = rec.get("time_ms")
        if ts is not None and isinstance(time_ms, int):
            sess.getfile_requested.append((ts, time_ms))
        return

    if msg == "getFile ok":
        time_ms = rec.get("time_ms")
        if ts is not None and isinstance(time_ms, int):
            sess.getfile_ok.append((ts, time_ms))
        return

    if msg == "getFile failed":
        outcome = rec.get("outcome", "unknown")
        sess.getfile_failures[outcome] += 1
        err = rec.get("error", "")
        m = FLOOD_WAIT_RE.search(err)
        wait_s = int(m.group(1)) if m else None
        if wait_s is not None:
            sess.flood_waits.append(wait_s)
        sess.getfile_failures_detail.append(
            {
                "ts": ts,
                "time_ms": rec.get("time_ms"),
                "outcome": outcome,
                "error": err,
                "flood_wait_s": wait_s,
            }
        )
        return


def analyze_lines(
    lines: Iterable[str],
    *,
    stream_filter: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> LogAnalysis:
    analysis = LogAnalysis(path="")

    for line in lines:
        analysis.lines_total += 1
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            analysis.parse_errors += 1
            continue
        analysis.lines_parsed += 1

        ts = parse_ts(rec.get("timestamp", ""))
        if ts:
            if analysis.first_ts is None or ts < analysis.first_ts:
                analysis.first_ts = ts
            if analysis.last_ts is None or ts > analysis.last_ts:
                analysis.last_ts = ts
            if since and ts < since:
                continue
            if until and ts > until:
                continue

        msg = rec.get("message", "")
        logger = rec.get("logger", "")
        level = rec.get("level", "")
        stream = rec.get("stream")

        include = True
        if stream_filter:
            if logger == "ingest.scheduler":
                sess = active_ingest_session(analysis.sessions, ts)
                include = sess is not None and sess.stream_id == stream_filter
            elif stream:
                include = stream == stream_filter
            elif logger == "scanner" and msg == "discovered live":
                include = rec.get("stream") == stream_filter
            else:
                include = False

        if include:
            analysis.message_counts[msg] += 1
            analysis.level_counts[level] += 1

        if logger == "scanner" and msg == "discovered live":
            analysis.discovered[rec.get("stream", "")] = rec.get("title", "")

        if logger == "ingest" or (logger == "ingest.rtmp" and stream):
            # Always record ingest started for scheduler time attribution.
            if msg == "ingest started" or not stream_filter or stream == stream_filter:
                ingest_event(rec, ts, analysis.sessions, analysis.discovered)

        if logger == "ingest.scheduler":
            scheduler_event(rec, ts, analysis.sessions)

        if not include:
            continue

        if level == "warn" and logger not in ("ingest", "ingest.scheduler", "ingest.rtmp"):
            analysis.global_warnings.append(f"{fmt_ts(ts)} [{logger}] {msg}")
        if level == "error" and logger not in ("ingest", "ingest.scheduler", "ingest.rtmp"):
            analysis.global_errors.append(f"{fmt_ts(ts)} [{logger}] {msg}")

    analysis.sessions = finalize_sessions(analysis.sessions, analysis.last_ts)

    if stream_filter:
        analysis.sessions = [s for s in analysis.sessions if s.stream_id == stream_filter]
        analysis.discovered = {
            k: v for k, v in analysis.discovered.items() if k == stream_filter
        }

    return analysis


def live_lag_summary(lags: list[int]) -> dict[str, Any]:
    behind = [x for x in lags if x >= 0]
    ahead = [x for x in lags if x < 0]
    at_threshold = [x for x in behind if x >= 4000]
    return {
        "probes": len(lags),
        "behind": stats([float(x) for x in behind]),
        "ahead": stats([float(x) for x in ahead]),
        "at_threshold": len(at_threshold),
    }


def render_live_lag(lags: list[int]) -> list[str]:
    if not lags:
        return []
    summary = live_lag_summary(lags)
    lines = [
        f"  Live lag:    probes={summary['probes']} "
        f"at_threshold(≥4s)={summary['at_threshold']}"
    ]
    behind = summary["behind"]
    if behind:
        lines.append(
            f"    behind live: mean={behind['mean']:.0f}ms "
            f"median={behind.get('median', behind['mean']):.0f}ms "
            f"max={behind['max']:.0f}ms"
        )
    ahead = summary["ahead"]
    if ahead:
        lines.append(
            f"    ahead of live: {ahead['count']} probes "
            f"(head past branch cliff; min={ahead['min']:.0f}ms)"
        )
    return lines


def resync_intervals(times: list[datetime]) -> list[float]:
    if len(times) < 2:
        return []
    return [(times[i] - times[i - 1]).total_seconds() for i in range(1, len(times))]


def analyze_duplicates(parts: list[tuple[datetime, int]]) -> dict[str, Any]:
    counts: Counter[int] = Counter(ms for _, ms in parts)
    dups = {ms: c for ms, c in counts.items() if c > 1}
    extra = sum(c - 1 for c in counts.values() if c > 1)
    return {
        "total": len(parts),
        "unique_ts": len(counts),
        "dup_timestamps": len(dups),
        "extra_publishes": extra,
        "dups": dups,
    }


def analyze_pacing(parts: list[tuple[datetime, int]]) -> dict[str, Any]:
    if len(parts) < 2:
        return {}
    wall = (parts[-1][0] - parts[0][0]).total_seconds()
    media_span = (parts[-1][1] - parts[0][1]) / 1000
    rate = len(parts) / wall if wall > 0 else 0.0
    drift_per_12min = (1.0 - rate) * 720 if wall > 0 else 0.0

    intervals: list[float] = []
    for i in range(1, len(parts)):
        dt = (parts[i][0] - parts[i - 1][0]).total_seconds()
        if 0.5 < dt < 2.0:
            intervals.append(dt)

    media_deltas = [parts[i][1] - parts[i - 1][1] for i in range(1, len(parts))]
    bad_media = sum(1 for d in media_deltas if d != 1000)

    return {
        "wall_s": wall,
        "media_span_s": media_span,
        "segments": len(parts),
        "publish_rate": rate,
        "drift_per_12min_s": drift_per_12min,
        "intervals": stats(intervals),
        "non_1s_media_steps": bad_media,
    }


def heartbeat_emit_windows(
    heartbeats: list[tuple[datetime, int, int]], *, count: int = 5
) -> list[dict[str, float]]:
    if len(heartbeats) < 2:
        return []
    windows: list[dict[str, float]] = []
    j = len(heartbeats) - 1
    for _ in range(count):
        i = 0
        for k in range(j - 1, -1, -1):
            if (heartbeats[j][0] - heartbeats[k][0]).total_seconds() >= 59:
                i = k
                break
        dw = (heartbeats[j][0] - heartbeats[i][0]).total_seconds()
        ds = heartbeats[j][1] - heartbeats[i][1]
        if dw >= 59 and ds > 0:
            windows.append(
                {
                    "start": heartbeats[i][0],
                    "duration_s": dw,
                    "segments": ds,
                    "rate": ds / dw,
                }
            )
        j = i
        if j <= 0:
            break
    return windows


def probe_quarters(probes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(probes) < 4:
        return []
    n = len(probes)
    q = n // 4
    out: list[dict[str, Any]] = []
    labels = ["Q1", "Q2", "Q3", "Q4"]
    for i, label in enumerate(labels):
        seg = probes[i * q : (i + 1) * q if i < 3 else n]
        if len(seg) < 2:
            continue
        dt = (seg[-1]["ts"] - seg[0]["ts"]).total_seconds()
        lag0 = seg[0]["lag_ms"]
        lag1 = seg[-1]["lag_ms"]
        head0, head1 = seg[0].get("head_ms"), seg[-1].get("head_ms")
        live0, live1 = seg[0].get("live_ms"), seg[-1].get("live_ms")
        gap_rate = 0.0
        if dt > 0 and all(isinstance(x, int) for x in (head0, head1, live0, live1)):
            gap_rate = ((live1 - live0) - (head1 - head0)) / 1000 / dt
        out.append(
            {
                "label": label,
                "lag_start": lag0,
                "lag_end": lag1,
                "duration_s": dt,
                "gap_rate": gap_rate,
                "probes": len(seg),
            }
        )
    return out


def probe_drift_windows(
    probes: list[dict[str, Any]], catchups: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if len(probes) < 2:
        return []
    bounds: list[Optional[datetime]] = [None]
    for cu in catchups:
        ts = cu.get("ts")
        if isinstance(ts, datetime):
            bounds.append(ts)
    bounds.append(datetime(9999, 12, 31, tzinfo=timezone.utc))

    windows: list[dict[str, Any]] = []
    for w in range(len(bounds) - 1):
        t0, t1 = bounds[w], bounds[w + 1]
        win = [
            p
            for p in probes
            if (t0 is None or p["ts"] > t0) and p["ts"] < t1
        ]
        if len(win) < 2:
            continue
        dt = (win[-1]["ts"] - win[0]["ts"]).total_seconds()
        lag0, lag1 = win[0]["lag_ms"], win[-1]["lag_ms"]
        head0, head1 = win[0].get("head_ms"), win[-1].get("head_ms")
        live0, live1 = win[0].get("live_ms"), win[-1].get("live_ms")
        gap_rate = 0.0
        head_rate = live_rate = 0.0
        if dt > 0 and all(isinstance(x, int) for x in (head0, head1, live0, live1)):
            head_rate = (head1 - head0) / 1000 / dt
            live_rate = (live1 - live0) / 1000 / dt
            gap_rate = (live_rate - head_rate)
        windows.append(
            {
                "index": w + 1,
                "start": win[0]["ts"],
                "end": win[-1]["ts"],
                "probes": len(win),
                "duration_s": dt,
                "lag_start": lag0,
                "lag_end": lag1,
                "lag_delta_ms": lag1 - lag0,
                "gap_rate": gap_rate,
                "head_rate": head_rate,
                "live_rate": live_rate,
            }
        )
    return windows


def flood_waits_by_minute(failures: list[dict[str, Any]]) -> dict[str, int]:
    by_min: Counter[str] = Counter()
    for f in failures:
        ts = f.get("ts")
        if isinstance(ts, datetime):
            by_min[ts.strftime("%H:%M")] += 1
    return dict(sorted(by_min.items()))


def fetch_lead_ms(
    parts: list[tuple[datetime, int]],
    requests: list[tuple[datetime, int]],
) -> dict[int, int]:
    """How far ahead getFile requested time_ms is vs published part time_ms."""
    leads: Counter[int] = Counter()
    for pts, pms in parts:
        req_ms = pms + REBUFFER_MS
        candidates = [r for r in requests if r[1] == req_ms and r[0] <= pts]
        if candidates:
            leads[candidates[-1][1] - pms] += 1
    return dict(sorted(leads.items()))


def excess_lag_summary(excess: list[int]) -> dict[str, Any]:
    if not excess:
        return {}
    return {
        "count": len(excess),
        "min": min(excess),
        "max": max(excess),
        "mean": statistics.mean(excess),
        "near_threshold": sum(1 for e in excess if e >= CATCHUP_NEAR_EXCESS_MS),
        "at_threshold": sum(1 for e in excess if e >= CATCHUP_EXCESS_THRESHOLD_MS),
    }


def session_verdict(sess: StreamSession) -> dict[str, Any]:
    """Pass/fail checks used in manual log reviews."""
    issues: list[str] = []
    warnings: list[str] = []

    dup = analyze_duplicates(sess.parts)
    pacing = analyze_pacing(sess.parts)
    excess_sum = excess_lag_summary(sess.live_edge_excess_lags)

    parts_gap = sess.parts_in - sess.segments_out
    if parts_gap != 0:
        issues.append(f"parts_in − segments_out = {parts_gap:+d}")
    if dup["dup_timestamps"] > 0:
        issues.append(f"{dup['dup_timestamps']} duplicate time_ms ({dup['extra_publishes']} extra publishes)")
    if sess.live_edge_catchups:
        periodic = sum(1 for c in sess.live_edge_catchups if c.get("reason") == "periodic")
        issues.append(
            f"{len(sess.live_edge_catchups)} live-edge catch-up(s)"
            + (f" ({periodic} periodic)" if periodic else "")
        )
    if sess.live_edge_cooldown_skips > 0:
        warnings.append(f"{sess.live_edge_cooldown_skips} catch-up(s) skipped due to cooldown")

    rate = pacing.get("publish_rate", 0.0)
    if pacing and abs(rate - TARGET_PUBLISH_RATE) > PUBLISH_RATE_TOLERANCE:
        drift = pacing.get("drift_per_12min_s", 0.0)
        if abs(drift) > DRIFT_WARN_PER_12MIN_S:
            issues.append(f"publish rate {rate:.4f}/s (drift {drift:+.1f} s per 12 min)")
        else:
            warnings.append(f"publish rate {rate:.4f}/s (minor drift {drift:+.1f} s per 12 min)")

    if excess_sum.get("at_threshold", 0) > 0:
        issues.append(
            f"{excess_sum['at_threshold']} probe(s) at catch-up threshold "
            f"(excess≥{CATCHUP_EXCESS_THRESHOLD_MS}ms)"
        )
    elif excess_sum.get("near_threshold", 0) > 0:
        warnings.append(
            f"{excess_sum['near_threshold']} probe(s) near catch-up "
            f"(excess≥{CATCHUP_NEAR_EXCESS_MS}ms)"
        )

    if sess.max_parts_gap > 0:
        issues.append(f"heartbeat parts/segments gap peaked at {sess.max_parts_gap}")

    backward = [g for g in sess.timeline_gaps_ms if g < 0]
    if backward:
        warnings.append(f"{len(backward)} backward time_ms jumps in part stream")

    big_forward = [g for g in sess.timeline_gaps_ms if g >= 30000]
    if big_forward:
        warnings.append(f"{len(big_forward)} branch cliff(s) ≥30s in part timeline")

    status = "PASS"
    if issues:
        status = "FAIL"
    elif warnings:
        status = "WARN"

    return {
        "status": status,
        "issues": issues,
        "warnings": warnings,
        "publish_rate": rate,
        "drift_per_12min_s": pacing.get("drift_per_12min_s"),
        "catchups": len(sess.live_edge_catchups),
        "dup_timestamps": dup["dup_timestamps"],
    }


def render_pacing_section(sess: StreamSession) -> list[str]:
    lines: list[str] = []
    dup = analyze_duplicates(sess.parts)
    pacing = analyze_pacing(sess.parts)
    if not pacing:
        return lines

    w = lines.append
    w("  Pacing & publish rate")
    w(
        f"    segments={pacing['segments']:,}  wall={pacing['wall_s']:.1f}s  "
        f"telegram span={pacing['media_span_s']:.0f}s"
    )
    w(
        f"    publish rate={pacing['publish_rate']:.4f}/s  "
        f"drift={pacing['drift_per_12min_s']:+.1f} s per 12 min"
    )
    iv = pacing.get("intervals", {})
    if iv:
        w(
            f"    part interval: mean={iv['mean']:.4f}s "
            f"min={iv['min']:.3f}s max={iv['max']:.3f}s (n={int(iv['count'])})"
        )
    w(
        f"    unique time_ms={dup['unique_ts']:,}  dup_ts={dup['dup_timestamps']}  "
        f"extra publishes={dup['extra_publishes']}"
    )
    if dup["dups"]:
        for ms, cnt in sorted(dup["dups"].items())[:8]:
            w(f"      dup {ms}: {cnt}x")
        if len(dup["dups"]) > 8:
            w(f"      ... and {len(dup['dups']) - 8} more")
    if pacing.get("non_1s_media_steps", 0):
        w(f"    non-1s time_ms steps: {pacing['non_1s_media_steps']}")
    return lines


def render_probe_section(sess: StreamSession) -> list[str]:
    if not sess.probes:
        return []
    lines: list[str] = []
    w = lines.append
    w("  Live-edge probes")

    raw = stats([float(p["lag_ms"]) for p in sess.probes])
    w(
        f"    raw lag_ms: min={raw['min']:.0f} max={raw['max']:.0f} "
        f"mean={raw['mean']:.0f} (n={int(raw['count'])})"
    )
    excess_vals = [p["excess_lag_ms"] for p in sess.probes if p.get("excess_lag_ms") is not None]
    if excess_vals:
        ex = excess_lag_summary([int(x) for x in excess_vals])
        w(
            f"    excess_lag_ms: min={ex['min']} max={ex['max']} mean={ex['mean']:.0f}  "
            f"near≥{CATCHUP_NEAR_EXCESS_MS}ms={ex['near_threshold']}  "
            f"at≥{CATCHUP_EXCESS_THRESHOLD_MS}ms={ex['at_threshold']}"
        )

    for q in probe_quarters(sess.probes):
        w(
            f"    {q['label']}: lag {q['lag_start']}→{q['lag_end']} over {q['duration_s']:.0f}s  "
            f"gap_rate={q['gap_rate']:+.5f}/s"
        )

    drift_windows = probe_drift_windows(sess.probes, sess.live_edge_catchups)
    if drift_windows:
        w("    drift windows (between catch-ups):")
        for win in drift_windows:
            w(
                f"      #{win['index']} {win['start'].strftime('%H:%M:%S')}→"
                f"{win['end'].strftime('%H:%M:%S')}  lag {win['lag_start']}→{win['lag_end']} "
                f"({win['lag_delta_ms']:+d}ms)  gap_rate={win['gap_rate']:+.5f}/s"
            )

    w("    last probes:")
    for p in sess.probes[-8:]:
        ex = p.get("excess_lag_ms")
        ex_s = str(ex) if ex is not None else "?"
        w(f"      {p['ts'].strftime('%H:%M:%S')}  lag={p['lag_ms']}  excess={ex_s}")

    if sess.live_edge_cooldown_skips:
        w(f"    cooldown skips: {sess.live_edge_cooldown_skips}")
    return lines


def render_emit_windows(sess: StreamSession) -> list[str]:
    windows = heartbeat_emit_windows(sess.heartbeats)
    if not windows:
        return []
    lines = ["  60s emit windows (newest first):"]
    for win in windows:
        lines.append(
            f"    {win['start'].strftime('%H:%M:%S')}  +{win['duration_s']:.0f}s  "
            f"segs={win['segments']:.0f}  rate={win['rate']:.5f}/s"
        )
    return lines


def render_flood_wait_section(sess: StreamSession) -> list[str]:
    if not sess.getfile_failures_detail:
        return []
    lines = [
        f"  getFile failures: {sum(sess.getfile_failures.values())} "
        f"by_outcome={dict(sess.getfile_failures)}"
    ]
    if sess.flood_waits:
        s = stats([float(x) for x in sess.flood_waits])
        lines.append(
            f"    FLOOD_WAIT: {len(sess.flood_waits)} events  "
            f"mean={s['mean']:.0f}s max={s['max']:.0f}s"
        )
    by_min = flood_waits_by_minute(sess.getfile_failures_detail)
    if by_min:
        lines.append(f"    by minute: {by_min}")
    return lines


def render_segment_flow(sess: StreamSession) -> list[str]:
    if not sess.parts:
        return []
    lines = ["  Segment flow"]
    leads = fetch_lead_ms(sess.parts, sess.getfile_requested)
    if leads:
        lines.append(f"    fetch head − published part (time_ms): {leads}")
    lines.append(
        f"    getFile req/ok: {len(sess.getfile_requested)}/{len(sess.getfile_ok)}"
    )
    return lines


def render_verdict_section(sess: StreamSession) -> list[str]:
    v = session_verdict(sess)
    lines = [f"  Verdict: {v['status']}"]
    for item in v["issues"]:
        lines.append(f"    ISSUE: {item}")
    for item in v["warnings"]:
        lines.append(f"    WARN:  {item}")
    if v["status"] == "PASS":
        lines.append(
            f"    publish={v['publish_rate']:.4f}/s  catchups={v['catchups']}  "
            f"dup_ts={v['dup_timestamps']}"
        )
    return lines


def render_end_to_end_lag(sess: StreamSession) -> list[str]:
    if not sess.probes:
        return []
    raw = stats([float(p["lag_ms"]) for p in sess.probes])
    ingest_lo = max(2000, raw["min"])
    ingest_hi = raw["max"]
    return [
        "  End-to-end lag estimate",
        f"    TGTV ingest (raw):     ~{ingest_lo/1000:.0f}–{ingest_hi/1000:.0f} s",
        "    MediaMTX packaging:      +~1 s",
        f"    Live-edge HLS client:    ~{(ingest_lo+1000)/1000:.0f}–{(ingest_hi+1000)/1000:.0f} s behind Telegram",
        "    VLC (playlist start):    +up to 45 s (hlsSegmentCount × 1s DVR window)",
    ]


def render_report(analysis: LogAnalysis) -> str:
    lines: list[str] = []
    w = lines.append

    w("TGTV log analysis")
    w("=" * 72)
    w(f"File span:     {fmt_ts(analysis.first_ts)} → {fmt_ts(analysis.last_ts)}")
    if analysis.first_ts and analysis.last_ts:
        w(f"Duration:      {fmt_dur((analysis.last_ts - analysis.first_ts).total_seconds())}")
    w(f"Lines:         {analysis.lines_parsed:,} parsed / {analysis.lines_total:,} total")
    if analysis.parse_errors:
        w(f"Parse errors:  {analysis.parse_errors:,}")

    w("")
    w("Log levels")
    for level, count in analysis.level_counts.most_common():
        w(f"  {level or '(none)':8s} {count:8,}  ({pct(count, analysis.lines_parsed)})")

    w("")
    w("Top messages")
    for msg, count in analysis.message_counts.most_common(15):
        w(f"  {count:8,}  {msg}")

    if analysis.discovered:
        w("")
        w(f"Discovered streams ({len(analysis.discovered)})")
        for sid, title in sorted(analysis.discovered.items()):
            w(f"  {sid}  {title}")

    if not analysis.sessions:
        w("")
        w("No ingest sessions found in selected range.")
        return "\n".join(lines)

    w("")
    w(f"Ingest sessions ({len(analysis.sessions)})")
    w("-" * 72)

    for sess in analysis.sessions:
        title = f" — {sess.title}" if sess.title else ""
        w("")
        w(f"Stream {sess.stream_id}{title}")
        w(f"  Period:      {fmt_ts(sess.started_at)} → {fmt_ts(sess.ended_at)}")
        if sess.started_at and sess.ended_at:
            w(f"  Duration:    {fmt_dur((sess.ended_at - sess.started_at).total_seconds())}")
        if sess.unified is not None:
            w(f"  Mode:        unified={sess.unified} bootstrap={sess.bootstrap or '?'}")

        parts_gap = sess.parts_in - sess.segments_out
        w(f"  Throughput:  parts_in={sess.parts_in:,} segments_out={sess.segments_out:,} "
          f"delta={parts_gap:+d} max_heartbeat_gap={sess.max_parts_gap}")

        for section in (
            render_pacing_section,
            render_emit_windows,
            render_probe_section,
            render_segment_flow,
            render_flood_wait_section,
            render_end_to_end_lag,
        ):
            block = section(sess)
            if block:
                w("")
                for line in block:
                    w(line)

        if sess.since_last_part:
            s = stats(sess.since_last_part)
            w(
                f"  Part pacing: mean={fmt_dur(s['mean'])} median={fmt_dur(s.get('median', s['mean']))} "
                f"max={fmt_dur(s['max'])} (since_last_part from heartbeats)"
            )
            slow = sum(1 for v in sess.since_last_part if v > 2.0)
            if slow:
                w(f"  Slow parts:  {slow} heartbeats with since_last_part > 2s "
                  f"({pct(slow, len(sess.since_last_part))})")

        tg_iv = resync_intervals(sess.telegram_resyncs)
        sch_iv = resync_intervals(sess.scheduler_resyncs)
        w(f"  Resyncs:     telegram={len(sess.telegram_resyncs)} scheduler={len(sess.scheduler_resyncs)}")
        if tg_iv:
            s = stats(tg_iv)
            w(
                f"    telegram interval: mean={fmt_dur(s['mean'])} median={fmt_dur(s.get('median', s['mean']))} "
                f"min={fmt_dur(s['min'])} max={fmt_dur(s['max'])}"
            )
        if sch_iv:
            s = stats(sch_iv)
            w(
                f"    scheduler interval: mean={fmt_dur(s['mean'])} median={fmt_dur(s.get('median', s['mean']))} "
                f"min={fmt_dur(s['min'])} max={fmt_dur(s['max'])}"
            )
            burst = sum(1 for v in sch_iv if v < 2.0)
            if burst:
                w(f"    scheduler bursts: {burst} resyncs < 2s apart ({pct(burst, len(sch_iv))})")

        if sess.live_edge_catchups:
            w(f"  Live edge:   {len(sess.live_edge_catchups)} catch-up events")
            reasons = Counter(c.get("reason", "") for c in sess.live_edge_catchups)
            w(f"    reasons: {dict(reasons)}")
            for cu in sess.live_edge_catchups[:5]:
                ts = cu.get("ts")
                ts_s = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else "?"
                w(
                    f"    catch-up {ts_s} lag={cu.get('lag_ms')} "
                    f"excess={cu.get('excess_lag_ms')} reason={cu.get('reason')}"
                )

        if sess.getfile_failures and not sess.getfile_failures_detail:
            w(f"  getFile:     failures={sum(sess.getfile_failures.values())} "
              f"by_outcome={dict(sess.getfile_failures)}")
        if sess.flood_waits and not sess.getfile_failures_detail:
            s = stats([float(x) for x in sess.flood_waits])
            w(
                f"  FLOOD_WAIT:  {len(sess.flood_waits)} events "
                f"mean={s['mean']:.0f}s max={s['max']:.0f}s"
            )

        if sess.timeline_gaps_ms:
            forward = [g for g in sess.timeline_gaps_ms if g > 1000]
            backward = [g for g in sess.timeline_gaps_ms if g < 0]
            w(f"  Timeline:    {len(forward)} forward gaps >1s, {len(backward)} backward jumps")
            if forward:
                s = stats([float(g) for g in forward])
                w(
                    f"    forward gaps: mean={s['mean']:.0f}ms median={s.get('median', s['mean']):.0f}ms "
                    f"max={s['max']:.0f}ms"
                )
                big = [g for g in forward if g >= 30000]
                if big:
                    w(f"    branch cliffs (≥30s): {len(big)} "
                      f"values={[f'{g/1000:.0f}s' for g in big[:8]]}{'...' if len(big) > 8 else ''}")

        if sess.ffmpeg_discontinuities:
            s = stats([float(x) for x in sess.ffmpeg_discontinuities])
            # values are in 100ns units from ffmpeg; divide by 1e7 for seconds
            w(f"  FFmpeg TS:   {len(sess.ffmpeg_discontinuities)} discontinuities "
              f"(mean {s['mean']/1e7:.1f}s, max {s['max']/1e7:.1f}s)")

        if sess.stale_drops:
            w(f"  Stale drops: {sess.stale_drops} parts dropped after resync")

        if sess.recovery_deferred:
            w(f"  Recovery:    deferred={dict(sess.recovery_deferred)}")

        if sess.warnings:
            w(f"  Warnings:    {len(sess.warnings)} (showing up to 5)")
            for item in sess.warnings[:5]:
                w(f"    {item}")
        if sess.errors:
            w(f"  Errors:      {len(sess.errors)} (showing up to 5)")
            for item in sess.errors[:5]:
                w(f"    {item}")

        w("")
        for line in render_verdict_section(sess):
            w(line)

    if analysis.global_warnings:
        w("")
        w(f"Other warnings ({len(analysis.global_warnings)}, first 5)")
        for item in analysis.global_warnings[:5]:
            w(f"  {item}")

    if analysis.global_errors:
        w("")
        w(f"Other errors ({len(analysis.global_errors)}, first 5)")
        for item in analysis.global_errors[:5]:
            w(f"  {item}")

    return "\n".join(lines)


def to_json(analysis: LogAnalysis) -> dict[str, Any]:
    def sess_dict(sess: StreamSession) -> dict[str, Any]:
        return {
            "stream_id": sess.stream_id,
            "title": sess.title,
            "started_at": sess.started_at.isoformat() if sess.started_at else None,
            "ended_at": sess.ended_at.isoformat() if sess.ended_at else None,
            "parts_in": sess.parts_in,
            "segments_out": sess.segments_out,
            "parts_segments_delta": sess.parts_in - sess.segments_out,
            "max_heartbeat_parts_gap": sess.max_parts_gap,
            "since_last_part": stats(sess.since_last_part),
            "pacing": analyze_pacing(sess.parts),
            "duplicates": {
                k: v
                for k, v in analyze_duplicates(sess.parts).items()
                if k != "dups"
            },
            "emit_windows_60s": [
                {
                    "start": w["start"].isoformat(),
                    "duration_s": w["duration_s"],
                    "segments": w["segments"],
                    "rate": w["rate"],
                }
                for w in heartbeat_emit_windows(sess.heartbeats)
            ],
            "probe_quarters": probe_quarters(sess.probes),
            "probe_drift_windows": [
                {
                    **{k: v for k, v in win.items() if k not in ("start", "end")},
                    "start": win["start"].isoformat(),
                    "end": win["end"].isoformat(),
                }
                for win in probe_drift_windows(sess.probes, sess.live_edge_catchups)
            ],
            "excess_lag": excess_lag_summary(
                [int(x) for x in sess.live_edge_excess_lags]
            ),
            "telegram_resync_count": len(sess.telegram_resyncs),
            "telegram_resync_intervals": stats(resync_intervals(sess.telegram_resyncs)),
            "scheduler_resync_count": len(sess.scheduler_resyncs),
            "scheduler_resync_intervals": stats(resync_intervals(sess.scheduler_resyncs)),
            "live_edge_catchups": len(sess.live_edge_catchups),
            "live_edge_cooldown_skips": sess.live_edge_cooldown_skips,
            "live_edge_lag": live_lag_summary(sess.live_edge_lags),
            "getfile_failures": dict(sess.getfile_failures),
            "flood_waits": stats([float(x) for x in sess.flood_waits]),
            "flood_waits_by_minute": flood_waits_by_minute(sess.getfile_failures_detail),
            "fetch_lead_ms": fetch_lead_ms(sess.parts, sess.getfile_requested),
            "timeline_gaps_ms": stats([float(g) for g in sess.timeline_gaps_ms if g > 1000]),
            "ffmpeg_discontinuities": stats([float(x) for x in sess.ffmpeg_discontinuities]),
            "stale_drops": sess.stale_drops,
            "recovery_deferred": dict(sess.recovery_deferred),
            "verdict": session_verdict(sess),
        }

    return {
        "span": {
            "first": analysis.first_ts.isoformat() if analysis.first_ts else None,
            "last": analysis.last_ts.isoformat() if analysis.last_ts else None,
        },
        "lines_parsed": analysis.lines_parsed,
        "parse_errors": analysis.parse_errors,
        "level_counts": dict(analysis.level_counts),
        "top_messages": analysis.message_counts.most_common(30),
        "discovered": analysis.discovered,
        "sessions": [sess_dict(s) for s in analysis.sessions],
    }


def render_verdict_only(analysis: LogAnalysis) -> str:
    lines: list[str] = ["TGTV log verdict", "=" * 72]
    if analysis.first_ts and analysis.last_ts:
        lines.append(
            f"Span: {fmt_ts(analysis.first_ts)} → {fmt_ts(analysis.last_ts)} "
            f"({fmt_dur((analysis.last_ts - analysis.first_ts).total_seconds())})"
        )
    if not analysis.sessions:
        lines.append("No ingest sessions found.")
        return "\n".join(lines)
    for sess in analysis.sessions:
        title = f" — {sess.title}" if sess.title else ""
        lines.append("")
        lines.append(f"Stream {sess.stream_id}{title}")
        if sess.started_at and sess.ended_at:
            lines.append(f"  Duration: {fmt_dur((sess.ended_at - sess.started_at).total_seconds())}")
        for line in render_verdict_section(sess):
            lines.append(line)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze TGTV JSON logs")
    parser.add_argument("log", type=Path, help="Path to tgtv.log (JSON lines)")
    parser.add_argument("--stream", help="Limit analysis to one stream ID")
    parser.add_argument("--since", help="Only include lines at or after this UTC time (ISO8601)")
    parser.add_argument("--until", help="Only include lines at or before this UTC time (ISO8601)")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    parser.add_argument(
        "--verdict-only",
        action="store_true",
        help="Print only pass/fail verdict per session",
    )
    args = parser.parse_args()

    if not args.log.is_file():
        print(f"error: not a file: {args.log}", file=sys.stderr)
        return 1

    since = parse_ts(args.since) if args.since else None
    until = parse_ts(args.until) if args.until else None

    with args.log.open(encoding="utf-8", errors="replace") as fh:
        analysis = analyze_lines(
            fh,
            stream_filter=args.stream,
            since=since,
            until=until,
        )
    analysis.path = str(args.log)

    if args.json:
        print(json.dumps(to_json(analysis), indent=2))
    elif args.verdict_only:
        print(render_verdict_only(analysis))
    else:
        print(render_report(analysis))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
