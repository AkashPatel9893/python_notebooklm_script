#!/usr/bin/env python3
"""NCERT → NotebookLM batch pipeline.

Two phases, fully resumable, with per-chapter / per-artifact tracking in a
local CSV (``progress.csv``) that mirrors the source Google Sheet and adds
tracking columns.

    python run.py sync       # pull the sheet into progress.csv (adds new rows)
    python run.py generate   # phase 1: upload PDFs + generate artifacts
    python run.py download   # phase 2: download generated artifacts to disk
    python run.py status     # print a progress summary
    python run.py all        # generate, then download

Phase 1 creates one NotebookLM notebook per (class, subject, chapter), uploads
the chapter PDF as a source, and generates the configured artifacts. Chapters
are processed CONCURRENTLY (bounded by --concurrency) so we never block one
chapter waiting on another. Every step's outcome is written back to
progress.csv immediately, so the run can be interrupted and re-run at any time:
finished work is skipped, failed work is retried up to --max-attempts.

Phase 2 downloads every completed artifact into:

    output/<Class>/<Subject>/<Chapter>/
        source.pdf
        study_guide.md
        quiz.json
        flashcards.json
        mind_map.json
        audio.mp3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from notebooklm import ArtifactType, NotebookLMClient

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SHEET_ID = "1pvR_vpZjeEK9d3UxuFaFGX0GS_S8ObUu_dS6jWmlKeE"
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / "output"
PROGRESS_CSV = HERE / "progress.csv"
LOG_FILE = HERE / "pipeline.log"

LANGUAGE = "en"

# Which artifacts to generate per chapter. Any subset of ARTIFACT_SPECS keys.
ARTIFACTS: list[str] = [
    "quiz",
    "flashcards",
    "mind_map",
    "slide_deck",
    "audio",
    "cinematic_video",
]

# Defaults (overridable on the CLI).
DEFAULT_CONCURRENCY = 3          # chapters generated in parallel
DEFAULT_MAX_ATTEMPTS = 3         # retries per row before giving up
SOURCE_READY_TIMEOUT = 600.0     # wait for PDF to finish indexing

# --------------------------------------------------------------------------- #
# Artifact registry: how to generate + download each type.
#   generate: coroutine fn(client, notebook_id, source_ids) -> id_str
#   download: coroutine fn(client, notebook_id, out_path, artifact_id) -> path
#   filename: output file name inside the chapter folder
#   timeout : seconds to wait for generation to complete
# --------------------------------------------------------------------------- #


async def _gen_study_guide(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_study_guide(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_quiz(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_quiz(nb, sids)
    return s.task_id


async def _gen_flashcards(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_flashcards(nb, sids)
    return s.task_id


async def _gen_audio(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_audio(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_video(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_video(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_cinematic_video(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    # Veo 3 cinematic video — ~30-40 min, requires a Google AI Ultra subscription.
    s = await c.artifacts.generate_cinematic_video(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_infographic(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_infographic(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_slide_deck(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_slide_deck(nb, sids, LANGUAGE)
    return s.task_id


async def _gen_data_table(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    s = await c.artifacts.generate_data_table(
        nb, sids, LANGUAGE,
        instructions="Extract the key facts, definitions and figures as a table.",
    )
    return s.task_id


async def _gen_mind_map(c: NotebookLMClient, nb: str, sids: list[str]) -> str:
    # Synchronous-style: returns a MindMapResult, no completion polling needed.
    r = await c.artifacts.generate_mind_map(nb, sids, LANGUAGE)
    return getattr(r, "note_id", None) or "mind_map"


ARTIFACT_SPECS: dict[str, dict[str, Any]] = {
    "study_guide": {
        "generate": _gen_study_guide,
        "download": lambda c, nb, p, aid: c.artifacts.download_report(nb, p, aid),
        "filename": "study_guide.md",
        "timeout": 900.0,
        "sync_gen": False,
        "type": ArtifactType.REPORT,
        "report_subtype": "study_guide",
    },
    "quiz": {
        "generate": _gen_quiz,
        "download": lambda c, nb, p, aid: c.artifacts.download_quiz(nb, p, aid, "json"),
        "filename": "quiz.json",
        "timeout": 900.0,
        "sync_gen": False,
        "type": ArtifactType.QUIZ,
    },
    "flashcards": {
        "generate": _gen_flashcards,
        "download": lambda c, nb, p, aid: c.artifacts.download_flashcards(nb, p, aid, "json"),
        "filename": "flashcards.json",
        "timeout": 900.0,
        "sync_gen": False,
        "type": ArtifactType.FLASHCARDS,
    },
    "mind_map": {
        "generate": _gen_mind_map,
        # note-backed mind map: auto-pick by passing artifact_id=None
        "download": lambda c, nb, p, aid: c.artifacts.download_mind_map(nb, p, None),
        "filename": "mind_map.json",
        "timeout": 0.0,
        "sync_gen": True,
        "type": ArtifactType.MIND_MAP,
    },
    "audio": {
        "generate": _gen_audio,
        "download": lambda c, nb, p, aid: c.artifacts.download_audio(nb, p, aid),
        "filename": "audio.mp3",
        "timeout": 1500.0,
        "sync_gen": False,
        "type": ArtifactType.AUDIO,
    },
    "video": {
        "generate": _gen_video,
        "download": lambda c, nb, p, aid: c.artifacts.download_video(nb, p, aid),
        "filename": "video.mp4",
        "timeout": 2700.0,
        "sync_gen": False,
        "type": ArtifactType.VIDEO,
    },
    "cinematic_video": {
        "generate": _gen_cinematic_video,
        "download": lambda c, nb, p, aid: c.artifacts.download_video(nb, p, aid),
        "filename": "cinematic_video.mp4",
        "timeout": 3600.0,  # Veo 3: ~30-40 min
        "sync_gen": False,
        "type": ArtifactType.VIDEO,
    },
    "infographic": {
        "generate": _gen_infographic,
        "download": lambda c, nb, p, aid: c.artifacts.download_infographic(nb, p, aid),
        "filename": "infographic.png",
        "timeout": 1200.0,
        "sync_gen": False,
        "type": ArtifactType.INFOGRAPHIC,
    },
    "slide_deck": {
        "generate": _gen_slide_deck,
        "download": lambda c, nb, p, aid: c.artifacts.download_slide_deck(nb, p, aid),
        "filename": "slides.pdf",
        "timeout": 1200.0,
        "sync_gen": False,
        "type": ArtifactType.SLIDE_DECK,
    },
    "data_table": {
        "generate": _gen_data_table,
        "download": lambda c, nb, p, aid: c.artifacts.download_data_table(nb, p, aid),
        "filename": "data_table.csv",
        "timeout": 900.0,
        "sync_gen": False,
        "type": ArtifactType.DATA_TABLE,
    },
}

# --------------------------------------------------------------------------- #
# CSV schema. Tracking columns cover only the ACTIVE artifacts (ARTIFACTS),
# so the tracker stays lean. If you add/remove an artifact from ARTIFACTS the
# schema follows on the next write (old columns are dropped, new ones added).
# --------------------------------------------------------------------------- #

SHEET_COLS = ["class", "subject", "book", "chapter", "pdf_url", "book_index_url"]
BASE_COLS = [
    "notebook_id", "source_id",
    "gen_status", "gen_attempts", "gen_error",
    "dl_status", "dl_attempts", "dl_error",
    "updated_at",
]


def artifact_cols() -> list[str]:
    cols: list[str] = []
    for name in ARTIFACTS:  # only the artifacts we actually generate
        cols += [f"{name}_id", f"{name}_gen", f"{name}_dl"]
    return cols


FIELDNAMES = SHEET_COLS + BASE_COLS + artifact_cols()

log = logging.getLogger("pipeline")

LOCK_FILE = HERE / ".pipeline.lock"


# --------------------------------------------------------------------------- #
# Single-run lock: prevents two `generate`/`download` processes from sharing
# (and clobbering) progress.csv at the same time.
# --------------------------------------------------------------------------- #


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


def acquire_lock() -> None:
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip() or "0")
        except ValueError:
            pid = 0
        if _pid_alive(pid):
            sys.exit(
                f"Another pipeline run is active (PID {pid}). Wait for it to finish, "
                f"or stop it. If you're certain none is running, delete {LOCK_FILE}."
            )
    LOCK_FILE.write_text(str(os.getpid()))


def release_lock() -> None:
    try:
        if LOCK_FILE.exists() and LOCK_FILE.read_text().strip() == str(os.getpid()):
            LOCK_FILE.unlink()
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# State store: progress.csv as the single source of truth
# --------------------------------------------------------------------------- #


def row_key(row: dict[str, str]) -> str:
    return f"{row['class'].strip()}|{row['subject'].strip()}|{row['chapter'].strip()}"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    """In-memory mirror of progress.csv with atomic, lock-guarded writes."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, str]] = {}
        self._lock = asyncio.Lock()

    def load(self) -> None:
        if not PROGRESS_CSV.exists():
            return
        with PROGRESS_CSV.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                # Backfill any new columns added since the file was written.
                for col in FIELDNAMES:
                    r.setdefault(col, "")
                self.rows[row_key(r)] = r

    def sync_from_sheet(self) -> int:
        """Pull the sheet; add new rows, refresh sheet-derived columns. Returns #added."""
        resp = httpx.get(SHEET_CSV_URL, follow_redirects=True, timeout=60.0)
        resp.raise_for_status()
        reader = csv.DictReader(resp.text.splitlines())
        added = 0
        for raw in reader:
            row = {
                "class": (raw.get("Class") or "").strip(),
                "subject": (raw.get("Subject") or "").strip(),
                "book": (raw.get("Book") or "").strip(),
                "chapter": (raw.get("Chapter") or "").strip(),
                "pdf_url": (raw.get("PDF URL") or "").strip(),
                "book_index_url": (raw.get("Book Index URL") or "").strip(),
            }
            if not row["class"] or not row["chapter"]:
                continue
            key = row_key(row)
            if key in self.rows:
                self.rows[key].update(row)  # refresh sheet columns
                continue
            new = {c: "" for c in FIELDNAMES}
            new.update(row)
            new["gen_status"] = "pending"
            new["dl_status"] = "pending"
            new["gen_attempts"] = "0"
            new["dl_attempts"] = "0"
            self.rows[key] = new
            added += 1
        self._write()
        return added

    def _write(self) -> None:
        PROGRESS_CSV.parent.mkdir(parents=True, exist_ok=True)
        tmp = PROGRESS_CSV.with_suffix(".csv.tmp")
        with tmp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            w.writeheader()
            for r in self.rows.values():
                w.writerow(r)
        os.replace(tmp, PROGRESS_CSV)

    async def save(self) -> None:
        async with self._lock:
            self._write()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def chapter_dir(row: dict[str, str]) -> Path:
    return OUTPUT_DIR / row["class"] / row["subject"] / row["chapter"]


_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _url_candidates(url: str) -> list[str]:
    """Return host variants to try. Some hosts (e.g. ncert.nic.in) reset the
    bare apex but serve fine on the www. subdomain — try both."""
    cands = [url]
    if "://www." in url:
        cands.append(url.replace("://www.", "://", 1))
    else:
        cands.append(url.replace("://", "://www.", 1))
    return cands


async def download_pdf(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    headers = {"User-Agent": _UA}
    last_err: Exception | None = None
    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0, headers=headers) as hc:
        for cand in _url_candidates(url):
            try:
                async with hc.stream("GET", cand) as resp:
                    resp.raise_for_status()
                    with tmp.open("wb") as f:
                        async for chunk in resp.aiter_bytes():
                            f.write(chunk)
                os.replace(tmp, dest)
                return
            except Exception as e:  # noqa: BLE001 — try next candidate
                last_err = e
                tmp.unlink(missing_ok=True)
    raise RuntimeError(f"PDF download failed for {url}: {type(last_err).__name__}: {last_err}")


async def find_existing_artifact(
    client: NotebookLMClient, nb_id: str, spec: dict[str, Any]
) -> Any | None:
    """Ask NotebookLM whether this artifact already exists in the notebook.

    Guards against duplicate generation when a previous run was interrupted
    mid-flight (progress.csv not yet updated). Returns the artifact to *adopt*:
    a completed one, or one still processing/pending (so we wait on it instead
    of firing a second generation). Returns None when only failed/no artifacts
    exist, so a fresh generation (i.e. a real retry) proceeds.
    """
    arts = await client.artifacts.list(nb_id, spec["type"])
    sub = spec.get("report_subtype")
    if sub:
        arts = [a for a in arts if getattr(a, "report_subtype", None) == sub]
    if not arts:
        return None
    completed = [a for a in arts if a.is_completed]
    if completed:
        return completed[0]
    in_flight = [a for a in arts if a.status in (1, 2)]  # 1=processing, 2=pending
    return in_flight[0] if in_flight else None


_RL_MARKERS = (
    "rate limit", "ratelimit", "rate-limit", "quota", "resource_exhausted",
    "resource exhausted", "too many requests", "user_displayable_error", "429",
)


def _looks_rate_limited(e: Exception) -> bool:
    """Heuristic: did this exception come from a Google rate limit / quota cap?"""
    code = getattr(e, "error_code", None) or getattr(e, "code", None) or ""
    text = f"{code} {e}".lower()
    return any(m in text for m in _RL_MARKERS)


def roll_up(row: dict[str, str], kind: str, active: list[str]) -> str:
    """Overall status from per-artifact cells:
    'done' (all done) > 'partial' (some done) > 'rate_limited' (transient, retryable)
    > 'failed' (a real failure)."""
    field = "_gen" if kind == "gen" else "_dl"
    states = [row.get(f"{a}{field}", "") for a in active]
    if states and all(s == "done" for s in states):
        return "done"
    if any(s == "done" for s in states):
        return "partial"
    if any(s == "rate_limited" for s in states) and not any(s == "failed" for s in states):
        return "rate_limited"
    return "failed"


# --------------------------------------------------------------------------- #
# Phase 1: generate
# --------------------------------------------------------------------------- #


async def generate_row(
    client: NotebookLMClient,
    store: Store,
    row: dict[str, str],
    active: list[str],
    sem: asyncio.Semaphore,
    max_attempts: int,
    nb_index: dict[str, str],
) -> None:
    key = row_key(row)
    async with sem:
        # Skip if everything we want is already generated.
        if all(row.get(f"{a}_gen") == "done" for a in active):
            row["gen_status"] = "done"
            await store.save()
            log.info("[%s] gen already complete — skip", key)
            return
        # Give up ONLY on a genuinely failed row after max_attempts tries.
        # max_attempts <= 0 means "never give up". rate_limited/partial rows are
        # never skipped here — only a real 'failed'.
        if (
            max_attempts > 0
            and int(row.get("gen_attempts") or 0) >= max_attempts
            and row.get("gen_status") == "failed"
        ):
            log.warning("[%s] gen at max attempts (%s) — skip (raise --max-attempts "
                        "or use 0 for unlimited)", key, max_attempts)
            return

        row["gen_attempts"] = str(int(row.get("gen_attempts") or 0) + 1)
        row["gen_error"] = ""
        row["updated_at"] = now()
        await store.save()

        try:
            # 1. PDF on disk (also lands in the chapter folder for phase 2).
            pdf_path = chapter_dir(row) / "source.pdf"
            log.info("[%s] downloading PDF", key)
            await download_pdf(row["pdf_url"], pdf_path)

            # 2. Notebook. Reuse our stored id; else adopt an existing notebook
            #    with the same title (recovers from a crash right after create);
            #    else create one.
            title = f"{row['class']} | {row['subject']} | {row['chapter']}"
            if not row.get("notebook_id"):
                if title in nb_index:
                    row["notebook_id"] = nb_index[title]
                    log.info("[%s] adopted existing notebook %s", key, nb_index[title])
                else:
                    nb = await client.notebooks.create(title)
                    row["notebook_id"] = nb.id
                    nb_index[title] = nb.id
                    log.info("[%s] created notebook %s", key, nb.id)
                await store.save()
            nb_id = row["notebook_id"]

            # 3. Source. Reuse stored id; else adopt an already-uploaded source
            #    in the notebook; else upload. Always confirm it is READY.
            if not row.get("source_id"):
                existing = await client.sources.list(nb_id)
                if existing:
                    row["source_id"] = existing[0].id
                    log.info("[%s] adopted existing source %s", key, existing[0].id)
                else:
                    log.info("[%s] uploading source", key)
                    src = await client.sources.add_file(
                        nb_id, pdf_path, wait=True, wait_timeout=SOURCE_READY_TIMEOUT
                    )
                    row["source_id"] = src.id
                await store.save()
            await client.sources.wait_until_ready(
                nb_id, row["source_id"], timeout=SOURCE_READY_TIMEOUT
            )
            log.info("[%s] source ready %s", key, row["source_id"])
            sids = [row["source_id"]]

            # 4. Artifacts (sequential within a chapter; chapters run in parallel).
            #    Before generating, ASK NotebookLM whether the artifact already
            #    exists — adopt a completed one, or wait on one still in flight,
            #    instead of firing a duplicate generation (idempotent on re-run).
            for name in active:
                if row.get(f"{name}_gen") == "done":
                    continue
                spec = ARTIFACT_SPECS[name]
                try:
                    existing_art = await find_existing_artifact(client, nb_id, spec)
                    if existing_art is not None and existing_art.is_completed:
                        row[f"{name}_id"] = existing_art.id
                        row[f"{name}_gen"] = "done"
                        log.info("[%s] %s already present — adopted %s",
                                 key, name, existing_art.id)
                        continue

                    if existing_art is not None:
                        art_id = existing_art.id
                        log.info("[%s] %s already in flight — waiting on %s",
                                 key, name, art_id)
                    else:
                        log.info("[%s] generating %s", key, name)
                        art_id = await spec["generate"](client, nb_id, sids)
                    # Persist the id BEFORE waiting, so a crash mid-wait lets the
                    # next run adopt this artifact instead of regenerating it.
                    row[f"{name}_id"] = art_id or ""
                    await store.save()

                    if not spec["sync_gen"]:
                        final = await client.artifacts.wait_for_completion(
                            nb_id, art_id, timeout=spec["timeout"]
                        )
                        if not final.is_complete:
                            # Rate-limit / quota rejection is transient, not a real
                            # failure — flag it distinctly so it stays retryable and
                            # never counts toward the give-up threshold.
                            if final.is_rate_limited or final.is_removed:
                                row[f"{name}_gen"] = "rate_limited"
                                row["gen_error"] = f"{name}: rate-limited ({final.status})"
                                log.warning("[%s] %s RATE-LIMITED — will retry", key, name)
                                continue
                            raise RuntimeError(
                                f"{name} ended {final.status} "
                                f"({final.error or final.error_code or 'no detail'})"
                            )
                    row[f"{name}_gen"] = "done"
                    log.info("[%s] %s done", key, name)
                except Exception as e:  # noqa: BLE001 — record + continue per artifact
                    if _looks_rate_limited(e):
                        row[f"{name}_gen"] = "rate_limited"
                        row["gen_error"] = f"{name}: rate-limited ({e})"[:500]
                        log.warning("[%s] %s RATE-LIMITED — will retry", key, name)
                    else:
                        row[f"{name}_gen"] = "failed"
                        row["gen_error"] = f"{name}: {e}"[:500]
                        log.error("[%s] %s FAILED: %s", key, name, e)
                finally:
                    row["updated_at"] = now()
                    await store.save()

            row["gen_status"] = roll_up(row, "gen", active)
        except Exception as e:  # noqa: BLE001 — row-level failure (PDF/notebook/source)
            row["gen_status"] = "failed"
            row["gen_error"] = str(e)[:500]
            log.error("[%s] row FAILED: %s", key, e)
        finally:
            row["updated_at"] = now()
            await store.save()
        log.info("[%s] gen_status=%s", key, row["gen_status"])


async def phase_generate(args: argparse.Namespace) -> None:
    active = parse_artifacts(args.artifacts)
    store = Store()
    store.load()
    store.sync_from_sheet()

    targets = select_rows(store, args, "gen", active)
    if not targets:
        log.info("nothing to generate")
        return
    log.info("generating %d chapter(s), %d concurrent, artifacts=%s",
             len(targets), args.concurrency, ",".join(active))

    sem = asyncio.Semaphore(args.concurrency)
    async with NotebookLMClient.from_storage() as client:
        # Title→id map of existing notebooks, so an interrupted prior run can
        # adopt its notebook instead of creating a duplicate.
        nb_index = {nb.title: nb.id for nb in await client.notebooks.list()}
        await asyncio.gather(*[
            generate_row(client, store, row, active, sem, args.max_attempts, nb_index)
            for row in targets
        ])
    summarize(store)


# --------------------------------------------------------------------------- #
# Phase 2: download
# --------------------------------------------------------------------------- #


async def download_row(
    client: NotebookLMClient,
    store: Store,
    row: dict[str, str],
    active: list[str],
    sem: asyncio.Semaphore,
    max_attempts: int,
) -> None:
    key = row_key(row)
    async with sem:
        if not row.get("notebook_id"):
            return
        if all(row.get(f"{a}_dl") == "done" for a in active
               if row.get(f"{a}_gen") == "done"):
            row["dl_status"] = roll_up(row, "dl", [a for a in active if row.get(f"{a}_gen") == "done"]) or "done"
        # retry guard (max_attempts <= 0 means never give up)
        if (
            max_attempts > 0
            and int(row.get("dl_attempts") or 0) >= max_attempts
            and row.get("dl_status") == "failed"
        ):
            log.warning("[%s] dl at max attempts — skip", key)
            return

        row["dl_attempts"] = str(int(row.get("dl_attempts") or 0) + 1)
        row["dl_error"] = ""
        nb_id = row["notebook_id"]
        cdir = chapter_dir(row)
        cdir.mkdir(parents=True, exist_ok=True)

        downloadable = [a for a in active if row.get(f"{a}_gen") == "done"]
        for name in downloadable:
            spec = ARTIFACT_SPECS[name]
            out = cdir / spec["filename"]
            if row.get(f"{name}_dl") == "done" and out.exists() and out.stat().st_size > 0:
                continue
            try:
                log.info("[%s] downloading %s", key, name)
                await spec["download"](client, nb_id, str(out), row.get(f"{name}_id") or None)
                row[f"{name}_dl"] = "done"
            except Exception as e:  # noqa: BLE001
                row[f"{name}_dl"] = "failed"
                row["dl_error"] = f"{name}: {e}"[:500]
                log.error("[%s] download %s FAILED: %s", key, name, e)
            finally:
                row["updated_at"] = now()
                await store.save()

        row["dl_status"] = roll_up(row, "dl", downloadable) if downloadable else "pending"
        row["updated_at"] = now()
        await store.save()
        log.info("[%s] dl_status=%s", key, row["dl_status"])


async def phase_download(args: argparse.Namespace) -> None:
    active = parse_artifacts(args.artifacts)
    store = Store()
    store.load()
    if not store.rows:
        store.sync_from_sheet()

    targets = select_rows(store, args, "dl", active)
    targets = [r for r in targets if r.get("notebook_id")]
    if not targets:
        log.info("nothing to download (run `generate` first)")
        return
    log.info("downloading %d chapter(s), %d concurrent", len(targets), args.concurrency)

    sem = asyncio.Semaphore(args.concurrency)
    async with NotebookLMClient.from_storage() as client:
        await asyncio.gather(*[
            download_row(client, store, row, active, sem, args.max_attempts)
            for row in targets
        ])
    summarize(store)


# --------------------------------------------------------------------------- #
# Selection / reporting
# --------------------------------------------------------------------------- #


def parse_artifacts(spec: str | None) -> list[str]:
    if not spec:
        return list(ARTIFACTS)
    names = [s.strip() for s in spec.split(",") if s.strip()]
    bad = [n for n in names if n not in ARTIFACT_SPECS]
    if bad:
        sys.exit(f"unknown artifact(s): {bad}; valid: {list(ARTIFACT_SPECS)}")
    return names


def select_rows(
    store: Store, args: argparse.Namespace, phase: str, active: list[str]
) -> list[dict[str, str]]:
    """Pick rows to process, judged against the *currently active* artifact set
    (not the rolled-up status, which reflects whatever set ran last)."""

    def is_done(r: dict[str, str]) -> bool:
        if phase == "gen":
            return all(r.get(f"{a}_gen") == "done" for a in active)
        # download: every generated artifact in the active set is downloaded
        gen_ok = [a for a in active if r.get(f"{a}_gen") == "done"]
        return bool(gen_ok) and all(r.get(f"{a}_dl") == "done" for a in gen_ok)

    def has_failure(r: dict[str, str]) -> bool:
        field = "_gen" if phase == "gen" else "_dl"
        return any(r.get(f"{a}{field}") in ("failed", "rate_limited") for a in active)

    rows = list(store.rows.values())
    if args.only_failed:
        rows = [r for r in rows if has_failure(r)]
    else:
        rows = [r for r in rows if not is_done(r)]
    if args.limit:
        rows = rows[: args.limit]
    return rows


def summarize(store: Store) -> None:
    rows = list(store.rows.values())
    def tally(field: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in rows:
            out[r.get(field) or "pending"] = out.get(r.get(field) or "pending", 0) + 1
        return out
    log.info("── summary ──  total=%d  gen=%s  dl=%s",
             len(rows), tally("gen_status"), tally("dl_status"))


def cmd_status(_: argparse.Namespace) -> None:
    store = Store()
    store.load()
    if not store.rows:
        print("No progress.csv yet. Run `python run.py sync`.")
        return
    print(f"{'CLASS':<9} {'SUBJECT':<12} {'CHAPTER':<11} {'GEN':<8} {'DL':<8} {'NOTEBOOK'}")
    for r in store.rows.values():
        print(f"{r['class']:<9} {r['subject']:<12} {r['chapter']:<11} "
              f"{r.get('gen_status',''):<8} {r.get('dl_status',''):<8} {r.get('notebook_id','')}")
    summarize(store)


def cmd_sync(_: argparse.Namespace) -> None:
    store = Store()
    store.load()
    added = store.sync_from_sheet()
    print(f"Synced sheet → {PROGRESS_CSV} ({added} new row(s), {len(store.rows)} total)")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help="chapters processed in parallel")
    p.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS,
                   help="tries on a genuinely-failed row before giving up (0 = never give up). "
                        "Does NOT apply to rate-limited rows, which always retry.")
    p.add_argument("--only-failed", action="store_true",
                   help="only (re)process rows in failed/partial state")
    p.add_argument("--limit", type=int, default=0, help="cap number of rows (0 = all)")
    p.add_argument("--artifacts", type=str, default="",
                   help=f"comma list (default: {','.join(ARTIFACTS)}); valid: {','.join(ARTIFACT_SPECS)}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(LOG_FILE, encoding="utf-8")],
    )
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("sync", help="pull sheet into progress.csv").set_defaults(fn=cmd_sync)
    sub.add_parser("status", help="print progress summary").set_defaults(fn=cmd_status)

    def locked(coro_factory: Callable[[argparse.Namespace], Awaitable[None]]):
        """Run an async phase under the single-run lock."""
        def runner(ns: argparse.Namespace) -> None:
            acquire_lock()
            try:
                asyncio.run(coro_factory(ns))
            finally:
                release_lock()
        return runner

    g = sub.add_parser("generate", help="phase 1: upload + generate")
    add_common(g)
    g.set_defaults(fn=locked(phase_generate))

    d = sub.add_parser("download", help="phase 2: download artifacts")
    add_common(d)
    d.set_defaults(fn=locked(phase_download))

    a = sub.add_parser("all", help="generate then download")
    add_common(a)

    async def _all(ns: argparse.Namespace) -> None:
        await phase_generate(ns)
        await phase_download(ns)

    a.set_defaults(fn=locked(_all))

    args = parser.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
