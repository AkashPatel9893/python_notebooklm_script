# NCERT → NotebookLM batch pipeline

Drives the chapters listed in a Google Sheet through NotebookLM: uploads each
chapter PDF, generates study artifacts, and downloads everything into a tidy
`Class / Subject / Chapter` folder tree. Fully resumable — interrupt and re-run
any time; finished work is skipped, failures are retried.

## Setup

```bash
# from the repo root
python3 -m venv .venv && source .venv/bin/activate
pip install -e .            # installs the local notebooklm-py + httpx

python3 -m pip install "notebooklm-py[browser]"

python3 -m playwright install chromium

notebooklm login           # one-time Google auth (already done if auth check passes)
notebooklm auth check --test --json   # expect "status": "ok", "token_fetch": true
```

## Usage

```bash
cd pipeline

python run.py sync         # pull the sheet → progress.csv (adds new rows)
python run.py generate     # phase 1: download PDFs, create notebooks, generate artifacts
python run.py download     # phase 2: download artifacts to ./output/...
python run.py status       # print a progress table
python run.py all          # generate then download
```

Useful flags on `generate` / `download` / `all`:

| Flag | Meaning |
|------|---------|
| `--concurrency N` | chapters processed in parallel (default 3) |
| `--max-attempts N` | retries per row before giving up (default 3) |
| `--only-failed` | only (re)process rows in `failed`/`partial` state |
| `--limit N` | cap the number of rows (handy for a test run) |
| `--artifacts a,b` | override which artifacts to make (default: `quiz,flashcards,mind_map,slide_deck,audio,cinematic_video,infographic`) |

Examples:

```bash
python run.py generate --limit 1 --artifacts quiz          # quick smoke test
python run.py generate --only-failed                       # retry just the failures
python run.py download --concurrency 5
```

## What gets tracked (`progress.csv`)

`progress.csv` mirrors the sheet (Class/Subject/Book/Chapter/PDF URL/Index) and
adds tracking columns — it's the single source of truth, safe to re-upload to
Google Sheets:

- `notebook_id`, `source_id` — the NotebookLM notebook + uploaded source per chapter
- `gen_status`, `dl_status` — overall state: `pending` / `done` / `partial` / `failed`
- `gen_attempts`, `dl_attempts`, `gen_error`, `dl_error` — retry bookkeeping
- per artifact: `<name>_id`, `<name>_gen`, `<name>_dl` (e.g. `quiz_id`, `slide_deck_gen`, `cinematic_video_dl`)

## Output layout

```
output/
  Class 11/
    Chemistry/
      Chapter 1/
        source.pdf
        quiz.json
        flashcards.json
        mind_map.json
        slides.pdf
        audio.mp3
        cinematic_video.mp4
        infographic.png
```

## Notes

- One notebook per (class, subject, chapter); the chapter PDF is uploaded as its source.
- Phase 1 waits for each artifact to finish so `gen_status` is accurate; chapters
  run concurrently, so one slow chapter never blocks the others.
- Audio/quiz/flashcards generation is rate-limited by Google and may fail —
  just re-run `generate --only-failed` later.
- The NCERT host (`ncert.nic.in`) resets the bare apex domain; the downloader
  automatically retries on `www.ncert.nic.in`.
- Configurable defaults (sheet ID, artifact set, timeouts, paths) live at the
  top of `run.py`.
```
