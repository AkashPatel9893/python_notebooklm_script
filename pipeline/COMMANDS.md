# Commands — copy & paste

Step-by-step commands to run the NCERT → NotebookLM pipeline. Run them in the
**Terminal** app (or VS Code terminal). Copy each block exactly.

---

## 0. One-time setup (already done — only if starting fresh)

```bash
cd /Users/akash/Desktop/notebooklm-py
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
notebooklm login          # opens browser, sign in to Google once
```

---

## 1. Activate the environment (do this EVERY new terminal)

> Without this you'll get `zsh: command not found: python`.

```bash
cd /Users/akash/Desktop/notebooklm-py
source .venv/bin/activate
cd pipeline
```

Check it worked — your prompt should show `(.venv)` and this prints a path:

```bash
which python
```

If you'd rather skip activation, use the full path instead of `python` in every
command below:
`/Users/akash/Desktop/notebooklm-py/.venv/bin/python run.py ...`

---

## 2. Confirm auth is good

```bash
notebooklm auth check --test --json
```

Look for `"status": "ok"` and `"token_fetch": true`.
If it fails: `notebooklm login`

### Log out / switch Google account

```bash
notebooklm auth logout        # clear the current account's saved login
notebooklm login              # sign in again (can be a different account)
```

`auth logout` removes the saved cookie file (`storage_state.json`) and cached
browser profile for the active profile. To switch accounts without losing the
first one, use named profiles instead: `notebooklm profile create work`, then
`notebooklm -p work login`.

---

## 3. Pull the sheet into the tracker

```bash
python run.py sync
```

Creates/updates `progress.csv` (one row per class/subject/chapter).

---

## 4. Phase 1 — generate content

```bash
python run.py generate
```

- Creates one NotebookLM notebook per chapter, uploads the PDF, and generates
  all 6 artifacts: quiz, flashcards, mind map, slide deck, audio, cinematic video.
- Chapters run 3 at a time. Leave it running — it can take a while
  (audio/quiz/flashcards are rate-limited by Google).

### Watch progress (open a SECOND terminal, then activate as in step 1)

```bash
python run.py status
```

or live log:

```bash
tail -f /Users/akash/Desktop/notebooklm-py/pipeline/pipeline.log
```

### Retry only what failed (run as many times as needed)

```bash
python run.py generate --only-failed
```

---

## 5. Phase 2 — download everything to disk

```bash
python run.py download
```

Files land here:

```
pipeline/output/<Class>/<Subject>/<Chapter>/
    source.pdf
    quiz.json
    flashcards.json
    mind_map.json
    slides.pdf
    audio.mp3
    cinematic_video.mp4
```

Retry failed downloads:

```bash
python run.py download --only-failed
```

---

## 6. Do both phases in one go

```bash
python run.py all
```

---

## Handy options (add to `generate` / `download` / `all`)

| Command | What it does |
|---------|--------------|
| `python run.py generate --concurrency 1` | safest — one chapter at a time (fewest rate-limit errors) |
| `python run.py generate --concurrency 5` | faster — more parallel, more retries likely |
| `python run.py generate --limit 1` | process just 1 chapter (quick test) |
| `python run.py generate --artifacts study_guide,quiz` | only make these artifact types |
| `python run.py generate --only-failed` | retry only rows that failed |
| `python run.py generate --max-attempts 0` | never give up — retry genuinely-failed rows forever |
| `python run.py generate --max-attempts 10` | allow up to 10 tries on a genuinely-failed row |
| `python run.py status` | print the progress table |

Valid artifact names for `--artifacts`:
`quiz, flashcards, mind_map, slide_deck, audio, cinematic_video, study_guide, video, infographic, data_table`
(default set: `quiz, flashcards, mind_map, slide_deck, audio, cinematic_video`)

---

## Where everything lives

| Path | What |
|------|------|
| `pipeline/progress.csv` | tracker: notebook IDs + per-artifact status (re-upload to Google Sheets anytime) |
| `pipeline/output/` | downloaded files in Class/Subject/Chapter folders |
| `pipeline/pipeline.log` | full run log |
| `pipeline/run.py` | the pipeline (config defaults at the top) |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `zsh: command not found: python` | You skipped step 1 — `source .venv/bin/activate` (or use the full `.venv/bin/python` path). |
| Auth error / cookie expired | `notebooklm login`, then re-run. |
| Generation failed (rate limit) | Normal for audio/quiz/flashcards. Re-run `python run.py generate --only-failed`. |
| PDF download fails | The script auto-retries `www.ncert.nic.in`; if it still fails the host may be down — re-run later. |
| Want to start a chapter over | Edit `progress.csv`: clear that row's `notebook_id` and `*_gen` cells, then re-run. |
| `Another pipeline run is active (PID …)` | Only one `generate`/`download` can run at a time (they share `progress.csv`). Wait for the other to finish. If none is really running, delete `pipeline/.pipeline.lock`. |

## Good to know

- **No duplicates.** Before generating, the pipeline asks NotebookLM whether that
  artifact already exists and *adopts* it (or waits on one still generating)
  instead of making a second copy. This also covers artifacts NotebookLM
  auto-creates when a source is added. Safe to re-run anytime.
- **One run at a time.** A lock file blocks a second simultaneous `generate`/
  `download`. Read-only `status` always works.
- **Crash-safe.** Notebook id, source id, and each artifact id are written to
  `progress.csv` *before* waiting, so an interrupted run resumes by adopting,
  not regenerating.
- **Rate limits ≠ failures.** A Google rate-limit/quota rejection is recorded as
  `rate_limited` (not `failed`). It stays retryable forever — it never counts
  toward the `--max-attempts` give-up — so just re-run `generate` (or
  `generate --only-failed`) later and it'll pick up where it left off. Only a
  genuine error (bad source, real generation failure) becomes `failed`.
- **`--max-attempts` only limits genuinely-failed rows.** Default 3: after a row
  *really* fails 3 times it's skipped (so a broken PDF doesn't loop forever).
  It never applies to `rate_limited`. Pass `--max-attempts 0` to **never give
  up** on anything. Raise it (e.g. `--max-attempts 10`) for more retries.

### Status values you'll see

| `gen_status` / `_gen` | Meaning |
|---|---|
| `done` | generated (or adopted) successfully |
| `partial` | some artifacts done, some not |
| `rate_limited` | hit Google's rate limit — transient, will retry |
| `failed` | a real error — retried up to `--max-attempts` |
| `pending` | not started yet |
```
