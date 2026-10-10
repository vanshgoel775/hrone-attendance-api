# Employee Attendance & Analytics API

FastAPI + MongoDB. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # or export MONGO_URI / MONGO_DB (real environment variables win over .env)
uvicorn app.main:app --port 8000
```
Needs MongoDB 6.0+ (uses `$setWindowFields` and `$densify`). Indexes are created at startup, idempotently.

## What is here
* Part A: fixes to the starter, listed in `REVIEW.md`.
* Part B: punch-out, PATCH regularization (with audit history), monthly / department-summary / late-leaderboard / trend analytics (aggregation pipelines).
* Part C: `GET /admin/explain/{endpoint}` and `DECISIONS.md`.
* `tests/check_against_reference.py`: seeds awkward cases into a throwaway database, compares every analytics endpoint with a brute-force reference and fires parallel requests (`ALLOW_WIPE=1 python tests/check_against_reference.py`; add `--big` for the 100k explain check). It deletes the two collections in `MONGO_DB`.

## Decisions on things the contract leaves open
* `moving_avg_7d` averages the **rounded** (4-decimal) daily rates that the API reports, then rounds half-up again.
* A PATCH that loses a concurrent-edit race gets **409** (nothing is overwritten, no history entry is lost).
* A PATCH "changes nothing" check looks at `status`, `punch_in` and `punch_out`; a request that changes none of those is 422.
* Analytics read the stored `work_hours`, `late_minutes`, `overtime_minutes` and `half_day`; they never recompute them.
