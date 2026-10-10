# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (duplicate guard, join key); `(department, emp_code)` for the filtered, sorted list; `(department, joined_on)` and `joined_on` for headcount. `attendance_logs`: unique `(emp_code, date)` (race guard, monthly lookups); `(date desc, emp_code)` for the list sort and month ranges; `(emp_code, punch_in desc)` for finding the record to punch out. Rejected: an index with `status`; it is low-cardinality and slows every write.

2. **Punch-in race.** Both requests find the employee and compute the same date, then call `insert_one`. The unique `(emp_code, date)` index lets one insert win; the other gets `DuplicateKeyError`, which I return as 409. There is no check-then-insert.

3. **Ties.** I rank with `$rank` over total late minutes and keep `rank <= limit`, so everyone tied at the cutoff is returned, even if that is more than `limit` rows, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (joined on or before month end) and `$lookup`s their logs, so someone with no logs still counts as one.

5. **100x data.** I would stop recomputing from raw logs: keep daily per-department rollups and shard `attendance_logs` by `emp_code`.
