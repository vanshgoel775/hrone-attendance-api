"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (real environment variables win over a local .env)

Layout of this single file
  1. configuration and database handles
  2. time helpers and the business rules R1-R5 (pure functions, no I/O)
  3. request models (validation) and response serialisers
  4. indexes (created idempotently at startup)
  5. employees, attendance, analytics and admin/explain endpoints

Conventions
  * Instants are BSON datetimes in MongoDB (UTC, timezone-aware because the client is
    created with tz_aware=True) and integer epoch milliseconds at the HTTP boundary.
  * Calendar dates / months / shift times are plain strings (IST), as in DATA_MODEL.md.
  * Analytics are aggregation pipelines over the STORED derived values. Anything that must
    be rounded half-up is carried through the pipeline as an exact integer (hundredths /
    ten-thousandths / half-days) and only turned into a decimal number in Python.
"""
import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

load_dotenv()  # override=False: variables already set in the environment win

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration
# --------------------------------------------------------------------------- #
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,  # PyMongo would otherwise hand back naive datetimes
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
EMPLOYEES = db["employees"]
LOGS = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

MS_MIN = 100_000_000_000  # below this a value "looks like seconds" -> 422
MS_MAX = 4_102_444_800_000  # 2100-01-01

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
NON_PRESENCE = ["ABSENT", "LEAVE"]
GRACE_SECONDS = 10 * 60  # R2
MIN_OVERTIME_MINUTES = 30  # R3
HALF_DAY_HUNDREDTHS = 450  # R5: rounded work_hours < 4.50
MAX_SHIFT = timedelta(hours=24)

_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_MONTH_RE = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
_MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
_HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


# --------------------------------------------------------------------------- #
# 2. Time helpers and business rules
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime (exact, no float arithmetic)."""
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Datetime (aware, or naive meaning UTC) -> integer epoch milliseconds."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def instant_from_ms(ms: int) -> datetime:
    """R1: instants are truncated to whole seconds before any calculation."""
    return ms_to_dt(ms - ms % 1000)


def now_instant() -> datetime:
    return instant_from_ms(dt_to_ms(datetime.now(UTC)))


def as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def valid_date(value: str) -> bool:
    if not _DATE_RE.match(value):
        return False
    try:
        date_cls.fromisoformat(value)
    except ValueError:
        return False
    return True


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def shift_moment(day: str, hhmm: str) -> datetime:
    """The IST instant at HH:MM on the IST calendar date `day`."""
    d = date_cls.fromisoformat(day)
    return datetime(d.year, d.month, d.day, int(hhmm[:2]), int(hhmm[3:5]), tzinfo=IST)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning
    punch-in (earlier than shift_end) belongs to the shift that started the day before."""
    ist = punch_in.astimezone(IST)
    day = ist.date()
    if is_overnight(shift_start, shift_end) and ist.strftime("%H:%M") < shift_end:
        day -= timedelta(days=1)
    return day.isoformat()


def compute_late_minutes(punch_in: datetime, day: str, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; then floor minutes
    since shift_start (not since the end of the grace period)."""
    seconds = int((punch_in - shift_moment(day, shift_start)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, day: str, shift_start: str, shift_end: str) -> int:
    """R3: floor minutes from shift_end to punch_out, only when that is >= 30."""
    end = shift_moment(day, shift_end)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    minutes = int((punch_out - end).total_seconds()) // 60  # floor, also for negatives
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def work_hundredths(punch_in: datetime, punch_out: datetime) -> int:
    """R4 in exact integer arithmetic: hours*100, rounded half-up."""
    seconds = int((punch_out - punch_in).total_seconds())
    return (seconds * 100 + 1800) // 3600


def derive_fields(emp: dict, day: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """The four API-computed fields for a record (DATA_MODEL.md, section 3)."""
    if punch_in is None:  # ABSENT / LEAVE
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, day, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hundredths = work_hundredths(punch_in, punch_out)
    return {
        "work_hours": hundredths / 100,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, day, emp["shift_start"], emp["shift_end"]),
        "half_day": hundredths < HALF_DAY_HUNDREDTHS,  # R5 uses the ROUNDED work_hours
    }


def round_half_up(value: Decimal, places: int) -> float:
    q = Decimal(1).scaleb(-places)
    return float(value.quantize(q, rounding=ROUND_HALF_UP))


def month_bounds(month: str) -> tuple[str, str]:
    """First and last IST calendar day of 'YYYY-MM' as 'YYYY-MM-DD' strings."""
    try:
        year, mon = int(month[:4]), int(month[5:7])
        last = calendar.monthrange(year, mon)[1]
        date_cls(year, mon, 1)
    except ValueError:
        raise unprocessable("month", "invalid month") from None
    return f"{month}-01", f"{month}-{last:02d}"


def count_weekdays(first: date_cls, last: date_cls) -> int:
    """Mon-Fri days in [first, last] (R7), by arithmetic: whole weeks * 5 + the remainder."""
    if last < first:
        return 0
    total_days = (last - first).days + 1
    weeks, extra = divmod(total_days, 7)
    count = weeks * 5
    start_weekday = first.weekday()
    count += sum(1 for i in range(extra) if (start_weekday + i) % 7 < 5)
    return count


# --------------------------------------------------------------------------- #
# 3. Models and serialisers
# --------------------------------------------------------------------------- #
EpochMs = Annotated[int, Field(strict=True, ge=MS_MIN, le=MS_MAX)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=_HHMM_PATTERN)
    shift_end: str = Field(default="18:30", pattern=_HHMM_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on_is_a_date(cls, v: str) -> str:
        if not valid_date(v):
            raise ValueError("joined_on must be a valid YYYY-MM-DD date")
        return v

    @model_validator(mode="after")
    def _shift_times_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    # Derived fields are not declared, so a client that sends them is simply ignored.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def unprocessable(field: str, message: str, where: str = "query") -> HTTPException:
    """422 with the same body shape FastAPI uses for its own validation errors."""
    return HTTPException(422, detail=[{"loc": [where, field], "msg": message, "type": "value_error"}])


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _history_value(field: str, value: Any) -> Any:
    return dt_to_ms(value) if field in ("punch_in", "punch_out") else value


def history_out(entry: dict) -> dict:
    changes = {
        field: {"from": _history_value(field, c.get("from")), "to": _history_value(field, c.get("to"))}
        for field, c in (entry.get("changes") or {}).items()
    }
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Stored attendance document -> API shape (no _id, epoch ms, defaults for legacy rows)."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 4. Application and indexes
# --------------------------------------------------------------------------- #
_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent. Each index is created independently so one bad index cannot stop startup;
    if MongoDB is unreachable we give up at once (and /health retries later) instead of
    waiting for every index in turn."""
    global _indexes_ready
    wanted = [
        # employees
        (EMPLOYEES, [("emp_code", ASCENDING)], {"unique": True, "name": "emp_code_unique"}),
        (EMPLOYEES, [("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "dept_emp_code"}),
        (EMPLOYEES, [("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "dept_joined_on"}),
        (EMPLOYEES, [("joined_on", ASCENDING)], {"name": "joined_on"}),
        # attendance_logs
        (LOGS, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "emp_date_unique"}),
        (LOGS, [("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_desc_emp"}),
        (LOGS, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_punch_in_desc"}),
    ]
    for coll, keys, opts in wanted:
        try:
            coll.create_index(keys, **opts)
        except ServerSelectionTimeoutError as exc:
            log.error("MongoDB not reachable, indexes not created yet: %s", exc)
            return
        except PyMongoError as exc:  # e.g. duplicate keys in the seeded data for a unique index
            log.error("could not create index %s on %s: %s", opts.get("name"), coll.name, exc)
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    log.exception("database error")
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable") from None
    if not _indexes_ready:  # MongoDB was down at startup: catch up as soon as it answers
        ensure_indexes()
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# 5a. Employees
# --------------------------------------------------------------------------- #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = ms_to_dt(dt_to_ms(datetime.now(UTC)))  # server clock, ms precision
    try:
        EMPLOYEES.insert_one(doc)  # the unique index decides concurrent duplicates
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists") from None
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    query = {} if department is None else {"department": department}
    total = EMPLOYEES.count_documents(query)
    cursor = EMPLOYEES.find(query).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5b. Attendance
# --------------------------------------------------------------------------- #
def get_employee_or_404(emp_code: str) -> dict:
    emp = EMPLOYEES.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = instant_from_ms(body.punched_at) if body.punched_at is not None else now_instant()
    day = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, day, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        LOGS.insert_one(doc)  # unique (emp_code, date) index: exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date") from None
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    out = instant_from_ms(body.punched_at) if body.punched_at is not None else now_instant()

    # most recent record whose punch_in is at or before the punch-out (works across midnight)
    rec = LOGS.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": out}}, sort=[("punch_in", DESCENDING)])
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")

    pin = as_utc(rec["punch_in"])
    if out <= pin:
        raise unprocessable("punched_at", "punched_at must be after the punch-in", "body")
    if out - pin > MAX_SHIFT:
        raise unprocessable("punched_at", "punched_at is more than 24 hours after the punch-in", "body")

    fields = derive_fields(emp, rec["date"], pin, out)
    fields["punch_out"] = out
    # Conditional update: only the first of several simultaneous punch-outs matches.
    updated = LOGS.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": fields},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def build_attendance_query(
    emp_code: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    status: Optional[str],
    page: int,
    page_size: int,
) -> dict:
    """Shared by GET /attendance and its explain, so the explained query is the real one."""
    for name, value in (("date_from", date_from), ("date_to", date_to)):
        if value is not None and not valid_date(value):
            raise unprocessable(name, "must be a valid YYYY-MM-DD date")
    if date_from and date_to and date_from > date_to:
        raise unprocessable("date_from", "date_from must not be after date_to")
    flt: dict[str, Any] = {}
    if emp_code is not None:
        flt["emp_code"] = emp_code
    if date_from or date_to:
        flt["date"] = {}
        if date_from:
            flt["date"]["$gte"] = date_from
        if date_to:
            flt["date"]["$lte"] = date_to
    if status is not None:
        flt["status"] = status
    return {
        "filter": flt,
        "sort": [("date", DESCENDING), ("emp_code", ASCENDING)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
    }


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = LOGS.count_documents(q["filter"])
    cursor = LOGS.find(q["filter"]).sort(q["sort"]).skip(q["skip"]).limit(q["limit"])
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: str, body: RegularizeIn):
    if not valid_date(date):
        raise unprocessable("date", "must be a valid YYYY-MM-DD date", "path")
    emp = get_employee_or_404(emp_code)
    rec = LOGS.find_one({"emp_code": emp_code, "date": date})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    old_in = as_utc(rec["punch_in"]) if rec.get("punch_in") else None
    old_out = as_utc(rec["punch_out"]) if rec.get("punch_out") else None
    new_status = body.status or rec["status"]

    if new_status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable("punch_in", f"punch times cannot be supplied with status {new_status}", "body")
        new_in = new_out = None
    else:
        new_in = instant_from_ms(body.punch_in) if body.punch_in is not None else old_in
        new_out = instant_from_ms(body.punch_out) if body.punch_out is not None else old_out
        if new_in is None:
            raise unprocessable("punch_in", f"status {new_status} requires a punch_in", "body")
        if attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != date:
            raise unprocessable("punch_in", "punch_in must stay on the record's attendance date", "body")
        if new_out is not None and (new_out <= new_in or new_out - new_in > MAX_SHIFT):
            raise unprocessable("punch_out", "punch_out must be after punch_in and within 24 hours", "body")

    if (new_status, dt_to_ms(new_in), dt_to_ms(new_out)) == (rec["status"], dt_to_ms(old_in), dt_to_ms(old_out)):
        raise unprocessable("body", "request changes nothing", "body")

    new_values = {"status": new_status, "punch_in": new_in, "punch_out": new_out}
    new_values.update(derive_fields(emp, date, new_in, new_out) if new_in else derive_fields(emp, date, None, None))
    old_values = {
        "status": rec["status"],
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day") or False),
    }
    changes = {}
    for field, new in new_values.items():
        old = old_values[field]
        same = dt_to_ms(old) == dt_to_ms(new) if field in ("punch_in", "punch_out") else old == new
        if not same:
            changes[field] = {"from": old, "to": new}

    entry = {"at": ms_to_dt(dt_to_ms(datetime.now(UTC))), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: apply only if the document is still exactly what we read
    # (including the length of its history). The loser of a race gets 409 and nothing is lost.
    history = rec.get("history")
    guard: dict[str, Any] = {
        "_id": rec["_id"],
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "history": {"$size": len(history)} if isinstance(history, list) else {"$exists": False},
    }
    updated = LOGS.find_one_and_update(
        guard,
        {"$set": new_values, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "the record was modified concurrently; retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 5c. Analytics - shared aggregation fragments
# --------------------------------------------------------------------------- #
_IS_PRESENT = {"$in": ["$status", PRESENCE]}
_IS_WEEKDAY = {
    "$lte": [
        {
            "$isoDayOfWeek": {
                "$dateFromString": {
                    "dateString": "$date",
                    "format": "%Y-%m-%d",
                    "onError": datetime(1970, 1, 4),  # a Sunday: a malformed date counts as weekend
                    "onNull": datetime(1970, 1, 4),
                }
            }
        },
        5,
    ]
}
# half-days: a full day is 2, a half day is 1, so present_days = sum / 2 stays exact
_PRESENT_HALF_DAYS = {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 1, 2]}
_LATE = {"$ifNull": ["$late_minutes", 0]}
_HAS_WORK_HOURS = {"$and": [_IS_PRESENT, {"$isNumber": "$work_hours"}]}


def stats_accumulators() -> dict:
    """$group accumulators over STORED values (never recomputed from punch times)."""
    return {
        "p2": {"$sum": {"$cond": [{"$and": [_IS_PRESENT, _IS_WEEKDAY]}, _PRESENT_HALF_DAYS, 0]}},
        "leave": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
        "on_duty": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
        "late_count": {"$sum": {"$cond": [{"$gt": [_LATE, 0]}, 1, 0]}},
        "late_total": {"$sum": _LATE},
        "ot_total": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        # work hours in exact hundredths so the mean can be rounded half-up without float noise
        "wh_sum": {"$sum": {"$cond": [_HAS_WORK_HOURS, {"$toInt": {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}}, 0]}},
        "wh_cnt": {"$sum": {"$cond": [_HAS_WORK_HOURS, 1, 0]}},
    }


def _int(value: Any) -> int:
    return int(value or 0)


# ---- employee monthly ----------------------------------------------------- #
def employee_monthly_pipeline(emp_code: str, start: str, end: str) -> list[dict]:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": None, **stats_accumulators()}},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Annotated[str, Query(pattern=_MONTH_PATTERN)]):
    start, end = month_bounds(month)
    emp = get_employee_or_404(emp_code)
    row = next(LOGS.aggregate(employee_monthly_pipeline(emp_code, start, end)), {})

    first_day = max(date_cls.fromisoformat(start), date_cls.fromisoformat(emp["joined_on"]))  # R7
    working_days = count_weekdays(first_day, date_cls.fromisoformat(end))
    present2 = _int(row.get("p2"))
    pct = None
    if working_days > 0:
        pct = round_half_up(Decimal(present2) / 2 / working_days * 100, 2)  # exact rational, R8
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present2 / 2,
        "leave_days": _int(row.get("leave")),
        "late_count": _int(row.get("late_count")),
        "total_late_minutes": _int(row.get("late_total")),
        "total_overtime_minutes": _int(row.get("ot_total")),
        "attendance_pct": pct,
    }


# ---- department summary --------------------------------------------------- #
def department_summary_pipeline(start: str, end: str, department: Optional[str]) -> list[dict]:
    emp_filter: dict[str, Any] = {"joined_on": {"$lte": end}}  # R9
    if department is not None:
        emp_filter["department"] = department
    return [
        # Start from EMPLOYEES so people with zero logs still count towards headcount.
        {"$match": emp_filter},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": start, "$lte": end}}},
                    {"$group": {"_id": None, **stats_accumulators()}},
                ],
                "as": "s",
            }
        },
        {"$addFields": {"s": {"$ifNull": [{"$first": "$s"}, {}]}}},  # no logs -> empty stats
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                **{
                    key: {"$sum": {"$ifNull": [f"$s.{key}", 0]}}
                    for key in ("p2", "leave", "on_duty", "late_count", "late_total", "wh_sum", "wh_cnt")
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: Annotated[str, Query(pattern=_MONTH_PATTERN)], department: Optional[str] = None):
    start, end = month_bounds(month)
    items = []
    for row in EMPLOYEES.aggregate(department_summary_pipeline(start, end, department)):
        wh_cnt = _int(row.get("wh_cnt"))
        avg = round_half_up(Decimal(_int(row.get("wh_sum"))) / wh_cnt / 100, 2) if wh_cnt else None
        items.append(
            {
                "department": row["_id"],
                "headcount": _int(row["headcount"]),
                "present_days": _int(row.get("p2")) / 2,
                "avg_work_hours": avg,
                "late_count": _int(row.get("late_count")),
                "total_late_minutes": _int(row.get("late_total")),
                "leave_count": _int(row.get("leave")),
                "on_duty_count": _int(row.get("on_duty")),
            }
        )
    return {"month": month, "items": items}


# ---- late leaderboard ----------------------------------------------------- #
def late_leaderboard_pipeline(start: str, end: str, limit: int, department: Optional[str]) -> list[dict]:
    pipeline: list[dict] = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},  # logs of unknown employees are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"e.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$e.name",
                "department": "$e.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: Annotated[str, Query(pattern=_MONTH_PATTERN)],
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    start, end = month_bounds(month)
    rows = LOGS.aggregate(late_leaderboard_pipeline(start, end, limit, department))
    items = [
        {
            "rank": _int(r["rank"]),
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": _int(r["total_late_minutes"]),
            "late_count": _int(r["late_count"]),
        }
        for r in rows
    ]
    return {"month": month, "items": items}


# ---- department trend ----------------------------------------------------- #
def department_trend_pipeline(codes: list[str], joined: list[str], start: str, end: str) -> list[dict]:
    start_dt = datetime.fromisoformat(start).replace(tzinfo=UTC)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=UTC)
    present_half_days = {"$cond": [_IS_PRESENT, _PRESENT_HALF_DAYS, 0]}
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": start, "$lte": end}}},
        {
            "$group": {
                "_id": "$date",
                "p2": {"$sum": present_half_days},
                "late_count": {"$sum": {"$cond": [{"$gt": [_LATE, 0]}, 1, 0]}},
            }
        },
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # Gap filling: one row per calendar day. The upper bound is pushed 2 days out and the
        # surplus rows are trimmed below, so it does not matter whether $densify treats it as
        # inclusive or exclusive.
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [start_dt, end_dt + timedelta(days=2)]}}},
        {
            "$addFields": {
                "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
                "p2": {"$ifNull": ["$p2", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
            }
        },
        {"$match": {"date": {"$lte": end}}},
        {
            "$addFields": {
                "is_wd": {"$lte": [{"$isoDayOfWeek": "$d"}, 5]},
                # R9: department employees who had joined by that day
                "headcount": {
                    "$size": {"$filter": {"input": {"$literal": joined}, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}
                },
            }
        },
        {
            "$addFields": {
                # attendance_rate in exact ten-thousandths, half-up:
                #   floor((10000*p2 + headcount) / (2*headcount))   where p2 = 2 * present_count
                "rate_i": {
                    "$cond": [
                        {"$and": ["$is_wd", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": [10000, "$p2"]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {"$addFields": {"rate_n": {"$cond": [{"$eq": ["$rate_i", None]}, 0, 1]}}},
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "win_sum": {"$sum": "$rate_i", "window": {"documents": [-6, 0]}},  # $sum ignores nulls
                    "win_n": {"$sum": "$rate_n", "window": {"documents": [-6, 0]}},
                },
            }
        },
        {"$sort": {"date": 1}},
        {"$project": {"_id": 0, "date": 1, "is_wd": 1, "headcount": 1, "p2": 1, "late_count": 1, "rate_i": 1, "win_sum": 1, "win_n": 1}},
    ]


def trend_inputs(department: str, start: str, end: str) -> tuple[list[str], list[str]]:
    for name, value in (("from", start), ("to", end)):
        if not valid_date(value):
            raise unprocessable(name, "must be a valid YYYY-MM-DD date")
    if end < start:
        raise unprocessable("to", "to must not be before from")
    if (date_cls.fromisoformat(end) - date_cls.fromisoformat(start)).days + 1 > 92:
        raise unprocessable("to", "range must not exceed 92 days")
    people = list(EMPLOYEES.find({"department": department}, {"emp_code": 1, "joined_on": 1, "_id": 0}))
    if not people:
        raise HTTPException(404, "department not found")
    return [p["emp_code"] for p in people], sorted(p["joined_on"] for p in people)


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, Query(alias="from")],
    to: str,
):
    codes, joined = trend_inputs(department, from_, to)
    items = []
    for row in LOGS.aggregate(department_trend_pipeline(codes, joined, from_, to)):
        rate_i = row.get("rate_i")
        win_n = _int(row.get("win_n"))
        moving = None
        if win_n:
            # mean of the (already rounded) daily rates, half-up to 4 decimals, in integers
            moving = ((2 * _int(row["win_sum"]) + win_n) // (2 * win_n)) / 10000
        items.append(
            {
                "date": row["date"],
                "is_working_day": bool(row["is_wd"]),
                "headcount": _int(row["headcount"]),
                "present_count": _int(row["p2"]) / 2,
                "late_count": _int(row["late_count"]),
                "attendance_rate": None if rate_i is None else int(rate_i) / 10000,
                "moving_avg_7d": moving,
            }
        )
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 5d. Admin: explain
# --------------------------------------------------------------------------- #
ExplainEndpoint = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def _require(**params: Any) -> None:
    missing = [name for name, value in params.items() if value is None]
    if missing:
        raise HTTPException(
            422,
            detail=[{"loc": ["query", name], "msg": "Field required", "type": "missing"} for name in missing],
        )


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=_MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Runs MongoDB explain (executionStats) on the same query/pipeline the real endpoint builds."""
    if endpoint == "attendance_list":
        q = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        collection = LOGS.name
        sort = {field: direction for field, direction in q["sort"]}
        find: dict[str, Any] = {"find": collection, "filter": q["filter"], "sort": sort, "limit": q["limit"]}
        if q["skip"]:
            find["skip"] = q["skip"]
        command = {"explain": find, "verbosity": "executionStats"}
    else:
        if endpoint == "employee_monthly":
            _require(emp_code=emp_code, month=month)
            start, end = month_bounds(month)
            get_employee_or_404(emp_code)
            collection, pipeline = LOGS.name, employee_monthly_pipeline(emp_code, start, end)
        elif endpoint == "department_summary":
            _require(month=month)
            start, end = month_bounds(month)
            collection, pipeline = EMPLOYEES.name, department_summary_pipeline(start, end, department)
        elif endpoint == "late_leaderboard":
            _require(month=month)
            start, end = month_bounds(month)
            collection, pipeline = LOGS.name, late_leaderboard_pipeline(start, end, limit, department)
        else:  # department_trend
            _require(department=department, **{"from": from_}, to=to)
            codes, joined = trend_inputs(department, from_, to)
            collection, pipeline = LOGS.name, department_trend_pipeline(codes, joined, from_, to)
        command = {"explain": {"aggregate": collection, "pipeline": pipeline, "cursor": {}}, "verbosity": "executionStats"}

    result = db.command(command)
    # BSON types (dates, Int64, ...) -> plain JSON
    payload = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": payload}
