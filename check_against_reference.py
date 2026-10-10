"""
End-to-end check against a REAL MongoDB and a RUNNING server.

It seeds a database with the awkward cases from the assignment (employees with no logs, ties,
weekend records, mid-month joiners, overnight shifts, legacy rows, logs of unknown employees),
calls every analytics endpoint and compares the answers with a brute-force Python reference
written straight from the business rules. It also fires parallel requests at the race-prone
endpoints and runs the explain endpoint on a ~100k-record collection.

    # 1. use a THROWAWAY database: this script deletes both collections in MONGO_DB
    export MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_check
    uvicorn app.main:app --port 8000 &           # same MONGO_URI / MONGO_DB
    ALLOW_WIPE=1 python tests/check_against_reference.py [--big]

--big additionally loads 100,000 attendance records and asserts the explain plans use indexes.
"""
import json
import os
import random
import sys
import threading
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
BASE = os.getenv("BASE_URL", "http://localhost:8000")
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
if os.getenv("ALLOW_WIPE") != "1":
    sys.exit("refusing to run: this script deletes the employees and attendance_logs collections. Set ALLOW_WIPE=1.")
mongo = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True)[os.getenv("MONGO_DB", "attendance_db")]


def ms(s):  # IST wall clock "2026-07-06 09:40:01" -> epoch milliseconds
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST).timestamp() * 1000)


def ms_to_dt(x):
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=x)


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


class _C:  # tiny stand-in for a test client: get(path) -> (status, json)
    @staticmethod
    def get(path):
        return call("GET", path)


c = _C()
failures = []


def check(cond, label):
    if not cond:
        failures.append(label)
        print("FAIL", label)


PRES = {"PRESENT","WFH","ON_DUTY"}
def hu(x, places):  # exact half-up of a Fraction
    q = Fraction(10**places)
    return float((x*q + Fraction(1,2)).__floor__() / q)

def ref_present2(d):  # half-days present weekday
    if d["status"] not in PRES or date.fromisoformat(d["date"]).weekday() >= 5: return 0
    return 1 if d.get("half_day") else 2

def in_month(d, mo): return d["date"].startswith(mo)

def ref_monthly(emps, logs, code, mo):
    e = next(x for x in emps if x["emp_code"]==code)
    y, mm = int(mo[:4]), int(mo[5:])
    first = date(y, mm, 1); last = (date(y+(mm==12), mm%12+1, 1) - timedelta(days=1))
    start = max(first, date.fromisoformat(e["joined_on"]))
    wd = sum(1 for i in range((last-start).days+1) if (start+timedelta(days=i)).weekday()<5) if last>=start else 0
    mine = [d for d in logs if d["emp_code"]==code and in_month(d, mo)]
    p2 = sum(ref_present2(d) for d in mine)
    return {"emp_code":code,"month":mo,"working_days":wd,"present_days":p2/2,"leave_days":sum(d["status"]=="LEAVE" for d in mine),
            "late_count":sum(d.get("late_minutes",0)>0 for d in mine),"total_late_minutes":sum(d.get("late_minutes",0) for d in mine),
            "total_overtime_minutes":sum(d.get("overtime_minutes",0) for d in mine),
            "attendance_pct": None if wd==0 else hu(Fraction(p2,2)/wd*100, 2)}

def ref_summary(emps, logs, mo, dept=None):
    y, mm = int(mo[:4]), int(mo[5:]); last = (date(y+(mm==12), mm%12+1, 1) - timedelta(days=1)).isoformat()
    out = {}
    for e in emps:
        if e["joined_on"] > last or (dept and e["department"]!=dept): continue
        out.setdefault(e["department"], []).append(e["emp_code"])
    items = []
    for dep in sorted(out):
        codes = set(out[dep]); rs = [d for d in logs if d["emp_code"] in codes and in_month(d, mo)]
        whs = [Fraction(str(d["work_hours"])) for d in rs if d["status"] in PRES and d.get("work_hours") is not None]
        items.append({"department":dep,"headcount":len(codes),"present_days":sum(ref_present2(d) for d in rs)/2,
          "avg_work_hours": None if not whs else hu(sum(whs)/len(whs), 2),
          "late_count":sum(d.get("late_minutes",0)>0 for d in rs),"total_late_minutes":sum(d.get("late_minutes",0) for d in rs),
          "leave_count":sum(d["status"]=="LEAVE" for d in rs),"on_duty_count":sum(d["status"]=="ON_DUTY" for d in rs)})
    return {"month":mo,"items":items}

def ref_leaderboard(emps, logs, mo, limit, dept=None):
    byc = {e["emp_code"]: e for e in emps}; tot = {}
    for d in logs:
        if in_month(d, mo) and d["emp_code"] in byc and d.get("late_minutes",0)>0:
            t = tot.setdefault(d["emp_code"], [0,0]); t[0]+=d["late_minutes"]; t[1]+=1
    rows = [(k,v) for k,v in tot.items() if not dept or byc[k]["department"]==dept]
    rows.sort(key=lambda kv:(-kv[1][0], kv[0]))
    out=[]
    for i,(k,v) in enumerate(rows):
        rank = 1 + sum(1 for _,w in rows if w[0] > v[0])
        if rank<=limit: out.append({"rank":rank,"emp_code":k,"name":byc[k]["name"],"department":byc[k]["department"],"total_late_minutes":v[0],"late_count":v[1]})
    return {"month":mo,"items":out}

def ref_trend(emps, logs, dept, a, b):
    es = [e for e in emps if e["department"]==dept]; codes={e["emp_code"] for e in es}
    days=[]; d0=date.fromisoformat(a)
    while d0<=date.fromisoformat(b): days.append(d0); d0+=timedelta(days=1)
    rows=[]; rates=[]
    for d in days:
        ds=d.isoformat(); hc=sum(e["joined_on"]<=ds for e in es)
        rs=[x for x in logs if x["emp_code"] in codes and x["date"]==ds]
        pc=Fraction(sum((0 if x["status"] not in PRES else (1 if x.get("half_day") else 2)) for x in rs),2)
        wdy=d.weekday()<5
        rate=hu(pc/hc,4) if (wdy and hc>0) else None
        rates.append(rate)
        win=[r for r in rates[-7:] if r is not None]
        mv=None if not win else hu(sum(Fraction(str(r)) for r in win)/len(win),4)
        rows.append({"date":ds,"is_working_day":wdy,"headcount":hc,"present_count":float(pc),"late_count":sum(x.get("late_minutes",0)>0 for x in rs),"attendance_rate":rate,"moving_avg_7d":mv})
    return {"department":dept,"items":rows}


def seed(rng, big=False):
    mongo.employees.delete_many({})
    mongo.attendance_logs.delete_many({})
    depts = ["Eng", "Sales", "Support", "Ghost"]
    emps = []
    for i in range(1, 25):
        d = depts[i % 3] if i < 22 else "Ghost"
        joined = rng.choice(["2026-01-05", "2026-06-01", "2026-07-13", "2026-07-20", "2026-08-05", "2026-07-01"]) if d != "Ghost" else "2026-09-15"
        ss, se = ("22:00", "06:00") if rng.random() < .2 else ("09:30", "18:30")
        emps.append({"emp_code": f"EMP{i:04d}", "name": f"E{i}", "email": "a@b.co", "department": d, "shift_start": ss,
                     "shift_end": se, "joined_on": joined, "created_at": ms_to_dt(1783312500000)})
    mongo.employees.insert_many([dict(e) for e in emps])
    logs = []
    span = 70
    for e in emps:
        if rng.random() < .2:
            continue  # employee with no logs
        for off in range(span):
            day = date(2026, 6, 20) + timedelta(days=off)
            if rng.random() < .35:
                continue  # a day with no record
            st = rng.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [6, 2, 1, 1, 1])[0]
            doc = {"emp_code": e["emp_code"], "date": day.isoformat(), "status": st}
            if st in PRES:
                pin = ms_to_dt(ms(day.isoformat() + " 09:00:00") + rng.randint(0, 14400) * 1000)
                po = None if rng.random() < .1 else pin + timedelta(seconds=rng.randint(3600, 14 * 3600))
                wh = None if po is None else round((po - pin).total_seconds() / 3600 + 1e-9, 2)
                if rng.random() < .1 and wh is not None:
                    wh = rng.choice([4.5, 4.49, 3.5, 8.12, 8.13, 0.01, 12.99])  # stored values are authoritative
                doc.update(punch_in=pin, punch_out=po, work_hours=wh, late_minutes=rng.choice([0, 0, 0, 10, 35, 35, 50, 5, 120]),
                           overtime_minutes=rng.choice([0, 0, 30, 45]), half_day=(wh is not None and wh < 4.5), history=[])
                if rng.random() < .15:
                    doc.pop("half_day")
                if rng.random() < .15:
                    doc.pop("history")  # legacy rows
            else:
                doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False, history=[])
            logs.append(doc)
    logs.append({"emp_code": "EMP9999", "date": "2026-07-08", "status": "PRESENT", "punch_in": ms_to_dt(ms("2026-07-08 12:00:00")),
                 "punch_out": None, "work_hours": None, "late_minutes": 500, "overtime_minutes": 0, "half_day": False, "history": []})
    mongo.attendance_logs.insert_many([dict(d) for d in logs])
    if big:  # filler volume for the explain/speed check: ~100k records over 2000 extra employees
        filler = []
        for i in range(100, 2100):
            code = f"EMP{i:05d}"
            filler.append({"emp_code": code, "name": code, "email": "a@b.co", "department": rng.choice(depts[:3]), "shift_start": "09:30",
                           "shift_end": "18:30", "joined_on": "2026-01-05", "created_at": ms_to_dt(1783312500000)})
        mongo.employees.insert_many([dict(f) for f in filler])
        rows = []
        for f in filler:
            for off in range(50):
                day = (date(2026, 6, 1) + timedelta(days=off)).isoformat()
                rows.append({"emp_code": f["emp_code"], "date": day, "status": "PRESENT", "punch_in": ms_to_dt(ms(day + " 09:35:00")),
                             "punch_out": ms_to_dt(ms(day + " 18:40:00")), "work_hours": 9.08, "late_minutes": rng.choice([0, 15]),
                             "overtime_minutes": 0, "half_day": False, "history": []})
        mongo.attendance_logs.insert_many(rows, ordered=False)
        emps += filler
        logs += rows
    return emps, logs


def compare(got, want, label):
    check(got == want, label)


def run_analytics(emps, logs):
    for mo in ("2026-07", "2026-08", "2026-06", "2026-02", "2026-09"):
        for e in emps[:24]:
            s, j = c.get(f"/analytics/employees/{e['emp_code']}/monthly?month={mo}")
            compare((s, j), (200, ref_monthly(emps, logs, e["emp_code"], mo)), f"monthly {e['emp_code']} {mo}")
        for dept in (None, "Eng", "Sales", "Ghost", "Nope"):
            q = f"&department={dept}" if dept else ""
            compare(c.get(f"/analytics/departments/summary?month={mo}{q}"), (200, ref_summary(emps, logs, mo, dept)), f"summary {mo} {dept}")
            for lim in (1, 2, 3, 10, 50):
                compare(c.get(f"/analytics/leaderboard/late?month={mo}&limit={lim}{q}"), (200, ref_leaderboard(emps, logs, mo, lim, dept)),
                        f"leaderboard {mo} {dept} limit={lim}")
    for dept in ("Eng", "Sales", "Support", "Ghost"):
        for a, b in [("2026-07-01", "2026-07-31"), ("2026-07-06", "2026-07-06"), ("2026-06-25", "2026-08-10"), ("2026-07-04", "2026-07-05"), ("2026-08-25", "2026-09-20")]:
            compare(c.get(f"/analytics/departments/{dept}/trend?from={a}&to={b}"), (200, ref_trend(emps, logs, dept, a, b)), f"trend {dept} {a}..{b}")
    check(c.get("/analytics/departments/Nope/trend?from=2026-07-01&to=2026-07-10")[0] == 404, "trend unknown department -> 404")
    check(c.get("/analytics/departments/Eng/trend?from=2026-01-01&to=2026-04-03")[0] == 422, "trend 93 days -> 422")


def run_races():
    def parallel(n, fn):
        out = []
        ts = [threading.Thread(target=lambda: out.append(fn())) for _ in range(n)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return sorted(out)

    code = call("POST", "/employees", {"emp_code": "EMP7001", "name": "Race", "email": "r@x.io", "department": "Eng", "joined_on": "2026-01-05"})[0]
    check(code == 201, "create race employee")
    check(parallel(20, lambda: call("POST", "/employees", {"emp_code": "EMP7002", "name": "R", "email": "r@x.io", "department": "Eng", "joined_on": "2026-01-05"})[0]).count(201) == 1,
          "20 parallel employee creates -> exactly one 201")
    pin = ms("2026-07-09 09:00:00")
    r = parallel(20, lambda: call("POST", "/attendance/punch-in", {"emp_code": "EMP7001", "punched_at": pin})[0])
    check(r.count(201) == 1 and r.count(409) == 19, f"parallel punch-in: {r}")
    r = parallel(20, lambda: call("POST", "/attendance/punch-out", {"emp_code": "EMP7001", "punched_at": ms("2026-07-09 18:40:00")})[0])
    check(r.count(200) == 1 and r.count(409) == 19, f"parallel punch-out: {r}")
    counter = iter(range(100))
    lock = threading.Lock()

    def patch():
        with lock:
            i = next(counter)
        return call("PATCH", "/attendance/EMP7001/2026-07-09", {"reason": "race test", "regularized_by": f"u{i}", "punch_in": ms(f"2026-07-09 09:{i + 10:02d}:00")})[0]

    r = parallel(10, patch)
    s, j = call("GET", "/attendance?emp_code=EMP7001")
    check(all(x in (200, 409) for x in r) and len(j["items"][0]["history"]) == r.count(200), f"parallel PATCH keeps every history entry: {r}")


def run_explain():
    for ep, q in {"attendance_list": "?emp_code=EMP00100&date_from=2026-06-01&date_to=2026-06-30",
                  "employee_monthly": "?emp_code=EMP00100&month=2026-06", "department_summary": "?month=2026-06",
                  "late_leaderboard": "?month=2026-06", "department_trend": "?department=Eng&from=2026-06-01&to=2026-06-30"}.items():
        s, j = call("GET", f"/admin/explain/{ep}{q}")
        text = json.dumps(j)
        check(s == 200 and "IXSCAN" in text and "COLLSCAN" not in text, f"explain {ep}: status={s} IXSCAN={'IXSCAN' in text} COLLSCAN={'COLLSCAN' in text}")


if __name__ == "__main__":
    big = "--big" in sys.argv
    emps, logs = seed(random.Random(int(os.getenv("SEED", "1"))), big=big)
    if not big:
        run_analytics(emps, logs)  # the brute-force reference is too slow for the 100k set
    run_races()
    if big:
        run_explain()
    print("ALL CHECKS PASSED" if not failures else f"{len(failures)} FAILED")
    sys.exit(1 if failures else 0)
