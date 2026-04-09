from datetime import date, datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Query, Depends, Request
from supabase import create_client, Client
import os
import time
import logging
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("train_api")

load_dotenv()

# ── Startup timestamp (do mierzenia czasu od cold-startu) ─────────────────────
_startup_time: float = time.time()
_first_request_time: Optional[float] = None
_requests_since_startup: int = 0

app = FastAPI(title="Train Delays API")

origins = [
    "http://localhost:3000",
    "https://marekk13.github.io",
    "https://spoznienia.me",
]

def custom_key_func(request: Request) -> str:
    ip = get_remote_address(request)
    # Rozdzielamy buckety na podstawie nagłówka by skrypty nie blokowały przeglądarki na tym samym IP
    if request.headers.get("x-custom-client") == "spoznienia-frontend":
        return f"front:{ip}"
    return f"pub:{ip}"

limiter = Limiter(key_func=custom_key_func)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    global _startup_time
    _startup_time = time.time()
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    logger.info("=== API SERVER STARTED (cold start) at %s ===", datetime.utcnow().isoformat())


@app.middleware("http")
async def log_requests(request: Request, call_next):
    global _first_request_time, _requests_since_startup

    now = time.time()
    uptime_s = now - _startup_time
    _requests_since_startup += 1
    req_no = _requests_since_startup

    if _first_request_time is None:
        _first_request_time = now
        logger.info(
            "[REQ#%d] FIRST request after startup! Uptime: %.1fs | %s %s",
            req_no, uptime_s, request.method, request.url.path
        )
    else:
        idle_s = now - _first_request_time
        logger.info(
            "[REQ#%d] %s %s | uptime=%.1fs | req_since_start=%d",
            req_no, request.method, request.url.path, uptime_s, req_no
        )

    t0 = time.time()
    response = await call_next(request)
    elapsed_ms = (time.time() - t0) * 1000

    logger.info(
        "[REQ#%d] -> %d | %.0fms",
        req_no, response.status_code, elapsed_ms
    )
    return response


def get_db() -> Client:
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Database credentials not configured.")
    return create_client(url, key)


class TrainSummary(BaseModel):
    id: str
    date: str
    number: str
    name: Optional[str]
    category: Optional[str]
    from_station: Optional[str]
    to_station: Optional[str]
    is_domestic: bool
    occupancy: Optional[str]
    scheduled_departure: Optional[str]
    scheduled_arrival: Optional[str]
    delay_at_destination: Optional[int] = 0

class StationScheduleItem(BaseModel):
    train_number: str
    train_category: Optional[str]
    from_station: Optional[str]
    to_station: Optional[str]
    scheduled_arrival: Optional[str]
    scheduled_departure: Optional[str]
    delay_arrival_min: Optional[int]
    delay_departure_min: Optional[int]
    is_delayed: bool
    train_id: str

class StopDetail(BaseModel):
    station_name: str
    stop_order: int
    arrival_time: Optional[str]
    departure_time: Optional[str]
    delay_minutes_arrival: Optional[int]
    delay_minutes_departure: Optional[int]
    distance_from_start_km: float
    is_domestic: bool
    difficulties: List[dict] = []

class TrainDetail(TrainSummary):
    stops: List[StopDetail]

# --- Endpoints ---

@app.get("/stations", response_model=List[str])
@limiter.limit("60/minute")
@cache(expire=3600)  # Czas trzymania: 1 godzina
def list_stations(request: Request, db: Client = Depends(get_db)):
    """
    Returns a list of domestic station names ranked by passenger volume (cacheable on frontend).
    """
    try:
        response = db.table("stations")\
            .select("name")\
            .eq("is_domestic", True)\
            .order("passenger_volume_rank", nullsfirst=False)\
            .execute()
        return [s["name"] for s in response.data]
    except Exception as e:
        logger.error("Error in list_stations: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/train-runs", response_model=List[TrainSummary])
@limiter.limit("60/minute")
@cache(expire=60)
def list_trains(
    request: Request,
    date: Optional[date] = None,
    number: Optional[str] = None,
    station: Optional[str] = None, # Simple/Global filter
    offset: int = 0,
    limit: int = 500,
    db: Client = Depends(get_db)
):
    # Default date: Yesterday
    if not date:
        date = datetime.today() - timedelta(days=1)
    
    # Use the SQL View
    query = db.table("view_train_summaries").select("*")
    query = query.eq("date", date)
    
    if number:
        query = query.ilike("number", f"%{number}%")
        
    if station:
        query = query.or_(f"from_station.ilike.{station},to_station.ilike.{station}")

    query = query.range(offset, offset + limit - 1)
    response = query.execute()
    
    return response.data

@app.get("/stations/{name}/schedule", response_model=List[StationScheduleItem])
@limiter.limit("60/minute")
@cache(expire=30)
def get_station_schedule(
    request: Request,
    name: str,
    date: Optional[date] = None,
    db: Client = Depends(get_db)
):
    """
    Get the schedule (board) for a specific station.
    This uses a database RPC function for efficient filtering of intermediate stops.
    """
    if not date:
        date = datetime.today() - timedelta(days=1)

    # Call the SQL function (RPC)
    try:
        response = db.rpc("get_station_schedule", {
            "p_station_name": name, 
            "p_date": date.isoformat()
        }).execute()
        
        return response.data
    except Exception as e:
        logger.error("Error in get_station_schedule (station=%s, date=%s): %s", name, date, str(e))
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/train-runs/{train_id}", response_model=TrainDetail)
@limiter.limit("60/minute")
@cache(expire=60)
def get_train_detail(request: Request, train_id: str, db: Client = Depends(get_db)):
    # 1. Parse ID: YYYYMMDDnnnn -> Date, Number
    if len(train_id) < 9:
        raise HTTPException(status_code=400, detail="Invalid ID format. Expected YYYYMMDDnnnn")
    
    date_part = f"{train_id[:4]}-{train_id[4:6]}-{train_id[6:8]}"
    number_part = train_id[8:]
    
    # 2. Get Train Info
    train_res = db.table("view_train_summaries")\
        .select("*")\
        .eq("date", date_part)\
        .eq("number", number_part)\
        .execute()
        
    if not train_res.data:
        raise HTTPException(status_code=404, detail="Train not found")
    
    train = train_res.data[0]
    internal_id = train['internal_id'] 
    
    # 3. Get Stops
    stops_res = db.table("run_stops")\
        .select("*, stations(name, is_domestic), run_stop_difficulties(*, difficulties(description))")\
        .eq("run_id", internal_id)\
        .order("stop_order")\
        .execute()
        
    stops_data = []
    for s in stops_res.data:
        diffs = []
        if s.get('run_stop_difficulties'):
            for d in s['run_stop_difficulties']:
                 if d.get('difficulties'):
                     diffs.append({
                         "description": d['difficulties']['description'],
                         "location": d.get('location')
                     })
        
        stops_data.append(StopDetail(
            station_name=s['stations']['name'],
            stop_order=s['stop_order'],
            arrival_time=s['scheduled_arrival'],
            departure_time=s['scheduled_departure'],
            delay_minutes_arrival=s['delay_arrival_min'],
            delay_minutes_departure=s['delay_departure_min'],
            distance_from_start_km=s['distance_from_start_km'],
            is_domestic=s['stations']['is_domestic'],
            difficulties=diffs
        ))
        
    return {**train, "stops": stops_data}
