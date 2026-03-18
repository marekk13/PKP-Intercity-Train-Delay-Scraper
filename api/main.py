from datetime import date, timedelta
from typing import List, Optional
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Query, Depends
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta

load_dotenv()
app = FastAPI(title="Train Delays API")

# domeny które mogą rozmawiać z backendem
origins = [
    "http://localhost:3000",          # testy lokalne frontendu
    "https://marekk13.github.io",     # strona na GH Pages
    "https://spoznienia.me",          # domena docelowa
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,           
    allow_credentials=True,
    allow_methods=["*"],             
    allow_headers=["*"],              
)

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
    direction: Optional[str]
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
    difficulties: List[str] = []

class TrainDetail(TrainSummary):
    stops: List[StopDetail]

# --- Endpoints ---

@app.get("/train-runs", response_model=List[TrainSummary])
def list_trains(
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
        query = query.or_(f"from_station.ilike.%{station}%,to_station.ilike.%{station}%")

    query = query.range(offset, offset + limit - 1)
    response = query.execute()
    
    return response.data

@app.get("/stations/{name}/schedule", response_model=List[StationScheduleItem])
def get_station_schedule(
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/train-runs/{train_id}", response_model=TrainDetail)
def get_train_detail(train_id: str, db: Client = Depends(get_db)):
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
        .select("*, stations(name), run_stop_difficulties(*, difficulties(description))")\
        .eq("run_id", internal_id)\
        .order("stop_order")\
        .execute()
        
    stops_data = []
    for s in stops_res.data:
        diffs = []
        if s.get('run_stop_difficulties'):
            for d in s['run_stop_difficulties']:
                 if d.get('difficulties'):
                     diffs.append(d['difficulties']['description'])
        
        stops_data.append(StopDetail(
            station_name=s['stations']['name'],
            stop_order=s['stop_order'],
            arrival_time=s['scheduled_arrival'],
            departure_time=s['scheduled_departure'],
            delay_minutes_arrival=s['delay_arrival_min'],
            delay_minutes_departure=s['delay_departure_min'],
            distance_from_start_km=s['distance_from_start_km'],
            difficulties=diffs
        ))
        
    return {**train, "stops": stops_data}
