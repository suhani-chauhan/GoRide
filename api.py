from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
import ride_service as svc

app = FastAPI(title="Ride Booking")
templates = Jinja2Templates(directory="templates")


class EstimateRequest(BaseModel):
    city_id: int
    pickup: str = Field(min_length=3)
    dropoff: str = Field(min_length=3)
    vehicle_type_id: int


class RideRequest(EstimateRequest):
    payment_method_id: int
    passenger_name: str = Field(min_length=2)
    passenger_email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    passenger_phone: str = ""
    tip_amount: float = Field(default=0, ge=0, le=100)


@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(request, "home.html")


@app.get("/api/options")
def options():
    return {"cities": svc.CITY_MAPPING, "vehicle_types": svc.VEHICLE_TYPE_MAPPING,
            "payment_methods": svc.PAYMENT_METHOD_MAPPING}


@app.post("/api/estimate")
def estimate(req: EstimateRequest):
    try:
        return svc.estimate(req.city_id, req.pickup, req.dropoff, req.vehicle_type_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/book")
def book(req: RideRequest):
    try:
        return svc.book(req.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(502, f"Could not publish to Event Hubs: {e}")


@app.post("/api/cancel/{ride_id}")
def cancel(ride_id: str):
    try:
        return svc.cancel(ride_id)
    except KeyError:
        raise HTTPException(404, "Ride not found")
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(502, f"Could not publish to Event Hubs: {e}")


@app.get("/api/rides")
def rides():
    return svc.history()


@app.get("/api/stats")
def stats():
    return svc.stats()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)