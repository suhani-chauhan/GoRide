import os, json, uuid, random, hashlib, threading
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from data import (CITY_MAPPING, VEHICLE_TYPE_MAPPING, PAYMENT_METHOD_MAPPING,
                  VEHICLE_MAKE_MAPPING)

load_dotenv()
fake = Faker()
PEAK_HOURS = (7, 8, 17, 18, 19)
CITY_CENTERS = {1: (40.7128, -74.0060), 2: (34.0522, -118.2437), 3: (41.8781, -87.6298),
                4: (29.7604, -95.3698), 5: (33.4484, -112.0740), 6: (39.9526, -75.1652),
                7: (29.4241, -98.4936), 8: (32.7157, -117.1611), 9: (32.7767, -96.7970),
                10: (37.3382, -121.8863)}

RIDES = {}  # in-memory history: ride_id -> latest event (resets on restart)
_producer, _lock = None, threading.Lock()


def publish(event: dict):
    global _producer
    with _lock:
        if _producer is None:
            _producer = EventHubProducerClient.from_connection_string(
                os.getenv("CONNECTION_STRING"), eventhub_name=os.getenv("EVENT_HUBNAME"))
        batch = _producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        _producer.send_batch(batch)


def _find(rows, key, value):
    for r in rows:
        if r[key] == value:
            return r
    raise ValueError(f"Unknown {key}: {value}")


def _seed(*parts):
    s = "|".join(str(p).strip().lower() for p in parts)
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


def estimate(city_id, pickup, dropoff, vehicle_type_id):
    _find(CITY_MAPPING, "city_id", city_id)
    vt = _find(VEHICLE_TYPE_MAPPING, "vehicle_type_id", vehicle_type_id)
    rng = random.Random(_seed(city_id, pickup, dropoff))  # same input -> same estimate
    distance = round(rng.uniform(1, 25), 2)
    duration = int(distance * 3) + rng.randint(3, 10)
    surge = 1.5 if datetime.now().hour in PEAK_HOURS else 1.0
    distance_fare = round(distance * vt["per_mile"], 2)
    time_fare = round(duration * vt["per_minute"], 2)
    subtotal = round((vt["base_rate"] + distance_fare + time_fare) * surge, 2)
    return {"distance_miles": distance, "duration_minutes": duration,
            "base_fare": vt["base_rate"], "distance_fare": distance_fare,
            "time_fare": time_fare, "surge_multiplier": surge, "subtotal": subtotal}


def summary(e):
    return {"ride_id": e["ride_id"], "confirmation_number": e["confirmation_number"],
            "city": _find(CITY_MAPPING, "city_id", e["pickup_city_id"])["city"],
            "pickup": e["pickup_address"], "dropoff": e["dropoff_address"],
            "vehicle_type": _find(VEHICLE_TYPE_MAPPING, "vehicle_type_id", e["vehicle_type_id"])["vehicle_type"],
            "total_fare": e["total_fare"],
            "status": "Cancelled" if e["ride_status_id"] == 2 else "Confirmed"}


def book(d):
    est = estimate(d["city_id"], d["pickup"], d["dropoff"], d["vehicle_type_id"])
    _find(PAYMENT_METHOD_MAPPING, "payment_method_id", d["payment_method_id"])
    now = datetime.now()
    pickup_t = now + timedelta(minutes=random.randint(3, 10))
    dropoff_t = pickup_t + timedelta(minutes=est["duration_minutes"])
    rng = random.Random(_seed(d["city_id"], d["pickup"], d["dropoff"]))
    lat, lon = CITY_CENTERS[d["city_id"]]
    tip = round(d["tip_amount"], 2)
    event = {
        "ride_id": str(uuid.uuid4()),
        "confirmation_number": fake.bothify("??#-####-??##"),
        "passenger_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, d["passenger_email"].lower())),
        "driver_id": str(uuid.uuid4()),
        "vehicle_id": str(uuid.uuid4()),
        "pickup_location_id": str(uuid.uuid4()),
        "dropoff_location_id": str(uuid.uuid4()),
        "vehicle_type_id": d["vehicle_type_id"],
        "vehicle_make_id": random.choice(VEHICLE_MAKE_MAPPING)["vehicle_make_id"],
        "payment_method_id": d["payment_method_id"],
        "ride_status_id": 1,
        "pickup_city_id": d["city_id"],
        "dropoff_city_id": d["city_id"],
        "cancellation_reason_id": 4,
        "passenger_name": d["passenger_name"],
        "passenger_email": d["passenger_email"],
        "passenger_phone": d["passenger_phone"],
        "driver_name": fake.name(),
        "driver_rating": round(random.uniform(4.0, 5.0), 2),
        "driver_phone": fake.phone_number(),
        "driver_license": fake.bothify("??-???-#######"),
        "vehicle_model": fake.word().capitalize(),
        "vehicle_color": random.choice(["Black", "White", "Gray", "Silver", "Blue", "Red"]),
        "license_plate": fake.bothify("???-####"),
        "pickup_address": d["pickup"],
        "pickup_latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
        "pickup_longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
        "dropoff_address": d["dropoff"],
        "dropoff_latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
        "dropoff_longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
        "distance_miles": est["distance_miles"],
        "duration_minutes": est["duration_minutes"],
        "booking_timestamp": now.isoformat(),
        "pickup_timestamp": pickup_t.isoformat(),
        "dropoff_timestamp": dropoff_t.isoformat(),
        "base_fare": est["base_fare"],
        "distance_fare": est["distance_fare"],
        "time_fare": est["time_fare"],
        "surge_multiplier": est["surge_multiplier"],
        "subtotal": est["subtotal"],
        "tip_amount": tip,
        "total_fare": round(est["subtotal"] + tip, 2),
        "rating": None,
        "event_timestamp": now.isoformat(),  # new: used later to order updates
    }
    publish(event)
    RIDES[event["ride_id"]] = event
    return summary(event)


def cancel(ride_id):
    event = RIDES[ride_id]  # KeyError -> 404
    if event["ride_status_id"] == 2:
        raise ValueError("Ride already cancelled")
    cancelled = {**event, "ride_status_id": 2, "cancellation_reason_id": 2,
                 "tip_amount": 0.0, "total_fare": 0.0,
                 "event_timestamp": datetime.now().isoformat()}
    publish(cancelled)
    RIDES[ride_id] = cancelled
    return summary(cancelled)


def history(limit=20):
    return [summary(e) for e in list(RIDES.values())[::-1][:limit]]


def stats():
    all_ = list(RIDES.values())
    done = [e for e in all_ if e["ride_status_id"] == 1]
    revenue = round(sum(e["total_fare"] for e in done), 2)
    return {"total_bookings": len(all_), "cancelled": len(all_) - len(done),
            "revenue": revenue, "avg_fare": round(revenue / len(done), 2) if done else 0}