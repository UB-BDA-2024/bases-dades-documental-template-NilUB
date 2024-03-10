from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoClient
from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

# Canvis pràctica 2 -> Aplicats
def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    for attr, value in data.dict().items():
        if value is None:
            continue
        redis.set(f"{sensor_id}_{attr}", value)
    return data

def get_data(redis:RedisClient , sensor_id: int, db: Session) -> schemas.Sensor:
    db_sensor = get_sensor(db, sensor_id)

    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db_sensor.temperature = float(redis.get(f"{sensor_id}_temperature"))
    db_sensor.humidity = float(redis.get(f"{sensor_id}_humidity"))
    db_sensor.battery_level = float(redis.get(f"{sensor_id}_battery_level"))
    db_sensor.last_seen = redis.get(f"{sensor_id}_last_seen")

    print(db_sensor)
    return db_sensor

# Aquest métode sembla funcionar bé, per lo que esta ben implementat!
def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

# Implementem el get_sensors_near!
# OJO -> response = client.get("/sensors/near?latitude=1.0&longitude=1.0&radius=1") TEST
# Aleshores hem de posar radius a la capcelera, dono per sentat que sera un int

def get_sensors_near(redis: RedisClient, mongo: MongoClient, db: Session, latitude: float, longitude: float, radius: int):
    pass

