from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from . import models, schemas
import json

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

# Editem aquest métode per adaptar-lo a la base MondoDB
def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    sensor_data_db = sensor.dict()
    # Guardem al nostre document
    mongo.insertDocument(sensor_data_db)
    return db_sensor

# Canvis pràctica 2 -> Aplicats
def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.SensorData:
    sensor_data_in_db = data
    redis.set(sensor_id, json.dumps(data.dict()))
    return sensor_data_in_db

def get_data(redis:RedisClient , sensor_id: int) -> schemas.SensorData:
    db_sensor_data = redis.get(sensor_id)

    if db_sensor_data is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    response = json.loads(db_sensor_data)
    return response

# Aquest métode sembla funcionar bé, però falta implementar el borrar de les bases de dades
def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    # borrem de la base de Mongo
    mongodb.deleteDocument(db_sensor.name)
    # Borrem de la base de Redis -> Usarem la id
    redis.delete(sensor_id)

    return db_sensor

# Implementem el get_sensors_near!
# OJO -> response = client.get("/sensors/near?latitude=1.0&longitude=1.0&radius=1") TEST
# Aleshores hem de posar radius a la capcelera, dono per sentat que sera un int

def get_sensors_near(mongo: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    response = []

    # Segons el radi establert
    my_query = {
        "latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
        "longitud": {"$gte": longitude - radius, "$lte" : longitude + radius}
    }

    # I ara amb el find recuperem les dades, que compleixin la condició
    sensors_near = mongo.collection.find(my_query)

    # per cada sensor, obtenim les dades de Redis i PostgreSQL
    for sensor in sensors_near:
        # Obtenim el sensor mitjançant el seu nom i les seves dades
        sensor_in_db = get_sensor_by_name(db, sensor['name'])
        sensor_data_in_db = get_data(redis, sensor_in_db.id)

        # extreurem les seves dades
        db_data ={
            'id': sensor_in_db.id,
            'name': sensor_in_db.name
        }
        
        # actualitzem les dades noves
        db_data.update(sensor_data_in_db)

        # I afegim el nou sensor a la llista de nears!
        response.append(db_data)

    return response