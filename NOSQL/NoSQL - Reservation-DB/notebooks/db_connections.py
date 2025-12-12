# IMPORTO BASES DE DATOS
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
import os

# =====================
# CONEXION CON MONGODB
# =====================
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin123")

client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@mongo:27017/")


# =====================
# CONEXION CON NEO4J
# =====================
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j123")

db_neo4j = GraphDatabase.driver(
    "bolt://neo4j:7687",
    auth=("neo4j", NEO4J_PASSWORD)
)


# =====================
# CONEXION CON REDIS
# =====================
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "redis123")

db_redis = redis.Redis(
    host="redis",
    port=6379,
    password=REDIS_PASSWORD,
    decode_responses=True
)
