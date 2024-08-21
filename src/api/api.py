import os
import logging
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
database_url = os.getenv('MONGO_DATABASE_URL')

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("App")

app = FastAPI()

def get_mongo_client():
    # Create a MongoClient instance with a connection timeout of 5 seconds
    return pymongo.MongoClient(database_url, serverSelectionTimeoutMS=5000)

def get_collection():
    retries = 5
    for attempt in range(retries):
        try:
            mongo_client = get_mongo_client()
            mongo_client.server_info()  # Triggers a server selection to ensure the connection is valid
            mongo_db = mongo_client.get_database()  # Use the default database from connection URL
            collection = mongo_db.weather_data
            
            logger.info("Connected to MongoDB.")
            return collection
        except (ConnectionFailure, OperationFailure) as e:
            logger.warning(f"MongoDB connection failed. Retrying ... (Attempt {attempt + 1}/{retries})")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    raise Exception("Failed to connect to MongoDB after several retries.")

class CityWeather(BaseModel):
    city: str
    average_temp_c: float
    lastest_condition_text: str

@app.get("/exercise/{location_country}", response_model=list[CityWeather])
async def get_city_weather(location_country: str, collection=Depends(get_collection)):
    # Query Pipeline to aggregate data by country
    pipeline = [
        {"$match": {"country": location_country}},
        {
            "$group": {
                "_id": "$city",
                "average_temp_c": {"$avg": "$temp_c"},
                "lastest_condition_text": {"$last": "$condition"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "average_temp_c": 1,
                "lastest_condition_text": 1
            }
        }
    ]

    try:
        cursor = collection.aggregate(pipeline)
        result = list(cursor)

        if not result:
            raise HTTPException(status_code=404, detail="No data found for the given country")

        return result

    except pymongo.errors.PyMongoError as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)