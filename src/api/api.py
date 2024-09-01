import logging
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pymongo
from  db.db import MongoDB
from pymongo.collection import Collection

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")
app = FastAPI()

class CityWeather(BaseModel):
    city: str
    average_temp_c: float
    lastest_condition_text: str

def get_collection() -> Collection:
    mongo_db_instance = MongoDB()  # Initialize MongoDB instance
    return mongo_db_instance.get_collection()

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
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)