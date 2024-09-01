import os
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from dotenv import load_dotenv

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DB")
class MongoDB:
    def __init__(self):
        # Load environment variables from .env
        load_dotenv()
        self.database_url = os.getenv('MONGO_DATABASE_URL')

    def get_mongo_client(self, database_url):
        # Create a MongoClient instance with a connection timeout of 5 seconds
        return pymongo.MongoClient(database_url, serverSelectionTimeoutMS=5000)

    def get_collection(self):
        retries = 5
        for attempt in range(retries):
            try:
                mongo_client = self.get_mongo_client(self.database_url)
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
