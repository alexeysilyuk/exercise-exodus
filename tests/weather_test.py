import pytest
import pymongo
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from src.api.api import app, get_collection  # Import only `app`

client = TestClient(app)

# Sample data to use in tests
sample_data = [
    {"city": "Beer Sheva", "average_temp_c": 22.5, "lastest_condition_text": "Clear"},
    {"city": "Michigan", "average_temp_c": 18.0, "lastest_condition_text": "Rainy"}
]

@pytest.mark.asyncio
@patch('src.api.api.get_collection')
async def test_get_city_weather_success(mock_get_collection):
    # Mock the collection's aggregate method
    mock_collection = MagicMock()
    mock_collection.aggregate.return_value = sample_data
    mock_get_collection.return_value = mock_collection

    # Override the dependency
    app.dependency_overrides[get_collection] = lambda: mock_collection

    response = client.get("/exercise/SomeCountry")
    
    assert response.status_code == 200
    assert response.json() == sample_data

    # Clear the override after the test
    app.dependency_overrides.clear()
    
@pytest.mark.asyncio    
@patch('src.api.api.get_collection')  # Patch the correct path
async def test_get_city_weather_no_data(mock_get_collection):
    mock_collection = MagicMock()
    mock_collection.aggregate.return_value = []
    mock_get_collection.return_value = mock_collection
    
    # Override the dependency
    app.dependency_overrides[get_collection] = lambda: mock_collection
    
    response = client.get("/exercise/SomeCountry")
    
    assert response.status_code == 404
    assert response.json() == {"detail": "No data found for the given country"}
    
    # Clear the override after the test
    app.dependency_overrides.clear()


def mock_get_collection_with_error():
    raise pymongo.errors.PyMongoError("Failed to connect to MongoDB.")

@pytest.fixture
def override_get_collection_with_error():
    app.dependency_overrides[get_collection] = mock_get_collection_with_error
    yield
    app.dependency_overrides.clear()

def mock_get_collection_with_error():
    mock_collection = MagicMock()
    mock_collection.aggregate.side_effect = pymongo.errors.PyMongoError("Database error")
    return mock_collection

@pytest.fixture
def override_get_collection_with_error():
    app.dependency_overrides[get_collection] = mock_get_collection_with_error
    yield
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_get_city_weather_db_error(override_get_collection_with_error):
    response = client.get("/exercise/SomeCountry")
    
    assert response.status_code == 500
    assert response.json() == {"detail": "Internal Server Error"}
    
    
@patch('src.db.db.MongoDB.get_mongo_client', side_effect=pymongo.errors.ConnectionFailure("Connection failed"))  # Patch the correct path
def test_get_city_weather_retry_logic(mock_get_client):
    with pytest.raises(Exception, match="Failed to connect to MongoDB."):
        from src.db.db import MongoDB  # Import inside the test to ensure it's patched
        MongoDB().get_collection()