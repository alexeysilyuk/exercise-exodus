import os
import pytest
from unittest.mock import patch, MagicMock, mock_open
from src.scheduler.scheduler import Scheduler

@pytest.fixture
def scheduler():
    # Mock boto3 client to avoid region validation issues
    with patch.dict(os.environ, {
        'AWS_ACCESS_KEY_ID': 'fake_access_key',
        'AWS_SECRET_ACCESS_KEY': 'fake_secret_key',
        'WEATHER_API_KEY': 'fake_weather_api_key',
        'AWS_REGION_NAME': 'fake_region',
        'AWS_ACCOUNT_ID': 'fake_account',
        'PROBE_INTERVAL_SECONDS': '10'
    }), patch('src.scheduler.scheduler.boto3.client') as mock_boto_client:
        mock_sqs_client = MagicMock()  # Mock the SQS client
        mock_boto_client.return_value = mock_sqs_client
        return Scheduler()
    
@patch('src.scheduler.scheduler.aiohttp.ClientSession')
def test_scheduler_initialization(mock_client_session, scheduler):
    assert scheduler.aws_access_key_id == 'fake_access_key'
    assert scheduler.aws_secret_access_key == 'fake_secret_key'
    assert scheduler.weather_api_key == 'fake_weather_api_key'
    assert scheduler.aws_region_name == 'fake_region'
    assert scheduler.aws_account_id == 'fake_account'
    assert scheduler.probe_interval_seconds == 10
    assert scheduler.sqs == scheduler.sqs  # Check if the SQS client is the expected MagicMock instance
    assert scheduler.queue_url == 'https://sqs.fake_region.amazonaws.com/fake_account/exercise-exodus'
    

def test_load_list_of_cities_file_not_found(scheduler):
    with patch('os.path.exists', return_value=False):
        scheduler.__init__()
        assert scheduler.list_of_cities_country == []

def test_load_list_of_cities_file_found(scheduler):
    cities_mock = ["CityA, CountryA", "CityB, CountryB"]
    with patch('os.path.exists', return_value=True):
        with patch('builtins.open', mock_open(read_data="\n".join(cities_mock))):
            scheduler.__init__()
            assert scheduler.list_of_cities_country == cities_mock