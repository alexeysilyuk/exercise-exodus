import os
import json
import asyncio
import aiohttp
import boto3
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
weather_api_key = os.getenv('WEATHER_API_KEY')
aws_region_name = os.getenv('AWS_REGION_NAME')
aws_account_id = os.getenv('AWS_ACCOUNT_ID')
probe_interval_seconds = os.getenv('PROBE_INTERVAL_SECONDS', '60')
probe_interval_seconds = int(probe_interval_seconds) 

# Initialize SQS client
sqs = boto3.client(
    'sqs',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region_name
)

# SQS queue URL
queue_url = f'https://sqs.{aws_region_name}.amazonaws.com/{aws_account_id}/exercise-exodus'

# Get the directory where the script is located
current_file_path = os.path.dirname(os.path.abspath(__file__))

# Path to the text file containing the list of cities
list_of_cities_file_path = os.path.join(current_file_path, 'assets/list_of_cities.txt')

# Load cities list from file
with open(list_of_cities_file_path, 'r') as file:
    list_of_cities_country = [line.strip() for line in file.readlines()]


async def fetch_weather_data(session, city):
    api_url = f'http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q={city}'
    async with session.get(api_url) as response:
        return await response.json()


async def send_to_sqs(city, weather_data):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'City': {
                'DataType': 'String',
                'StringValue': city
            },
        },
        MessageBody=str(weather_data)
    )
    print(f"Sent weather data for {city} to SQS. Message ID: {response['MessageId']}")


async def process_city_weather(session, city):
    try:
        # Fetch weather data for the city and country
        weather_data = await fetch_weather_data(session, city)      
        weather_data_json = json.dumps(weather_data)

        # Send the data to SQS as a JSON string
        await send_to_sqs(city, weather_data_json)

    except Exception as e:
        print(f"Can't get data for {city}: {e}")

async def scheduler():
    async with aiohttp.ClientSession() as session:
        while True:
            # Create coroutines to run tasks concurrently
            tasks = [process_city_weather(session, line) for line in list_of_cities_country]
            await asyncio.gather(*tasks)
            
            # Sleep for 1 minute between probes
            print(f"Sleeping for {probe_interval_seconds} seconds...")
            await asyncio.sleep(probe_interval_seconds)


if __name__ == '__main__':
    asyncio.run(scheduler())
    
    

