import os
import json
import asyncio
import aiohttp
import boto3
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

class Scheduler:
    def __init__(self):
        # Get environment variables
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.weather_api_key = os.getenv('WEATHER_API_KEY')
        self.aws_region_name = os.getenv('AWS_REGION_NAME')
        self.aws_account_id = os.getenv('AWS_ACCOUNT_ID')
        self.probe_interval_seconds = int(os.getenv('PROBE_INTERVAL_SECONDS', '60'))

        # Initialize SQS client
        self.sqs = boto3.client(
            'sqs',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name
        )

        # SQS queue URL
        self.queue_url = f'https://sqs.{self.aws_region_name}.amazonaws.com/{self.aws_account_id}/exercise-exodus'

        # Get the directory where the script is located
        self.current_file_path = os.path.dirname(os.path.abspath(__file__))

        # Path to the text file containing the list of cities
        self.list_of_cities_file_path = os.path.join(self.current_file_path, 'assets/list_of_cities.txt')
        
        # Check if the file exists and load cities list from file or initialize with an empty array
        if os.path.exists(self.list_of_cities_file_path):
            with open(self.list_of_cities_file_path, 'r') as file:
                self.list_of_cities_country = [line.strip() for line in file.readlines()]
        else:
            print(f"File '{self.list_of_cities_file_path}' not found. Creating an empty list.")
            self.list_of_cities_country = []

    async def __fetch_weather_data(self, session, city):
        api_url = f'http://api.weatherapi.com/v1/current.json?key={self.weather_api_key}&q={city}'
        async with session.get(api_url) as response:
            if response.status!= 200:
                raise Exception(f"API request failed with status code {response.status}")
            return await response.json()

    async def __send_to_sqs(self, city, weather_data_json):
        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageAttributes={
                'City': {
                    'DataType': 'String',
                    'StringValue': city
                },
            },
            MessageBody=weather_data_json
        )
        print(f"Sent weather data for {city} to SQS. Message ID: {response['MessageId']}")

    async def __process_city_weather(self, session, city):
        try:
            # Fetch weather data for the city
            weather_data = await self.__fetch_weather_data(session, city)
            weather_data_json = json.dumps(weather_data)

            # Send the data to SQS as a JSON string
            await self.__send_to_sqs(city, weather_data_json)

        except Exception as e:
            print(f"Can't get data for {city}: {e}")

    async def run(self):
        async with aiohttp.ClientSession() as session:
            while True:
                # Create coroutines to run tasks concurrently
                tasks = [self.__process_city_weather(session, line) for line in self.list_of_cities_country]
                await asyncio.gather(*tasks)
                
                # Sleep for 1 minute between probes
                print(f"Sleeping for {self.probe_interval_seconds} seconds...")
                await asyncio.sleep(self.probe_interval_seconds)

if __name__ == '__main__':
    scheduler = Scheduler()
    asyncio.run(scheduler.run())
